# -*- coding: utf-8 -*-
# @Time : 2023/4/22 14:17
# @Author : Professor He
# @Email : hechangjia1018@163.com
# @File : pdfParser.py
# @Project : IntelligentDocumentProcessing

import time
from multiprocessing import Pool
from functools import partial
import argparse
from multiprocessing import cpu_count
from pdfplumber.utils import cluster_objects
from operator import itemgetter
import pdfplumber
from collections import defaultdict, Counter
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, ProcessPoolExecutor, as_completed
import re
import copy
import os
import json
from decimal import Decimal
from src.util.config_util import logger


def get_current_bulk_content(page):
    if page is None:
        return []
    content = page.extract_text()
    if content:
        content = content.split('\n')
        content = [st for st in content if st]
    else:
        content = []
    return content


def read_bulk_data_for_pure_text(path, bulks, cur_slice):
    with pdfplumber.open(path) as pdf:
        pages = pdf.pages
        size = len(pages)
        if size < bulks:
            print('total page is less than bulk, directly every process deal with each page')
            contents = []
            if cur_slice > size:
                print('none data has been processed')
                return [[]]
            else:
                content = get_current_bulk_content(pages[cur_slice - 1])
                contents.append(content)
                return contents
        containers_per_bulk = size // bulks
        contents = []
        start = (cur_slice - 1) * containers_per_bulk
        if cur_slice == bulks:
            end = size
        else:
            end = cur_slice * containers_per_bulk
        for i in range(start, end):
            page = pages[i]
            if page is None:
                contents.append([])
                continue
            content = page.extract_text()
            if not content:
                tmp = []
            else:
                tmp = content.split('\n')
                tmp = [st for st in tmp if st]
            contents.append(tmp)
    return contents


def process_number(parallel_num, path):
    cpu_kernel = cpu_count()
    available_kernel = min(cpu_kernel // 2, cpu_kernel - 2)
    if parallel_num != 1:
        if isinstance(parallel_num, int):
            if available_kernel > parallel_num:
                available_kernel = parallel_num
    f = Path(path)
    size = f.stat().st_size
    size = round(size / 1024 / 1024, 3)
    if size < 4:
        advice_number = 4
    else:
        advice_number = 8
    process_kernel = min(advice_number, available_kernel)
    return process_kernel


def parallel_process(path, parallel_num=1, parse_type=1):
    process_kernel = process_number(parallel_num, path)
    print('current process:', process_kernel)
    pool = Pool(processes=process_kernel)
    if parse_type == 1:
        parse_file = partial(read_bulk_data_for_pure_text, path)
    else:
        parse_file = partial(read_bulk_data_for_various_information, path)
    parse_bulk = partial(parse_file, process_kernel)
    slice_number = [i + 1 for i in range(process_kernel)]
    bulk_results = pool.map(parse_bulk, slice_number)
    pool.close()
    pool.join()
    if parse_type == 1:
        final_result = []
        for bulk_result in bulk_results:
            final_result.extend(bulk_result)
        return final_result
    else:
        contents, leftsize, rightsize, chars2gap, rightsize2frequency, leftsize2frequency \
            = merge_multi_process_bulk_result(bulk_results)
        return contents, leftsize, rightsize, chars2gap, rightsize2frequency, leftsize2frequency


def merge_multi_process_bulk_result(total_results):
    contents, leftsize, rightsize, chars2gap = [], defaultdict(list), defaultdict(list), dict()
    for bulk_result in total_results:
        bulk_contents, bulk_leftsize, bulk_rightsize, bulk_chars2gap = bulk_result
        contents.extend(bulk_contents)
        for k, v in bulk_leftsize.items():
            if k not in leftsize:
                leftsize[k] = v
            else:
                leftsize[k].extend(v)
        for k, v in bulk_rightsize.items():
            if k not in rightsize:
                rightsize[k] = v
            else:
                rightsize[k].extend(v)
        for k, v in bulk_chars2gap.items():
            if k not in chars2gap:
                chars2gap[k] = v
            else:
                chars2gap[k] = max(chars2gap[k], v)
    for k, v in leftsize.items():
        v.sort()
    for k, v in rightsize.items():
        v.sort(reverse=True)

    rightsize2frequency = dict()
    for charsize, position in rightsize.items():
        for i in range(len(position)):
            position[i] = int(position[i])
        if len(position) > 10:
            cur_rightsize2frequency = Counter(position)
            cur_rightsize2frequency = sorted(cur_rightsize2frequency.items(), key=lambda x: x[1], reverse=True)
            if cur_rightsize2frequency[0][1] > 1:
                rightsize2frequency[charsize] = cur_rightsize2frequency[0][0]
            else:
                rightsize2frequency[charsize] = position[len(position) // 2]
        else:
            rightsize2frequency[charsize] = position[len(position) // 2]
    rightsize2frequency = get_size2frequency(rightsize)
    leftsize2frequency = get_size2frequency(leftsize)

    return contents, leftsize, rightsize, chars2gap, rightsize2frequency, leftsize2frequency


def get_size2frequency(left_rightsize, is_right=True):
    rightsize2frequency = dict()
    for charsize, position in left_rightsize.items():
        for i in range(len(position)):
            position[i] = int(position[i])
        if len(position) > 10:
            cur_rightsize2frequency = Counter(position)
            if is_right:

                cur_rightsize2frequency = sorted(cur_rightsize2frequency.items(), key=lambda x: x[1], reverse=True)
            else:
                cur_rightsize2frequency = sorted(cur_rightsize2frequency.items(), key=lambda x: x[1], reverse=False)
            if cur_rightsize2frequency[0][1] > 1:
                rightsize2frequency[charsize] = cur_rightsize2frequency[0][0]
            else:
                rightsize2frequency[charsize] = position[len(position) // 2]
        else:
            rightsize2frequency[charsize] = position[len(position) // 2]

    return rightsize2frequency


def read_bulk_data_for_various_information(path, bulks, cur_slice):
    tolerance_y = 3
    with pdfplumber.open(path) as pdf:
        pages = pdf.pages
        size = len(pages)
        leftsize = defaultdict(list)
        rightsize = defaultdict(list)
        chars2gap = dict()
        if size < bulks:
            print('total page is less than bulk, directly every process deal with each page')
            contents = []
            if cur_slice > size:
                print('none data has been processed')
                return [[]], leftsize, rightsize, chars2gap
            else:
                cur_page = pages[cur_slice - 1]
                every_page_contents = get_each_page_data(cur_page, tolerance_y, leftsize, rightsize, chars2gap)
                contents.append(every_page_contents)
                return contents, leftsize, rightsize, chars2gap
        containers_per_bulk = size // bulks
        contents = []
        start = (cur_slice - 1) * containers_per_bulk
        if cur_slice == bulks:
            end = size
        else:
            end = cur_slice * containers_per_bulk
        for i in range(start, end):
            cur_page = pages[i]
            every_page_contents = get_each_page_data(cur_page, tolerance_y, leftsize, rightsize, chars2gap)
            contents.append(every_page_contents)

    return contents, leftsize, rightsize, chars2gap


def get_each_page_data(cur_page, tolerance_y, leftsize, rightsize, chars2gap):
    every_page_contents = []
    chars = cur_page.chars
    doctop_clusters = cluster_objects(chars, "doctop", tolerance_y)
    get_last_first_char_information(doctop_clusters, leftsize, rightsize, every_page_contents, chars2gap)
    return every_page_contents


def get_last_first_char_information(doctop_clusters, leftsize, rightsize, every_page_contents, chars2gap):
    lines_number = len(doctop_clusters)
    for i in range(lines_number):
        line = doctop_clusters[i]
        first_token = line[0]
        first_char_size = round(first_token['size'], 2)
        first_position = first_token['x0']
        leftsize[first_char_size].append(first_position)
        last_token = line[-1]
        last_char_size = round(last_token['size'], 2)
        last_position = last_token['x0']
        rightsize[last_char_size].append(last_position)
        line_information = get_every_line_information(line, chars2gap)
        line_information['first_char_position'] = round(first_position, 2)
        line_information['last_char_position'] = round(last_position, 2)
        every_page_contents.append(line_information)


def get_every_line_information(line, chars2gap):
    char_size = []
    text = []
    total_char = len(line)
    for i in range(total_char):
        char = line[i]
        size = round(char['size'], 2)
        char_size.append(size)
        text.append(char['text'])
    size2num = Counter(char_size)
    if len(size2num) > 1:
        pass
        # print('the number of size of char in a line is more than one')
    size_frequency = sorted(size2num.items(), key=lambda x: x[1], reverse=True)
    mode = size_frequency[0][0]
    line_information = {'text': ''.join(text), 'mode_size': mode}
    if mode not in chars2gap:
        gap = 0.000
        for i in range(total_char):
            char = line[i]
            if len(line) > 5:
                if i > 0:
                    pre_char = line[i - 1]
                    gap = round(max(char['x0'] - pre_char['x0'], gap), 3)
        chars2gap[mode] = gap
    return line_information


def get_most_similar_mode_size(nums, target):
    nums = sorted(nums)
    idx = 0
    n = len(nums)
    distance = abs(nums[0] - target)
    for i in range(1, n):
        cur_distance = abs(nums[i] - target)
        if cur_distance > distance:
            distance = cur_distance
            idx = i

    return nums[idx]


def judge_catalogue(previous_text, current_text):
    # when the paragraph is catalogue,the first char is leftest and the last char is rightest.
    s_p_1 = '([\d]+[\.]+[\d]+$)'  # judge float
    is_float = get_re_format1_result(previous_text, s_p_1)
    s_p_2 = '([\.·_\-]{3,}[\d\-\s_]+$)'
    Iscatalogue = get_re_format1_result(previous_text, s_p_2)
    if not is_float and Iscatalogue:
        return True
    s_p_3 = '([\d\-\s_]+$)'
    pre_Iscatalogue = get_re_format1_result(previous_text, s_p_3)
    current_Iscatalogue = get_re_format1_result(current_text, s_p_2)
    if pre_Iscatalogue and current_Iscatalogue:
        return True
    return False


def judge_new_line(previous, current, leftsize, rightsize, chars2gap, main_size, rightsize2frequency,
                   left_size2frequency):
    """
    merge the current line and previous line according the format. this function can be adjusted when processing
    new format of pdf.
    """
    if not previous:
        return True
    special_tokens = ['', '', '', '•', '']
    special_characters = [',', '，']
    if previous['mode_size'] != current['mode_size']:
        return True
    first_position = current['first_char_position']
    last_position = previous['last_char_position']
    last_position_current = current['last_char_position']
    first_position_previous = previous['first_char_position']
    # if current['mode_size'] > main_size:
    #     pass
    if special_character(current):
        return True

    size = previous['mode_size']
    if size in rightsize:
        text_distribute_right_position = rightsize[size][0]
    else:
        alternative_size = get_most_similar_mode_size(rightsize.keys(), size)
        text_distribute_right_position = rightsize[alternative_size][0]
    if size in leftsize:
        text_distribute_left_position = leftsize[size][0]
    else:
        alternative_size = get_most_similar_mode_size(leftsize.keys(), size)
        text_distribute_left_position = leftsize[alternative_size][0]
    previous_text = previous['text']
    current_text = current['text']

    if judge_catalogue(previous_text, current_text):
        return True
    if current_text[0] in special_tokens:
        return True
    gaps = determine_gap(size, main_size, chars2gap)
    # judge long title
    gap1 = first_position_previous - first_position
    gap2 = last_position_current - last_position
    gap3 = abs(first_position_previous - leftsize[size][0])
    gap4 = abs(last_position - rightsize[size][0])
    s_p_1 = '(^[\(\（]+[\da-zA-Z一二三四五六七八九十]+[\)\）]*[、、·\．]*)'
    s_p_2 = '(^[\(\（]*[\da-zA-Z一二三四五六七八九十]+[\)\）]*[、、·\．]+)'
    s_p_3 = '(^[\(\（]*[\da-zA-Z一二三四五六七八九十]+[\)\）]+[、、·\．]*)'
    s_p_4 = '(^[\da-zA-Z一二三四五六七八九十\.]+[、、·\．]+)'
    s_p_5 = '(^[\d]+[·\.]+[\d]+)'
    special_tokens_head_previous_1 = get_re_format1_result(previous_text, s_p_1)
    special_tokens_head_previous_2 = get_re_format1_result(previous_text, s_p_2)
    special_tokens_head_previous_3 = get_re_format1_result(previous_text, s_p_3)
    special_tokens_head_previous_4 = get_re_format1_result(previous_text, s_p_4)
    special_tokens_head_previous_5 = get_re_format1_result(previous_text, s_p_5)
    special_tokens_head_current_1 = get_re_format1_result(current_text, s_p_1)
    special_tokens_head_current_2 = get_re_format1_result(current_text, s_p_2)
    special_tokens_head_current_3 = get_re_format1_result(current_text, s_p_3)
    special_tokens_head_current_4 = get_re_format1_result(current_text, s_p_4)
    special_tokens_head_current_5 = get_re_format1_result(current_text, s_p_5)
    special_tokens_head_previous = special_tokens_head_previous_1 or special_tokens_head_previous_2 or \
                                   special_tokens_head_previous_3 or special_tokens_head_previous_4 or \
                                   special_tokens_head_previous_5
    special_tokens_head_current = special_tokens_head_current_1 or special_tokens_head_current_2 or \
                                  special_tokens_head_current_3 or special_tokens_head_current_4 or \
                                  special_tokens_head_current_5

    # distance of previous line indentation is too long
    if not special_tokens_head_previous and determine_new_line_by_left_position(leftsize, left_size2frequency, previous,
                                                                                main_size):
        if first_position_previous - first_position < size + 2:
            # though some paragraph indentation is too long, next line
            return True
    if special_tokens_head_previous and not special_tokens_head_current:
        if abs(gap1 - gap2) < size and abs(gap1) > size * 4:
            return False
    if abs(gap1 - gap2) < size // 2.5 and size > main_size and gap3 < size and gap4 < size:
        return False
    if determine_new_line_by_left_position(leftsize, left_size2frequency, current, main_size):
        return True

    if abs(last_position - text_distribute_right_position) > float(gaps) + 0.001:
        # rightest position is not the current rightest position(whole is not local)
        if last_position_current - last_position > size * 2 - 3:  # the size of title is
            return True
        if rightsize2frequency[size] - last_position > size * 2 - 3:
            return True
        if special_tokens_head_current:
            return True
        if first_position_previous - first_position > size * 8:
            return True
        if special_tokens_head_previous and not special_tokens_head_current:
            return False
        if size * 8 > first_position_previous - first_position > size * 2 - 3:
            return False
        if first_position_previous - first_position > size - 2:
            if abs(last_position_current - last_position) < size - 2:
                # some text format is irregular
                return False
            if abs(rightsize2frequency[size] - last_position) < size - 2 and len(current_text) < 10:
                return False
        return True  # to do

    if abs(last_position - text_distribute_right_position) < float(gaps) + 0.001 < \
            abs(first_position - text_distribute_left_position):
        # the position of last char in previous line is rightest, but the position of first char in current line
        # is not leftest
        # if previous_text[0] in special_tokens and current_text[0] not in special_tokens:
        #     return False
        # s_p_1 = '(^[\(\（]*[\da-zA-Z一二三四五六七八九十]+[\)\）]*[、、]*)'
        # special_tokens_head_previous = get_re_format1_result(previous_text, s_p_1)
        # special_tokens_head_current = get_re_format1_result(current_text, s_p_1)
        if special_tokens_head_previous and not special_tokens_head_current:
            return False
        if first_position_previous - first_position > size * 2 - 3:
            return False
        if abs(first_position - text_distribute_left_position) < size * 2 - 3:
            return False
        return True
    is_small_paragraph = small_sequence_paragraph(current)
    if is_small_paragraph:
        return True
    if current['mode_size'] == main_size:
        is_new_paragraph = judge_main_body_paragraph(previous, current, main_size)
        if is_new_paragraph:
            return True
    return False


def special_character(text):
    s_p_1 = '([\(（]+(签字)[\)）]+$)'
    has_match = get_re_format1_result(text['text'], s_p_1)
    if has_match:
        return has_match


def determine_new_line_by_left_position(left_size, left_size2frequency, current, main_size):
    size = current['mode_size']
    first_char_position = current['first_char_position']
    whole_left_size_position = float(left_size[size][0])

    if left_size2frequency[size] == whole_left_size_position:
        standard_position = whole_left_size_position
    else:
        standard_position = min(left_size2frequency[size], whole_left_size_position + 2 * size)
    if size <= main_size:
        if first_char_position - standard_position > 5 * size:
            return True
    else:

        if abs(first_char_position - whole_left_size_position) < size // 3:
            main_left_size_position = left_size2frequency[main_size]
            if first_char_position - main_left_size_position > main_size * 3 + 2:
                return True
            if len(left_size[size]) < 3 and first_char_position - left_size[size][0] > size + 2:
                return True
        else:

            if first_char_position - standard_position > 2.5 * size + 2:
                return True


def determine_gap(size, main_size, chars2gap):
    if size == main_size:
        return size
    gap = float(chars2gap[size])
    if gap < 1:
        return size
    gap = min(gap, size * 1.2)
    return gap


# 2023/02/09 supplement
def small_sequence_paragraph(current):
    current_text = current['text']
    s_p_1 = '(^[\(\（]*[\da-zA-Z一二三四五六七八九十]+[\)\）]*[、、]+)'
    is_small_paragraph = get_re_format1_result(current_text, s_p_1)
    if is_small_paragraph:
        return is_small_paragraph
    s_p_2 = '(^[\d]+[\.]+[\d]+)'
    is_small_paragraph = get_re_format1_result(current_text, s_p_2)
    if is_small_paragraph:
        return is_small_paragraph
    s_p_3 = '(^[\(\（]+[\da-zA-Z一二三四五六七八九十]+[\)\）]+[、、]*)'
    is_small_paragraph = get_re_format1_result(current_text, s_p_3)
    if is_small_paragraph:
        return is_small_paragraph
    return is_small_paragraph


def judge_larger_main_body_paragraph(first_position, first_position_previous, last_position_current, last_position,
                                     mode_size):
    if first_position_previous - first_position > mode_size:
        return False


def judge_main_body_paragraph(previous, current, char_size):
    previous_first_char_position = previous['first_char_position']
    current_first_char_position = current['first_char_position']
    new_paragraph_character = set([',' '，'])
    if current_first_char_position - previous_first_char_position >= char_size * 2 - 2:
        if previous['text'][-1] not in new_paragraph_character:
            return True  # To Do
    if 'first_char_position_bak' in previous:
        pre_first_char_position_bak = previous['first_char_position_bak']
        if current_first_char_position - pre_first_char_position_bak >= char_size * 2 - 2:
            if previous['text'][-1] not in new_paragraph_character:
                return True  # To Do
    return False


def merge_different_line2paragraph(all_pages_contents, leftsize, rightsize, chars2gap):
    paragraphs = []
    page_number = len(all_pages_contents)
    for i in range(page_number):
        page = all_pages_contents[i]
        line_number = len(page)
        for j in range(line_number - 1):
            if i == 0 and j == 0:
                paragraphs.append(page[j]['text'])
                continue
            if i != 0 and j == 0:
                if len(all_pages_contents[i - 1]) < 2:
                    previous = None
                else:
                    previous = all_pages_contents[i - 1][-2]
            else:
                previous = page[j - 1]
            current = page[j]
            new_line_flag = judge_new_line(previous, current, leftsize, rightsize, chars2gap)
            if new_line_flag:
                paragraphs.append(current['text'])
                continue

            merge_line = paragraphs.pop() + current['text']
            paragraphs.append(merge_line)
    return paragraphs


def get_re_format1_result(st, s_p):
    pattern = re.compile(s_p)
    search = re.search(pattern, st)
    if search:
        return True
    return False


def filter_page_number(st):
    s_p_1 = '(第[\d]+页)'
    has_match = get_re_format1_result(st, s_p_1)
    if has_match:
        return has_match
    s_p_1 = '(^[\d\-—\s]+$)'
    has_match = get_re_format1_result(st, s_p_1)
    if has_match:
        return has_match
    s_p_2 = '([\.]+[\d]+$)'

    return has_match


def merge_different_line2paragraph_from_different_results(pure_texts, position_texts, leftsize, rightsize, chars2gap,
                                                          main_size, rightsize2frequency, leftsize2frequency):
    paragraphs_information = []
    page_number = len(pure_texts)
    for i in range(page_number):
        current_page_pure = pure_texts[i]
        current_page_position = position_texts[i]
        line_number = len(current_page_pure)
        for j in range(line_number):
            current_line_new = copy.deepcopy(current_page_position[j])
            del current_line_new['text']
            if i == 0 and j == 0:
                current_line_new['text'] = current_page_pure[j]
                paragraphs_information.append(current_line_new)
                continue
            if i != 0 and j == 0:
                text = current_page_position[j]['text']
                is_page_number = filter_page_number(text)
                if is_page_number:
                    continue
                if len(position_texts[i - 1]) == 1:
                    # previous page has no pure text, except page information
                    is_page_number = filter_page_number(position_texts[i - 1][-1]['text'])
                    if is_page_number:
                        previous = None
                    else:
                        if paragraphs_information:
                            previous = paragraphs_information[-1]
                        else:
                            previous = None
                else:
                    if paragraphs_information:
                        previous = paragraphs_information[-1]
                    else:
                        previous = None
            else:
                text = current_page_position[j]['text']
                if isinstance(text, str):
                    is_page_number = filter_page_number(current_page_position[j]['text'])
                    if is_page_number:
                        continue
                else:
                    print(text)
                if paragraphs_information:
                    previous = paragraphs_information[-1]
                else:
                    previous = None
            current_line = current_page_position[j]

            new_line_flag = judge_new_line(previous, current_line, leftsize, rightsize, chars2gap,
                                           main_size, rightsize2frequency, leftsize2frequency)
            if new_line_flag:
                current_line_new['text'] = current_page_pure[j]
                paragraphs_information.append(current_line_new)
                continue
            tmp_paragraphs_information = copy.deepcopy(paragraphs_information.pop())
            merge_text = tmp_paragraphs_information['text'] + current_page_pure[j]
            tmp_paragraphs_information['text'] = merge_text
            tmp_paragraphs_information['last_char_position'] = current_line['last_char_position']
            if 'first_char_position_bak' in tmp_paragraphs_information:
                tmp_paragraphs_information['first_char_position_bak'] = \
                    min(tmp_paragraphs_information['first_char_position_bak'], current_line['first_char_position'])
            else:
                tmp_paragraphs_information['first_char_position_bak'] = \
                    min(tmp_paragraphs_information['first_char_position'], current_line['first_char_position'])
            paragraphs_information.append(tmp_paragraphs_information)

    return paragraphs_information


def get_different_parse_result_by_line(path, parallel):
    parse_file = partial(parallel_process, path)
    parse_bulk = partial(parse_file, parallel)
    executor = ThreadPoolExecutor(max_workers=2)
    task1 = executor.submit(lambda x: parse_bulk(*x), [1])
    task2 = executor.submit(lambda x: parse_bulk(*x), [2])
    tasks = [task1, task2]
    wait(tasks, return_when=ALL_COMPLETED)
    task_results = []
    for task in tasks:
        res = task.result()
        task_results.append(res)
    return task_results


def get_different_parse_result_parallel(path, parallel):
    parse_file = partial(parallel_process, path)
    # parse_bulk = partial(parse_file, parallel)
    process_kernel = process_number(parallel, path)
    parse_various_text = partial(read_bulk_data_for_various_information, path)
    parse_pure_text = partial(read_bulk_data_for_pure_text, path)
    process_kernel = max(2, process_kernel)
    first_process = process_kernel // 2
    second_process = process_kernel - first_process

    parse_bulk_pure_text = partial(parse_pure_text, first_process)
    parse_bulk_various_text = partial(parse_various_text, second_process)
    first_results = []
    second_results = []
    final_result = []
    logger.info("spend {} multi-processes parsing pdf".format(str(process_kernel)))
    with ProcessPoolExecutor(process_kernel) as executor:
        tasks = []
        for i in range(first_process):
            tasks.append(executor.submit(parse_bulk_pure_text, i + 1))
        for i in range(second_process):
            tasks.append(executor.submit(parse_bulk_various_text, i + 1))

        for j in range(first_process):
            try:
                task = tasks[j]
                first_results.extend(task.result())
            except Exception as e:
                logger.error(e.args)

        for j in range(second_process):
            try:
                task = tasks[j + first_process]
                second_results.append(task.result())
            except Exception as e:
                logger.error(e.args)

    final_second_result = merge_multi_process_bulk_result(second_results)
    final_result.append(first_results)
    final_result.append(final_second_result)
    return final_result


def get_paragraph_information_from_parse_result(path, parallel=1):
    """
    this function is used to parse document the type of which is pdf, get the pure text from pdf,
    :param path:
    :param parallel:
    :return:
    """
    # task_results = get_different_parse_result_by_line(path, parallel)
    task_results = get_different_parse_result_parallel(path, parallel)
    results01 = task_results[0]
    results02 = task_results[1]
    assert len(results01) == len(results02[0])
    every_line = [len(results01[i]) == len(results02[0][i]) for i in range(len(results01))]
    # assert all(every_line)
    if all(every_line):
        pure_contexts = results01
    else:
        pure_contexts = []
        for i in range(len(results01)):
            if every_line[i]:
                pure_contexts.append(results01[i])
            else:
                pure_pages_context = []
                pages_context = results02[0][i]
                for line in pages_context:
                    pure_pages_context.append(line['text'])
                pure_contexts.append(pure_pages_context)
    main_size = get_main_body_mode_size(results02[0])
    paragraphs_information = merge_different_line2paragraph_from_different_results(pure_contexts, results02[0],
                                                                                   results02[1], results02[2],
                                                                                   results02[3], main_size,
                                                                                   results02[4], results02[5])

    return paragraphs_information, main_size


def get_main_body_mode_size(paragraphs):
    from collections import defaultdict
    size2frequency = defaultdict(int)
    pages_size = defaultdict(int)

    for page in paragraphs:
        text = page[-1]['text']

        if filter_page_number(text):
            pages_size[page[-1]['mode_size']] += 1
        for paragraph in page:
            size2frequency[paragraph['mode_size']] += 1

    for size, frequency in pages_size.items():
        size2frequency[size] -= frequency
    size2frequency = sorted(size2frequency.items(), key=lambda x: x[1])
    return size2frequency[-1][0]


def get_pure_text_title_from_pdf(filepath, parallel=8):
    paragraphs, main_size = get_paragraph_information_from_parse_result(filepath, parallel)
    pure_texts = []
    pure_titles = []
    for paragraph in paragraphs:
        mode_size = float(paragraph['mode_size'])
        text = paragraph['text']
        text = text.strip()
        if not text:
            continue
        if isinstance(text, str):
            has_match = filter_page_number(text)
            if has_match:
                continue
        pure_texts.append(text)
        if mode_size > main_size:
            pure_titles.append(text)
    return pure_texts, pure_titles


def get_content_from_pdf_by_sentence(filepath):
    paragraphs, main_size = get_paragraph_information_from_parse_result(filepath, parallel=1)
    pure_texts = []
    size = len(paragraphs)
    for i in range(size):
        current_text = paragraphs[i]
        text = current_text['text']
        text = text.strip()
        if not text:
            continue
        if isinstance(text, str):
            has_match = filter_page_number(text)
            if has_match:
                continue
        if i != 0:

            previous_text = paragraphs[i - 1]
            is_catalogue = judge_catalogue(previous_text, current_text)
            if is_catalogue:
                continue
        sentences = text.split('。')

        pure_texts.extend(sentences)

    return pure_texts


def main(source, destination, parallel=1):
    paragraphs = get_paragraph_information_from_parse_result(source, parallel)
    with open(destination, 'w', encoding='utf-8') as f:
        for paragraph in paragraphs:
            paragraph['mode_size'] = float(paragraph['mode_size'])
            paragraph['first_char_position'] = float(paragraph['first_char_position'])
            paragraph['last_char_position'] = float(paragraph['last_char_position'])
            paragraph_text = json.dumps(paragraph, ensure_ascii=False)
            f.write(paragraph_text)
            f.write('\n')


def process_batch_data(source, target):
    files = os.listdir(source)
    idx = 0
    processed = os.listdir(target)
    for file in files:

        fullpath = os.path.join(source, file)
        begin_time = time.time()
        try:
            new_file = file[:-3] + 'txt'
            if new_file in processed:
                continue
            new_file_name = os.path.join(target, new_file)
            content = main(fullpath, new_file_name, 1)
            idx += 1
        except Exception as e:
            print(e.args)
            print(fullpath)
            idx += 1
        # new_file = file[:-3] + 'txt'
        # if new_file in processed:
        #     continue
        # new_file_name = os.path.join(target, new_file)
        # content = main(fullpath, new_file_name, 1)
        # idx += 1
        print(str(file), 'time need: ', time.time() - begin_time)