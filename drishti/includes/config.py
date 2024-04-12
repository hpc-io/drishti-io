#!/usr/bin/env python3

import os
import json

from rich.console import Console, Group
from rich.padding import Padding
from rich.panel import Panel
from rich.terminal_theme import TerminalTheme
from rich.terminal_theme import MONOKAI

from drishti.includes.parser import *


RECOMMENDATIONS = 0
HIGH = 1
WARN = 2
INFO = 3
OK = 4

ROOT = os.path.abspath(os.path.dirname(__file__))

TARGET_USER = 1
TARGET_DEVELOPER = 2
TARGET_SYSTEM = 3

insights_operation = []
insights_metadata = []
insights_dxt = []

insights_total = dict()

insights_total[HIGH] = 0
insights_total[WARN] = 0
insights_total[RECOMMENDATIONS] = 0

thresholds = {
    'imbalance_operations': [0.1, False],
    'small_bytes': [1048576, False],
    'small_requests': [0.1, False],
    'small_requests_absolute': [1000, False],
    'misaligned_requests': [0.1, False],
    'metadata_time_rank': [30, False],
    'random_operations': [0.2, False],
    'random_operations_absolute': [1000, False],
    'imbalance_stragglers': [0.15, False],
    'imbalance_size': [0.3, False],
    'interface_stdio': [0.1, False],
    'collective_operations': [0.5, False],
    'collective_operations_absolute': [1000, False],
    'backtrace': [2, False]
}

INSIGHTS_STDIO_HIGH_USAGE = 'S01'
INSIGHTS_POSIX_WRITE_COUNT_INTENSIVE = 'P01'
INSIGHTS_POSIX_READ_COUNT_INTENSIVE = 'P02'
INSIGHTS_POSIX_WRITE_SIZE_INTENSIVE = 'P03'
INSIGHTS_POSIX_READ_SIZE_INTENSIVE = 'P04'
INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_USAGE = 'P05'
INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_USAGE = 'P06'
INSIGHTS_POSIX_HIGH_MISALIGNED_MEMORY_USAGE = 'P07'
INSIGHTS_POSIX_HIGH_MISALIGNED_FILE_USAGE = 'P08'
INSIGHTS_POSIX_REDUNDANT_READ_USAGE = 'P09'
INSIGHTS_POSIX_REDUNDANT_WRITE_USAGE = 'P10'
INSIGHTS_POSIX_HIGH_RANDOM_READ_USAGE = 'P11'
INSIGHTS_POSIX_HIGH_SEQUENTIAL_READ_USAGE = 'P12'
INSIGHTS_POSIX_HIGH_RANDOM_WRITE_USAGE = 'P13'
INSIGHTS_POSIX_HIGH_SEQUENTIAL_WRITE_USAGE = 'P14'
INSIGHTS_POSIX_HIGH_SMALL_READ_REQUESTS_SHARED_FILE_USAGE = 'P15'
INSIGHTS_POSIX_HIGH_SMALL_WRITE_REQUESTS_SHARED_FILE_USAGE = 'P16'
INSIGHTS_POSIX_HIGH_METADATA_TIME = 'P17'
INSIGHTS_POSIX_SIZE_IMBALANCE = 'P18'
INSIGHTS_POSIX_TIME_IMBALANCE = 'P19'
INSIGHTS_POSIX_INDIVIDUAL_WRITE_SIZE_IMBALANCE = 'P21'
INSIGHTS_POSIX_INDIVIDUAL_READ_SIZE_IMBALANCE = 'P22'
INSIGHTS_MPI_IO_NO_USAGE = 'M01'
INSIGHTS_MPI_IO_NO_COLLECTIVE_READ_USAGE = 'M02'
INSIGHTS_MPI_IO_NO_COLLECTIVE_WRITE_USAGE = 'M03'
INSIGHTS_MPI_IO_COLLECTIVE_READ_USAGE = 'M04'
INSIGHTS_MPI_IO_COLLECTIVE_WRITE_USAGE = 'M05'
INSIGHTS_MPI_IO_BLOCKING_READ_USAGE = 'M06'
INSIGHTS_MPI_IO_BLOCKING_WRITE_USAGE = 'M07'
INSIGHTS_MPI_IO_AGGREGATORS_INTRA = 'M08'
INSIGHTS_MPI_IO_AGGREGATORS_INTER = 'M09'
INSIGHTS_MPI_IO_AGGREGATORS_OK = 'M10'

DETAILS_MAX_SIZE = 10

csv_report = []
codes = []


def init_console():
    console = Console(record=True)

    if args.export_size:
        console.width = int(args.export_size)

    insights_operation.clear()
    insights_metadata.clear()

    insights_total[HIGH] = 0
    insights_total[WARN] = 0
    insights_total[RECOMMENDATIONS] = 0

    for name in thresholds:
        thresholds[name][1] = False

    return console


def set_export_theme():
    if args.export_theme_light:
        export_theme = TerminalTheme(
            (255, 255, 255),
            (0, 0, 0),
            [
                (26, 26, 26),
                (244, 0, 95),
                (152, 224, 36),
                (253, 151, 31),
                (157, 101, 255),
                (244, 0, 95),
                (88, 209, 235),
                (120, 120, 120),
                (98, 94, 76),
            ],
            [
                (244, 0, 95),
                (152, 224, 36),
                (224, 213, 97),
                (157, 101, 255),
                (244, 0, 95),
                (88, 209, 235),
                (246, 246, 239),
            ],
        )
    else:
        export_theme = MONOKAI
    return export_theme


def load_json():
    codes = []
    if not args.split_files:
        if args.json:
            f = open(args.json)
            data = json.load(f)

            for key, values in data.items():
                for value in values:
                    code = value['code']
                    codes.append(code)

                    level = value['level']
                    issue = value['issue']
                    recommendation = []
                    for rec in value['recommendations']:
                        new_message = {'message': rec}
                        recommendation.append(new_message)

                    insights_dxt.append(
                        message(code, TARGET_DEVELOPER, level, issue, recommendation)
                    )


def validate_thresholds():
    """
    Validate thresholds defined by the user.
    """
    if args.config:
        f = open(args.config)
        data = json.load(f)

        for category, thresholds_spec in data.items():
            for threshold_name, threshold_value in thresholds_spec.items():
                thresholds[category + '_' + threshold_name][0] = threshold_value
                
        assert(thresholds['imbalance_operations'][0] >= 0.0 and thresholds['imbalance_operations'][0] <= 1.0)
        assert(thresholds['small_requests'][0] >= 0.0 and thresholds['small_requests'][0] <= 1.0)
        assert(thresholds['misaligned_requests'][0] >= 0.0 and thresholds['misaligned_requests'][0] <= 1.0)
        assert(thresholds['random_operations'][0] >= 0.0 and thresholds['random_operations'][0] <= 1.0)

        assert(thresholds['metadata_time_rank'][0] >= 0.0)


def convert_bytes(bytes_number):
    """
    Convert bytes into formatted string.
    """
    tags = [
        'bytes',
        'KB',
        'MB',
        'GB',
        'TB',
        'PB',
        'EB'
    ]

    i = 0
    double_bytes = bytes_number

    while (i < len(tags) and bytes_number >= 1024):
        double_bytes = bytes_number / 1024.0
        i = i + 1
        bytes_number = bytes_number / 1024

    return str(round(double_bytes, 2)) + ' ' + tags[i] 


def message(code, target, level, issue, recommendations=None, details=None):
    """
    Display the message on the screen with level, issue, and recommendation.
    """
    icon = ':arrow_forward:'

    if level in (HIGH, WARN):
        insights_total[level] += 1

    if level == HIGH:
        color = '[red]'
    elif level == WARN:
        color = '[orange1]'
    elif level == OK:
        color = '[green]'
    else:
        color = ''

    messages = [
        '{}{}{} {}'.format(
            color,
            icon,
            ' [' + code + ']' if args.code else '',
            issue
        )
    ]

    if args.export_csv:
        csv_report.append(code)

    if details:
        for detail in details[:DETAILS_MAX_SIZE]:
            messages.append('  {}:left_arrow_curving_right: {}'.format(
                    color,
                    detail['message']
                )
            )

    if recommendations:
        if not args.only_issues:
            messages.append('  [white]:left_arrow_curving_right: [b]Recommendations:[/b]')

            for recommendation in recommendations:
                messages.append('    :left_arrow_curving_right: {}'.format(recommendation['message']))

                if args.verbose and 'sample' in recommendation:
                    messages.append(
                        Padding(
                            Panel(
                                recommendation['sample'],
                                title='Solution Example Snippet',
                                title_align='left',
                                padding=(1, 2)
                            ),
                            (1, 0, 1, 7)
                        )
                    )

        insights_total[RECOMMENDATIONS] += len(recommendations)

    return Group(
        *messages
    )


'''
Pre-load
'''
load_json()
validate_thresholds()
