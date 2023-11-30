#!/usr/bin/env python3

import os
import json

from rich.console import Console, Group
from rich.padding import Padding
from rich.panel import Panel
from rich.terminal_theme import TerminalTheme
from rich.terminal_theme import MONOKAI

from .parser import *


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

THRESHOLD_OPERATION_IMBALANCE = 0.1
THRESHOLD_SMALL_REQUESTS = 0.1
THRESHOLD_SMALL_REQUESTS_ABSOLUTE = 1000
THRESHOLD_MISALIGNED_REQUESTS = 0.1
THRESHOLD_METADATA = 0.1
THRESHOLD_METADATA_TIME_RANK = 30  # seconds
THRESHOLD_RANDOM_OPERATIONS = 0.2
THRESHOLD_RANDOM_OPERATIONS_ABSOLUTE = 1000
THRESHOLD_STRAGGLERS = 0.15
THRESHOLD_IMBALANCE = 0.30
THRESHOLD_INTERFACE_STDIO = 0.1
THRESHOLD_COLLECTIVE_OPERATIONS = 0.5
THRESHOLD_COLLECTIVE_OPERATIONS_ABSOLUTE = 1000
THRESHOLD_SMALL_BYTES = 1048576 # 1 MB

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

# TODO: need to verify the threashold to be between 0 and 1
# TODO: read thresholds from file


def init_console():
    console = Console(record=True)
    if args.export_size: console.width = int(args.export_size)

    insights_operation.clear()
    insights_metadata.clear()

    insights_total[HIGH] = 0
    insights_total[WARN] = 0
    insights_total[RECOMMENDATIONS] = 0
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
    assert(THRESHOLD_OPERATION_IMBALANCE >= 0.0 and THRESHOLD_OPERATION_IMBALANCE <= 1.0)
    assert(THRESHOLD_SMALL_REQUESTS >= 0.0 and THRESHOLD_SMALL_REQUESTS <= 1.0)
    assert(THRESHOLD_MISALIGNED_REQUESTS >= 0.0 and THRESHOLD_MISALIGNED_REQUESTS <= 1.0)
    assert(THRESHOLD_METADATA >= 0.0 and THRESHOLD_METADATA <= 1.0)
    assert(THRESHOLD_RANDOM_OPERATIONS >= 0.0 and THRESHOLD_RANDOM_OPERATIONS <= 1.0)

    assert(THRESHOLD_METADATA_TIME_RANK >= 0.0)


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

    while (i < len(tags) and  bytes_number >= 1024):
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
if not args.split_files:
    load_json()

