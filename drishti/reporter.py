#!/usr/bin/env python3

import os
import sys
import argparse

from subprocess import call


LOG_TYPE_DARSHAN = 0
LOG_TYPE_RECORDER = 1

parser = argparse.ArgumentParser(
    description='Drishti: '
)

parser.add_argument(
    'log_path',
    help='Input .darshan file or recorder folder'
)

parser.add_argument(
    '--issues',
    default=False,
    action='store_true',
    dest='only_issues',
    help='Only displays the detected issues and hides the recommendations'
)

parser.add_argument(
    '--html',
    default=False,
    action='store_true',
    dest='export_html',
    help='Export the report as an HTML page'
)

parser.add_argument(
    '--svg',
    default=False,
    action='store_true',
    dest='export_svg',
    help='Export the report as an SVG image'
)

parser.add_argument(
    '--light',
    default=False,
    action='store_true',
    dest='export_theme_light',
    help='Use a light theme for the report when generating files'
)

parser.add_argument(
    '--size',
    default=False,
    dest='export_size',
    help='Console width used for the report and generated files'
)

parser.add_argument(
    '--verbose',
    default=False,
    action='store_true',
    dest='verbose',
    help='Display extended details for the recommendations'
)

parser.add_argument(
    '--code',
    default=False,
    action='store_true',
    dest='code',
    help='Display insights identification code'
)

parser.add_argument(
    '--path',
    default=False,
    action='store_true',
    dest='full_path',
    help='Display the full file path for the files that triggered the issue'
)

parser.add_argument(
    '--csv',
    default=False,
    action='store_true',
    dest='export_csv',
    help='Export a CSV with the code of all issues that were triggered'
)

parser.add_argument(
    '--json', 
    default=False, 
    dest='json',
    help=argparse.SUPPRESS
)

parser.add_argument(
    '--split',
    default=False,
    action='store_true',
    dest='split_files',
    help='Split the files and generate report for each file'
)

args = parser.parse_args()


def clear():
    """
    Clear the screen with the comment call based on the operating system.
    """
    _ = call('clear' if os.name == 'posix' else 'cls')


def check_log_type(path):
    if path.endswith('.darshan'):
        if not os.path.isfile(path):
            print('Unable to open .darshan file.')
            sys.exit(os.EX_NOINPUT)
        else: return LOG_TYPE_DARSHAN
    else: # check whether is a valid recorder log
        if not os.path.isdir(path):
            print('Unable to open recorder folder.')
            sys.exit(os.EX_NOINPUT)
        else: return LOG_TYPE_RECORDER


def main():
    log_type = check_log_type(args.log_path)
    
    if log_type == LOG_TYPE_DARSHAN:
        from . import handle_darshan
        handle_darshan.handler(args)

    elif log_type == LOG_TYPE_RECORDER:
        if args.split_files:
            from . import handle_recorder_split
            handle_recorder_split.handler(args)
        else:
            from . import handle_recorder
            handle_recorder.handler(args)

