#!/usr/bin/env python3

import os
import sys
from subprocess import call
from drishti.includes.parser import *

'''
                         |- handler_darshan   -|
                         |                     |
reporter -> /handlers -> |- handler_recorder  -|   -| 
                         |                     |    |    
                         |- handler_xxx ...   -|    |
    ________________________________________________|
    |
    |-----> /includes -> module -> config -> parser
'''

LOG_TYPE_DARSHAN = 0
LOG_TYPE_RECORDER = 1


def clear():
    """
    Clear the screen with the comment call based on the operating system.
    """
    _ = call('clear' if os.name == 'posix' else 'cls')


def check_log_type(paths: list[str]) -> int | None:
    is_darshan = True
    is_recorder = True
    multiple_logs = len(paths) > 1

    for path in paths:
        if path.endswith('.darshan'):
            if not os.path.isfile(path):
                print('Unable to open .darshan file.')
                sys.exit(os.EX_NOINPUT)
            else:
                is_darshan = True and is_darshan
                is_recorder = False and is_recorder
        else:  # check whether is a valid recorder log
            if not os.path.isdir(path):
                print('Unable to open recorder folder.')
                sys.exit(os.EX_NOINPUT)
            else:
                is_recorder = True and is_recorder
                is_darshan = False and is_darshan

    if multiple_logs:
        if is_darshan:
            return LOG_TYPE_DARSHAN
        else:
            print('Only .darshan files are supported for multiple logs.') #TODO
            sys.exit(os.EX_NOINPUT)
    else:
        if is_darshan and not is_recorder:
            return LOG_TYPE_DARSHAN
        elif is_recorder and not is_darshan:
            return LOG_TYPE_RECORDER
        else:
            print('Unable to reliably determine the log type.')
            sys.exit(os.EX_NOINPUT)


def main():
    log_type = check_log_type(args.log_paths)

    if log_type == LOG_TYPE_DARSHAN:
        from drishti.handlers.handle_darshan import handler

    elif log_type == LOG_TYPE_RECORDER:
        from drishti.handlers.handle_recorder import handler

    handler()


if __name__ == '__main__':
    main()
