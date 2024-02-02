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
        from drishti.handlers.handle_darshan import handler

    elif log_type == LOG_TYPE_RECORDER:
        from drishti.handlers.handle_recorder import handler
    
    handler()

