""" Debugginh Utilities """
#!/usr/bin/python
# -*- coding: utf-8 -*-

# --------------------------------------------------
# File Name: debug.py
# Purpose:
# Creation Date: 21-09-2016
# Last Modified: Wed, Jan 25, 2017  2:40:51 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2016 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------


import inspect
from subprocess import call
import sys


def die():
    """ Close all running python instances """
    call(["cmd", "/c /cygwin64/bin/bash.exe /home/Admin/bin/zap pyth"])
    sys.exit()


def debug():
    """ Print debug information by inrospection of funcions and line details """
    sys.stdout.flush()
    #cf = inspect.currentframe()
    #filename = inspect.getframeinfo(cf).filename
    current_f = inspect.stack()[1][3]
    parent_f = inspect.stack()[2][3]
    print  ">> line", inspect.currentframe().f_back.f_lineno, \
                    ".", current_f, ".", parent_f # , ".", filename

    sys.stdout.flush()
    #die()


