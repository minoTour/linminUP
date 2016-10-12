""" Progress bar utilities """
#!/usr/bin/python
# -*- coding: utf-8 -*-

# --------------------------------------------------
# File Name: pbar.py
# Purpose:
# Creation Date: 24-03-2016
# Last Modified: Wed, Oct 12, 2016 11:30:25 AM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2016 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------


import progressbar


def mk_bar(bar_size):
    """ Make a progress bar  """
    return progressbar.ProgressBar(maxval=bar_size,  \
        widgets=[progressbar.Bar('=', '[', ']')  \
            , ' ', progressbar.Percentage()])

