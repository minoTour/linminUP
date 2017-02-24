#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: align_bwa.py
# Purpose:
# Creation Date: 2014 - 2015
# Last Modified: Wed, Jan 25, 2017  2:40:50 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import re
import sys
import subprocess
import threading

class LocalStore():
    """ Threading example class

    The run() method will be started and it will run in the background
    until the application exits.

    This class will store local information on read counts as used in minoTour.
    Categories will be 1minwin,exp_start_time,bases,maxlen,minlen,average_length,readcount,passcount,cumuduration,cumulength,distchan
    """

    def __init__(self):
        """ Constructor

        :type interval: int
        :param interval: Check interval, in seconds
        """
        print "initialising LocalStore for reads."
        self.storagedict=dict()
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

    def addread(self,readtype):
        if readtype not in self.storagedict():
            self.storagedict[readtype]=dict[]
