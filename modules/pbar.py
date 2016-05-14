#!/usr/bin/python
# -*- coding: utf-8 -*-

# --------------------------------------------------
# File Name: pbar.py
# Purpose:
# Creation Date: 24-03-2016
# Last Modified: Thu Mar 24 16:31:56 2016
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2016 The Author(s) All Rights Reserved
# Credits: 
# --------------------------------------------------


import progressbar
from time import sleep



def mkBar(n):
	return progressbar.ProgressBar(maxval=n, 
    		widgets=[progressbar.Bar('=', '[', ']') 
			, ' ', progressbar.Percentage()])


def pbar(a):
	n = len(a)
	bar = mkBar(n)
	bar.start()
	for i in xrange(n):
   	 	bar.update(i)
	bar.finish()
	return a


#pbar(xrange(2**12))
	

