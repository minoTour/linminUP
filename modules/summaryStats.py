#!/usr/bin/python
# -*- coding: utf-8 -*-

# --------------------------------------------------
# File Name: summaryStats.py
# Purpose:
# Creation Date: 09-03-2016
# Last Modified: Thu Mar 10 10:56:06 2016
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2016 The Author(s) All Rights Reserved
# Credits: 
# --------------------------------------------------


def insertSummaryStats( basenameid, dbname, db):

    	cursor = db.cursor()

	tablename = 'summaryStats'

	sql = \
	    "INSERT INTO %s (basename_id,qname,flag)" \ 
				% (tablename, basenameid)

	# print sql
	cursor.execute(sql)
	db.commit()

#--------------------------------------------------------------------------------

def updateSummaryStats( basenameid, dbname, db):

    	cursor = db.cursor()

	tablename = 'summaryStats'

	sql = 'UPDATE ' + dbname + '.' + qname.split('.')[-1] \
	    + ' SET align=\'1\' WHERE basename_id="%s" ' \
	    % basenameid  # ML

	# print sql
	cursor.execute(sql)
	db.commit()

#--------------------------------------------------------------------------------



