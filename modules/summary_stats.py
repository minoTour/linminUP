""" Summary Statistics Utiliites """
#!/usr/bin/python
# -*- coding: utf-8 -*-

# --------------------------------------------------
# File Name: summary_stats.py
# Purpose:
# Creation Date: 09-03-2016
# Last Modified: Wed, Oct 12, 2016 11:30:26 AM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2016 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------


def insert_summary_stats(basenameid, _, dbase):

    """ Inster Summary Statistics into Databse """

    cursor = dbase.cursor()

    tablename = 'summary_stats'

    sql = \
        "INSERT INTO %s (%s,qname,flag)"  \
                            % (tablename, basenameid)

    # print sql
    cursor.execute(sql)
    dbase.commit()

#--------------------------------------------------------------------------------

def update_summary_stats(basenameid, dbname, dbase):
    """ Update Summary Statistics in Database """

    cursor = dbase.cursor()

    qname = 'summary_stats'

    sql = 'UPDATE ' + dbname + '.' + qname.split('.')[-1] \
        + ' SET align=\'1\' WHERE basename_id="%s" ' \
        % basenameid  # ML

    # print sql
    cursor.execute(sql)
    dbase.commit()

#--------------------------------------------------------------------------------



