#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: telem.py
# Purpose:
# Creation Date: 2014 - 2015
# Last Modified: Mon, Nov 09, 2015  3:11:53 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import threading
from sql import upload_telem_data, upload_2dalignment_data


# ---------------------------------------------------------------------------

class tel_threader(threading.Thread):

    def __init__(self, db, sql):
        threading.Thread.__init__(self)
        self.db = db
        self.sql = sql

    def run(self):
        run_insert(self.db, self.sql)


# ---------------------------------------------------------------------------

def run_insert(dbx, sql):
    try:
        cursorx = dbx.cursor()
        cursorx.execute(sql)
        dbx.commit()
        cursorx.close()
    except Exception, err:
        print 'mysql pool failed', err
    return


# ---------------------------------------------------------------------------

class tel_twodalign_threader(threading.Thread):

    def __init__(
        self,
        basenameid,
        channel,
        events,
        db,
        ):
        threading.Thread.__init__(self)
        self.basenameid = basenameid
        self.channel = channel
        self.events = events
        self.db = db

    def run(self):
        upload_2dalignment_data(self.basenameid, self.channel,
                                self.events, self.db)


# ---------------------------------------------------------------------------

class tel_template_comp_threader(threading.Thread):

    def __init__(
        self,
        basenameid,
        tablechannel,
        events,
        db,
        ):
        threading.Thread.__init__(self)
        self.basenameid = basenameid
        self.tablechannel = tablechannel
        self.events = events
        self.db = db

    def run(self):
        upload_telem_data(self.basenameid, self.tablechannel,
                          self.events, self.db)


# ---------------------------------------------------------------------------

def init_tel_threads2(connections, tel_data):
    backgrounds = []
    d = 0
    for read_type in tel_data.keys():

        # if (args.verbose is True):
        #    print "using TEL pool thread", d

        db = connections[d]

        # print "connection: %d, %s"%(d, db)

        if read_type is 'basecalled_2d':
            background = tel_twodalign_threader(tel_data[read_type][0],
                    tel_data[read_type][1], tel_data[read_type][2], db)
        if read_type is 'basecalled_template':
            background = \
                tel_template_comp_threader(tel_data[read_type][0],
                    tel_data[read_type][1], tel_data[read_type][2], db)
        if read_type is 'basecalled_complement':
            background = \
                tel_template_comp_threader(tel_data[read_type][0],
                    tel_data[read_type][1], tel_data[read_type][2], db)
        background.start()
        backgrounds.append(background)
        d += 1
    for background in backgrounds:
        background.join()


# ---------------------------------------------------------------------------

def init_tel_threads(connections, sqls):
    backgrounds = []
    for d in xrange(0, len(sqls)):

        # if (args.verbose is True):
        #    print "using pool thread", d

        db = connections[d]
        sql = sqls[d]

        # print "connection: %d, %s"%(d, db)

        background = tel_threader(db, sql)
        background.start()
        backgrounds.append(background)

    for background in backgrounds:

        background.join()


# ---------------------------------------------------------------------------
