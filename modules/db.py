#!/usr/bin/python
# -*- coding: utf-8 -*-

# --------------------------------------------------
# File Name: db.py
# Purpose:
# Creation Date: 23-01-2017
# Last Modified: Wed, Jan 25, 2017  2:40:51 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2016 The Author(s) All Rights Reserved
# Credits: 
# --------------------------------------------------

import MySQLdb


def connect(args):
    return MySQLdb.connect(host=args.dbhost, user=args.dbusername,
                         passwd=args.dbpass, port=args.dbport)

def cursor_execute(args,db,cursor,sql):
    try:
      cursor.execute(sql)
      db.commit()
    except (AttributeError, MySQLdb.OperationalError):
      db = connect(args)
      cursor = db.cursor()
      cursor.execute(sql)
      db.commit()
    return args, db, cursor



