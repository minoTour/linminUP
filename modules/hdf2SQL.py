#!/usr/bin/python
# -*- coding: utf-8 -*-

# --------------------------------------------------
# File Name: hdf2SQL.py
# Purpose:
# Creation Date: 08-06-2016
# Last Modified: Wed, Jan 25, 2017  2:40:51 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2016 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import numpy as np
import MySQLdb

def initDB(args):
    conn = MySQLdb.connect(\
           host=args.dbhost, \
           user=args.dbusername, \
           passwd=args.dbpass, \
           port=args.dbport)
    cur = conn.cursor()
    return cur, conn

def fixSpaces(f):
    return f.replace(' ', '_')

def toValues((_, b)):
    return "'"+b+"'"


def runSQL(cur, conn, sql):
    print sql
    try:
        cur.execute(sql)
        conn.commit()
        print "... OK"
    except:
        print "... FAIL"
        return cur.fetchall()



'''
def descTable(db, cur, conn, tab):
    print '\t'*0, db
    #runSQL(cur, conn, "USE "+db)
    print '\t'*1, tab
    fs = runSQL(cur, conn, "DESC "+tab)
    for f in fs:
        print "\t"*2, f
'''


def drop_sql(tname):
    sql = "DROP TABLE "+tname
    return sql


# And make a test table....

def mkField((s, (t, n))):
    s = fixSpaces(s)
    if t == 'txtdn':
        field = s+' TEXT DEFAULT NULL'
    if t == 'txtnn':
        field = s+' TEXT NOT NULL'
    if t == 'txt':
        field = s+' TEXT('+str(n)+') NOT NULL'
    if t == 'int':
        field = s+' INT('+str(n)+') NOT NULL'
    if t == 'auto':
        field = s+' INT('+str(n)+') NOT NULL AUTO_INCREMENT, PRIMARY KEY('+s+')'
    if t == 'vc':
        field = s+' VARCHAR('+str(n)+') NOT NULL'
    return field

def create_table_sql(tname, fields):
    fs = map(mkField, fields)
    colheaders = ', '.join(fs)
    sql = "CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=InnoDB" % (tname, colheaders)
    return sql


def insert_sql(tablename, fields):
    values = ", ".join(map(toValues, list(fields)))
    sql = 'INSERT INTO %s VALUES (%s);'% (tablename, values)
    return sql


def select_sql(tname):
    sql = "SELECT * FROM "+tname
    return sql


def setPK(t, f): return "ALTER TABLE "+t+" ADD PRIMARY KEY ("+f+") "


'''
def mkFields(k, v):
    return mkSQL((k, ('txt', len(v))))
'''

def h5_obj_to_table(cur, conn, obj, attr_hash, metadata_sql_list):
    #tname = "h5_" + obj.name.split('/')[-1]
    tname = obj.name.split('/')[-1]
    print tname

    #fs = [ (f, ('txt', 10)) for f in attr_hash.keys() ]
    #fs = [ (k, ('txt', len(v) )) for k, v in attr_hash.items() ]
    fs = [(k, ('txt', len(v))) for k, v in attr_hash.items()]

    #sql = drop_sql(tname)
    #runSQL(cur, conn, sql)
    sql = create_table_sql(tname, fs)
    runSQL(cur, conn, sql)
    #descTable(cur, conn, tname)

    sql = insert_sql(tname, attr_hash.items())
    #for _ in xrange(10):
    if sql not in metadata_sql_list:
        runSQL(cur, conn, sql)
        metadata_sql_list.append(sql)
        print "123 "*5, metadata_sql_list

        sql = select_sql(tname)
        xs = np.array(runSQL(cur, conn, sql))
        print xs

    print "-"*80



def explore(cur, conn, obj, i, metadata_sql_list):
    if i > 0:
        att_hash = {}
        att_hash['basenameid'] = '1'
        for k in obj.attrs.keys():
            att_hash[k] = obj.attrs[k]
        #print ("\t"*i), obj, att_hash
        try:
            h5_obj_to_table(cur, conn, obj, att_hash, metadata_sql_list)
        except:
            pass
    try:
        ks = obj.keys()
        for k in ks:
            #print "\t"*(i+1) +k
            #print '-'*80

            explore(cur, conn, obj[k], i+1, metadata_sql_list)
            #print '='*80

    except:
        pass
