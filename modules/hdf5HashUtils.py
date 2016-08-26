# --------------------------------------------------
# File Name: hdf5HashUtils.py
# Purpose:
# Creation Date: 05-11-2015
# Last Modified: Fri, Aug 26, 2016 12:25:45 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits: 
# --------------------------------------------------

import time

def frmt(tm):
          return time.strftime('%Y-%m-%d %H:%M:%S'
                        , time.gmtime(int(tm)))



def mysql_load_from_hashes(
    args, 
    db,
    cursor,
    tablename,
    data_hash,
    ):
    cols = list()
    vals = list()
    for (colhead, entry) in data_hash.iteritems():
        if isinstance(entry, basestring):
            vals.append("'%s'" % entry)
        else:
            vals.append(str(entry))
    cols = ','.join(data_hash.keys())
    values = ','.join(vals)
    sql = 'INSERT INTO %s (%s) VALUES (%s) ' % (tablename, cols, values)

    if args.verbose == "high": print sql

    cursor.execute(sql)
    db.commit()
    ids = cursor.lastrowid
    return ids


# ---------------------------------------------------------------------------

def make_hdf5_object_attr_hash(args, hdf5object, fields):
    att_hash = dict()
    for field in fields:
        if field in hdf5object.attrs.keys():

            # print "filed: ",field (args.ref_fasta is not None), hdf5object.attrs[field]

            att_hash[field] = hdf5object.attrs[field]
    if args.verbose == "high": print att_hash
    return att_hash


