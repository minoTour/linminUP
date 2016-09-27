""" hdf5 hsah utilities """
# --------------------------------------------------
# File Name: hdf5_hash_utils.py
# Purpose:
# Creation Date: 05-11-2015
# Last Modified: Sun, Sep 25, 2016 11:17:18 AM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import time

def frmt(date_time):
    """ Format time stamp """
    return time.strftime('%Y-%m-%d %H:%M:%S', \
                    time.gmtime(int(date_time)))


def mysql_load_from_hashes(
        args,
        dbase,
        cursor,
        tablename,
        data_hash,
    ):
    """ Load hash data into database """
    cols = list()
    vals = list()
    for (_, entry) in data_hash.iteritems():
        if isinstance(entry, basestring):
            vals.append("'%s'" % entry)
        else:
            vals.append(str(entry))
    cols = ','.join(data_hash.keys())
    values = ','.join(vals)
    sql = 'INSERT INTO %s (%s) VALUES (%s) ' % (tablename, cols, values)

    if args.verbose == "high":
        print sql

    cursor.execute(sql)
    dbase.commit()
    ids = cursor.lastrowid
    return ids


# ---------------------------------------------------------------------------

def make_hdf5_object_attr_hash(args, hdf5object, fields):
    """ Make hash from hdf5 object atttributes """
    att_hash = dict()
    for field in fields:
        if field in hdf5object.attrs.keys():

        # print "filed: ",field (args.ref_fasta is not None), hdf5object.attrs[field]

            att_hash[field] = hdf5object.attrs[field]
    if args.verbose == "high":
        print att_hash
    return att_hash


