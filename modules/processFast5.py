#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: processFast5.py
# Purpose:
# Creation Date: 2014 - 2015
# Last Modified: Wed, Jan 25, 2017  2:40:52 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import os
import sys
import time
#import datetime
from Bio import SeqIO
import hashlib
from StringIO import StringIO

from align_bwa import *
#from align_lastal import *
from hdf5_hash_utils import *

from sql import upload_model_data
#from telem import init_tel_threads2
from checkRead import check_read_type, getBasecalltype,testtime

from debug import debug

from hdf2SQL import explore

import processNanonetFast5 as nanonet
import processMetrichorFast5 as metrichor
import processMinKnowV1_0Fast5 as minknow

from processFast5Utils import *

#--------------------------------------------------------------------------------

def do_alignments(read_type, args, oper, dbname, dbcheckhash, ref_fasta_hash,fastqhash,bwaclassrunner,connection_pool,basename,basenameid):
   if args.verbose == "high": print "!!!!! Trying to align"
   #if read_type == "Basecall_1D":
   if 1:

    if dbname in ref_fasta_hash:  # so we're doing an alignment
        if fastqhash:
        #if 1:
        # sanity check for the quality schors in the hdf5 file.
        # this will not exist if it's malformed.
            '''

            # DEPRECATIN LAST MS 11.10.16

            if args.last_align is True:
                if args.verbose == "high":
                    print 'LAST aligning....'
                init_last_threads(oper, args, connection_pool[dbname] \
                                , fastqhash, basename, basenameid \
                                , dbname, dbcheckhash, ref_fasta_hash)
                if args.verbose == "high":
                    print '... finished last aligning.'
            '''
            if args.bwa_align is True:
                if args.verbose == "high":
                    print 'BWA aligning....'
                bwaclassrunner.setjob(args,
                   ref_fasta_hash,
                   connection_pool[dbname],
                   fastqhash,
                   basename,
                   basenameid,
                   dbname,
                   )
                if args.verbose == "high":
                    print '... finished BWA aligning.'

# --------------------------------------------------------------------------
def process_readtypes(args, read_type, basename, basenameid, basecalldirs, tracking_id_hash, general_hash, read_info_hash, passcheck, hdf, db, cursor,fastqhash, dbname, dbcheckhash):


    if read_type == 4:
        return nanonet.process_nanonet_readtypes(args, read_type, basename, basenameid, basecalldirs, tracking_id_hash, general_hash, read_info_hash, passcheck, hdf, db, dbname, cursor, fastqhash, dbcheckhash)
    if read_type in [1,2,3]:
        return metrichor.process_metrichor_readtypes(args, read_type, basename, basenameid, basecalldirs, tracking_id_hash, general_hash, read_info_hash, passcheck, hdf, db, dbname, cursor,fastqhash, dbcheckhash)
    if read_type == 6:
        return minknow.process_integratedRNN_readtypes(args, read_type, basename, basenameid, basecalldirs, tracking_id_hash, general_hash, read_info_hash, passcheck, hdf, db, dbname, cursor, fastqhash, dbcheckhash)




#-------------------------------------------------------------------------------

def process_model_data(args, basecalldirs, basenameid, hdf, db, cursor, dbname, dbcheckhash):

    if basecalldirs==[]: return ()
    basecalldir, basecalldir1, basecalldir2 = basecalldirs

    # ------------ Do model details -------------------

    '''

    # DEPRECATING TELEM MS 11.10.16

    if args.telem is True:
        if dbname not in dbcheckhash['modelcheck']:
            dbcheckhash['modelcheck'][dbname] = dict()

        log_string = basecalldir + 'Log'
        if log_string in hdf:
            log_data = str(hdf[log_string][()])

            # print type(log), log

            lines = log_data.split('\n')
            template_model = None
            complement_model = None
            for l in lines:
                t = re.match('.*Selected model: "(.*template.*)".', l)
                if t:
                    template_model = t.group(1)
                c = re.match('.*Selected model: "(.*complement.*)".', l)
                if c:
                    complement_model = c.group(1)

            if template_model is not None:
                sql = \
                    "INSERT INTO %s (basename_id,template_model,complement_model) VALUES ('%s','%s',NULL)" \
                    % ('model_list', basenameid, template_model)
                if template_model not in dbcheckhash['modelcheck'
                        ][dbname]:
                    location = basecalldir + 'BaseCalled_template/Model'
                    if location in hdf:
                        upload_model_data('model_data', template_model,
                                location, hdf, cursor, db)
                        dbcheckhash['modelcheck'
                                    ][dbname][template_model] = 1

                if complement_model is not None:
                    sql = \
                        "INSERT INTO %s (basename_id,template_model,complement_model) VALUES ('%s','%s','%s')" \
                        % ('model_list', basenameid, template_model,
                           complement_model)
                    if complement_model not in dbcheckhash['modelcheck'
                            ][dbname]:
                        location = basecalldir \
                            + 'BaseCalled_complement/Model'
                        if location in hdf:
                            upload_model_data('model_data',
                                    complement_model, location, hdf,
                                    cursor, db)
                            dbcheckhash['modelcheck'
                                    ][dbname][complement_model] = 1
                if args.verbose == "high": print sql; debug()

                cursor.execute(sql)
                db.commit()
    '''

#-------------------------------------------------------------------------------
def process_barcode_data(args, hdf, db, cursor,basenameid):

    # # see if there is any barcoding info to addd

    barcode_hash = dict()
    for x in range(0, 9):
        string = '/Analyses/Barcoding_00%s/Summary/barcoding' % x

        # print string

        if string in hdf:

            # print "barcode", string

            barcode_hash = make_hdf5_object_attr_hash(args,
                    hdf[string], (
                'pos0_start',
                'schor',
                'design',
                'pos1_end',
                'pos0_end',
                'pos1_start',
                'variant',
                'barcode_arrangement',
                ))
            barcode_hash.update({'basename_id': basenameid})
            mysql_load_from_hashes(args,  db, cursor, 'barcode_assignment',
             barcode_hash)


            # print barcode_hash
            # for bk in barcode_hash.keys():
            #    print bk, barcode_hash[bk], type(barcode_hash[bk])

            break

#-------------------------------------------------------------------------------

def getBasenameData(args, read_type, hdf):
    if read_type in [1,2,3]:
        return metrichor.getMetrichorBasenameData(args, read_type, hdf)
    elif read_type == 6:
        return minknow.getIntegratedRNNBasenameData(args, read_type, hdf)
    elif read_type == 4:
        return nanonet.get_nanonet_basename_data(args, read_type, hdf)
    else:
        print "getBasenameData(): ERROR readtype not setup yet..." , read_type
        debug()
        sys.exit()

#--------------------------------------------------------------------------------
# Process Basecll Summary Data ... Basecall Summary hash

def process_basecalledSummary_data(args, read_type, basename, basenameid, basecalldirs, basecallindexpos, passcheck, tracking_id_hash, general_hash, hdf, db,cursor):

    if read_type in [1,2,3]:
       return metrichor.process_metrichor_basecalledSummary_data(args, read_type, basename, basenameid, basecalldirs, passcheck, tracking_id_hash, general_hash, hdf, db,cursor, basecallindexpos)
    elif read_type in [4]:
       basecalldir = "/Analyses/Basecall_RNN_1D_000/"
       return nanonet.process_nanonet_basecalledSummary_data(basecalldir, args, read_type, basename, basenameid, basecalldirs, passcheck, tracking_id_hash, general_hash, hdf, db,cursor)
    elif read_type in [6]:
       basecalldir = "/Analyses/Basecall_1D_000/"
       return minknow.process_minKnow_basecalledSummary_data(basecalldir, args, read_type, basename, basenameid, basecalldirs, passcheck, tracking_id_hash, general_hash, hdf, db,cursor)
    else:
        print "process_basecalledSummary_data(): ERROR readtype not setup yet..." , read_type
        debug()
        sys.exit()

#--------------------------------------------------------------------------------

def process_tracking_data(args, filepath, basename, checksum, passcheck, hdf, db, cursor):
    tracking_id_fields = [
        'basename',
        'asic_id',
        'asic_id_17',
        'asic_id_eeprom',
        'asic_temp',
        'device_id',
        'exp_script_purpose',
        'exp_script_name',
        #'exp_start_time',
        'flow_cell_id',
        'heatsink_temp',
        'hostname',
        'run_id',
        'version_name',
        ]
    tracking_id_hash = make_hdf5_object_attr_hash(args,
            hdf['/UniqueGlobalKey/tracking_id'], tracking_id_fields)
    expStartTime = testtime(hdf['UniqueGlobalKey/tracking_id'].attrs['exp_start_time'])
    tracking_id_hash.update({'basename': basename,
                            'file_path': filepath, 'md5sum': checksum, 'exp_start_time':expStartTime})
    hdf5object = hdf['/UniqueGlobalKey/channel_id']

    for x in ('channel_number', 'digitisation', 'offset',
              'sampling_rate'):
        if x in hdf5object.attrs.keys():
            value = str(hdf5object.attrs[x])

            # print x, value

            tracking_id_hash.update({x: value})

    if args.verbose == "high":
        print '@'*40
        print "# tracking_id_hash"
        for x in tracking_id_hash: print x


    tracking_id_hash.update({'pass': passcheck})
    #print "db is:",db
    if args.verbose == "high": debug()
    basenameid = mysql_load_from_hashes(args, db, cursor, 'tracking_id',
            tracking_id_hash)
    return basenameid, tracking_id_hash


#-------------------------------------------------------------------------------

def process_configGeneral_data(args, configdata, basename, basenameid, basecalldirs, read_type, passcheck, hdf, tracking_id_hash, db, cursor):
    #print read_type
    if read_type in [1,2,3]:
         return metrichor.process_metrichor_configGeneral_data(args, configdata, basename, basenameid, basecalldirs, read_type, passcheck, hdf, tracking_id_hash, db, cursor)
    elif read_type in [4,6]:
         return nanonet.process_nanonet_configGeneral_data(args, configdata, basename, basenameid, basecalldirs, read_type, passcheck, hdf, tracking_id_hash, db, cursor)
    else:
        print "process_configGeneral_data(): ERROR readtype not setup yet..." , read_type
        debug()
        sys.exit()


#-------------------------------------------------------------------------------

def process_fast5(
    oper,
    db,
    connection_pool,
    args,
    ref_fasta_hash,
    dbcheckhash,
    filepath,
    hdf,
    dbname,
    cursor,
    bwaclassrunner,
    metadata_sql_list,
    ):

    read_type = check_read_type(args, filepath,hdf)

    passcheck = 0
    if '/pass/' in filepath:    passcheck = 1
    if '\\pass\\' in filepath:  passcheck = 1
    if '/skip/' in filepath:    passcheck = -1
    if '\\skip\\' in filepath:  passcheck = -1

    # MS ... Auto create tables from hdf file groups ....
    if args.verbose is True: print "Importing HDF to mySQL...."

    # Only process the minoTour_meta Group for now ....
    try:
        hdf5object = hdf['minoTour_meta/']
        explore(cursor, connection_pool, hdf5object,  0, metadata_sql_list) # !!
        print "... done."
    except:
        pass


    if read_type == 6:
        if args.verbose == "high":
            print "minKNOW v1.0 File...."
            debug()

        process_minKNOW_v1_0_fast5(
                read_type, passcheck, oper, db, connection_pool, args, ref_fasta_hash,
                dbcheckhash, filepath, hdf, dbname, cursor,bwaclassrunner
                )

    if read_type == 4:
        if args.verbose == "high":
            print "Nanonet File...."

        process_nanonet_fast5(
                read_type, passcheck, oper, db, connection_pool, args, ref_fasta_hash,
                dbcheckhash, filepath, hdf, dbname, cursor,bwaclassrunner
                )


    if read_type in [1,2,3]:
        if args.verbose == "high":
            print "Metrichor File...."
            debug()
        process_metrichor_basecalled_fast5(
                read_type, passcheck, oper, db, connection_pool, args, ref_fasta_hash,
                dbcheckhash, filepath, hdf, dbname, cursor, bwaclassrunner
                )
        #processFast5( read_type, passcheck, oper, db, connection_pool, args, ref_fasta_hash, dbcheckhash, filepath, hdf, dbname, cursor, bwaclassrunner)

    hdf.close()


#-------------------------------------------------------------------------------



def process_nanonet_fast5(
        read_type,
        passcheck,
        oper,
        db,
        connection_pool,
        args,
        ref_fasta_hash,
        dbcheckhash,
        filepath,
        hdf,
        dbname,
        cursor,
        bwaclassrunner,
    ):
    fastqhash = dict()
    general_hash = {}

    basename = '.'.join(os.path.split(filepath)[-1].split('.')[:-1]) # MS
    if args.verbose == "high":
        print "basename", basename
        debug()

    # GET BASENAME AND CONFIGDATA ...
    basecallindexpos, basecalldirs, configdata, read_info_hash  = \
                getBasenameData(args, read_type, hdf)


    # PROCESS TRACKING ID DATA ...
    # # get all the tracking_id data, make primary entry for basename, and get basenameid
    checksum = hashlib.md5(open(filepath, 'rb').read()).hexdigest()
    basenameid, tracking_id_hash = \
        process_tracking_data(args, filepath, basename, \
        checksum,  passcheck, hdf, db, cursor)

    # PROCESS CONFIG GENERAL DATA ....
    # # get all the data from Configuration/general, then add Event Detection mux pore number
    general_hash = \
        process_configGeneral_data(args, configdata, basename, \
            basenameid, basecalldirs, read_type,  passcheck, \
            hdf, tracking_id_hash, db, cursor)



    # PROCESS BASECALLED SUMMARY DATA ....
    # # get all the basecall summary split hairpin data
    if basename != '':
        process_basecalledSummary_data(args, read_type
            , basename, basenameid , basecalldirs , basecallindexpos, passcheck, tracking_id_hash, general_hash, hdf, db,cursor)

    # PROCESS BARCODE DATA ...
    process_barcode_data(args, hdf, db, cursor,basenameid)

    # PROCESS MODEL DATA  ...
    process_model_data(args, basecalldirs, basenameid, hdf, db, cursor, dbname, dbcheckhash)

    # PROCESS READ TYPES ....
    #process_readtypes(args, read_type, basename, basenameid, basecalldirs, tracking_id_hash, general_hash, read_info_hash, passcheck, hdf, db, cursor)
    process_readtypes(args, read_type, basename, basenameid, basecalldirs, tracking_id_hash, general_hash, read_info_hash, passcheck, hdf, db, cursor,fastqhash, dbname, dbcheckhash)

    # DO ALIGNMENTS ....
    #do_alignments(read_type, args, dbname, ref_fasta_hash)
    do_alignments(read_type, args, oper, dbname, dbcheckhash, ref_fasta_hash,fastqhash,bwaclassrunner,connection_pool,basename,basenameid)

    # PROCESS TELEM DATA ....
    tel_data_hash = {}
    '''
    # DEPRECATING TELEM MS 11.10.16
    if args.telem is True:
        init_tel_threads2(connection_pool[dbname], tel_data_hash)
    '''

#-------------------------------------------------------------------------------


def process_metrichor_basecalled_fast5(
        read_type,
        passcheck,
        oper,
        db,
        connection_pool,
        args,
        ref_fasta_hash,
        dbcheckhash,
        filepath,
        hdf,
        dbname,
        cursor,
        bwaclassrunner
    ):

    fastqhash = dict()

    try: checksum = hashlib.md5(open(filepath, 'rb').read()).hexdigest()
    except:
        err_string = "process_metrichor_fast5(): error checksum ", filepath
        print >> sys.stderr, err_string
        sys.exit()

    #--------------------------------------------------------------------------------
    # ## find the right basecall_2D location, get configuaration genral data, and define the basename.

    # GET BASENAME AND CONFIGDATA ...
    basename = '.'.join(os.path.split(filepath)[-1].split('.')[:-1]) # MS

    basecallindexpos, basecalldirs, configdata, read_info_hash =  \
        getBasenameData(args, read_type, hdf)


    # PROCESS TRACKING ID DATA ...
    # # get all the tracking_id data, make primary entry for basename, and get basenameid
    basenameid, tracking_id_hash = \
        process_tracking_data(args, filepath, basename, \
        checksum,  passcheck, hdf, db, cursor)

    # PROCESS CONFIG GENERAL DATA ....
    # # get all the data from Configuration/general, then add Event Detection mux pore number
    general_hash = \
        process_configGeneral_data(args, configdata, basename, \
        basenameid, basecalldirs, read_type,  passcheck, hdf,
        tracking_id_hash, db, cursor)

    # PROCESS BASECALLED SUMMARY DATA ....
    # # get all the basecall summary split hairpin data
    if basename != '':
        process_basecalledSummary_data(args, read_type
            , basename, basenameid , basecalldirs , basecallindexpos, passcheck, tracking_id_hash, general_hash, hdf, db,cursor)

    # PROCESS BARCODE DATA ...
    process_barcode_data(args, hdf, db, cursor,basenameid)

    # PROCESS MODEL DATA  ...
    process_model_data(args, basecalldirs, basenameid, hdf, db, cursor, dbname, dbcheckhash)

    # PROCESS READ TYPES ....
    process_readtypes(args, read_type, basename, basenameid, basecalldirs, tracking_id_hash, general_hash, read_info_hash, passcheck, hdf, db, cursor,fastqhash, dbname, dbcheckhash)

    # DO ALIGNMENTS ....
    do_alignments(read_type, args, oper, dbname, dbcheckhash, ref_fasta_hash,fastqhash,bwaclassrunner,connection_pool,basename,basenameid)

    # PROCESS TELEM DATA ....
    tel_data_hash = {}
    '''
    # DEPRECATING TELEM MS 11.10.16
    if args.telem is True:
        init_tel_threads2(connection_pool[dbname], tel_data_hash)
    '''

#--------------------------------------------------------------------------------


def  process_minKNOW_v1_0_fast5( \
        read_type,
        passcheck,
        oper,
        db,
        connection_pool,
        args,
        ref_fasta_hash,
        dbcheckhash,
        filepath,
        hdf,
        dbname,
        cursor,
        bwaclassrunner
    ):

    fastqhash = dict()
    basename = '.'.join(os.path.split(filepath)[-1].split('.')[:-1]) # MS
    if args.verbose == "high":
        print "basename", basename
        debug()

    # GET BASENAME AND CONFIGDATA ...
    basecallindexpos, basecalldirs, configdata, read_info_hash = \
                getBasenameData(args, read_type, hdf)

    # PROCESS TRACKING ID DATA ...
    # # get all the tracking_id data, make primary entry for basename, and get basenameid
    checksum = hashlib.md5(open(filepath, 'rb').read()).hexdigest()
    basenameid, tracking_id_hash = \
        process_tracking_data(args, filepath, basename, \
        checksum,  passcheck, hdf, db, cursor)


    # PROCESS CONFIG GENERAL DATA ....
    # # get all the data from Configuration/general, then add Event Detection mux pore number
    general_hash = \
        process_configGeneral_data(args, configdata, basename, \
            basenameid, basecalldirs, read_type,  passcheck, \
            hdf, tracking_id_hash, db, cursor)

    # PROCESS BASECALLED SUMMARY DATA ....
    # # get all the basecall summary split hairpin data
    if basename != '':
        process_basecalledSummary_data(args, read_type , basename, basenameid ,
                basecalldirs , basecallindexpos, passcheck, tracking_id_hash, general_hash, hdf, db,cursor)

    # PROCESS BARCODE DATA ...
    process_barcode_data(args, hdf, db, cursor,basenameid)

    # PROCESS MODEL DATA  ...
    process_model_data(args, basecalldirs, basenameid, hdf, db, cursor, dbname, dbcheckhash)

    # PROCESS READ TYPES ....
    #process_readtypes(args, read_type, basename, basenameid, basecalldirs, tracking_id_hash, general_hash, read_info_hash, passcheck, hdf, db, cursor)
    process_readtypes(args, read_type, basename, basenameid, basecalldirs,
                tracking_id_hash, general_hash, read_info_hash, passcheck, hdf, db, cursor,fastqhash, dbname, dbcheckhash)

    # DO ALIGNMENTS ....
    #do_alignments(read_type, args, dbname, ref_fasta_hash)
    do_alignments(read_type, args, oper, dbname, dbcheckhash, ref_fasta_hash,fastqhash,bwaclassrunner,connection_pool,basename,basenameid)

    '''

    # DEPRECATING TELEM MS 11.10.16

    # PROCESS TELEM DATA ....
    tel_data_hash = {}
    if args.telem is True:
        init_tel_threads2(connection_pool[dbname], tel_data_hash)
    '''

#-------------------------------------------------------------------------------
