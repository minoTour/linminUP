#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: processMinKnowV1.0Fast5.py
# Purpose:
# Creation Date: 2014 - 2015
# Last Modified: Wed, Oct 12, 2016 11:30:26 AM
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
from checkRead import check_read_type, getBasecalltype

from debug import debug

from hdf2SQL import explore

from processFast5Utils import *

# ---------------------------------------------------------------------------
def process_integratedRNN_readtypes(args, read_type, basename, basenameid, basecalldirs, tracking_id_hash, general_hash, read_info_hash, passcheck, hdf, db, dbname, cursor, fastqhash, dbcheckhash):

    events_hash = {}

    # Process Fasta basecall data ....
    if basename is '': return ()

    basecalldir = "/Analyses/Basecall_1D_000/"

    if args.verbose == "high": print "basecalldir", basecalldir

    #fastqhash = dict()

    # tel_sql_list=list()

    readtypes = \
        { 'basecalled_template': basecalldir + 'BaseCalled_template/'           }


    tel_data_hash = dict()
    template_start = 0
    template_end = 0
    g_template_start = 0
    g_template_end = 0


    for (readtype, location) in readtypes.iteritems():
        if args.verbose == "high": print "Readtype", readtype, basecalldir, location
        if location in hdf:
            fastq = hdf[location + 'Fastq'][()]
            try:
                rec = SeqIO.read(StringIO(fastq), 'fastq')
            except Exception, err:
                err_string = \
                    '%s:\tError reading fastq oject from base: %s type: %s error: %s' \
                    % (time.strftime('%Y-%m-%d %H:%M:%S'), basename,
                       readtype, err)
                print >> sys.stderr, err_string
                with open(dbcheckhash['logfile'][dbname], 'a') as \
                    logfilehandle:
                    logfilehandle.write(err_string + os.linesep)
                    logfilehandle.close()
                continue

            sequence = str(rec.seq)
            seqlen = len(sequence)
            rec.id = basename + '.' + readtype

            qual = chr_convert_array(db,
                    rec.letter_annotations['phred_quality'])
            fastqhash[rec.id] = \
                {'quals': rec.letter_annotations['phred_quality'],
                 'seq': sequence}

            sampling_rate = float(tracking_id_hash['sampling_rate'] )
            duration = float(general_hash['duration']) / 60.
            exp_start_time = float(tracking_id_hash['exp_start_time'] )



            if location + 'Alignment' in hdf:  # so its 2D
                # print "we're looking at a 2D read",template_start,"\n\n"

                g_start_time = g_template_start
                g_end_time = g_template_end
                start_time = template_start
                end_time = template_end
                events_hash = {
                        'basename_id': basenameid,
                        'seqid': rec.id,
                        'sequence': sequence,
                        'qual': qual,
                        'seqlen': seqlen,
                        'start_time': template_start,
                        'exp_start_time': tracking_id_hash['exp_start_time' ],
                        'pass': passcheck,
                        'duration': duration, # float(general_hash['duration']),
                        'sampling_rate': sampling_rate
                        }

                events_hash = calc_timing_windows(events_hash, start_time, end_time, g_start_time, g_end_time)

                mysql_load_from_hashes(args, db, cursor, readtype, events_hash)

                '''

                # DEPRECATING TELEM MS 11.10.16 

                if args.telem is True:
                    alignment = hdf[location + 'Alignment'][()]
                    if args.verbose == "high":
                        print "ALIGNMENT", type(alignment)

                    channel = general_hash['channel'][-1]


                    tel_data_hash[readtype] = [basenameid, channel,
                            alignment]

                    # upload_2dalignment_data(basenameid,channel,alignment,db)
                    # tel_sql_list.append(t_sql)
                '''

            complement_and_template_fields = []
            '''
                'basename',
                'seqid',
                'duration',
                'start_time',
                'scale',
                'shift',
                'gross_shift',
                'drift',
                'scale_sd',
                'var_sd',
                'var',
                'sequence',
                'qual',
                ]
            '''

            if location + 'Fastq' in hdf:
            # so its template
                '''
                events_hash = make_hdf5_object_attr_hash(args,
                        hdf[location + 'Events'],
                        complement_and_template_fields)
                if location + 'Model' in hdf:
                    model_hash = make_hdf5_object_attr_hash(args,
                        hdf[location + 'Model'],
                        complement_and_template_fields)
                    events_hash.update(model_hash)
                '''

                # #Logging the start time of a template read to pass to the 2d read in order to speed up mysql processing
                exp_start_time = float(tracking_id_hash['exp_start_time' ])
                duration = float(read_info_hash['duration' ]) / 60.

                start_time = float(read_info_hash['start_time'] ) /sampling_rate

                events_hash.update({
                    'basename_id': basenameid,
                    'seqid': rec.id,
                    'sequence': sequence,
                    'qual': qual,
                    'seqlen': seqlen,
                    'start_time': start_time,
                    'exp_start_time': exp_start_time,
                    'pass': passcheck,
                    'sampling_rate': sampling_rate,
                    'duration': duration,
                    })

                events_hash, timings = get_main_timings(events_hash, location, hdf)

                if readtype == 'basecalled_template':
                    _, template_start, template_end, g_template_start, \
                                                g_template_end = timings



                mysql_load_from_hashes(args, db, cursor, readtype,
                        events_hash)

#--------------------------------------------------------------------------------

def getIntegratedRNNBasenameData(args, read_type, hdf):
    for element in hdf['/Raw/Reads']:

        basecallindexpos=0 # MS 10.10.16

        read_id_fields = [
            'duration',
            'read_number',
            'start_mux',
            'start_time',
            ]

        read_info_hash = make_hdf5_object_attr_hash(args,
                hdf['/Raw/Reads/' + element],
                read_id_fields)

    configdatastring = ''

    x = read_info_hash['read_number']
    string = '/Raw/Reads/Read_%s' % x
    if args.verbose == "high":
        print string
        debug()
    if string in hdf:
        configdatastring = string
        configdata = hdf[configdatastring]

    if args.verbose == "high":
        print configdata
        debug()

    return basecallindexpos, [string,string,string],configdata, read_info_hash


#--------------------------------------------------------------------------------

def process_minKnow_basecalledSummary_data(basecalldir, args, read_type, basename, basenameid, basecalldirs, passcheck, tracking_id_hash, general_hash, hdf, db,cursor):


    basecall_summary_hash = {}

    basecall_summary_fields = []
    '''
    # # get all the basecall summary split hairpin data
        'abasic_dur',
        'abasic_index',
        'abasic_peak',
        'duration_comp',
        'duration_temp',
        'end_index_comp',
        'end_index_temp',
        'hairpin_abasics',
        'hairpin_dur',
        'hairpin_events',
        'hairpin_peak',
        'median_level_comp',
        'median_level_temp',
        'median_sd_comp',
        'median_sd_temp',
        'num_comp',
        'num_events',
        'num_temp',
        'pt_level',
        'range_comp',
        'range_temp',
        'split_index',
        'start_index_comp',
        'start_index_temp',
        ]

    basecall_summary_hash = make_hdf5_object_attr_hash(args,
                                            hdf[basecalldir],
                                            basecall_summary_fields)
    '''

    if basecalldir + '/BaseCalled_template' in hdf:
        hdf5object = hdf[basecalldir + '/BaseCalled_template']

        # print "Got event location"

        for x in (
            'drift',
            'mean_qscore',
            'num_skips',
            'num_stays',
            'scale',
            'scale_sd',
            'sequence_length', ####
            'shift',
            'strand_score',
            'var',
            'var_sd',
            ):
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])
                basecall_summary_hash.update({x + 'T': value})

    # # load basecall summary hash into mysql

    basecall_summary_hash.update({'basename_id': basenameid})
    basecall_summary_hash.update({'pass': passcheck})
    basecall_summary_hash.update({'exp_start_time': tracking_id_hash['exp_start_time' ]})

    basecall_summary_hash = copy_timings(basecall_summary_hash, general_hash)
    if args.verbose is True:
        print basecall_summary_hash
        print general_hash
        debug()

    mysql_load_from_hashes(args, db, cursor, 'basecall_summary',
                           basecall_summary_hash)


