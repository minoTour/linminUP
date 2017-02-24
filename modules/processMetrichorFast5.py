#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: processMetrichorFast5.py
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
from checkRead import getBasecalltype

from debug import debug

from hdf2SQL import explore

from processFast5Utils import *

#--------------------------------------------------------------------------------

def process_metrichor_readtypes(args, read_type, basename, basenameid, basecalldirs, tracking_id_hash, \
            general_hash, read_info_hash, passcheck, hdf, db,dbname,  cursor,fastqhash, dbcheckhash):

    if basecalldirs == []: return  {}

    basecalldir, basecalldir1, basecalldir2 = basecalldirs


    sampling_rate = float(tracking_id_hash['sampling_rate'] )

    if read_type == 1:
        readtypes =  \
        { 'basecalled_template'  : basecalldir + 'BaseCalled_template/'
        , 'basecalled_complement': basecalldir + 'BaseCalled_complement/'
        , 'basecalled_2d'        : basecalldir + 'BaseCalled_2D/'
        }

    if read_type == 3:
        readtypes =  \
        { 'basecalled_template'  : basecalldir + 'BaseCalled_template/'
        , 'basecalled_complement': basecalldir + 'BaseCalled_complement/'
        , 'basecalled_2d'         : basecalldir2 + 'BaseCalled_2D/'
        #, 'basecalled_1d'       : basecalldir + 'BaseCalled_1D/' # MS ??
        }

    if read_type == 2:
        readtypes =  \
        { 'basecalled_template'   : basecalldir1 + 'BaseCalled_template/'
        , 'basecalled_complement' : basecalldir1 +'BaseCalled_complement/'
        , 'basecalled_2d'         : basecalldir2 + 'BaseCalled_2D/'
        }


    if args.verbose == "high": debug()
    #fastqhash = dict()

    # tel_sql_list=list()

    tel_data_hash = dict()
    events_hash = {}

    template_start = 0
    template_end = 0
    g_template_start = 0
    g_template_end = 0

    for (readtype, location) in readtypes.iteritems():
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




            #---------------------------------------------
            # 2D read .....
            if location + 'Alignment' in hdf:  # so its 2D

                if args.verbose is True:
                    print "we're looking at a 2D read\n\n"

                duration = float(events_hash['duration']) / 60.
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
                        'duration': duration,
                        'sampling_rate': sampling_rate
                        }

                events_hash = calc_timing_windows(events_hash, start_time, end_time, g_start_time, g_end_time)

                mysql_load_from_hashes(args, db, cursor,
                                            readtype, events_hash)


                '''
                # DEPRECATING telem MS 11.10.16
                if args.telem is True:
                    alignment = hdf[location + 'Alignment'][()]

                    channel = general_hash['channel'][-1]
                    tel_data_hash[readtype] = [basenameid, channel,
                            alignment]

                    # upload_2dalignment_data(basenameid,channel,alignment,db)
                    # tel_sql_list.append(t_sql)
                '''
            #-------------------------------------------------

            complement_and_template_fields = [
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

            #if location + 'Events' in hdf and location + 'Model' in hdf:
            if location + 'Events' in hdf and location + 'Fastq' in hdf:
            # so its either template or complement
                events_hash = make_hdf5_object_attr_hash(args,
                        hdf[location + 'Events'],
                        complement_and_template_fields)
                if location + 'Model' in hdf:
                    model_hash = make_hdf5_object_attr_hash(args,
                        hdf[location + 'Model'],
                        complement_and_template_fields)
                    events_hash.update(model_hash)

                # #Logging the start time of a template read to pass to the 2d read in order to speed up mysql processing

                exp_start_time = float(tracking_id_hash['exp_start_time' ])
                events_hash.update({
                        'basename_id': basenameid,
                        'seqid': rec.id,
                        'sequence': sequence,
                        'qual': qual,
                        'seqlen': seqlen,
                        #'start_time': start_time,
                        'exp_start_time': exp_start_time,
                        'pass': passcheck,
                        'sampling_rate': sampling_rate
                        })

                #print "events_hash"
                events_hash, timings = get_main_timings(events_hash, location, hdf)

                if readtype == 'basecalled_template':
                    _, template_start, template_end, g_template_start, \
                                                g_template_end = timings


                mysql_load_from_hashes(args, db, cursor,
                                            readtype, events_hash)


                # -------- This inserts telemetry data.
                # It is optional under the flags above.
                # -------- Modified to calculate some means and averages
        # ------- so we are going to do this everytime
                # if (args.telem is True):
                    # print "start telem",  (time.time())-starttime
                    # ## Do Events

                events = hdf[location + 'Events'][()]
                tablechannel = readtype + '_' + general_hash['channel'
                        ][-1]
                tel_data_hash[readtype] = [basenameid, tablechannel,
                        events]


#--------------------------------------------------------------------------------

def getMetrichorBasenameData(args, read_type, hdf):
    basecalltype = getBasecalltype(args, read_type)
    basecalldir = ''
    basecalldir1 = ''
    basecalldir2 = ''
    basecalldirconfig = ''
    basecallindexpos='' #ML

    for x in range(0, 9):
        string = '/Analyses/%s_00%s/Configuration/general' \
            % (basecalltype, x)
        basecalldirconfig = string

        if args.verbose == "high":
            print "read_type is",read_type
            print string,basecalltype


        if string in hdf:
            basecallindexpos=x #ml
            basecalldir = '/Analyses/%s_00%s/' % (basecalltype, x)

            if read_type==1:
                basecalldir1 = '/Analyses/Basecall_2D_00%s/' % \
                                (basecallindexpos)
            else:
                basecalldir1 = '/Analyses/Basecall_1D_00%s/' % \
                                (basecallindexpos)

            basecalldir2 = '/Analyses/Basecall_2D_00%s/' % \
                                (basecallindexpos)
            break

            if args.verbose == "high":
                    print string
                    print basecalldir
                    print basecalldir1
                    print basecalldir2
                    debug()

        ### Hard code fix for old reads... test read type 1
        string = '/Analyses/%s_00%s/Configuration/general' \
            % ("Basecall_2D", x)
        basecalldirconfig = string
        #print "read_type is",read_type
        #print string,basecalltype
        if string in hdf:
            basecallindexpos=x #ml
            basecalldir = '/Analyses/%s_00%s/' % ("Basecall_2D", x)

            #if read_type==1:
            basecalldir1 = '/Analyses/Basecall_2D_00%s/' % \
                                (basecallindexpos)

            basecalldir2 = '/Analyses/Basecall_2D_00%s/' % \
                                (basecallindexpos)
            break

            if args.verbose == "high":
                    print string
                    print basecalldir
                    print basecalldir1
                    print basecalldir2
                    debug()



    try:
        configdata = hdf[basecalldirconfig]
        basename = configdata.attrs['basename']

    except:
        print "process_metrichor_basecalled_fast5(): WARNING: Basecalled file without basecall data... "
        return -1,[],None, {}

    result = basecalldir, basecalldir1, basecalldir2

    return basecallindexpos, result,  configdata , {}



#--------------------------------------------------------------------------------
def process_metrichor_basecalledSummary_data(args, read_type, basename, basenameid, basecalldirs, passcheck, tracking_id_hash, general_hash, hdf, db,cursor, basecallindexpos):
    if basecalldirs==[]: return ()
    basecalldir, basecalldir1, basecalldir2 = basecalldirs

    basecall_summary_fields = [
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


    if read_type == 1:
        basecall_summary_hash = make_hdf5_object_attr_hash(args,
                hdf[basecalldir + 'Summary/split_hairpin'],
                basecall_summary_fields)

    if read_type == 2:
        basecall_summary_hash=make_hdf5_object_attr_hash(args,
                hdf['/Analyses/Hairpin_Split_00'+
                        str(basecallindexpos)+'/Summary/split_hairpin'],
                basecall_summary_fields)

    if read_type == 3: # MS NB Cludge to get it to run ....
        basecall_summary_hash = make_hdf5_object_attr_hash(args,
            hdf[basecalldir], # MS ...  + 'Summary/split_hairpin'],
            basecall_summary_fields)

    # # adding info about other the basecalling itself

    if basecalldir1 + 'Summary/basecall_1d_complement' in hdf:
        hdf5object = hdf[basecalldir1 + 'Summary/basecall_1d_complement']

        if args.verbose is True:
                print "Got event location"
                debug()

        for x in (
            'drift',
            'mean_qschor',
            'num_skips',
            'num_stays',
            'scale',
            'scale_sd',
            'sequence_length',
            'shift',
            'strand_schor',
            'var',
            'var_sd',
            ):
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])

                # print x, value

                basecall_summary_hash.update({x + 'C': value})

    # # adding info about other the basecalling itself

    if basecalldir1 + 'Summary/basecall_1d_template' in hdf:
        hdf5object = hdf[basecalldir1 + 'Summary/basecall_1d_template']

        if args.verbose is True:
                print "Got event location"
                debug()

        for x in (
            'drift',
            'mean_qschor',
            'num_skips',
            'num_stays',
            'scale',
            'scale_sd',
            'sequence_length',
            'shift',
            'strand_schor',
            'var',
            'var_sd',
            ):
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])
                basecall_summary_hash.update({x + 'T': value})

    if basecalldir2 + 'Summary/basecall_2d' in hdf:
        hdf5object = hdf[basecalldir2 + 'Summary/basecall_2d']

        # print "Got event location"

        for x in ('mean_qschor', 'sequence_length'):
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])
                basecall_summary_hash.update({x + '2': value})

    basecall_summary_hash.update({'basename_id': basenameid})
    basecall_summary_hash.update({'pass': passcheck})
    basecall_summary_hash.update({'exp_start_time': tracking_id_hash['exp_start_time' ]})

    basecall_summary_hash = \
            copy_timings(basecall_summary_hash, general_hash)

    # # load basecall summary hash into mysql
    mysql_load_from_hashes(args, db, cursor, 'basecall_summary',
                           basecall_summary_hash)


#--------------------------------------------------------------------------------
def process_metrichor_configGeneral_data(args, configdata, basename, basenameid, basecalldirs, read_type, passcheck, hdf, tracking_id_hash, db, cursor):

    if basecalldirs == []: return  {}
    basecalldir, basecalldir1, basecalldir2 = basecalldirs

    general_fields = [
        'basename',
        'local_folder',
        'workflow_script',
        'workflow_name',
        'read_id',
        'use_local',
        'tag',
        'model_path',
        'complement_model',
        'max_events',
        'input',
        'min_events',
        'config',
        'template_model',
        'channel',
        'metrichor_version',
        'metrichor_time_stamp',
        ]

    general_hash = make_hdf5_object_attr_hash(args, configdata,
            general_fields)


    general_hash.update({'basename_id': basenameid})
    if (len(basecalldir)>0): #ML
        metrichor_info=hdf[basecalldir] #ML
        try: general_hash.update({'metrichor_version':metrichor_info.attrs['chimaera version'], 'metrichor_time_stamp':metrichor_info.attrs['time_stamp']}) #ML
        except: general_hash.update({'metrichor_version':metrichor_info.attrs['version'], 'metrichor_time_stamp':metrichor_info.attrs['time_stamp']}) #ML
    else: #ML
        general_hash.update({'metrichor_version':'N/A', 'metrichor_time_stamp':''}) #ML

    # # get event detection for the read; define mux pore nuber
    location = \
        '/Analyses/EventDetection_000/Reads/Read_%s' \
        % general_hash['read_id']
    if location in hdf:
        hdf5object = hdf[location]


        if args.verbose is True:
            print "Got event location", location
            debug()

        for x in (
            'start_mux',
            'end_mux',
            'abasic_event_index',
            'abasic_found',
            'abasic_peak_height',
            'duration',
            'hairpin_event_index',
            'hairpin_found',
            'hairpin_peak_height',
            'hairpin_polyt_level',
            'median_before',
            'read_number',
            'scaling_used',
            'start_time',
            ):
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])
                general_hash.update({x: value})

        #print "general_hash"
        sampling_rate = float(tracking_id_hash['sampling_rate']) #* 60.


        # Specific to catch read_id as different class:

        for x in 'read_id':
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])
                general_hash.update({'read_name': value})

        general_hash.update({'read_type': getBasecalltype(args, read_type)})

        # Add pass flag to general_hash

        general_hash.update({'pass': passcheck})

        exp_start_time = int(tracking_id_hash['exp_start_time' ])

        general_hash.update({'exp_start_time': exp_start_time})

        general_hash['sampling_rate'] = sampling_rate
        #print 'sampling_rate',  sampling_rate
        start_time = float(hdf5object.attrs['start_time']) #/ sampling_rate
        general_hash['start_time'] = start_time
        #print "start_time", general_hash['start_time']
        try:
            general_hash, _ = get_main_timings(general_hash, location, hdf)
        except:
            print "Problem Metrichor"

    # ## load general_hash into mysql

    mysql_load_from_hashes(args, db, cursor, 'config_general', general_hash)
    return general_hash


#--------------------------------------------------------------------------------
