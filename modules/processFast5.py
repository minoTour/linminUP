	#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: processFast5.py
# Purpose:
# Creation Date: 2014 - 2015
# Last Modified: Mon Nov 16 16:01:17 2015
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import os
import sys
from Bio import SeqIO
import hashlib
import time
from StringIO import StringIO

from align_bwa import *
from align_lastal import *
from hdf5HashUtils import *

from sql import upload_model_data
from telem import init_tel_threads2
from checkRead import check_read_type


# ---------------------------------------------------------------------------

def chr_convert_array(db, array):
    string = str()
    for val in array:
        string += chr(val + 33)
    return db.escape_string(string)


# ---------------------------------------------------------------------------

# def chr_convert_array(array):
#    string='"'
#    for val in array:
#        string+=chr(val+64)
#    string+='"'
#    return string
# ---------------------------------------------------------------------------

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
    ):

    checksum = hashlib.md5(open(filepath, 'rb').read()).hexdigest()

    # print checksum, type(checksum)
    # ## find the right basecall_2D location, get configuaration genral data, and define the basename.

    """basecalltype = 'Basecall_1D_CDNA'
    basecalltype2 = 'Basecall_2D'
    basecalldir = ''
    basecalldirconfig = ''

    # print "REF", ref_fasta_hash

    for x in range(0, 9):
        string = '/Analyses/%s_00%s/Configuration/general' \
            % (basecalltype, x)
        if string in hdf:
            basecalldir = '/Analyses/%s_00%s/' % (basecalltype, x)
            basecalldirconfig = string
            break
        string = '/Analyses/%s_00%s/Configuration/general' \
            % (basecalltype2, x)
        if string in hdf:
            basecalldir = '/Analyses/%s_00%s/' % (basecalltype2, x)
            basecalldirconfig = string
            break
    """

    file_type = check_read_type(filepath,hdf)
    #print "FILETYPE is", file_type

    if file_type == 2:
        basecalltype="Basecall_1D" #ML
        basecalltype2="Basecall_2D"
        basecalldir=''
        basecalldirconfig=''
        basecallindexpos='' #ML
        string2='' #ML
        for x in range (0,9):
            string2 = '/Analyses/Hairpin_Split_00%s/Configuration/general' % (x) #ML
            if (string2 in hdf):
                basecallindexpos=x #ml
                #print "BASECALLINDEXPOS",basecallindexpos
                basecalldirconfig=string2 #ML

        string='/Analyses/%s_00%s/Configuration/general' % (basecalltype, basecallindexpos)
        #print string
        if (string in hdf):
        #    print "YES 1"
            basecalldir='/Analyses/%s_00%s/' % (basecalltype,basecallindexpos)
            #basecallindexpos=x #ml
            #break

        string='/Analyses/%s_00%s/Configuration/general' % (basecalltype2, basecallindexpos)
        #print string
        if (string2 in hdf):
            #print "YES 2"
            basecalldir='/Analyses/%s_00%s/' % (basecalltype2,basecallindexpos)
            #basecalldirconfig=string2 #ML
            #break
    if file_type == 1:
        basecalltype = 'Basecall_1D_CDNA'
        basecalltype2 = 'Basecall_2D'
        basecalldir = ''
        basecalldirconfig = ''
        basecallindexpos=''
        for x in range(0, 9):
            string = '/Analyses/%s_00%s/Configuration/general' \
                % (basecalltype, x)
            if string in hdf:
                basecalldir = '/Analyses/%s_00%s/' % (basecalltype, x)
                basecalldirconfig = string
                basecallindexpos=x
                break
            string = '/Analyses/%s_00%s/Configuration/general' \
                % (basecalltype2, x)
            if string in hdf:
                basecalldir = '/Analyses/%s_00%s/' % (basecalltype2, x)
                basecalldirconfig = string
                basecallindexpos=x
                break


    configdata = hdf[basecalldirconfig]
    basename = configdata.attrs['basename']  # = PLSP57501_17062014lambda_3216_1_ch101_file10_strand

    # # get all the tracking_id data, make primary entry for basename, and get basenameid

    tracking_id_fields = [
        'basename',
        'asic_id',
        'asic_id_17',
        'asic_id_eeprom',
        'asic_temp',
        'device_id',
        'exp_script_purpose',
        'exp_script_name',
        'exp_start_time',
        'flow_cell_id',
        'heatsink_temp',
        'hostname',
        'run_id',
        'version_name',
        ]
    tracking_id_hash = make_hdf5_object_attr_hash(args,
            hdf['/UniqueGlobalKey/tracking_id'], tracking_id_fields)
    tracking_id_hash.update({'basename': basename,
                            'file_path': filepath, 'md5sum': checksum})
    hdf5object = hdf['/UniqueGlobalKey/channel_id']

        # print "Got event location"

    for x in ('channel_number', 'digitisation', 'offset',
              'sampling_rate'):
        if x in hdf5object.attrs.keys():
            value = str(hdf5object.attrs[x])

            # print x, value

            tracking_id_hash.update({x: value})

    # range is a specifal case:
    # for x in ('range'):
    #    if (x in hdf5object.attrs.keys() ):
    #        value=str(hdf5object.attrs[x])
    #        print x, value
    #        tracking_id_hash.update({'range_val ':value})

    passcheck = 0
    if '/pass/' in filepath:
        passcheck = 1
    if '\\pass\\' in filepath:
        passcheck = 1
    tracking_id_hash.update({'pass': passcheck})
    basenameid = mysql_load_from_hashes(db, cursor, 'tracking_id',
            tracking_id_hash)

    # # get all the data from Configuration/general, then add Event Detection mux pore number

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

    eventdectionreadstring = \
        '/Analyses/EventDetection_000/Reads/Read_%s' \
        % general_hash['read_id']
    if eventdectionreadstring in hdf:
        hdf5object = hdf[eventdectionreadstring]

        # print "Got event location"

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

                # print x, value

                general_hash.update({x: value})

        # Specific to catch read_id as different class:

        for x in 'read_id':
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])

                # print 'read_name', value

                general_hash.update({'read_name': value})

        # Add pass flag to general_hash

        general_hash.update({'pass': passcheck})
        general_hash.update({'exp_start_time': tracking_id_hash['exp_start_time'
                            ]})
        general_hash.update({'1minwin': int(hdf5object.attrs['start_time'
                            ] / float(tracking_id_hash['sampling_rate'
                            ]) / 60)})  # '1minwin':int(template_start/(60))
        general_hash.update({'5minwin': int(hdf5object.attrs['start_time'
                            ] / float(tracking_id_hash['sampling_rate'
                            ]) / 60 / 5)})  # '1minwin':int(template_start/(60))
        general_hash.update({'10minwin': int(hdf5object.attrs['start_time'
                            ] / float(tracking_id_hash['sampling_rate'
                            ]) / 60 / 10)})  # '1minwin':int(template_start/(60))
        general_hash.update({'15minwin': int(hdf5object.attrs['start_time'
                            ] / float(tracking_id_hash['sampling_rate'
                            ]) / 60 / 15)})  # '1minwin':int(template_start/(60))

        # if ('start_mux' in hdf5object.attrs.keys() ):
        #    start_mux=str(hdf5object.attrs['start_mux'])
            # print "start_mux", start_mux
        #    general_hash.update({'start_mux':start_mux})
        # if ('end_mux' in hdf5object.attrs.keys() ):
        #    stop_mux=str(hdf5object.attrs['end_mux'])
            # print "stop_mux", stop_mux
        #    general_hash.update({'end_mux':stop_mux})

    # ## load general_hash into mysql

    mysql_load_from_hashes(db, cursor, 'config_general', general_hash)

    # # get all the basecall summary split hairpin data

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
    if file_type == 1:
        basecall_summary_hash = make_hdf5_object_attr_hash(args,
            hdf[basecalldir + 'Summary/split_hairpin'],
            basecall_summary_fields)
    if file_type == 2:
        basecall_summary_hash=make_hdf5_object_attr_hash(args, hdf['/Analyses/Hairpin_Split_00'+str(basecallindexpos)+'/Summary/split_hairpin'],basecall_summary_fields)
    #print '/Analyses/Hairpin_Split_00'+str(basecallindexpos)+'/Summary/split_hairpin'
    #print basecall_summary_hash
    # # adding info about other the basecalling itself

    if basecalldir + 'Summary/basecall_1d_complement' in hdf:
        hdf5object = hdf[basecalldir + 'Summary/basecall_1d_complement']

        # print "Got event location"

        for x in (
            'drift',
            'mean_qscore',
            'num_skips',
            'num_stays',
            'scale',
            'scale_sd',
            'sequence_length',
            'shift',
            'strand_score',
            'var',
            'var_sd',
            ):
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])

                # print x, value

                basecall_summary_hash.update({x + 'C': value})

    # # adding info about other the basecalling itself

    if basecalldir + 'Summary/basecall_1d_template' in hdf:
        hdf5object = hdf[basecalldir + 'Summary/basecall_1d_template']

        # print "Got event location"

        for x in (
            'drift',
            'mean_qscore',
            'num_skips',
            'num_stays',
            'scale',
            'scale_sd',
            'sequence_length',
            'shift',
            'strand_score',
            'var',
            'var_sd',
            ):
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])

                # print x, value

                basecall_summary_hash.update({x + 'T': value})

    if basecalldir + 'Summary/basecall_2d' in hdf:
        hdf5object = hdf[basecalldir + 'Summary/basecall_2d']

        # print "Got event location"

        for x in ('mean_qscore', 'sequence_length'):
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])

                # print x, value

                basecall_summary_hash.update({x + '2': value})

    # # Adding key indexes and time stamps

    basecall_summary_hash.update({'basename_id': basenameid})
    basecall_summary_hash.update({'pass': passcheck})
    basecall_summary_hash.update({'exp_start_time': tracking_id_hash['exp_start_time'
                                 ]})
    basecall_summary_hash.update({'1minwin': general_hash['1minwin']})
    basecall_summary_hash.update({'5minwin': general_hash['5minwin']})
    basecall_summary_hash.update({'10minwin': general_hash['10minwin']})
    basecall_summary_hash.update({'15minwin': general_hash['15minwin']})

    # print basecall_summary_hash

    # # load basecall summary hash into mysql

    mysql_load_from_hashes(db, cursor, 'basecall_summary',
                           basecall_summary_hash)

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
                'score',
                'design',
                'pos1_end',
                'pos0_end',
                'pos1_start',
                'variant',
                'barcode_arrangement',
                ))
            barcode_hash.update({'basename_id': basenameid})
            mysql_load_from_hashes( db, cursor, 'barcode_assignment',
             barcode_hash)

            # print barcode_hash
            # for bk in barcode_hash.keys():
            #    print bk, barcode_hash[bk], type(barcode_hash[bk])

            break

    # ------------ Do model details -------------------

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

                cursor.execute(sql)
                db.commit()

    # ---------------------------------------------------------------------------
    if file_type == 1:
        readtypes = {'basecalled_template': basecalldir \
                    + 'BaseCalled_template/',
                    'basecalled_complement': basecalldir \
                    + 'BaseCalled_complement/',
                    'basecalled_2d': basecalldir + 'BaseCalled_2D/'}
    if file_type == 2:
        readtypes = {'basecalled_template' : '/Analyses/Basecall_1D_00'+str(basecallindexpos)+"/"+'BaseCalled_template/',
    'basecalled_complement' : '/Analyses/Basecall_1D_00'+str(basecallindexpos)+"/"+'BaseCalled_complement/','basecalled_2d' : '/Analyses/Basecall_2D_00'+str(basecallindexpos)+"/"+'BaseCalled_2D/'} #ML


    fastqhash = dict()

    # tel_sql_list=list()

    tel_data_hash = dict()
    template_start = 0
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

            if location + 'Alignment' in hdf:  # so its 2D

                # print "we're looking at a 2D read",template_start,"\n\n"

                mysql_load_from_hashes(db, cursor, readtype, {
                    'basename_id': basenameid,
                    'seqid': rec.id,
                    'sequence': sequence,
                    'qual': qual,
                    'start_time': template_start,
                    'seqlen': seqlen,
                    'exp_start_time': tracking_id_hash['exp_start_time'
                            ],
                    '1minwin': int(template_start / 60),
                    '5minwin': int(template_start / (5 * 60)),
                    '10minwin': int(template_start / (10 * 60)),
                    '15minwin': int(template_start / (15 * 60)),
                    'pass': passcheck,
                    })
                if args.telem is True:
                    alignment = hdf[location + 'Alignment'][()]

                    # print "ALIGNMENT", type(alignment)

                    channel = general_hash['channel'][-1]
                    tel_data_hash[readtype] = [basenameid, channel,
                            alignment]

                    # upload_2dalignment_data(basenameid,channel,alignment,db)
                    # tel_sql_list.append(t_sql)

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
            if location + 'Events' in hdf and location + 'Model' in hdf:  # so its either template or complement
                events_hash = make_hdf5_object_attr_hash(args,
                        hdf[location + 'Events'],
                        complement_and_template_fields)
                model_hash = make_hdf5_object_attr_hash(args,
                        hdf[location + 'Model'],
                        complement_and_template_fields)

                # #Logging the start time of a template read to pass to the 2d read in order to speed up mysql processing

                if readtype == 'basecalled_template':
                    template_start = events_hash['start_time']
                events_hash.update(model_hash)
                events_hash.update({
                    'basename_id': basenameid,
                    'seqid': rec.id,
                    'sequence': sequence,
                    'qual': qual,
                    'seqlen': seqlen,
                    '1minwin': int(events_hash['start_time'] / 60),
                    '5minwin': int(events_hash['start_time'] / (5
                                   * 60)),
                    '10minwin': int(events_hash['start_time'] / (10
                                    * 60)),
                    '15minwin': int(events_hash['start_time'] / (15
                                    * 60)),
                    })
                events_hash.update({'exp_start_time': tracking_id_hash['exp_start_time'
                                   ], 'pass': passcheck})
                mysql_load_from_hashes(db, cursor, readtype,
                        events_hash)

                # -------- This inserts telemetry data. It is optional under the flags above.
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

                # # We want to calculate the mean current for a read here... how do we do that?
                # eventcounter=0
                # totalcurrent=0
                # meanlist=list()
                # for event in events:
                #    eventcounter+=1
                #    totalcurrent=totalcurrent + event[0]
                #    meanlist.append(event[0])
                # print numpy.median(numpy.array(meanlist))
                # print basenameid, basename,readtype,eventcounter,totalcurrent/eventcounter

    # ---------------------------------------------------------------------------

    if dbname in ref_fasta_hash:  # so we're doing an alignment
        if fastqhash:  # sanity check for the quality scores in the hdf5 file. this will not exist if it's malformed.
            if args.last_align is True:
                if args.verbose is True:
                    print 'last aligning....'
                init_last_threads(oper, args, connection_pool[dbname] \
				, fastqhash, basename, basenameid \
				, dbname, dbcheckhash, ref_fasta_hash)
            if args.bwa_align is True:
                if args.verbose is True:
                    print 'bwa aligning....'
                init_bwa_threads(
                    args,
                    ref_fasta_hash,
                    connection_pool[dbname],
                    fastqhash,
                    basename,
                    basenameid,
                    dbname,
                    )

            # for seqid in fastqhash.keys():    # use this for debugging instead of lines above that use threading
            #    #if ("template" in seqid):
            #    do_last_align(seqid, fastqhash[seqid], basename, basenameid, dbname, db)
            #    do_bwa_align(seqid, fastqhash[seqid], basename, basenameid, dbname, db)

    hdf.close()

    if args.telem is True:
        init_tel_threads2(connection_pool[dbname], tel_data_hash)


# ---------------------------------------------------------------------------
