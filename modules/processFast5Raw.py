#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: processFast5Raw.py
# Purpose:
# Creation Date: 2014 - 2015
# Last Modified: Thu, Nov 05, 2015  4:02:54 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import os
import hashlib

from hdf5HashUtils import *

#from align_dtw import *


def process_fast5_raw(
    db,
    args,
    filepath,
    hdf,
    dbname,
    cursor,
    ):
    checksum = hashlib.md5(open(filepath, 'rb').read()).hexdigest()
    basename = os.path.split(filepath)[-1].split('.')[0]  # =LomanLabz_013731_11rx_v2_3135_1_ch49_file28_strand

    # # get all the tracking_id data, make primary entry for basename, and get basenameid
    # tracking_id_fields=['basename','asic_id','asic_temp','device_id','exp_script_purpose','exp_start_time','flow_cell_id','heatsink_temp','run_id','version_name',]

    tracking_id_fields = [
        'basename',
        'asic_temp',
        'device_id',
        'exp_script_purpose',
        'exp_start_time',
        'flow_cell_id',
        'heatsink_temp',
        'run_id',
        'version_name',
        ]
    tracking_id_hash = make_hdf5_object_attr_hash(args,
            hdf['/UniqueGlobalKey/tracking_id'], tracking_id_fields)
    tracking_id_hash.update({'basename': basename,
                            'file_path': filepath, 'md5sum': checksum})
    for element in hdf['Analyses/EventDetection_000/Reads']:
        read_id_fields = [
            'duration',
            'hairpin_found',
            'hairpin_event_index',
            'read_number',
            'scaling_used',
            'start_mux',
            'start_time',
            ]
        read_info_hash = make_hdf5_object_attr_hash(args,
                hdf['Analyses/EventDetection_000/Reads/' + element],
                read_id_fields)

        # ............print read_info_hash['hairpin_found']

        if read_info_hash['hairpin_found'] == 1:
            tracking_id_hash.update({'hairpin_found': read_info_hash['hairpin_found'
                                    ]})
        else:
            tracking_id_hash.update({'hairpin_found': '0'})
    basenameid = mysql_load_from_hashes(db, cursor, 'pre_tracking_id',
            tracking_id_hash)

    rawconfigdatastring = ''
    for x in range(0, 10000):
        string = '/Analyses/EventDetection_000/Reads/Read_%s' % x
        if string in hdf:
            rawconfigdatastring = string
            break
    rawconfigdata = hdf[rawconfigdatastring]

    # print rawconfigdata

    if len(rawconfigdata) > 0:
        general_fields = [
            'abasic_event_index',
            'abasic_found',
            'abasic_peak_height',
            'duration',
            'hairpin_event_index',
            'hairpin_found',
            'hairpin_peak_height',
            'hairpin_polyt_level',
            'median_before',
            'read_id',
            'read_number',
            'scaling_used',
            'start_mux',
            'start_time',
            ]

        # general_fields=['abasic_event_index']

        general_hash = make_hdf5_object_attr_hash(args, rawconfigdata,
                general_fields)

        # print general_hash
        # print hdf[rawconfigdatastring+'/Events/']
        # print len(hdf[rawconfigdatastring+'/Events/'])
        # for element in hdf[rawconfigdatastring+'/Events/']:
        # ....print element

        sample_rate = ['sampling_rate']
        sample_hash = make_hdf5_object_attr_hash(args,
                hdf['/UniqueGlobalKey/channel_id'], sample_rate)

        # print sample_hash['sampling_rate']
        # print type(sample_hash['sampling_rate'])
        # print type(general_hash['start_time'])

        general_hash.update({'sample_rate': sample_hash['sampling_rate'
                            ]})
        general_hash.update({'start_time': general_hash['start_time']
                            / sample_hash['sampling_rate']})
        general_hash.update({'basename_id': basenameid,
                            'basename': basename,
                            'total_events': len(hdf[rawconfigdatastring
                            + '/Events/'])})

    # print general_hash
    # ## load general_hash into mysql

    mysql_load_from_hashes(db, cursor, 'pre_config_general',
                           general_hash)

    # ## Then at this point we just need to go on and do the preliminary alignment...

    return basenameid
    hdf.close()


# ---------------------------------------------------------------------------
