#!/usr/bin/python
# -*- coding: utf-8 -*-

# --------------------------------------------------
# File Name:
# Purpose:
# Creation Date: 02-08-2016
# Last Modified: Wed, Jan 25, 2017  2:40:52 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2016 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

from hdf5_hash_utils import *
from debug import debug
import sys



#--------------------------------------------------------------------------------

def get_main_timings(hsh, location, hdf):
    #print hsh
    sampling_rate = float(hsh['sampling_rate'])
    exp_start_time = float(hsh['exp_start_time'])
    #print "Here it is as",exp_start_time

    try: read_id = hsh['read_id']
    except: read_id = -1
    #print "Location:", location
    #print "read id:", read_id
    if read_id > -1 and location+"/Events" in hdf:

        events_table = hdf[location + "/Events"]
        starts = map(float, events_table['start']) #/ sampling_rate

        start_time = starts[0] / sampling_rate
        end_time = starts[-1] / sampling_rate
        #print "start time", start_time
        #start_time = float(events_table[0][1]) #/  sampling_rate # 60.
        #etnd_time = float(events_table[-1][-2]) #/ sampling_rate # 60.
    elif read_id > -1 and "Raw/Reads/" in hdf:
        reads = 'Raw/Reads/'
        for read in hdf[reads]:
            #print "OK Lets try this!"
            start_time = int(hdf[reads + read].attrs['start_time']) / sampling_rate
            duration = int(hdf[reads + read].attrs['duration']) / sampling_rate
            end_time = start_time + duration
            #print endTime
            #readTime = endTime/sample_rate
    elif location+"/Events" in hdf:
        events_table = hdf[location + "/Events"]
        starts = map(float, events_table['start']) #/ sampling_rate

        start_time = starts[0] / sampling_rate
        end_time = starts[-1] / sampling_rate
        #print "start time", start_time
    else:
        #print "are we here instead"
        start_time = float(hsh['start_time']) #* sampling_rate
        duration = float(hsh['duration']) #* sampling_rate
        end_time = start_time + duration


    g_start_time = exp_start_time + int(start_time)#  *60)
    g_end_time = exp_start_time + int(end_time)  #*60)

    timings = [exp_start_time, start_time, end_time, \
                g_start_time, g_end_time]

    #print timings # map(frmt, timings)


    hsh = calc_timing_windows(hsh, start_time, end_time \
                       , g_start_time, g_end_time)

    #for a in sorted(hsh.items()): print a

    return hsh, timings


def calc_timing_windows(hsh, start_time, end_time, g_start_time, g_end_time):

    # Scale global times to minutes .....
    start_time = start_time / 60.
    end_time = end_time / 60.
    g_start_time = g_start_time / 60.
    g_end_time = g_end_time / 60.

    hsh.update({
        '1minwin': end_time / 1,
        '5minwin': end_time / 5,
        '10minwin': end_time / 10,
        '15minwin': end_time / 15,

        's1minwin': start_time / 1,
        's5minwin': start_time / 5,
        's10minwin': start_time / 10,
        's15minwin': start_time / 15,

        'g_1minwin': g_end_time / 1,
        'g_5minwin': g_end_time / 5,
        'g_10minwin': g_end_time / 10,
        'g_15minwin': g_end_time / 15,

        'g_s1minwin': g_start_time / 1,
        'g_s5minwin': g_start_time / 5,
        'g_s10minwin': g_start_time / 10,
        'g_s15minwin': g_start_time / 15,
        })

    return hsh



def copy_timings(hsh, general_hash):
    fields = ['1minwin', '5minwin', '10minwin', '15minwin', 's1minwin', 's5minwin', 's10minwin', 's15minwin', 'g_1minwin', 'g_5minwin', 'g_10minwin', 'g_15minwin', 'g_s1minwin', 'g_s5minwin', 'g_s10minwin', 'g_s15minwin']
    for f in fields:
        hsh[f] = general_hash[f]
    return hsh

def chr_convert_array(db, array):
    string = str()
    for val in array:
        string += chr(val + 33)
    return db.escape_string(string)

#--------------------------------------------------------------------------------
'''
def getBasenameData(args, read_type, hdf):
    if rad_type in [1, 2, 3]:
        return getMetricoreBasenameData(args, read_type, hdf)
    elif read_type == 6:
        return getIntegratedRNNBasenameData(args, read_type, hdf)
    if read_type == 4:
        return get_nanonet_basename_data(args, read_type, hdf)
    else:
        print "getBasenameData(): ERROR readtype not setup yet...", read_type
        debug()
        sys.exit()

def get_nanonet_basename_data(args, read_type, hdf):
    for element in hdf['/Raw/Reads']:

        read_id_fields = [
            'duration',
            #'hairpin_found',
            #'hairpin_event_index',
            'read_number',
            #'scaling_used',
            'start_mux',
            'start_time',
            ]

        read_info_hash = make_hdf5_object_attr_hash( \
                args, \
                hdf['/Raw/Reads/' + element], \
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

    return '', [string, string, string], configdata, read_info_hash

'''


#--------------------------------------------------------------------------------
