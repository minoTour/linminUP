#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: processFast5Raw.py
# Purpose:
# Creation Date: 2014 - 2015
# Last Modified: Wed Mar 30 13:37:22 2016
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import os, sys
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
    basename = '.'.join(os.path.split(filepath)[-1].split('.')[:-1]) # MS
		  # =LomanLabz_013731_11rx_v2_3135_1_ch49_file28_strand
    if args.debug is True:
	print "basename", basename

    # # get all the tracking_id data, make primary entry for basename, and get basenameid
    # tracking_id_fields=['basename','asic_id','asic_temp','device_id','exp_script_purpose','exp_start_time','flow_cell_id','heatsink_temp','run_id','version_name',]

    tracking_id_fields = [
        #'basename',
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
 

    if args.debug is True:
    	print '@'*40
    	print "# tracking_id_hash"
    	for x in tracking_id_hash: print x


#--------------------------------------------------------------------------------
    for element in hdf['Analyses/EventDetection_000/Reads']:
	
        read_id_fields = [
            'duration',
            'hairpin_found',
            #'hairpin_event_index',
            'read_number',
            'scaling_used',
            'start_mux',
            'start_time',
            ]

        read_info_hash = make_hdf5_object_attr_hash(args,
                hdf['Analyses/EventDetection_000/Reads/' + element],
                read_id_fields)



        # ............print read_info_hash['hairpin_found']
    	if args.debug is True:
		print "-"*40
		print "# read_info_hash"
		for x in read_info_hash: print x

        if read_info_hash['hairpin_found'] == 1:
            tracking_id_hash.update({'hairpin_found': read_info_hash['hairpin_found'
                                    ]})
        else:
            tracking_id_hash.update({'hairpin_found': '0'})

    	if args.debug is True:
		print "="*40
		print "# read_info_hash"
		for x in read_info_hash: print x

    basenameid = mysql_load_from_hashes(args, db , cursor, 
				'pre_tracking_id', tracking_id_hash)

    rawconfigdatastring = ''

    #for x in range(0, 10000):
    if 1: 
        x = read_info_hash['read_number']
        string = '/Analyses/EventDetection_000/Reads/Read_%s' % x
        if string in hdf:
            rawconfigdatastring = string
            #break
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


	#print "line 138"

        sampling_hash = make_hdf5_object_attr_hash(args,
                hdf['/UniqueGlobalKey/channel_id'], ['sampling_rate'])

    	if args.debug is True:
		print '#'*40
		print "# sampling_hash"
		for x in sampling_hash: print x


        # print sampling_hash['sampling_rate']
        # print type(sampling_hash['sampling_rate'])
        # print type(general_hash['start_time'])

        general_hash.update({'sampling_rate': 
	     sampling_hash['sampling_rate' ]})
        general_hash.update({'start_time': 
	     general_hash['start_time'] / sampling_hash['sampling_rate']})
        general_hash.update({'basename_id': basenameid,
                            'basename': basename,
                            'total_events': 
			len(hdf[rawconfigdatastring + '/Events/'])})

    	if args.debug is True:
		print '\''*40
		print "# General Hash"
		for x in general_hash: print x

	# ------------------------------------------
    eventdectionreadstring = \
       '/Analyses/EventDetection_000/Reads/Read_%s' \
       % general_hash['read_number']

    if args.debug is True: print "read_number", general_hash['read_number']

    #sys.stdout.flush()
    #sys.exit()


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

                #print "###", x, value
		general_hash.update({x: value})
	# print general_hash


        # Specific to catch read_id as different class:
    	if args.debug is True:
		print '\"'*40
		print "line 213#> General Hash"
		for x in general_hash: print x



        for x in 'read_id':
            if x in hdf5object.attrs.keys():
                value = str(hdf5object.attrs[x])

                # print 'read_name', value

                general_hash.update({'read_name': value})

	# ------------------------------------------

    	if args.debug is True:
		print '\"'*40
		print "line 229 ## General Hash"
		for x in general_hash: print x


	exp_start_time = int(tracking_id_hash['exp_start_time' ])
	exp_start_time_f = frmt(exp_start_time)

	if args.debug is True: 
		print "@@ exp start_time: ", exp_start_time_f
        general_hash.update({'exp_start_time': exp_start_time})

	sampling_rate = float(general_hash['sampling_rate']) * 60.
	if args.debug is True: 
		print "@@ sampling_rate: ", sampling_rate 
        	

	start_time = \
	   float(hdf5object.attrs['start_time']) / sampling_rate
	if args.debug is True: 
		print "@@ start_time: ", start_time

	g_start_time = exp_start_time + int(start_time)*60

   	# ------------------------------------------
    	# 0.64b ...
    	# Use End time == Start time of final event ...

	end_time = \
	   float(hdf[ eventdectionreadstring + "/Events"][-1][-2]) \
				/ sampling_rate 
	g_end_time = exp_start_time + int(end_time)*60

	if args.debug is True: 
    		print "@@ start / end times A Line 296: ", start_time, end_time
    		print "@@ g_start / g_end times g_A Line 296: ", frmt(g_start_time), frmt(g_end_time)
        #sys.stdout.flush()
	#sys,exit()

    	template_start = start_time 
    	template_end = end_time 


	
	# Scale global times to minutes .....
	g_start_time = int(g_start_time / 60)
	g_end_time = int(g_end_time / 60)
    	g_template_start = g_start_time 
    	g_template_end = g_end_time 


   	# ------------------------------------------
	
        general_hash.update({'1minwin': int(end_time/ 1.)})  
        general_hash.update({'5minwin': int(end_time/ 5.)})  
        general_hash.update({'10minwin': int(end_time/ 10.)})  
        general_hash.update({'15minwin': int(end_time/ 15.)})
        general_hash.update({'s1minwin': int(start_time/ 1.)})  
        general_hash.update({'s5minwin': int(start_time/ 5.)})  
        general_hash.update({'s10minwin': int(start_time/ 10.)})  
        general_hash.update({'s15minwin': int(start_time/ 15.)})
        general_hash.update({'g_1minwin': int(g_end_time/ 1.)})  
        general_hash.update({'g_5minwin': int(g_end_time/ 5.)})  
        general_hash.update({'g_10minwin': int(g_end_time/ 10.)})  
        general_hash.update({'g_15minwin': int(g_end_time/ 15.)})
        general_hash.update({'g_s1minwin': int(g_start_time/ 1.)})  
        general_hash.update({'g_s5minwin': int(g_start_time/ 5.)})  
        general_hash.update({'g_s10minwin': int(g_start_time/ 10.)})  
        general_hash.update({'g_s15minwin': int(g_start_time/ 15.)})


	'''
	'''
    if args.debug is True:
    	print "~"*40
    	print "line 302 #>> general_hash"
    	for x in general_hash: print x
    	sys.stdout.flush()


    # print general_hash
    # ## load general_hash into mysql

    mysql_load_from_hashes(args, db, cursor, 'pre_config_general',
                           				general_hash)

    # ## Then at this point we just need to go on and do the preliminary alignment...

    hdf.close()
    return basenameid


# ---------------------------------------------------------------------------
