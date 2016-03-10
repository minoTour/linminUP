#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: folderDict.py
# Purpose:
# Creation Date: 04-11-2015
# Last Modified: Wed Mar  9 11:38:05 2016
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import os
import sys
import xmltodict
import h5py

def getHDFtime((f, mtime)):
    try:
	hdf = h5py.File(f)
        expStartTime = hdf['UniqueGlobalKey/tracking_id'].attrs['exp_start_time']
        reads = 'Analyses/EventDetection_000/Reads/'
        for read in hdf[reads]:
                startTime = hdf[ reads + read ].attrs['start_time'] 
	st =  int(expStartTime) + int(startTime)
    except:
	st = mtime
    return f, st 


def assignHDFtimes(d):
	return dict(map(getHDFtime, d.items()) )


# ---------------------------------------------------------------------------

def file_dict_of_folder(args, xml_file_dict, path):

    file_list_dict = dict()
    ref_list_dict = dict()

    # global xml_file_dict
    # xml_file_dict=dict()

    if os.path.isdir(path):
        print 'caching existing fast5 files in: %s' % path
        for (path, dirs, files) in os.walk(path):
            for f in files:
                if 'downloads' in path and args.preproc is False \
                    or args.preproc is True:
                    if 'muxscan' not in f and f.endswith('.fast5'):
                        file_list_dict[os.path.join(path, f)] = \
                            os.stat(os.path.join(path, f)).st_mtime

                    if args.batch_fasta is True:
                        if 'reference' in path:
                            if f.endswith('.fa') or f.endswith('.fasta'
                                    ) or f.endswith('.fna'):
                                ref_path = path
                                while 'downloads' \
                                    not in os.path.split(ref_path)[1]:
                                    ref_path = \
    os.path.split(ref_path)[0]
                                if ref_path not in ref_list_dict:
                                    ref_list_dict[ref_path] = list()
                                ref_list_dict[ref_path].append(os.path.join(path,
                                        f))

                    if 'XML' in path:
                        if f.endswith('.xml'):
                            xml_path = path
                            while 'downloads' \
                                not in os.path.split(xml_path)[1]:

                                # print xml_path, os.path.split(xml_path), len (os.path.split(xml_path))

                                xml_path = os.path.split(xml_path)[0]

                                # print "FINAL", xml_path

                            try:
                                xmlraw = open(os.path.join(path, f), 'r'
                                        ).read()
                                xmldict = xmltodict.parse(xmlraw)
                                if xml_path not in xml_file_dict:
                                    xml_file_dict[xml_path] = dict()
                                    xml_file_dict[xml_path]['study'] = \
    dict()
                                    xml_file_dict[xml_path]['experiment'
        ] = dict()
                                    xml_file_dict[xml_path]['run'] = \
    dict()
                                    xml_file_dict[xml_path]['sample'] = \
    dict()

                                if 'STUDY_SET' in xmldict:

                                    # print "STUDY", f

                                    primary_id = xmldict['STUDY_SET'
        ]['STUDY']['IDENTIFIERS']['PRIMARY_ID']

                                    # print "STUDY_ID", primary_id

                                    title = xmldict['STUDY_SET']['STUDY'
        ]['DESCRIPTOR']['STUDY_TITLE']

                                    # print "TITLE", title

                                    abstr = xmldict['STUDY_SET']['STUDY'
        ]['DESCRIPTOR']['STUDY_ABSTRACT']

                                    # print "ABSTRACT", abstr

                                    if primary_id \
    not in xml_file_dict[xml_path]['study']:
                                        xml_file_dict[xml_path]['study'
        ][primary_id] = dict()
                                    xml_file_dict[xml_path]['study'
        ][primary_id]['file'] = f
                                    xml_file_dict[xml_path]['study'
        ][primary_id]['xml'] = xmlraw
                                    xml_file_dict[xml_path]['study'
        ][primary_id]['title'] = title
                                    xml_file_dict[xml_path]['study'
        ][primary_id]['abstract'] = abstr
                                    xml_file_dict[xml_path]['study'
        ][primary_id]['path'] = path

                                if 'EXPERIMENT_SET' in xmldict:

                                    # print "EXPERIMENT", f

                                    primary_id = \
    xmldict['EXPERIMENT_SET']['EXPERIMENT']['IDENTIFIERS']['PRIMARY_ID']

                                    # print "EXP_ID", primary_id

                                    study_id = xmldict['EXPERIMENT_SET'
        ]['EXPERIMENT']['STUDY_REF']['IDENTIFIERS']['PRIMARY_ID']

                                    # print "STUDY_ID", study_id

                                    sample_id = xmldict['EXPERIMENT_SET'
        ]['EXPERIMENT']['DESIGN']['SAMPLE_DESCRIPTOR']['IDENTIFIERS'
        ]['PRIMARY_ID']

                                    # print "SAMPLE_ID", sample_id

                                    if primary_id \
    not in xml_file_dict[xml_path]['experiment']:
                                        xml_file_dict[xml_path]['experiment'
        ][primary_id] = dict()
                                    xml_file_dict[xml_path]['experiment'
        ][primary_id]['file'] = f
                                    xml_file_dict[xml_path]['experiment'
        ][primary_id]['xml'] = xmlraw
                                    xml_file_dict[xml_path]['experiment'
        ][primary_id]['sample_id'] = sample_id
                                    xml_file_dict[xml_path]['experiment'
        ][primary_id]['study_id'] = study_id

                                    # for a,b in xmldict['EXPERIMENT_SET']['EXPERIMENT'].items():
                                    # ....print a,b

                                if 'SAMPLE_SET' in xmldict:

                                    # print "SAMPLE_SET", f

                                    primary_id = xmldict['SAMPLE_SET'
        ]['SAMPLE']['IDENTIFIERS']['PRIMARY_ID']

                                    # print "SAMPLE_ID", primary_id

                                    if primary_id \
    not in xml_file_dict[xml_path]['sample']:
                                        xml_file_dict[xml_path]['sample'
        ][primary_id] = dict()
                                    xml_file_dict[xml_path]['sample'
        ][primary_id]['file'] = f
                                    xml_file_dict[xml_path]['sample'
        ][primary_id]['xml'] = xmlraw

                                if 'RUN_SET' in xmldict:

                                    # print "RUN", f

                                    primary_id = xmldict['RUN_SET'
        ]['RUN']['IDENTIFIERS']['PRIMARY_ID']
                                    exp_id = xmldict['RUN_SET']['RUN'
        ]['EXPERIMENT_REF']['IDENTIFIERS']['PRIMARY_ID']

                                    # print "RUN_ID", primary_id

                                    if primary_id \
    not in xml_file_dict[xml_path]['run']:
                                        xml_file_dict[xml_path]['run'
        ][primary_id] = dict()
                                    xml_file_dict[xml_path]['run'
        ][primary_id]['xml'] = xmlraw
                                    xml_file_dict[xml_path]['run'
        ][primary_id]['file'] = f
                                    xml_file_dict[xml_path]['run'
        ][primary_id]['exp_id'] = exp_id
                            except Exception, err:

                                err_string = \
                                    'Error with XML file: %s : %s' \
                                    % (f, err)
                                print >> sys.stderr, err_string
                                continue

    print 'found %d existing fast5 files to process first.' \
        % len(file_list_dict)
    if 0 < len(xml_file_dict):
        print 'found %d XML folders.' % len(xml_file_dict)
        counts = dict()
        for xmldir in xml_file_dict.keys():
            for xmltype in xml_file_dict[xmldir].keys():
                if xmltype not in counts:
                    counts[xmltype] = \
                        len(xml_file_dict[xmldir][xmltype])
                else:
                    counts[xmltype] += \
                        len(xml_file_dict[xmldir][xmltype])
        for xmltype in counts:
            print 'found %d %s xml files.' % (counts[xmltype], xmltype)

    if 0 < len(ref_list_dict):
        print 'found %d reference fasta folders.' % len(ref_list_dict)

        # print found_ref_note

        for path in ref_list_dict.keys():
            files = ','.join(ref_list_dict[path])
            process_ref_fasta(args, valid_ref_dir, bwa_index_dir,
                              files, ref_fasta_hash)

    # with open(dbcheckhash["logfile"][dbname],"a") as logfilehandle:
    # ....logfilehandle.write(found_fast5_note+os.linesep)
    # ....logfilehandle.close()

 
    # 0.63 ... # file_list_dict = assignHDFtimes(file_list_dict)

    return (file_list_dict, xml_file_dict)


