#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: folderDict.py
# Purpose:
# Creation Date: 04-11-2015
# Last Modified: Wed, Jan 25, 2017  2:40:51 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import os
import sys
import xmltodict
import h5py

from progressbar import *
from pbar import *


# ---------------------------------------------------------------------------

def moveFile(args, fast5file):
    print "moveFile(): " + fast5file
    sys.stdout.flush()

    sep = os.path.sep
    suffix = sep+".."+sep+".."+sep+"HDF_fail"+sep
    path_ = os.path.splitext(os.path.dirname(fast5file))[0] + suffix
    if not os.path.exists(path_):
        try:
            os.makedirs(path_)
        except:
            pass # MS 20.09.16

    fname = os.path.splitext(os.path.basename(fast5file))[0]
    fast5file_ = path_ + fname + ".fast5"
    os.rename(fast5file, fast5file_)

    print fast5file + " moved to " + fast5file_

# ---------------------------------------------------------------------------

def getHDFtime(args, f):
    try:
        with h5py.File(f) as hdf:
            
            expStartTime = hdf['UniqueGlobalKey/tracking_id'].attrs['exp_start_time']
            sample_rate = hdf['UniqueGlobalKey/channel_id'].attrs['sampling_rate']

            reads = 'Analyses/EventDetection_000/Reads/'

            for read in hdf[reads]:
                '''
                # 0.64a ..
                startTime = hdf[ reads + read ].attrs['start_time']
                readTime = startTime
                '''
                # -2.64b ...
                # We take "End time" to be Start time of final event  ...
                #endTime = hdf[ reads + read + "/Events"][-1][-2]
                endTime = hdf[reads + read + "/Events"]['start'][-1]
                #print endTime
                readTime = endTime/sample_rate
            #print expStartTime
            timestamp = int(expStartTime) + int(readTime)
            hdf.close()
    except:
        timestamp = -1
    return timestamp



def assignHDFtimes(args, d):

    print "Assigning HDF timestamps..."
    sys.stdout.flush()

    # return dict(map(getHDFtime, d.items()) )
    d_ = dict()
    ks = d.keys()

    bar = mk_bar(len(ks))
    bar.start()

    for i, k in enumerate(ks):
        d_[k] = getHDFtime(args, k)
        bar.update(i)

    bar.finish()

    return d_


# ---------------------------------------------------------------------------

bwa_index_dir = ""

def file_dict_of_folder(args, xml_file_dict, path):
    global bwa_index_dir

    bwa_index_dir = path
    print "bwa_index_dir: ", bwa_index_dir

    file_list_dict = dict()
    ref_list_dict = dict()

    # global xml_file_dict
    # xml_file_dict=dict()

    if os.path.isdir(path):
        print 'Caching existing fast5 files in: %s' % path
        for (path, dirs, files) in os.walk(path):

            for f in files:

              # TODO check w ML  ....

                if 0: # args.standalone is False:
                    if 'downloads' in path and args.preproc is False \
                            or args.preproc is True and \
                            ('downloads' in path or 'uploaded' in path) :
                        if 'muxscan' not in f and f.endswith('.fast5'):
                            file_list_dict[os.path.join(path, f)] = \
                                os.stat(os.path.join(path, f)).st_mtime
                        xml_file_dict = file_dict_of_folder_(args \
                                  , xml_file_dict, path, ref_list_dict, f)

                elif 'downloads' in path and args.preproc is False \
                      or args.preproc is True:
                          if 'muxscan' not in f and f.endswith('.fast5'):
                              file_list_dict[os.path.join(path, f)] = \
                                  os.stat(os.path.join(path, f)).st_mtime
                          xml_file_dict = file_dict_of_folder_(args \
                                      , xml_file_dict, path, ref_list_dict, f)
                else:
                      if 'muxscan' not in f and f.endswith('.fast5'):
                          file_list_dict[os.path.join(path, f)] = \
                              os.stat(os.path.join(path, f)).st_mtime
                      xml_file_dict = file_dict_of_folder_(args \
                                  , xml_file_dict, path, ref_list_dict, f)


    print 'Found %d existing fast5 files to process first.'  % len(file_list_dict)
    if 0 < len(xml_file_dict):
        print 'Found %d XML folders.' % len(xml_file_dict)
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
            print 'Found %d %s xml files.' % (counts[xmltype], xmltype)

    if 0 < len(ref_list_dict):
        print 'Found %d reference fasta folders.' % len(ref_list_dict)

        # print found_ref_note

        for path in ref_list_dict.keys():
            files = ','.join(ref_list_dict[path])
        # process_ref_fasta(args, valid_ref_dir
        #                       , bwa_index_dir
        #                       , bwa_index_dir
        #                      , files, ref_fasta_hash)

    # with open(dbcheckhash["logfile"][dbname],"a") as logfilehandle:
    # ....logfilehandle.write(found_fast5_note+os.linesep)
    # ....logfilehandle.close()



    if args.hdfTimes == True and len(file_list_dict)>0 :
        file_list_dict = assignHDFtimes(args, file_list_dict) # MS v0.64

    return file_list_dict, xml_file_dict




def file_dict_of_folder_(args, xml_file_dict, path, ref_list_dict, f):



    if args.batch_fasta is True:
        if 'reference' in path:
            if f.endswith('.fa') or f.endswith('.fasta'
                    ) or f.endswith('.fna'):
                ref_path = path
                while 'downloads' \
                    not in os.path.split(ref_path)[1]:
                    ref_path = os.path.split(ref_path)[0]
                if ref_path not in ref_list_dict:
                    ref_list_dict[ref_path] = list()
                ref_list_dict[ref_path].append(os.path.join(path, f))

    if 'XML' in path:
        if f.endswith('.xml'):
            xml_path = path
            while 'downloads' \
                not in os.path.split(xml_path)[1]:

                # print xml_path, os.path.split(xml_path), len (os.path.split(xml_path))

                xml_path = os.path.split(xml_path)[0]

                # print "FINAL", xml_path

            try:
                xmlraw = open(os.path.join(path, f), 'r').read()
                xmldict = xmltodict.parse(xmlraw)
                if xml_path not in xml_file_dict:
                    xml_file_dict[xml_path] = dict()
                    '''
                    xml_file_dict[xml_path]['study'] =  dict()
                    xml_file_dict[xml_path]['experiment' ] = dict()
                    xml_file_dict[xml_path]['run'] = dict()
                    xml_file_dict[xml_path]['sample'] = dict()
                    '''
                    for attr in ['study','experiment','run','sample']:
                        xml_file_dict[xml_path][attr] = dict()


                if 'STUDY_SET' in xmldict:
                    # print "STUDY", f

                    target_string = 'STUDY_SET/STUDY'
                    primary_id = xmldict[ target_string + \
                                        'IDENTIFIERS/PRIMARY_ID']
                    # print "STUDY_ID", primary_id

                    title = xmldict[ target_string + \
                                        '/DESCRIPTOR/STUDY_TITLE']
                    # print "TITLE", title

                    abstr = xmldict[ target_string + \
                                        '/DESCRIPTOR/STUDY_ABSTRACT']
                    # print "ABSTRACT", abstr

                    target = xml_file_dict[xml_path]['study' ]
                    if primary_id not in target: target[primary_id] = dict()
                    target[primary_id]['file'] = f
                    target[primary_id]['xml'] = xmlraw
                    target[primary_id]['title'] = title
                    target[primary_id]['abstract'] = abstr
                    target[primary_id]['path'] = path

                if 'EXPERIMENT_SET' in xmldict:
                    # print "EXPERIMENT", f

                    target_string = 'EXPERIMENT_SET/EXPERIMENT/'
                    primary_id = xmldict[ target_string + \
                                        '/IDENTIFIERS/PRIMARY_ID']
                    # print "EXP_ID", primary_id

                    study_id = xmldict[ target_string + \
                                        'STUDY_REF/IDENTIFIERS/PRIMARY_ID']
                    # print "STUDY_ID", study_id

                    sample_id = xmldict[ target_string + \
                        'DESIGN/SAMPLE_DESCRIPTOR/IDENTIFIERS/PRIMARY_ID']
                    # print "SAMPLE_ID", sample_id

                    target = xml_file_dict[xml_path]['experiment']
                    if primary_id not in target: target[primary_id] = dict()
                    target[primary_id]['file'] = f
                    target[primary_id]['xml'] = xmlraw
                    target[primary_id]['sample_id'] = sample_id
                    target[primary_id]['study_id'] = study_id

                    # for a,b in \
                    #   xmldict['EXPERIMENT_SET']['EXPERIMENT'].items():
                    # ....print a,b

                if 'SAMPLE_SET' in xmldict:
                    # print "SAMPLE_SET", f

                    primary_id = xmldict[
                        'SAMPLE_SET/SAMPLE/IDENTIFIERS/PRIMARY_ID']
                    # print "SAMPLE_ID", primary_id

                    target = xml_file_dict[xml_path]['sample' ]
                    if primary_id not in target: target[primary_id] = dict()
                    target[primary_id]['file'] = f
                    target[primary_id]['xml'] = xmlraw

                if 'RUN_SET' in xmldict:
                    # print "RUN", f

                    target_string = 'RUN_SET/RUN/'
                    primary_id = xmldict[ target_string + \
                                'IDENTIFIERS/PRIMARY_ID']
                    exp_id = xmldict[ target_string + \
                                'EXPERIMENT_REF/IDENTIFIERS/PRIMARY_ID']

                    # print "RUN_ID", primary_id

                    target = xml_file_dict[xml_path]['run']
                    if primary_id not in target: target[primary_id] = dict()
                    target[primary_id]['xml'] = xmlraw
                    target[primary_id]['file'] = f
                    target[primary_id]['exp_id'] = exp_id

            except Exception, err:

                err_string = 'Error with XML file: %s : %s' % (f, err)
                print >> sys.stderr, err_string
                #continue
    return xml_file_dict
