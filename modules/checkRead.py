#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: checkRead.py
# Purpose:
# Creation Date: 04-11-2015
# Last Modified: Tue Mar 29 15:35:06 2016
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import os
import sys
import psutil
import subprocess
import re
import time
import datetime


from sql import *
from hdf5HashUtils import *
from exitGracefully import terminateMinup, terminateSubProcesses
#from processFast5 import getBasecalltype
from progressbar import *
from pbar import *

def getBasecalltype(filetype):
    if filetype == 0: basecalltype = 'raw'
    if filetype == 1: basecalltype = 'Basecall_2D'
    if filetype == 2: basecalltype= "Hairpin_Split"
    if filetype == 3: basecalltype = 'Basecall_1D'
    # print "hdf basecalledtype:", basecalltype
    sys.stdout.flush()
    return basecalltype

# Unbuffered IO
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)

'''

def okSQLname(s): # MS
	return "." not in s and "-" not in s

	if okSQLname(args.custom_name): # MS
	else:
           print >> sys.stderr, \
           'Error: Invalid SQL name characters in custom_name -- exiting ...'
           terminateSubProcesses(args, dbcheckhash, minup_version)
	   sys.exit(1)
'''


def check_read(
    db,
    args,
    connection_pool,
    minup_version,
    comments,
    xml_file_dict,
    ref_fasta_hash,
    dbcheckhash,
    logfolder,
    filepath,
    hdf,
    cursor,
    oper,
    ):

    global runindex

    if args.debug is True:
    	print "Checking read ..."
    	sys.stdout.flush()

    filename = os.path.basename(filepath)
    if args.debug is True:
        print time.strftime('%Y-%m-%d %H:%M:%S'), 'processing:', \
            filename
	sys.stdout.flush()
    parts = filename.split('_')
    str = '_'

    # Changing the number below enables the removal of the random four digit number from run names on restart

    dbname = str.join(parts[0:len(parts) - 5])
    dbname = re.sub('[.!,; ]', '', dbname)
    if len(args.custom_name) > 0:
           dbname = args.minotourusername + '_' + args.custom_name + '_' \
            + dbname
    else:
        dbname = args.minotourusername + '_' + dbname
    if len(dbname) > 64:
        dbname = dbname[:64]
    if dbname.endswith('_'): #ml
        dbname = dbname[:-1] #ml




    # print "dbname is ",dbname
    # print "Parts were " ,parts

    # ---------------------------------------------------------------------------

    if dbname in dbcheckhash['dbname']:  # so data from this run has been seen before in this instance of minup so switch to it!
        if dbcheckhash['dbname'][dbname] is False:
            if args.verbose is True:
                print 'switching to database: ', dbname
		sys.stdout.flush()
            sql = 'USE %s' % dbname
            cursor.execute(sql)

            # ---------------------------------------------------------------------------

            try: runindex = dbcheckhash['runindex'][dbname] # MS ..
            except:
                print "checkRead(): line 112, dbcheckhash, key error: " \
				+ dbname
		sys.stdout.flush()
                #sys.exit()
		return ()


            comment_string = 'minUp switched runname'
            start_time = time.strftime('%Y-%m-%d %H:%M:%S')
            sql = \
                "INSERT INTO Gru.comments (runindex,runname,user_name,comment,name,date) VALUES (%s,'%s','%s','%s','%s','%s')" \
                % (
                runindex,
                dbname,
                args.minotourusername,
                comment_string,
                args.minotourusername,
                start_time,
                )

            # print sql

            db.escape_string(sql)
            cursor.execute(sql)
            db.commit()

            # ---------------------------------------------------------------------------


	    ks = dbcheckhash['dbname'].keys()
	    n = len(ks)
	    bar = mkBar(n)
	    bar.start()
	    for i, e in enumerate(ks):
		bar.update(i)
		dbcheckhash['dbname'][e] = False
	    bar.finish()
	    dbcheckhash['dbname'][dbname] = True


    # ---------------------------------------------------------------------------

    if dbname not in dbcheckhash['dbname']:  # # so the db has not been seen before.. time to set up lots of things...
        dbcheckhash['barcoded'][dbname] = False
        dbcheckhash['barcode_info'][dbname] = False
        dbcheckhash['logfile'][dbname] = os.path.join(os.path.sep,
                logfolder, dbname + '.minup.log')
        if args.verbose is True:
            print 'trying database: ', dbname
	    sys.stdout.flush()
        sql = "SHOW DATABASES LIKE \'%s\'" % dbname

        # print sql

        cursor.execute(sql)
        if cursor.fetchone():
            if args.verbose is True:
                print 'database exists!'
		sys.stdout.flush()

            # # drop the existing database, if selected

            if args.drop_db is True:
                sql = 'DROP DATABASE %s' % dbname

                # print sql

                cursor.execute(sql)
                db.commit()
                if args.verbose is True:
                    print 'database dropped.'
		    sys.stdout.flush()
            else:
                print >> sys.stderr, "="*80
                print >> sys.stderr, \
                    'WARNING: DATABASE \"%s\" already EXISTS.\nTo write over the data re-run the minUP command with option -d' % dbname
                print >> sys.stderr, "="*80
		sys.stdout.flush()
                if args.batch_fasta == False:

                                  # MS next 6 lines ...

                    print >> sys.stderr, \
                        'not in batch mode so exiting ...'
		    sys.stdout.flush()
                    terminateMinup(args, dbcheckhash, oper, minup_version)

        if args.drop_db is True:
            print 'Deleting exisiting run from Gru now ...'
	    sys.stdout.flush()
            sql = \
                'DELETE FROM Gru.userrun WHERE runindex IN (SELECT runindex FROM Gru.minIONruns WHERE runname = "%s")' \
                % dbname

            # print sql

            cursor.execute(sql)
            db.commit()
            sql = "DELETE FROM Gru.minIONruns WHERE runname = \'%s\'" \
                % dbname

            # print sql

            cursor.execute(sql)
            db.commit()
            print '.... Run deleted.'
	    sys.stdout.flush()

        # -------- mincontrol --------
        # # get the IP address of the host

        ip = '127.0.0.1'
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception, err:
            err_string = 'Error obtaining upload IP adress'
            #print >> sys.stderr, err_string
            print err_string

        # ---------------------------------------------------------------------------
        # -------- This bit adds columns to Gru.minIONruns --------

        modify_gru(cursor)

        # ---------------------------------------------------------------------------

        # -------- Create a new empty database

        #if args.verbose is True:
        print 'Making new database: ', dbname
	sys.stdout.flush()

        sql = 'CREATE DATABASE %s' % dbname
        cursor.execute(sql)
        sql = 'USE %s' % dbname
        cursor.execute(sql)

	# Create Tables ....
        create_general_table('config_general', cursor)
        create_trackingid_table('tracking_id', cursor)
        create_basecall_summary_info('basecall_summary', cursor)
        create_events_model_fastq_table('basecalled_template', cursor)
        create_events_model_fastq_table('basecalled_complement', cursor)
        create_basecalled2d_fastq_table('basecalled_2d', cursor)

        # ---------------------------------------------------------------------------

        if args.telem is True:
            for i in xrange(0, 10):
                temptable = 'caller_basecalled_template_%d' % i
                comptable = 'caller_basecalled_complement_%d' % i
                twod_aligntable = 'caller_basecalled_2d_alignment_%d' \
                    % i
                create_caller_table_noindex(temptable, cursor)
                create_caller_table_noindex(comptable, cursor)
                create_2d_alignment_table(twod_aligntable, cursor)
            create_model_list_table('model_list', cursor)
            create_model_data_table('model_data', cursor)

        # ---------------------------------------------------------------------------
        if args.preproc is True:
                create_pretrackingid_table('pre_tracking_id', cursor)  # make another table
                create_pre_general_table('pre_config_general', cursor)  # pre config general table

        # -------- Assign the correct reference fasta for this dbname if applicable

        if args.batch_fasta is not False:
            for refbasename in ref_fasta_hash.keys():
                common_path = \
                    os.path.commonprefix((ref_fasta_hash[refbasename]['path'
                        ], filepath)).rstrip('\\|\/|re|\\re|\/re')
                if common_path.endswith('downloads'):
                    ref_fasta_hash[dbname] = ref_fasta_hash[refbasename]

                    # del ref_fasta_hash[refbasename]

        if args.ref_fasta is not False:
            for refbasename in ref_fasta_hash.keys():  # there should only be one key
                ref_fasta_hash[dbname] = ref_fasta_hash[refbasename]

        # ---------------------------------------------------------------------------

        if dbname in ref_fasta_hash:  # great, we assigned the reference fasta to this dbname
            create_reference_table('reference_seq_info', cursor)
            create_5_3_prime_align_tables('last_align_basecalled_template'
                    , cursor)
            create_5_3_prime_align_tables('last_align_basecalled_complement'
                    , cursor)
            create_5_3_prime_align_tables('last_align_basecalled_2d',
                    cursor)


            if args.last_align is True:

                # create_align_table('last_align_basecalled_template', cursor)
                # create_align_table('last_align_basecalled_complement', cursor)
                # create_align_table('last_align_basecalled_2d', cursor)

                create_align_table_maf('last_align_maf_basecalled_template'
                        , cursor)
                create_align_table_maf('last_align_maf_basecalled_complement'
                        , cursor)
                create_align_table_maf('last_align_maf_basecalled_2d',
                        cursor)

            if args.bwa_align is True:
                create_align_table_sam('align_sam_basecalled_template',
                        cursor)
                create_align_table_sam('align_sam_basecalled_complement'
                        , cursor)
                create_align_table_sam('align_sam_basecalled_2d',
                        cursor)

            # dbcheckhash["mafoutdict"][dbname]=open(dbname+"."+process+".align.maf","w")

            if args.telem is True:
                create_ref_kmer_table('ref_sequence_kmer', cursor)


            if args.prealign is True:
                create_pre_align_table('pre_align_template', cursor)
                create_pre_align_table('pre_align_complement', cursor)
                create_pre_align_table('pre_align_2d', cursor)
                create_align_table_raw('last_align_raw_template',
                        cursor)
                create_align_table_raw('last_align_raw_complement',
                        cursor)
                create_align_table_raw('last_align_raw_2d', cursor)

            for refname in ref_fasta_hash[dbname]['seq_len'].iterkeys():

                # print "refname", refname

                reference = ref_fasta_hash[dbname]['seq_file'][refname]
                reflen = ref_fasta_hash[dbname]['seq_len'][refname]
                reflength = ref_fasta_hash[dbname]['seq_file_len'
                        ][reference]
                refid = mysql_load_from_hashes(args,db, cursor,
                        'reference_seq_info', {
                    'refname': refname,
                    'reflen': reflen,
                    'reffile': reference,
                    'ref_total_len': reflength,
                    })
                ref_fasta_hash[dbname]['refid'][refname] = refid
                if args.telem is True:
                    kmers = ref_fasta_hash[dbname]['kmer'][refname]
                    load_ref_kmer_hash(db, 'ref_sequence_kmer', kmers,
                            refid, cursor)

        # ---------------------------------------------------------------------------
        # -------- See if theres any ENA XML stuff to add.
        # -------- Need to do this now as it changes the "comment"
        # -------- in Gru.minionRuns entry
        # print "C", comment

        ena_flowcell_owner = None
        for xml_to_downloads_path in xml_file_dict.keys():

            # xmlpath=xml_file_dict["study"][study_id]["path"]

            common_path = os.path.commonprefix((xml_to_downloads_path,
                    filepath)).rstrip('\\|\/|re')
            if common_path.endswith('downloads'):
                print 'found XML data for:', dbname
		sys.stdout.flush()
                create_xml_table('XML', cursor)

                # ---------------------------------------------------------------------------
		downloadsPath = xml_file_dict[xml_to_downloads_path]

                for study_id in \
                    downloadsPath['study'].keys():
                    ena_flowcell_owner = study_id
                    study_xml = \
                        downloadsPath['study'][study_id]['xml']
                    study_file = \
                        downloadsPath['study'][study_id]['file']
                    study_title = \
                        downloadsPath['study'][study_id]['title']
                    study_abstract = \
                        downloadsPath['study'][study_id]['abstract']
                    exp_c = 'NA'
                    samp_c = 'NA'
                    run_c = 'NA'
                    mysql_load_from_hashes(args,db, cursor, 'XML', {
                        'type': 'study',
                        'primary_id': study_id,
                        'filename': study_file,
                        'xml': study_xml,
                        })
                    for exp_id in \
                        downloadsPath['experiment'].keys():
                        if study_id \
                            == downloadsPath['experiment'][exp_id]['study_id']:
                            exp_c = exp_id
                            exp_xml = \
                                downloadsPath['experiment'][exp_id]['xml']
                            exp_file = \
                                downloadsPath['experiment'][exp_id]['file']
                            sample_id = \
                                downloadsPath['experiment'][exp_id]['sample_id']
                            mysql_load_from_hashes(args,db, cursor, 'XML', {
                                'type': 'experiment',
                                'primary_id': exp_id,
                                'filename': exp_file,
                                'xml': exp_xml,
                                })

                            if sample_id \
                                in downloadsPath['sample'
                                    ]:
                                samp_c = sample_id
                                sample_xml = \
                                    downloadsPath['sample'][sample_id]['xml']
                                sample_file = \
                                    downloadsPath['sample'][sample_id]['file']
                                mysql_load_from_hashes(args,db, cursor, 'XML'
                                        , {
                                    'type': 'sample',
                                    'primary_id': sample_id,
                                    'filename': sample_file,
                                    'xml': sample_xml,
                                    })

                            for run_id in \
                                downloadsPath['run'].keys():
                                if exp_id \
                                    == downloadsPath['run'][run_id]['exp_id']:
                                    run_c = run_id
                                    run_xml = \
				    	downloadsPath['run'][run_id]['xml']
                                    run_file = \
    					downloadsPath['run'][run_id]['file']
                                    mysql_load_from_hashes(args,db, cursor,
        'XML', {
                                        'type': 'run',
                                        'primary_id': run_id,
                                        'filename': run_file,
                                        'xml': run_xml,
                                        })
                    comments[dbname] = \
                        'ENA data. Study:%s Title: %s Abstract: %s Experiment:%s Sample:%s Run:%s' \
                        % (
                        study_id,
                        study_title,
                        study_abstract,
                        exp_c,
                        samp_c,
                        run_c,
                        )

        # ---------------------------------------------------------------------------
        # --------- Make entries in the Gru database
        # try and get the right basecall-configuration general


        file_type = check_read_type(filepath,hdf)
        #print "FILETYPE is", file_type

        basecalltype=getBasecalltype(file_type) # MS
        basecalldir=''
        basecalldirconfig=''
        basecallindexpos='' #ML

	'''
	try:
         if file_type == 2:
            basecalltype2="Basecall_2D"
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
	except:
		print "checkReads(): error line 467."
		sys.exit()
        try:
          if file_type in [1,0]:
            basecalltype = 'Basecall_1D_CDNA'
            basecalltype2 = 'Basecall_2D'
            basecalldir = ''
            basecalldirconfig = ''
            basecallindexpos=''
	'''
	try: # MS
            for x in range(0, 9):
                string = '/Analyses/%s_00%s/Configuration/general' \
                    % (basecalltype, x)
                if string in hdf:
                    basecalldir = '/Analyses/%s_00%s/' % (basecalltype, x)
                    basecalldirconfig = string
                    basecallindexpos=x
                    break
		'''
                string = '/Analyses/%s_00%s/Configuration/general' \
                    % (basecalltype2, x)
                if string in hdf:
                    basecalldir = '/Analyses/%s_00%s/' % (basecalltype, x)
                    basecalldirconfig = string
                    basecallindexpos=x
                    break
		'''


        # print "basecalldirconfig", basecalldirconfig
        # # get some data out of tacking_id and general
	except:
		print "checkReads(): error line 496."
		sys.stdout.flush()
		sys.exit()

	#print basecalldirconfig
        #print basecalldir
        if len(basecalldirconfig) > 0:
            configdata = hdf[basecalldirconfig]
            if len(basecalldir) > 0:
                metrichor_info = hdf[basecalldir]

        # else:
        # ....configdata.attrs['workflow_name'] ="preanalysed"

        trackingid = hdf['/UniqueGlobalKey/tracking_id']

        expstarttimecode = \
            datetime.datetime.fromtimestamp(int(trackingid.attrs['exp_start_time'
                ])).strftime('%Y-%m-%d')
        flowcellid = trackingid.attrs['device_id']

        if len(basecalldirconfig) > 0:
            basecalleralg = configdata.attrs['workflow_name']
        else:
            basecalleralg = 'preanalysed'
        if len(basecalldir) > 0:
            #version = metrichor_info.attrs['chimaera version']
            try: version = metrichor_info.attrs['chimaera version'] # MS
            except: version = metrichor_info.attrs['version'] # MS
        else:
            version = 'unknown'
        runnumber = args.run_num
        flowcellowner = 'NULL'
        username = args.minotourusername
        if args.flowcell_owner is not None:
            flowcellowner = args.flowcell_owner
        if ena_flowcell_owner is not None:
            flowcellowner = ena_flowcell_owner

        # # get info on the reference sequence, if used

        big_reference = 'NOREFERENCE'
        big_reflength = '0'
        if dbname in ref_fasta_hash:  # so there's some reference data for this dbname
            big_reference = ref_fasta_hash[dbname]['big_name']
            big_reflength = ref_fasta_hash[dbname]['big_len']

        # # make entries into Gru for this new database

        comment = comments['default']
        if dbname in comments:
            comment = comments[dbname]

        process = 'noalign'
        if args.last_align is True:
            process = 'LAST'
        if args.bwa_align is True:
            process = 'BWA'

        wdir = args.watchdir
        if wdir.endswith('\\'):  # remove trailing slash for windows.
            wdir = wdir[:-1]
        sql = \
            "INSERT INTO Gru.minIONruns (date,user_name,flowcellid,runname,activeflag,comment,FlowCellOwner,RunNumber,reference,reflength,basecalleralg,version,minup_version,process,mt_ctrl_flag,watch_dir,host_ip) VALUES ('%s','%s','%s','%s',%s,'%s','%s',%s,'%s',%s,'%s','%s','%s','%s',%s,'%s','%s')" \
            % (
            expstarttimecode,
            args.minotourusername,
            flowcellid,
            dbname,
            1,
            comment,
            flowcellowner,
            runnumber,
            big_reference,
            big_reflength,
            basecalleralg,
            version,
            minup_version,
            process,
            1,
            wdir,
            ip,
            )

        #print sql

        #if args.verbose is True:
	print '... Database created.'
	sys.stdout.flush()

        db.escape_string(sql)
        cursor.execute(sql)
        db.commit()
        runindex = cursor.lastrowid
        dbcheckhash['runindex'][dbname] = runindex

        #print "Runindex:",runindex

        # # add us">> ", view_users

        if args.verbose is True:
            print "Adding users..."
	    sys.stdout.flush()

        view_users=[username]

        if args.view_users:
            extra_names = args.view_users.split(',')
            # view_users = args.view_users + extra_names # MS
            view_users = view_users + extra_names # MS

        for user_name in view_users:
            sql = \
                "SELECT user_id FROM Gru.users WHERE user_name =\'%s\'" \
                % user_name

            # print sql

            cursor.execute(sql)
            if 0 < cursor.rowcount:
                sql = \
                    'INSERT INTO Gru.userrun (user_id, runindex) VALUES ((SELECT user_id FROM Gru.users WHERE user_name =\'%s\') , (SELECT runindex FROM Gru.minIONruns WHERE runname = "%s") )' \
                    % (user_name, dbname)

                # print sql

                cursor.execute(sql)
                db.commit()
            else:
                print 'The MinoTour username "%s" does not exist. Please create it or remove it from the input arguments' \
                    % user_name
		sys.stdout.flush()
                sys.exit()

        # # Create comment table if it doesn't exist

        create_comment_table_if_not_exists('Gru.comments', cursor)

        # # Add first comment to table

        start_time = time.strftime('%Y-%m-%d %H:%M:%S')
        comment_string = 'minUp version %s started' % minup_version
        mysql_load_from_hashes(args,db, cursor, 'Gru.comments', {
            'runindex': runindex,
            'runname': dbname,
            'user_name': args.minotourusername,
            'comment': comment_string,
            'name': args.dbusername,
            'date': start_time,
            })

        # ---------------------------------------------------------------------------
        # --------- make log file and initinal entry

        with open(dbcheckhash['logfile'][dbname], 'w') as logfilehandle:
            logfilehandle.write('minup started at:\t%s%s'
                                % (start_time, os.linesep))
            logfilehandle.write('minup version:\t%s%s'
                                % (minup_version, os.linesep))
            logfilehandle.write('options:' + os.linesep)
            logfilehandle.write('minotour db host:\t%s%s'
                                % (args.dbhost, os.linesep))
            logfilehandle.write('minotour db user:\t%s%s'
                                % (args.dbusername, os.linesep))
            logfilehandle.write('minotour username:\t%s%s'
                                % (args.minotourusername, os.linesep))
            logfilehandle.write('minotour viewer usernames:\t%s%s'
                                % (view_users, os.linesep))
            logfilehandle.write('flowcell owner:\t%s%s'
                                % (flowcellowner, os.linesep))
            logfilehandle.write('run number:\t%s%s' % (args.run_num,
                                os.linesep))
            logfilehandle.write('watch directory:\t%s%s'
                                % (args.watchdir, os.linesep))
            logfilehandle.write('upload telemetry:\t%s%s'
                                % (args.telem, os.linesep))
            logfilehandle.write('Reference Sequences:' + os.linesep)
            if dbname in ref_fasta_hash:
                for refname in ref_fasta_hash[dbname]['seq_len'].iterkeys():
                    logfilehandle.write('Fasta:\t%s\tlength:\t%d%s'
                            % (ref_fasta_hash[dbname]['seq_file'
                            ][refname], ref_fasta_hash[dbname]['seq_len'
                            ][refname], os.linesep))
            else:
                logfilehandle.write('No reference sequence set'
                                    + os.linesep)

            logfilehandle.write('comment:\t%s%s' % (comment,
                                os.linesep))
            logfilehandle.write('Errors:' + os.linesep)
            logfilehandle.close()
        if args.pin is not False:
            if args.verbose is True:
                print 'starting mincontrol'
	        sys.stdout.flush()
            control_ip = ip
            if args.ip_address is not False:
                control_ip = args.ip_address

            # print "IP", control_ip
            # else the IP is the address of this machine

            create_mincontrol_interaction_table('interaction', cursor)
            create_mincontrol_messages_table('messages', cursor)
            create_mincontrol_barcode_control_table('barcode_control',
                    cursor)

            try:
		# MS to be tested ...
                terminateSubProcesses(args, dbcheckhash, oper, minup_version)
                if oper is 'linux':
                    cmd = \
                        'python mincontrol.py -dbh %s -dbu %s -dbp %d -pw %s -db %s -pin %s -ip %s' \
                        % (
                        args.dbhost,
                        args.dbusername,
                        args.dbport,
                        args.dbpass,
                        dbname,
                        args.pin,
                        control_ip,
                        )

                    # print "CMD", cmd

                    subprocess.Popen(cmd, stdout=None, stderr=None,
                            stdin=None, shell=True)
                if oper is 'windows':
                    cmd = \
                        'mincontrol.exe -dbh %s -dbu %s -dbp %d -pw %s -db %s -pin %s -ip %s' \
                        % (
                        args.dbhost,
                        args.dbusername,
                        args.dbport,
                        args.dbpass,
                        dbname,
                        args.pin,
                        control_ip,
                        )

                    # print "CMD", cmd

                    subprocess.Popen(cmd, stdout=None, stderr=None,
                            stdin=None, shell=True)  # , creationflags=subprocess.CREATE_NEW_CONSOLE)
            except Exception, err:
                err_string = 'Error starting mincontrol: %s ' % err
                print >> sys.stderr, err_string
	        sys.stdout.flush()
                with open(dbcheckhash['logfile'][dbname], 'a') as \
                    logfilehandle:
                    logfilehandle.write(err_string + os.linesep)
                    logfilehandle.close()

        # # connection_pool for this db

        connection_pool[dbname] = list()
        if args.last_align is True or args.bwa_align is True \
            or args.telem is True:
            try:
                db_a = MySQLdb.connect(host=args.dbhost,
                        user=args.dbusername, passwd=args.dbpass,
                        port=args.dbport, db=dbname)
                connection_pool[dbname].append(db_a)
                db_b = MySQLdb.connect(host=args.dbhost,
                        user=args.dbusername, passwd=args.dbpass,
                        port=args.dbport, db=dbname)
                connection_pool[dbname].append(db_b)
                db_c = MySQLdb.connect(host=args.dbhost,
                        user=args.dbusername, passwd=args.dbpass,
                        port=args.dbport, db=dbname)
                connection_pool[dbname].append(db_c)
            except Exception, err:

                print >> sys.stderr, \
                    "Can't setup MySQL connection pool: %s" % err
	        sys.stdout.flush()
                with open(dbcheckhash['logfile'][dbname], 'a') as \
                    logfilehandle:
                    logfilehandle.write(err_string + os.linesep)
                    logfilehandle.close()
		sys.stdout.flush()
                sys.exit()

        # --------- this bit last to set the active database in this hash

        if dbcheckhash['dbname']:
            for e in dbcheckhash['dbname'].keys():
                dbcheckhash['dbname'][e] = False
        dbcheckhash['dbname'][dbname] = True

    # check if this is barcoded. Have to check with every read when: dbcheckhash["barcoded"][dbname]=False
    # if its a barcoded read.

    if dbcheckhash['barcoded'][dbname] is False:  # this will test the first read of this database to see if its a barcoded run
        barcoded = False
        for x in range(0, 9):
            string = '/Analyses/Barcoding_00%s' % x

            # print string

            if string in hdf:
                barcoded = True
                barcode_align_obj = string + '/Barcoding/Aligns'
                break
        if barcoded is True:
            create_barcode_table('barcode_assignment', cursor)  # and create the table
            dbcheckhash['barcoded'][dbname] = True


    if dbcheckhash['barcode_info'][dbname] is False and args.pin \
        is not False:
        barcode_align_obj = False
        for x in range(0, 9):
            string = '/Analyses/Barcoding_00%s/Barcoding/Aligns' % x
            if string in hdf:
                barcode_align_obj = string
                break
        if barcode_align_obj is not False:
            barcode_align_obj = hdf[barcode_align_obj][()]
            bcs = list()
            for i in range(len(barcode_align_obj)):
                if barcode_align_obj[i].startswith('>'):
                    bc = re.split('>| ', barcode_align_obj[i])[-1]
                    b = "('%s',0)" % bc
                    bcs.append(b)
            sql = \
                'INSERT INTO barcode_control (barcodeid,complete) VALUES %s' \
                % ','.join(bcs)

            # print sql

            cursor.execute(sql)
            db.commit()
            dbcheckhash['barcode_info'][dbname] = True

    return dbname

# ---------------------------------------------------------------------------

def check_read_type(filepath, hdf):
    filetype = 0
    if 'Analyses/Basecall_1D_000/' in hdf:	 filetype = 3
    if 'Analyses/Basecall_2D_000/' in hdf:	 filetype = 1
    if 'Analyses/Hairpin_Split_000/' in hdf:	 filetype = 2
    return filetype

#---------------------------------------------------------------------------

def load_ref_kmer_hash(db, tablename, kmers, refid, cursor):
	sql="INSERT INTO %s (kmer, refid, count, total, freq) VALUES " % (tablename)
	totalkmercount = sum(kmers.itervalues())
	for kmer, count in kmers.iteritems():
		#n+=1
		f= 1 / (totalkmercount * float(count))
		freq ="{:.10f}".format(f)
		#print f, freq, totalkmercount, count
		sql+= "('%s',%s,%s,%s,%s)," % (kmer, refid, count, totalkmercount, freq)
	sql=sql[:-1]
	#print sql
	cursor.execute(sql)
	db.commit()

#---------------------------------------------------------------------------
