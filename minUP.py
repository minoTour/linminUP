#!/usr/bin/python
# -*- coding: utf-8 -*-

# --------------------------------------------------
# File Name: minup.v0.63.py
# Purpose: minup: a program to process & upload MinION fast5 files
#               in to the minoTour website in real-time or post-run.
# Creation Date: 2014 - 2016
# Last Modified: Thu, Jun  2, 2016  2:25:43 PM
# Author(s): written & designed by
#               Martin J. Blythe, Fei Sang, Mike Stout & Matt W. Loose
#               The DeepSeq Team, University of Nottingham, UK
# Copyright: 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------
import sys
import os
import time
from watchdog.observers.polling import PollingObserver as Observer
import re
import MySQLdb
import configargparse
import multiprocessing
import platform  # MS

# Unbuffered IO
# NB 1 is essential to catch ctrl-c ... 
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)  # MS
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', 0)  # MS


sys.path.append('modules')
from exitGracefully import terminateMinup
from processRefFasta import process_ref_fasta
from telem import *

from sql import okSQLname



# ---------------------------------------------------------------------------
# main

if __name__ == '__main__':

    multiprocessing.freeze_support()  # MS

    manager = multiprocessing.Manager()

    global minup_version
    fh = open("ver.txt", "r")
    ver_txt = fh.readline()
    fh.close()
    minup_version = re.sub("[^.0-9]", "", ver_txt.split('_')[0])
    __version__ = minup_version

    global oper

    oper = platform.system()  
    if oper is 'Windows':  # MS
        oper = 'windows'
    else:
        oper = 'linux'  # MS
    print oper  # MS

    global config_file
    global logfolder
    global valid_ref_dir
    global bwa_index_dir
    global last_index_dir
    xml_file_dict = dict()




                # # linux version
    if oper is 'linux':
        config_file = os.path.join(os.path.sep,
                                   os.path.dirname(os.path.realpath('__file__'
                                   )), 'minup_posix.config')
        logfolder = os.path.join(os.path.sep,
                                 os.path.dirname(os.path.realpath('__file__'
                                 )), 'minup_run_logs')
        valid_ref_dir = os.path.join(os.path.sep,
                os.path.dirname(os.path.realpath('__file__')),
                'valid_reference_fasta_files')
        bwa_index_dir = os.path.join(os.path.sep,
                os.path.dirname(os.path.realpath('__file__')),
                'bwa_indexes')
        last_index_dir = os.path.join(os.path.sep,
                os.path.dirname(os.path.realpath('__file__')),
                'last_indexes')

                # # windows version

    if oper is 'windows':
        config_file = os.path.join(os.path.sep, sys.prefix,
                                   'minup_windows.config')
        logfolder = os.path.join(os.path.sep, sys.prefix,
                                 'minup_run_logs')
        valid_ref_dir = os.path.join(os.path.sep, sys.prefix,
                'valid_reference_fasta_files')
        bwa_index_dir = os.path.join(os.path.sep, sys.prefix,
                'bwa_indexes')
        last_index_dir = os.path.join(os.path.sep, sys.prefix,
                'last_indexes')

#-------------------------------------------------------------------------------

# Parser for command line arguments ...

    parser = \
        configargparse.ArgParser(description='minup: A program to analyse minION fast5 files in real-time or post-run.'
                                 , default_config_files=[config_file])
    parser.add(
        '-w',
        '--watch-dir',
        type=str,
        required=True,
        default=None,
        help="The path to the folder containing the downloads directory with fast5 reads to analyse - e.g. C:\data\minion\downloads (for windows)."
            ,
        dest='watchdir',
        )


    parser.add(
        '-dbh',
        '--mysql-host',
        type=str,
        dest='dbhost',
        required=False,
        default='localhost',
        help="The location of the MySQL database. default is 'localhost'."
            ,
        )

    parser.add(
        '-dbu',
        '--mysql-username',
        type=str,
        dest='dbusername',
        required=True,
        default=None,
        help='The MySQL username with create & write privileges on MinoTour.'
            ,
        )

    parser.add(
        '-dbp',
        '--mysql-port',
        type=int,
        dest='dbport',
        required=False,
        default=3306,
        help="The MySQL port number, else the default port '3306' is used."
            ,
        )

    parser.add(
        '-pw',
        '--mysql-password',
        type=str,
        dest='dbpass',
        required=True,
        default=None,
        help='The password for the MySQL username with permission to upload to MinoTour.'
            ,
        )

    parser.add(
        '-f',
        '--align-ref-fasta',
        type=str,
        required=False,
        default=False,
        help='The reference fasta file to align reads against. Using this option enables read alignment provided LastAl and LastDB are in the path. Leaving this entry blank will upload the data without any alignment. To use multiple reference fasta files input them as one text string seperated by commas (no white spaces) '
            ,
        dest='ref_fasta',
        )

    parser.add(
        '-b',
        '--align-batch-fasta',
        action='store_true',
        required=False,
        default=False,
        help='Align reads in batch processing mode. Assumes the watch-dir (-w) is pointed at a directory with one or more "downloads" folders below it somewhere. Each "downloads" folder can have a subfolder named "reference" containing the fasta file(s) to align the fast5 reads in the corresponding "downloads" folder to '
            ,
        dest='batch_fasta',
        )
    parser.add(
        '-procs',
        '--proc_num',
        type=int,
        dest='procs',
        required=False,
        default=1,
        help='The number of processors to run this on.',
        )

                # parser.add('-n', '--aligning-threads', type=str, required=False, help="The number of threads to use for aligning", default=3, dest='threads')

    parser.add(
        '-u',
        '--minotour-username',
        type=str,
        required=True,
        help='The MinoTour username with permissions to upload data.',
        default=False,
        dest='minotourusername',
        )
    parser.add(
        '-s',
        '--minotour-sharing-usernames',
        type=str,
        required=False,
        default=False,
        help='A comma seperated list (with no whitespaces) of other MinoTour users who will also be able to view the data.'
            ,
        dest='view_users',
        )
    parser.add(
        '-o',
        '--flowcell-owner',
        type=str,
        required=False,
        default='minionowner',
        help="The name of the minion owner. 'minionowner' is the default"
            ,
        dest='flowcell_owner',
        )
    parser.add(
        '-r',
        '--run-number',
        type=str,
        required=False,
        default=0,
        help='The run number of the flowcell. The default value is 0. '
            ,
        dest='run_num',
        )
    parser.add(
        '-c',
        '--commment-true',
        action='store_true',
        help='Add a comment to the comments field for this run. Follow the prompt once minup starts . '
            ,
        default=False,
        dest='add_comment',
        )
    parser.add(
        '-last',
        '--last-align-true',
        action='store_true',
        help='align reads with LAST',
        default=False,
        dest='last_align',
        )
    parser.add(
        '-bwa',
        '--bwa-align-true',
        action='store_true',
        help='align reads with BWA',
        default=False,
        dest='bwa_align',
        )
    parser.add(
        '-bwa-opts',
        '--bwa-align-options',
        type=str,
        required=False,
        help="BWA options: Enter a comma-seperated list of BWA options without spaces or '-' characters e.g. k12,T0"
            ,
        default='T0',
        dest='bwa_options',
        )
    parser.add(
        '-last-opts',
        '--last-align-options',
        type=str,
        required=False,
        help="LAST options: Enter a comma-seperated list of LAST options without spaces or '-' characters e.g. s2,T0,Q0,a1"
            ,
        default='s2,T0,Q0,a1',
        dest='last_options',
        )
    parser.add(
        '-pin',
        '--security-pin',
        type=str,
        required=False,
        default=False,
        help='pin number for remote control',
        dest='pin',
        )
    parser.add(
        '-ip',
        '--ip-address',
        type=str,
        required=False,
        default=False,
        help="Used for remote control with option '-pin'. Provide IP address of the computer running minKNOW. The default is the IP address of this computer"
            ,
        dest='ip_address',
        )
    parser.add(
        '-t',
        '--insert-tel-true',
        action='store_true',
        help='Store all the telemetry data from the read files online. This feature is currently in development.'
            ,
        default=False,
        dest='telem',
        )
    parser.add(
        '-pre',
        '--insert-pre-true',
        action='store_true',
        help='Process raw reads prior to basecalling by metrichor.'
            ,
        default=False,
        dest='preproc',
        )
    parser.add(
        '-prealign',
        '--align-pre-true',
        action='store_true',
        help='Align raw reads prior to basecalling by metrichor.'
            ,
        default=False,
        dest='prealign',
        )
    parser.add(
        '-d',
        '--drop-db-true',
        action='store_true',
        help='Drop existing database if it already exists.',
        default=False,
        dest='drop_db',
        )
    parser.add(
        '-v',
        '--verbose-true',
        help='Print detailed messages while processing files.',
        type=str,
        required=False,
        default='none',
        dest='verbose',
        )
    parser.add(
        '-name',
        '--name-custom',
        type=str,
        required=False,
        default='',
        help='Provide a modifier to the database name. This allows you to upload the same dataset to minoTour more than once. The additional string should be as short as possible.'
            ,
        dest='custom_name',
        )
    parser.add(  # MS
        '-cs',
        '--commment-string',
        nargs='+',
        type=str,
        dest='added_comment',
        help='Add given string to the comments field for this run',
        default='',
        )

#                parser.add('-res', '--resume-upload', action='store_true', help="Add files to a partially uploaded database", default=False, dest='resume')

    parser.add(
        '-largerRef',
        '--larger-reference',
        action='store_true',
        help='Force processng on large reference genomes.',
        default=False,
        dest='largerRef',
        )


    parser.add(
        '-customup',
        '--custom-upload',
        action='store_true',
        help='Stop minUP when cached has remained zero for more than 30 seconds.',
        default=False,
        dest='customup',
        )

    parser.add( # MS ...
        '-qScale',
        '--scale-query',
        action='store_true',
        help='Scale query by max value in ref.',
        default=False,
        dest='qScale',
        )

    parser.add( # MS ...
        '-qryStartEnd',
        '--prealign-query-start-end-only',
        action='store_true',
        help='Pre-align using only a window at start and end of the query squiggle? False: pre-align using the whole query squiggle.',
        default=False,
        dest='qryStartEnd',
        )

    parser.add( # MS ...
        '-useHdfTimes',
        '--use-hdf5_file-timestamps',
        action='store_true',
        help='Sort read files using hdt file timestamps rather than file modified times.',
        default=False,
        dest='hdfTimes',
        )

    parser.add( # MS ...
        '-indexToRefDir',
        '--store-indexes-in-ref-folder',
        action='store_true',
        help='Store the bwa/last index files in the reference genome folder',
        default=False,
        dest='indexToRefDir',
        )

    parser.add( # MS ...
        '-debug',
        '--disable-MyHandler-try-except',
        action='store_true',
        help='Disable MyHandler try excep',
        default=False,
        dest='debug',
       )

    parser.add( # MS ...
        '-standalone',
        '--standalone-mode',
        action='store_true',
        help='Run without using metrichor download/uploaed folders.',
        default=False,
        dest='standalone',
        )


    # parser.add('-ver', '--version', action='store_true', help="Report the current version of minUP.", default=False, dest='version') # ML

    # MS ...
    parser.add_argument('-ver'
	, '--version'
	, action='version'
      	, version=('%(prog)s version={version}').format(version=__version__)
	) 

#-------------------------------------------------------------------------------

    args = parser.parse_args()

    if args.verbose == 'high':
        print args
        sys.stdout.flush()

    # MS ...
    if args.indexToRefDir is True:
    	ref_dir = os.path.sep.join(args.ref_fasta.split(os.path.sep)[:-1])
    	bwa_index_dir = ref_dir + '/bwa.indexes/'
    	last_index_dir = ref_dir + '/last.indexes/'




    if not os.path.exists(logfolder):
        os.makedirs(logfolder)
    if not os.path.exists(valid_ref_dir):
        os.makedirs(valid_ref_dir)
    if not os.path.exists(bwa_index_dir):
        os.makedirs(bwa_index_dir)
    if not os.path.exists(last_index_dir):
        os.makedirs(last_index_dir)

    # Check inputs are OK...
    if not okSQLname(args.custom_name): # MS
        print 'Error: Invalid SQL name characters in custom_name : %s\nExiting ...' % (args.custom_name)
	sys.stdout.flush()
        sys.exit(1)




    global dbcheckhash
    dbcheckhash = dict()
    dbcheckhash['dbname'] = dict()
    dbcheckhash['barcoded'] = dict()
    dbcheckhash['barcode_info'] = dict()
    dbcheckhash['runindex'] = dict()
    dbcheckhash['modelcheck'] = dict()
    dbcheckhash['logfile'] = dict()

                # dbcheckhash["mafoutdict"]=dict()
                # dbcheckhash["samoutdict"]=dict()

    global ref_fasta_hash
    ref_fasta_hash = dict()

    global dbname
    dbname = str()

    global comments
    comments = dict()

    global connection_pool
    connection_pool = dict()
    try:
        db = MySQLdb.connect(host=args.dbhost, user=args.dbusername,
                             passwd=args.dbpass, port=args.dbport)
        cursor = db.cursor()
    except Exception, err:

        print >> sys.stderr, "Can't connect to MySQL: %s" % err
	sys.stdout.flush()
        sys.exit(1)

                # if args.version == True: # ML
                #                print "minUP version is "+minup_version # ML
                #                sys.exit() # ML




    if args.ref_fasta is not False and args.batch_fasta is not False:
        print 'Both --align-ref-fasta (-f) and --align-batch-fasta (-b) were set. Select only one and try again.'
	sys.stdout.flush()
        sys.exit(1)

    if args.last_align is not False and args.bwa_align is not False:
        print 'Both --last-align-true (-last) and --bwa-align-true (-bwa) were set. Select only one and try again.'
	sys.stdout.flush()
        sys.exit(1)

    if args.ref_fasta is not False:
        process_ref_fasta(args, valid_ref_dir \
			, bwa_index_dir, last_index_dir  \
                        , args.ref_fasta, ref_fasta_hash)

    global bwaclassrunner
    bwaclassrunner = None
    if args.bwa_align is not False:
        from bwaClass import BwaClass
        bwaclassrunner = BwaClass(100,10)

    comments['default'] = 'No Comment'
    if args.added_comment is not '':  # MS
        comments['default'] = ' '.join(args.added_comment)  # MS
    if args.add_comment is True:
        comment = \
            raw_input('Type comment then press Enter to continue : ')
        comments['default'] = comment


    # MS -- Then, only import this if all is OK ....
    from MyHandler import MyHandler

    print 'monitor started.'
    try:
	check_read_args = connection_pool, minup_version, \
		comments, ref_fasta_hash, dbcheckhash, \
		logfolder, cursor
        observer = Observer()
        event_handler = MyHandler(dbcheckhash, oper, db, args, xml_file_dict, check_read_args, minup_version,bwaclassrunner)
        observer.schedule(event_handler, path=args.watchdir, recursive=True)
        observer.start()
        while True:
            time.sleep(1)

    except (KeyboardInterrupt, SystemExit):
        print 'stopping monitor....'
        observer.stop()
        terminateMinup(args, dbcheckhash, oper, minup_version)
	
	print "finished."
        observer.join()
	sys.stdout.flush()
	sys.exit(1)



