#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: sql.py
# Purpose:
# Creation Date: 04-11-2015
# Last Modified: Wed, May 18, 2016  3:10:59 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import sys
import time
import MySQLdb
from warnings import filterwarnings

def okSQLname(s): # MS 
       return "." not in s and "-" not in s


# ---------------------------------------------------------------------------

def create_align_table_sam(tablename, cursor):
    fields = (  # --------- now add extra columns as needed
        'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
        'basename_id INT(7) NOT NULL, INDEX (basename_id)',
        'qname VARCHAR(150)',
        'flag INT(4) ',
        'rname VARCHAR(150)',
        'pos INT(7)',
        'mapq INT(3)',
        'cigar TEXT',
        'rnext VARCHAR(100)',
        'pnext INT(4)',
        'tlen INT(4)',
        'seq TEXT',
        'qual TEXT',
        'n_m VARCHAR(10)',
        'm_d TEXT',
        'a_s VARCHAR(10)',
        'x_s VARCHAR(10)',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_barcode_table(tablename, cursor):
    fields = (
        'basename_id INT(10), PRIMARY KEY(basename_id)',
        'pos0_start INT(5) NOT NULL',
        'score INT(6) NOT NULL',
        'design VARCHAR(10) NOT NULL',
        'pos1_end INT(5) NOT NULL',
        'pos0_end INT(5) NOT NULL',
        'pos1_start INT(5) NOT NULL',
        'variant VARCHAR(8) NOT NULL',
        'barcode_arrangement VARCHAR(12) NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=InnoDB' \
        % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_xml_table(tablename, cursor):
    fields = \
        ('xmlindex INT(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(xmlindex)'
         , 'type VARCHAR(20) NOT NULL',
         'primary_id VARCHAR(30) NOT NULL',
         'filename VARCHAR(30) NOT NULL', 'xml TEXT DEFAULT NULL')
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=InnoDB' \
        % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_comment_table_if_not_exists(tablename, cursor):
    fields = (
        'comment_id INT(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(comment_id)'
            ,
        'runindex INT(11) NOT NULL',
        'runname TEXT NOT NULL',
        'user_name TEXT NOT NULL',
        'date DATETIME NOT NULL',
        'comment TEXT NOT NULL',
        'name TEXT NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = \
        'CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=InnoDB DEFAULT CHARSET=utf8' \
        % (tablename, colheaders)
    filterwarnings('ignore', "Table 'comments' already exists")
    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_pre_general_table(tablename, cursor):
    fields = (  # PLSP57501_17062014lambda_3216_1_ch101_file10_strand
                # = 2
                # = 1
                # = 124.29830200195312
                # = 199290
                # = 1382
                # = 1
                # = 125.83346438759253
                # = 0.6979265001059975
                # = 228.775431986964
                # = ad4812d9-0f84-468b-a932-c2f91b6152ef
                # = 5
                # = 1
                # = 3
                # = 2544301
                # THIS IS CALCULATED BY THE MINUP SCRIPT FROM THE EVENTS TABLE
                # THIS IS CALCULATED BY THE MINUP SCRIPT FROM THE EVENTS TABLE
        'basename_id INT(10) NOT NULL, PRIMARY KEY(basename_id)',
        'basename VARCHAR(150) NOT NULL, UNIQUE KEY(basename)',
        'abasic_event_index INT(10) DEFAULT NULL',
        'abasic_found INT(1) DEFAULT NULL',
        'abasic_peak_height varchar(40) DEFAULT NULL',
        'duration varchar(40) DEFAULT NULL',
        'hairpin_event_index INT(10) DEFAULT NULL',
        'hairpin_found INT(1) DEFAULT NULL',
        'hairpin_peak_height VARCHAR(40) DEFAULT NULL',
        'hairpin_polyt_level VARCHAR(40) DEFAULT NULL',
        'median_before VARCHAR(40) DEFAULT NULL',
        'read_id VARCHAR(40) DEFAULT NULL',
        'read_number INT(10) DEFAULT NULL',
        'scaling_used INT(1) DEFAULT NULL',
        'start_mux INT(1) DEFAULT NULL',
        'start_time varchar(20) DEFAULT NULL',
        'total_events INT(20) DEFAULT NULL',
	'sampling_rate float', 
        '1minwin INT NOT NULL, INDEX(1minwin)',
        '5minwin INT NOT NULL, INDEX(5minwin)',
        '10minwin INT NOT NULL, INDEX(10minwin)',
        '15minwin INT NOT NULL, INDEX(15minwin)',
        's1minwin INT NOT NULL, INDEX(s1minwin)',
        's5minwin INT NOT NULL, INDEX(s5minwin)',
        's10minwin INT NOT NULL, INDEX(s10minwin)',
        's15minwin INT NOT NULL, INDEX(s15minwin)',
        'g_1minwin INT NOT NULL, INDEX(g_1minwin)',
        'g_5minwin INT NOT NULL, INDEX(g_5minwin)',
        'g_10minwin INT NOT NULL, INDEX(g_10minwin)',
        'g_15minwin INT NOT NULL, INDEX(g_15minwin)',
        'g_s1minwin INT NOT NULL, INDEX(g_s1minwin)',
        'g_s5minwin INT NOT NULL, INDEX(g_s5minwin)',
        'g_s10minwin INT NOT NULL, INDEX(g_s10minwin)',
        'g_s15minwin INT NOT NULL, INDEX(g_s15minwin)',
        'exp_start_time INT(15) NOT NULL', # Maybe change to pre_tracking_id ?
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=InnoDB' \
        % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


    # return fields

# ---------------------------------------------------------------------------

def create_general_table(tablename, cursor):
    fields = (  # PLSP57501_17062014lambda_3216_1_ch101_file10_strand
                #  = basecall_2d_workflow.py"config_general"
                # = Basecall_2D_000
                #  = 10
                # = False
                # = channel_101_read_10
                # = /opt/metrichor/model
                #  = auto
                #  = 100000
                # = /tmp/input/PLSP57501_17062014lambda_3216_1_ch101_file10_strand.fast5
                # = 1000
                # = /opt/metrichor/config/basecall_2d.cfg
                #  = auto
                #  = 101
                # version = 0.8.3
                # time_stamp = 2014-Jul-02 09:10:13
                # = 1403015537
        'basename_id INT(10) NOT NULL, PRIMARY KEY(basename_id)',
        'basename VARCHAR(150) NOT NULL, UNIQUE KEY(basename)',
        'local_folder VARCHAR(50) NOT NULL',
        'workflow_script VARCHAR(50) DEFAULT NULL',
        'workflow_name VARCHAR(50) NOT NULL',
        'read_id INT(4) NOT NULL',
        'use_local VARCHAR(10) NOT NULL',
        'tag VARCHAR(50) NOT NULL',
        'model_path VARCHAR(50) NOT NULL',
        'complement_model TEXT(10) NOT NULL',
        'max_events INT(10) NOT NULL',
        'input VARCHAR(200) NOT NULL',
        'min_events INT(4) NOT NULL',
        'config VARCHAR(100) NOT NULL',
        'template_model VARCHAR(8) NOT NULL',
        'channel INT(4) NOT NULL',
        'metrichor_version VARCHAR(10) NOT NULL',
        'metrichor_time_stamp VARCHAR(20) NOT NULL',
        'abasic_event_index INT(1)',
        'abasic_found INT(1)',
        'abasic_peak_height FLOAT(25,17)',
        'duration INT(15)',
        'hairpin_event_index INT(10)',
        'hairpin_found INT(1)',
        'hairpin_peak_height FLOAT(25,17)',
        'hairpin_polyt_level FLOAT(25,17)',
        'median_before FLOAT(25,17)',
        'read_name VARCHAR(37)',
        'read_number int(10)',
        'scaling_used int(5)',
        'start_mux int(1)',
        'start_time int(20)',
        'end_mux INT(1) DEFAULT NULL',
        'exp_start_time INT(15) NOT NULL',
        '1minwin INT NOT NULL, INDEX(1minwin)',
        '5minwin INT NOT NULL, INDEX(5minwin)',
        '10minwin INT NOT NULL, INDEX(10minwin)',
        '15minwin INT NOT NULL, INDEX(15minwin)',
        's1minwin INT NOT NULL, INDEX(s1minwin)',
        's5minwin INT NOT NULL, INDEX(s5minwin)',
        's10minwin INT NOT NULL, INDEX(s10minwin)',
        's15minwin INT NOT NULL, INDEX(s15minwin)',
        'g_1minwin INT NOT NULL, INDEX(g_1minwin)',
        'g_5minwin INT NOT NULL, INDEX(g_5minwin)',
        'g_10minwin INT NOT NULL, INDEX(g_10minwin)',
        'g_15minwin INT NOT NULL, INDEX(g_15minwin)',
        'g_s1minwin INT NOT NULL, INDEX(g_s1minwin)',
        'g_s5minwin INT NOT NULL, INDEX(g_s5minwin)',
        'g_s10minwin INT NOT NULL, INDEX(g_s10minwin)',
        'g_s15minwin INT NOT NULL, INDEX(g_s15minwin)',
        'align INT DEFAULT 0,  INDEX(align)',
        'pass INT(1) NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=InnoDB' \
        % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


    # return fields

# ---------------------------------------------------------------------------

def create_trackingid_table(tablename, cursor):
    fields = (  # PLSP57501_17062014lambda_3216_1_ch101_file10_strand
                # PLSP57501_17062014lambda_3216_1_ch101_file10_strand
                #  = 48133
                #  = 48133
                # = 38.4
                # = MN02935
                # = sequencing_run
                # =./python/recipes/MAP_48Hr_Sequencing_Run_SQK_MAP006.py
                # = 1403015537
                #
                # = 35.625
                # = 9be694a4d40804eb6ea5761774723318ae3b3346
                # = 0.45.1.6 b201406111512
        'basename_id INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(basename_id)'
            ,
        'basename VARCHAR(150) NOT NULL',
        'asic_id BIGINT(15) NOT NULL',
        'asic_id_17 BIGINT(15) ',
        'asic_id_eeprom INT(5) ',
        'asic_temp DOUBLE(4,1) NOT NULL',
        'device_id TEXT(8) NOT NULL',
        'exp_script_purpose VARCHAR(50) NOT NULL',
        'exp_script_name VARCHAR(150)',
        'exp_start_time INT(15) NOT NULL',
        'flow_cell_id VARCHAR(10) NOT NULL',
        'heatsink_temp FLOAT(10) NOT NULL',
        'hostname TEXT',
        'run_id TEXT(40) NOT NULL',
        'version_name VARCHAR(30) NOT NULL',
        'file_path TEXT(300) NOT NULL',
        'channel_number int(7)',
        'digitisation float',
        'offset float',
        'range_val float',
        'sampling_rate float',
        'pass INT(1) NOT NULL',
        'md5sum TEXT(33) NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=InnoDB' \
        % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


    # return fields

# ---------------------------------------------------------------------------

def create_pretrackingid_table(tablename, cursor):
    fields = (  # PLSP57501_17062014lambda_3216_1_ch101_file10_strand
                # PLSP57501_17062014lambda_3216_1_ch101_file10_strand
                # 'asic_id INT(20) NOT NULL', #  = 48133
                # = 38.4
                # = MN02935
                # = sequencing_run
                # = 1403015537
                #
                # = 35.625
                # = 9be694a4d40804eb6ea5761774723318ae3b3346
                # = 0.45.1.6 b201406111512
        'basename_id INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(basename_id)'
            ,
        'basename VARCHAR(150) NOT NULL',
        'asic_temp DOUBLE(4,1) NOT NULL',
        'device_id TEXT(8) NOT NULL',
        'exp_script_purpose VARCHAR(50) NOT NULL',
        'exp_start_time INT(15) NOT NULL',
        'flow_cell_id VARCHAR(10) NOT NULL',
        'heatsink_temp FLOAT(10) NOT NULL',
        'run_id TEXT(40) NOT NULL',
        'version_name VARCHAR(30) NOT NULL',
        'file_path TEXT(300) NOT NULL',
        'md5sum TEXT(33) NOT NULL',
        'hairpin_found INT(1)',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=InnoDB' \
        % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


    # return fields

# ---------------------------------------------------------------------------

def create_reference_table(tablename, cursor):
    fields = ('refid INT(3) NOT NULL AUTO_INCREMENT, PRIMARY KEY(refid)'
              , 'refname VARCHAR(50), UNIQUE INDEX (refname)',
              'reflen INT(7), INDEX (reflen)',
              'reffile VARCHAR(100), INDEX (reffile)',
              'ref_total_len VARCHAR(100), INDEX (ref_total_len)')  # PLSP57501_17062014lambda_3216_1_ch101_file10_strand
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=InnoDB' \
        % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_basecalled2d_fastq_table(tablename, cursor):
    fields = (  # ['basename','VARCHAR(300) PRIMARY KEY'], # PLSP57501_17062014lambda_3216_1_ch101_file10_strand
                # PLSP57501_17062014lambda_3216_1_ch101_file10_strand.whatever
                # = 2347.2034000000003
                # = 1403015537
        'basename_id INT(10) NOT NULL, PRIMARY KEY (basename_id)',
        'seqid VARCHAR(150), UNIQUE INDEX (seqid)',
        'seqlen INT NOT NULL',
        'sequence MEDIUMTEXT',
        'start_time FLOAT(25,17) NOT NULL',
        'align INT DEFAULT 0,  INDEX(align)',
        '1minwin INT NOT NULL, INDEX(1minwin)',
        '5minwin INT NOT NULL, INDEX(5minwin)',
        '10minwin INT NOT NULL, INDEX(10minwin)',
        '15minwin INT NOT NULL, INDEX(15minwin)',
        's1minwin INT NOT NULL, INDEX(s1minwin)',
        's5minwin INT NOT NULL, INDEX(s5minwin)',
        's10minwin INT NOT NULL, INDEX(s10minwin)',
        's15minwin INT NOT NULL, INDEX(s15minwin)',
        'g_1minwin INT NOT NULL, INDEX(g_1minwin)',
        'g_5minwin INT NOT NULL, INDEX(g_5minwin)',
        'g_10minwin INT NOT NULL, INDEX(g_10minwin)',
        'g_15minwin INT NOT NULL, INDEX(g_15minwin)',
        'g_s1minwin INT NOT NULL, INDEX(g_s1minwin)',
        'g_s5minwin INT NOT NULL, INDEX(g_s5minwin)',
        'g_s10minwin INT NOT NULL, INDEX(g_s10minwin)',
        'g_s15minwin INT NOT NULL, INDEX(g_s15minwin)',
        'exp_start_time INT(15) NOT NULL',
        'qual MEDIUMTEXT',
        'index 1minalign (align,1minwin)',
        'index 5minalign (align,5minwin)',
        'index 10minalign (align,10minwin)',
        'index 15minalign (align,15minwin)',
        'index s1minalign (align,s1minwin)',
        'index s5minalign (align,s5minwin)',
        'index s10minalign (align,s10minwin)',
        'index s15minalign (align,s15minwin)',
        'index g_1minalign (align,g_1minwin)',
        'index g_5minalign (align,g_5minwin)',
        'index g_10minalign (align,g_10minwin)',
        'index g_15minalign (align,g_15minwin)',
        'index g_s1minalign (align,g_s1minwin)',
        'index g_s5minalign (align,g_s5minwin)',
        'index g_s10minalign (align,g_s10minwin)',
        'index g_s15minalign (align,g_s15minwin)',
        'pass INT(1) NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=InnoDB' \
        % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

# ['ID', 'INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)'],

def create_events_model_fastq_table(tablename, cursor):
    fields = (  # 'basename VARCHAR(300), PRIMARY KEY', # PLSP57501_17062014lambda_3216_1_ch101_file10_strand
                # PLSP57501_17062014lambda_3216_1_ch101_file10_strand.whatever
                # = 51.80799999999954
                # = 2347.2034000000003
                # = 1.0063618778594416
                # = 0.20855518951022478
                # = -0.10872176688437207
                # = 0.004143787533549812
                # = 0.9422581300419306
                # = 1.3286319210403454
                # = 1.0368718353240443
                # = 1403015537
        'basename_id INT(10) NOT NULL, PRIMARY KEY (basename_id)',
        'seqid VARCHAR(150) NOT NULL, UNIQUE INDEX (seqid)',
        'duration FLOAT(25,17) NOT NULL',
        'start_time FLOAT(25,17) NOT NULL',
        'scale FLOAT(25,17)',
        'shift FLOAT(25,17)',
        'gross_shift FLOAT(25,17) DEFAULT NULL',
        'drift FLOAT(25,17)',
        'scale_sd FLOAT(25,17)',
        'var_sd FLOAT(25,17)',
        'var FLOAT(25,17)',
        'seqlen INT NOT NULL',
        '1minwin INT NOT NULL, INDEX(1minwin)',
        '5minwin INT NOT NULL, INDEX(5minwin)',
        '10minwin INT NOT NULL, INDEX(10minwin)',
        '15minwin INT NOT NULL, INDEX(15minwin)',
        's1minwin INT NOT NULL, INDEX(s1minwin)',
        's5minwin INT NOT NULL, INDEX(s5minwin)',
        's10minwin INT NOT NULL, INDEX(s10minwin)',
        's15minwin INT NOT NULL, INDEX(s15minwin)',
        'g_1minwin INT NOT NULL, INDEX(g_1minwin)',
        'g_5minwin INT NOT NULL, INDEX(g_5minwin)',
        'g_10minwin INT NOT NULL, INDEX(g_10minwin)',
        'g_15minwin INT NOT NULL, INDEX(g_15minwin)',
        'g_s1minwin INT NOT NULL, INDEX(g_s1minwin)',
        'g_s5minwin INT NOT NULL, INDEX(g_s5minwin)',
        'g_s10minwin INT NOT NULL, INDEX(g_s10minwin)',
        'g_s15minwin INT NOT NULL, INDEX(g_s15minwin)',
        'align INT DEFAULT 0,  INDEX(align)',
        'pass INT(1) NOT NULL',
        'exp_start_time INT(15) NOT NULL',
        'sequence MEDIUMTEXT DEFAULT NULL',
        'qual MEDIUMTEXT DEFAULT NULL',
        'index 1minalign (align,1minwin)',
        'index 5minalign (align,5minwin)',
        'index 10minalign (align,10minwin)',
        'index 15minalign (align,15minwin)',
        'index s1minalign (align,s1minwin)',
        'index s5minalign (align,s5minwin)',
        'index s10minalign (align,s10minwin)',
        'index s15minalign (align,s15minwin)',
        'index g_1minalign (align,g_1minwin)',
        'index g_5minalign (align,g_5minwin)',
        'index g_10minalign (align,g_10minwin)',
        'index g_15minalign (align,g_15minwin)',
        'index g_s1minalign (align,g_s1minwin)',
        'index g_s5minalign (align,g_s5minwin)',
        'index g_s10minalign (align,g_s10minwin)',
        'index g_s15minalign (align,g_s15minwin)',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=InnoDB' \
        % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

# ['ID', 'INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)'],

def create_align_table(tablename, cursor):
    fields = (  # index
                # index
                # index
                # index
                # index
                # index
                # index for combined queries
        'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
        'basename_id INT(7), INDEX (basename_id)',
        'refid INT(3) DEFAULT NULL, INDEX (refid)',
        'alignnum INT(4) DEFAULT NULL, INDEX (alignnum)',
        'covcount INT(4) DEFAULT NULL, INDEX (covcount)',
        'alignstrand VARCHAR(1), INDEX (alignstrand)',
        'score INT(4), INDEX (score)',
        'seqpos INT(6) DEFAULT NULL, INDEX (seqpos)',
        'refpos INT(6) DEFAULT NULL, INDEX (refpos)',
        'seqbase VARCHAR(1) DEFAULT NULL, INDEX (seqbase)',
        'refbase VARCHAR(1) DEFAULT NULL, INDEX (refbase)',
        'seqbasequal INT(2) DEFAULT NULL, INDEX (seqbasequal)',
        'cigarclass VARCHAR(1) DEFAULT NULL, INDEX (cigarclass)',
        'index combindex (refid,refpos,cigarclass)',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_align_table_raw(tablename, cursor):
    fields = (  # index
        'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
        'basename_id INT(7), INDEX (basename_id)',
        'refid INT(3) DEFAULT NULL, INDEX (refid)',
        'alignnum INT(4) DEFAULT NULL, INDEX (alignnum)',
        'alignstrand VARCHAR(1) DEFAULT NULL, INDEX (alignstrand)',
        'score INT(4), INDEX (score)',
        'r_start INT(7) DEFAULT NULL',
        'q_start INT(5) DEFAULT NULL',
        'r_align_len INT(7) DEFAULT NULL',
        'q_align_len INT(5) DEFAULT NULL',
        )

    # 'index combindex (refid,refpos,cigarclass)') # index for combined queries

    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_align_table_maf(tablename, cursor):
    fields = (  # index
        'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
        'basename_id INT(7), INDEX (basename_id)',
        'refid INT(3) DEFAULT NULL, INDEX (refid)',
        'alignnum INT(4) DEFAULT NULL, INDEX (alignnum)',
        'alignstrand VARCHAR(1) DEFAULT NULL, INDEX (alignstrand)',
        'score INT(4), INDEX (score)',
        'r_start INT(7) DEFAULT NULL',
        'q_start INT(5) DEFAULT NULL',
        'r_align_len INT(7) DEFAULT NULL',
        'q_align_len INT(5) DEFAULT NULL',
        'r_align_string MEDIUMTEXT DEFAULT NULL',
        'q_align_string MEDIUMTEXT DEFAULT NULL',
        )

    # 'index combindex (refid,refpos,cigarclass)') # index for combined queries

    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_pre_align_table(tablename, cursor):
    fields = (  # index
        'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
        'basename_id INT(7), INDEX (basename_id)',
        'refid INT(3) DEFAULT NULL, INDEX (refid)',
        'alignstrand VARCHAR(1) DEFAULT NULL, INDEX (alignstrand)',
        'r_start INT(7) DEFAULT NULL',
        'q_start INT(5) DEFAULT NULL',
        'r_align_len INT(7) DEFAULT NULL',
        'q_align_len INT(5) DEFAULT NULL',
        )

    # 'index combindex (refid,refpos,cigarclass)') # index for combined queries

    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_caller_table(tablename, cursor):
    fields = (
        'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
        'basename_id INT(7) NOT NULL, INDEX (basename_id)',
        'mean FLOAT(25,17) NOT NULL',
        'start FLOAT(25,17) NOT NULL',
        'stdv FLOAT(25,17) NOT NULL',
        'length FLOAT(25,17) NOT NULL',
        'model_state VARCHAR(10) NOT NULL, INDEX (model_state)',
        'model_level FLOAT(25,17) NOT NULL',
        'move INT(64) NOT NULL',
        'p_model_state FLOAT(25,17) NOT NULL',
        'mp_state VARCHAR(10) NOT NULL, INDEX (mp_state)',
        'p_mp_state FLOAT(25,17) NOT NULL',
        'p_A FLOAT(25,17) NOT NULL',
        'p_C FLOAT(25,17) NOT NULL',
        'p_G FLOAT(25,17) NOT NULL',
        'p_T FLOAT(25,17) NOT NULL',
        'raw_index INT(64) NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=MyISAM' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------
# This removes indexes (performace improvement?

def create_caller_table_noindex(tablename, cursor):
    fields = (
        'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
        'basename_id INT(7) NOT NULL',
        'mean FLOAT(25,17) NOT NULL',
        'start FLOAT(25,17) NOT NULL',
        'stdv FLOAT(25,17) NOT NULL',
        'length FLOAT(25,17) NOT NULL',
        'model_state VARCHAR(10) NOT NULL',
        'model_level FLOAT(25,17) NOT NULL',
        'move INT(64) NOT NULL',
        'p_model_state FLOAT(25,17) NOT NULL',
        'mp_state VARCHAR(10) NOT NULL',
        'p_mp_state FLOAT(25,17) NOT NULL',
        'p_A FLOAT(25,17) NOT NULL',
        'p_C FLOAT(25,17) NOT NULL',
        'p_G FLOAT(25,17) NOT NULL',
        'p_T FLOAT(25,17) NOT NULL',
        'raw_index INT(64) NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=MyISAM' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_2d_alignment_table(tablename, cursor):
    fields = ('ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
              'basename_id INT(7) NOT NULL, INDEX (basename_id)',
              'template INT(5) NOT NULL', 'complement INT(5) NOT NULL',
              'kmer VARCHAR(10) NOT NULL')
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_basecall_summary_info(tablename, cursor):
    fields = (  # = 1403015537
        'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
        'basename_id INT(7) NOT NULL, INDEX (basename_id)',
        'abasic_dur float',
        'abasic_index int',
        'abasic_peak float',
        'duration_comp float',
        'duration_temp float',
        'end_index_comp int',
        'end_index_temp int',
        'hairpin_abasics int',
        'hairpin_dur float',
        'hairpin_events int',
        'hairpin_peak float',
        'median_level_comp float',
        'median_level_temp float',
        'median_sd_comp float',
        'median_sd_temp float',
        'num_comp int',
        'num_events int',
        'num_temp int',
        'pt_level float',
        'range_comp float',
        'range_temp float',
        'split_index int',
        'start_index_comp int',
        'start_index_temp int',
        'driftC float ',
        'mean_qscoreC float',
        'num_skipsC int',
        'num_staysC int',
        'scaleC float',
        'scale_sdC float',
        'sequence_lengthC int',
        'shiftC float',
        'strand_scoreC float',
        'varC float',
        'var_sdC float',
        'driftT float ',
        'mean_qscoreT float',
        'num_skipsT int',
        'num_staysT int',
        'scaleT float',
        'scale_sdT float',
        'sequence_lengthT int',
        'shiftT float',
        'strand_scoreT float',
        'varT float',
        'var_sdT float',
        'mean_qscore2 float',
        'sequence_length2 int',
        'exp_start_time INT(15) NOT NULL',
        '1minwin INT NOT NULL, INDEX(1minwin)',
        '5minwin INT NOT NULL, INDEX(5minwin)',
        '10minwin INT NOT NULL, INDEX(10minwin)',
        '15minwin INT NOT NULL, INDEX(15minwin)',
        's1minwin INT NOT NULL, INDEX(s1minwin)',
        's5minwin INT NOT NULL, INDEX(s5minwin)',
        's10minwin INT NOT NULL, INDEX(s10minwin)',
        's15minwin INT NOT NULL, INDEX(s15minwin)',
        'g_1minwin INT NOT NULL, INDEX(g_1minwin)',
        'g_5minwin INT NOT NULL, INDEX(g_5minwin)',
        'g_10minwin INT NOT NULL, INDEX(g_10minwin)',
        'g_15minwin INT NOT NULL, INDEX(g_15minwin)',
        'g_s1minwin INT NOT NULL, INDEX(g_s1minwin)',
        'g_s5minwin INT NOT NULL, INDEX(g_s5minwin)',
        'g_s10minwin INT NOT NULL, INDEX(g_s10minwin)',
        'g_s15minwin INT NOT NULL, INDEX(g_s15minwin)',
        'align INT DEFAULT 0,  INDEX(align)',
        'pass INT(1) NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_basic_read_info(tablename, cursor):
    fields = (
        'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
        'basename_id INT(7) NOT NULL, INDEX (basename_id)',
        'abasic_event_index INT(1) NOT NULL',
        'abasic_found INT(1) NOT NULL',
        'abasic_peak_height FLOAT(25,17)',
        'duration INT(15)',
        'hairpin_event_index INT(10)',
        'hairpin_found INT(1)',
        'hairpin_peak_height FLOAT(25,17)',
        'hairpin_polyt_level FLOAT(25,17)',
        'median_before FLOAT(25,17)',
        'read_id VARCHAR(37)',
        'read_number int(10)',
        'scaling_used int(5)',
        'start_mux int(1)',
        'start_time int(20)',
        '1minwin INT NOT NULL, INDEX(1minwin)',
        '5minwin INT NOT NULL, INDEX(5minwin)',
        '10minwin INT NOT NULL, INDEX(10minwin)',
        '15minwin INT NOT NULL, INDEX(15minwin)',
        's1minwin INT NOT NULL, INDEX(s1minwin)',
        's5minwin INT NOT NULL, INDEX(s5minwin)',
        's10minwin INT NOT NULL, INDEX(s10minwin)',
        's15minwin INT NOT NULL, INDEX(s15minwin)',
        'g_1minwin INT NOT NULL, INDEX(g_1minwin)',
        'g_5minwin INT NOT NULL, INDEX(g_5minwin)',
        'g_10minwin INT NOT NULL, INDEX(g_10minwin)',
        'g_15minwin INT NOT NULL, INDEX(g_15minwin)',
        'g_s1minwin INT NOT NULL, INDEX(g_s1minwin)',
        'g_s5minwin INT NOT NULL, INDEX(g_s5minwin)',
        'g_s10minwin INT NOT NULL, INDEX(g_s10minwin)',
        'g_s15minwin INT NOT NULL, INDEX(g_s15minwin)',
        'align INT DEFAULT 0,  INDEX(align)',
        'pass INT(1) NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_5_3_prime_align_tables(align_table_in, cursor):

    three_prime_table = align_table_in + '_3prime'
    five_prime_table = align_table_in + '_5prime'
    fields = (  # index
                # index
                # index
                # index
                # index
                # index
                # index for combined queries
        'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
        'basename_id INT(7) NOT NULL, INDEX (basename_id)',
        'refid INT(3) DEFAULT NULL, INDEX (refid)',
        'alignnum INT(4) DEFAULT NULL, INDEX (alignnum)',
        'covcount INT(4) DEFAULT NULL, INDEX (covcount)',
        'alignstrand VARCHAR(1), INDEX (alignstrand)',
        'score INT(4), INDEX (score)',
        'seqpos INT(6) DEFAULT NULL, INDEX (seqpos)',
        'refpos INT(6) DEFAULT NULL, INDEX (refpos)',
        'seqbase VARCHAR(1) DEFAULT NULL, INDEX (seqbase)',
        'refbase VARCHAR(1) DEFAULT NULL, INDEX (refbase)',
        'seqbasequal INT(2) DEFAULT NULL, INDEX (seqbasequal)',
        'cigarclass VARCHAR(1) DEFAULT NULL, INDEX (cigarclass)',
        'index combindex (refid,refpos,cigarclass)',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (three_prime_table,
            colheaders)
    cursor.execute(sql)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (five_prime_table,
            colheaders)
    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_ref_kmer_table(tablename, cursor):
    fields = (
        'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
        'kmer VARCHAR(10) NOT NULL, INDEX (kmer)',
        'refid INT(3) NOT NULL, INDEX (refid)',
        'count INT(7) NOT NULL',
        'total INT(7) NOT NULL',
        'freq float(13,10) NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_model_list_table(tablename, cursor):
    fields = ('basename_id INT(7), PRIMARY KEY(basename_id)',
              'template_model VARCHAR(200), INDEX (template_model)',
              'complement_model VARCHAR(200), INDEX (complement_model)')  # 'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_model_data_table(tablename, cursor):
    fields = (
        'ID INT(10) NOT NULL AUTO_INCREMENT, PRIMARY KEY(ID)',
        'model VARCHAR(200) NOT NULL, INDEX (model)',
        'kmer VARCHAR(10) NOT NULL, INDEX (kmer)',
        'variant INT(10) NOT NULL',
        'level_mean FLOAT(25,17) NOT NULL',
        'level_stdv FLOAT(25,17) NOT NULL',
        'sd_mean FLOAT(25,17) NOT NULL',
        'sd_stdv FLOAT(25,17) NOT NULL',
        'weight FLOAT(25,17) NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_mincontrol_interaction_table(tablename, cursor):
    fields = (
        'job_index INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (job_index)'
            ,
        'instruction MEDIUMTEXT NOT NULL',
        'target MEDIUMTEXT NOT NULL',
        'param1 MEDIUMTEXT',
        'param2 MEDIUMTEXT',
        'complete INT NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_mincontrol_messages_table(tablename, cursor):
    fields = (
        'message_index INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (message_index)'
            ,
        'message MEDIUMTEXT NOT NULL',
        'target MEDIUMTEXT NOT NULL',
        'param1 MEDIUMTEXT',
        'param2 MEDIUMTEXT',
        'complete INT NOT NULL',
        )
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def create_mincontrol_barcode_control_table(tablename, cursor):
    fields = \
        ('job_index INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (job_index)'
         , 'barcodeid MEDIUMTEXT NOT NULL', 'complete INT NOT NULL')
    colheaders = ','.join(fields)
    sql = 'CREATE TABLE %s (%s) ENGINE=InnoDB' % (tablename, colheaders)

    # print sql

    cursor.execute(sql)


# ---------------------------------------------------------------------------

def upload_2dalignment_data(
    basenameid,
    channel,
    alignment,
    db,
    ):
    cursor = db.cursor()
    sqlarray = list()
    for i in alignment:
        val_str = "(%d,%d,%d,'%s')" % (basenameid, i[0], i[1], i[2])
        sqlarray.append(val_str)
    stringvals = ','.join(sqlarray)
    sql = \
        'INSERT INTO caller_basecalled_2d_alignment_%s (basename_id,template,complement,kmer) VALUES %s;' \
        % (channel, stringvals)

    # print sql

    cursor.execute(sql)
    db.commit()


    # return sql

# ---------------------------------------------------------------------------

def upload_telem_data(
    basenameid,
    readtype,
    events,
    db,
    ):
    cursor = db.cursor()

    # -------- Going to be my worker thread function ---

    sqlarray = list()

    # print "EVENTS LEN", len(events[0])

    if len(events[0]) == 14:
        for i in events:
            val_str = \
                "(%d,%f,%f,%f,%f,'%s',%f,%d,%f,'%s',%f,%f,%f,%f,%f, 0)" \
                % (
                basenameid,
                i[0],
                i[1],
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                i[7],
                i[8],
                i[9],
                i[10],
                i[11],
                i[12],
                i[13],
                )
            sqlarray.append(val_str)
    else:

        # print "LEN", len(events[0])

        for i in events:
            if 'weights' in events.dtype.names:
                val_str = \
                    "(%d,%f,%f,%f,%f,'%s',%f,%d,%f,'%s',%f,%f,%f,%f,%f,0)" \
                    % (
                    basenameid,
                    i[0],
                    i[1],
                    i[2],
                    i[3],
                    i[4],
                    i[5],
                    i[6],
                    i[8],
                    i[9],
                    i[10],
                    i[11],
                    i[12],
                    i[13],
                    i[14],
                    )
            else:
                val_str = \
                    "(%d,%f,%f,%f,%f,'%s',%f,%d,%f,'%s',%f,%f,%f,%f,%f,%d)" \
                    % (
                    basenameid,
                    i[0],
                    i[1],
                    i[2],
                    i[3],
                    i[4],
                    i[5],
                    i[6],
                    i[7],
                    i[8],
                    i[9],
                    i[10],
                    i[11],
                    i[12],
                    i[13],
                    i[14],
                    )
            sqlarray.append(val_str)

    stringvals = ','.join(sqlarray)

    # print "processed telem",  (time.time())-starttime
    #print stringvals # MS
    #sys.exit(1)

    sql = \
        'INSERT INTO caller_%s (basename_id,mean,start,stdv,length,model_state,model_level,move,p_model_state,mp_state,p_mp_state,p_A,p_C,p_G,p_T,raw_index) VALUES %s;' \
        % (readtype, stringvals)
    cursor.execute(sql)
    db.commit()


# ---------------------------------------------------------------------------

def upload_model_data(
    tablename,
    model_name,
    model_location,
    hdf,
    cursor,
    db,
    ):
    table = hdf[model_location][()]
    sqlarray = list()
    for r in table:
        i = list(r)
        if len(i) == 6:
            i.insert(1, 0)
        eventdeats = "('" + str(model_name) + "'"
        for j in i:
            if isinstance(j, (int, long, float, complex)):
                eventdeats += ',' + str(j)
            else:
                eventdeats += ",'" + str(j) + "'"
        eventdeats += ')'
        sqlarray.append(eventdeats)
    stringvals = ','.join(sqlarray)
    sql = \
        'INSERT INTO %s (model,kmer,variant,level_mean,level_stdv,sd_mean,sd_stdv,weight) VALUES %s;' \
        % (tablename, stringvals)

    # print sql

    cursor.execute(sql)
    db.commit()


# ---------------------------------------------------------------------------
"""
def check_read_type(filepath, hdf):
    filetype = 1

    # print hdf

    if 'Analyses/Basecall_2D_000/' in hdf:

        # print "Basecalled File"

        filetype = 1
    else:

        # print "Raw File"

        filetype = 0
    return filetype
"""

# ---------------------------------------------------------------------------

def modify_gru(cursor):

    # -------- This bit adds columns to Gru.minIONruns ####
    # # Add column 'mt_ctrl_flag' to Gru.minIONruns table if it doesn't exist

    sql = \
        'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA="Gru" AND TABLE_NAME="minIONruns" AND column_name="mt_ctrl_flag" '

    # print sql

    cursor.execute(sql)
    if cursor.rowcount == 0:

        # print "adding mt_ctrl_flag to Gru.minIONruns"

        sql = \
            'ALTER TABLE Gru.minIONruns ADD mt_ctrl_flag INT(1) DEFAULT 0'

        # print sql

        cursor.execute(sql)
        db.commit()

    # # Add column 'watch_dir' to Gru.minIONruns table if it doesn't exist

    sql = \
        'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA="Gru" AND TABLE_NAME="minIONruns" AND column_name="watch_dir" '

    # print sql

    cursor.execute(sql)
    if cursor.rowcount == 0:

        # print "adding 'watch_dir' to Gru.minIONruns"

        sql = 'ALTER TABLE Gru.minIONruns ADD watch_dir TEXT(200)'

        # print sql

        cursor.execute(sql)
        db.commit()

    # # Add column 'host_ip' to Gru.minIONruns table if it doesn't exist

    sql = \
        'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA="Gru" AND TABLE_NAME="minIONruns" AND column_name="host_ip" '

    # print sql

    cursor.execute(sql)
    if cursor.rowcount == 0:

        # print "adding mt_ctrl_flag to Gru.minIONruns"

        sql = 'ALTER TABLE Gru.minIONruns ADD host_ip TEXT(16)'

        # print sql

        cursor.execute(sql)
        db.commit()


# ---------------------------------------------------------------------------
