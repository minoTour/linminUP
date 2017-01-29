#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: align_bwa.py
# Purpose:
# Creation Date: 2014 - 2015
# Last Modified: Wed, Jan 25, 2017  2:40:50 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import re
import sys
import subprocess
import threading

from cigar import translate_cigar_mdflag_to_ref

#from progressbar import *
#from pbar import *
from debug import debug


# ---------------------------------------------------------------------------
'''

class bwa_threader(threading.Thread):

    def __init__(
        self,
        args,
        ref_fasta_hash,
        seqid,
        fastqdata,
        basename,
        basenameid,
        dbname,
        db,
        ):
        threading.Thread.__init__(self)
        self.seqid = seqid
        self.fastqdata = fastqdata
        self.basename = basename
        self.basenameid = basenameid
        self.dbname = dbname
        self.db = db
        self.args = args
        self.ref_fasta_hash = ref_fasta_hash
        global bwaclassrunner

    def run(self):
        do_bwa_align(
            self.args,
            self.ref_fasta_hash,
            self.seqid,
            self.fastqdata,
            self.basename,
            self.basenameid,
            self.dbname,
            self.db,
            )


# ---------------------------------------------------------------------------

def init_bwa_threads(
    args,
    ref_fasta_hash,
    connections,
    fastqhash,
    basename,
    basenameid,
    dbname,
    ):
    backgrounds = []
    d = 0
    for seqid in fastqhash.keys():
        db = connections[d]
        fastqdata = fastqhash[seqid]
        background = bwa_threader(
            args,
            ref_fasta_hash,
            seqid,
            fastqdata,
            basename,
            basenameid,
            dbname,
            db,
            )
        background.start()
        backgrounds.append(background)
        d += 1
    for background in backgrounds:
        background.join()
'''


# ---------------------------------------------------------------------------

def do_bwa_align2(args,
    mydicttopass
    ):




    # Op BAM Description
    # M 0 alignment match (can be a sequence match or mismatch)cur
    # I 1 insertion to the reference
    # D 2 deletion from the reference
    # N 3 skipped region from the reference
    # S 4 soft clipping (clipped sequences present in SEQ)
    # H 5 hard clipping (clipped sequences NOT present inSEQ)
    # P 6 padding (silent deletion from padded reference)
    # = 7 sequence match
    # X 8 sequence mismatch

    colstring = \
        'basename_id,refid,alignnum,covcount,alignstrand,score,seqpos,refpos,seqbase,refbase,seqbasequal,cigarclass'

    options = '-' + args.bwa_options.replace(',', ' -')

    # print options
    # read='>testread\nGGCATGACATAAACAAACGTACTTGCCTGTCTGATATATCTTCGGCGTCTTGATCGTAGTTATACGTCATCATAGTGCGGGCCGCGTATTTGTGTTGTCG'
    # cmd='bwa mem -x ont2d -T0 %s.bwa.index %s.fasta ' % (ref_fasta_hash[dbname]["prefix"], basename)
    # cmd='cat %s.fasta | bwa mem -x ont2d -T0 %s.bwa.index -' % (basename, ref_fasta_hash[dbname]["prefix"])

    read = ""
    bwaindex = ""
    db = None # ""
    cursor = None # ""
    for seqid in mydicttopass:
        cursor = mydicttopass[seqid]["db"].cursor()
        db = mydicttopass[seqid]["db"]
        basename_id = mydicttopass[seqid]["basename_id"]
        #print basename_id
        #qualscores = fastqhash['quals']
        read = '%s>%s \r\n%s\r\n' % (read,seqid, mydicttopass[seqid]["fastqdata"]['seq'])
        bwaindex = mydicttopass[seqid]["ref_fasta_hash"][mydicttopass[seqid]["dbname"]]['bwa_index']
    #print read


    if args.verbose == "high":
        #read = read + '\r\n' + read # MS
        print read
        print "-"*80

    cmd = 'bwa mem -x ont2d %s %s -' % (options,
            bwaindex)

    #print cmd

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate(input=read)
    status = proc.wait()

    #print "BWA Error", err

    sam = out.encode('utf-8')
    samdata = sam.splitlines()

    if args.verbose == "high":
        for s in samdata:
            print s
            print "="*80


    sqls = [] # MS
    for line in samdata:

        if not line.startswith('@'):
            line = line.strip('\n')
            record = line.split('\t')
            if args.verbose == "high":
                print "Processing Alignment Record ", record[0]
                debug()
                print record
                print "x"*80


            if record[2] == '*':
                if args.verbose == "high":
                   print "Alignement failed:", line
                   debug()
            else:
                qname = record[0]
                qualscores = mydicttopass[qname]["fastqdata"]['quals']
                flag = int(record[1])
                rname = record[2]
                #refid = ref_fasta_hash[dbname]['refid'][rname]
                refid = mydicttopass[qname]["ref_fasta_hash"][mydicttopass[qname]["dbname"]]['refid'][rname]
                pos = int(record[3])
                mapq = int(record[4])
                cigar = record[5]
                rnext = record[6]
                pnext = record[7]
                tlen = int(record[8])
                seq = record[9]
                qual = record[10]
                n_m = record[11]
                m_d = record[12]
                a_s = record[13]
                x_s = record[14]

                align_strand = str()
                strand = str()

                basenameid = mydicttopass[qname]["basename_id"]
                dbname = mydicttopass[qname]["dbname"]

                if flag == 0 or flag == 2048:
                    strand = '+'
                    align_strand = 'F'

                if flag == 16 or flag == 2064:
                    strand = '-'
                    align_strand = 'R'

                tablename = str()
                if qname.endswith('2d'):
                    tablename = 'align_sam_basecalled_2d'
                if qname.endswith('complement'):
                    tablename = 'align_sam_basecalled_complement'
                if qname.endswith('template'):
                    tablename = 'align_sam_basecalled_template'
                sql = \
                    "INSERT INTO %s (basename_id,qname,flag,rname,pos,mapq,cigar,rnext,pnext,tlen,seq,qual,N_M,M_D,A_S,X_S) VALUES (%d,\'%s\',%d,\'%s\',%d,%d,\'%s\',\'%s\',\'%s\',%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')" \
                    % (
                    tablename,
                    basenameid,
                    qname,
                    flag,
                    rname,
                    pos,
                    mapq,
                    cigar,
                    rnext,
                    pnext,
                    tlen,
                    seq,
                    qual,
                    n_m,
                    m_d,
                    a_s,
                    x_s,
                    )

                # print sql

                sqls.append(sql) # MS
                #print sql
                #cursor.execute(sql)
                #db.commit()

                # print tablename

                # ---------------------------------------------------------------------------

                r_pos = int(record[3]) - 1
                readbases = list(record[9])

                # translate_cigar_mdflag_to_ref(cigar,m_d,r_pos,readbases)

                align_info = translate_cigar_mdflag_to_ref(cigar,
                        m_d, r_pos, readbases)

                # result={"q_start":q_start, "q_stop":q_stop, "q_start_base":r_array[0],"q_stop_base":r_array[-1], "r_start":r_start, "r_stop":r_stop, "r_start_base":r_array[0],"r_stop_base":r_array[-1]  }
                # --------------------------------------------------------------------------- do 5' 3' aligned read base position calc. ########

                tablename = 'last_align_' + qname.rsplit('.', 1)[1]

                # lo.pprint tablename

                if args.verbose == "high":
                    align_message = '%s\n\tAligned:%s:%s-%s (%s) ' \
                        % (qname, rname, align_info['r_start'],
                           align_info['r_stop'], strand)
                    print align_message

                # print line

                # ---------------------------------------------------------------------------I think this is the point we know a read is aligning.

                # print dbname,qname.split('.')[1]

                sql = 'UPDATE ' + dbname + '.' + qname.split('.')[-1] \
                    + ' SET align=\'1\' WHERE basename_id="%s" ' \
                    % basenameid  # ML

                sqls.append(sql) # MS
                #cursor.execute(sql)
                #db.commit()

                if flag == 0:  # so it's a primary alignment, POSITIVE strand
                    fiveprimetable = tablename + '_5prime'
                    threeprimetable = tablename + '_3prime'

                    # print "Qs", len(qualscores), int(align_info["q_stop"])
                    # --------------------------------------------------------------------------- five prime

                    valstring = '('
                    valstring += '%s,' % basenameid  # basename_id
                    valstring += '%s,' % refid  # refid
                    valstring += '%s,' % '1'  # alignnum
                    valstring += '%s,' % '0'  # covcount
                    valstring += "\'%s\'," % align_strand  # alignstrand
                    valstring += '%s,' % mapq  # score
                    valstring += '%s,' % align_info['q_start']  # seqpos
                    valstring += '%s,' % align_info['r_start']  # refpos
                    valstring += "\'%s\'," % align_info['q_start_base']  # seqbase
                    valstring += "\'%s\'," % align_info['r_start_base']  # refbase
                    valstring += '%s,' \
                        % qualscores[int(align_info['q_start']) - 1]  # seqbasequal
                    valstring += '%s' % '7'  # cigarclass
                    valstring += ')'

                    # primes[tablename]['fiveprime']['string']=valstring

                    sql = 'INSERT INTO %s (%s) VALUES %s' \
                        % (fiveprimetable, colstring, valstring)

                    # print "B", sql

                    sqls.append(sql) # MS
                    #cursor.execute(sql)
                    #db.commit()

                    # --------------------------------------------------------------------------- three prime

                    valstring = '('
                    valstring += '%s,' % basenameid  # basename_id
                    valstring += '%s,' % refid  # refid
                    valstring += '%s,' % '1'  # alignnum
                    valstring += '%s,' % '0'  # covcount
                    valstring += "\'%s\'," % align_strand  # alignstrand
                    valstring += '%s,' % mapq  # score
                    valstring += '%s,' % align_info['q_stop']  # seqpos
                    valstring += '%s,' % align_info['r_stop']  # refpos
                    valstring += "\'%s\'," % align_info['q_stop_base']  # seqbase
                    valstring += "\'%s\'," % align_info['r_stop_base']  # refbase
                    valstring += '%s,' \
                        % qualscores[int(align_info['q_stop']) - 1]  # seqbasequal
                    valstring += '%s' % '7'  # cigarclass
                    valstring += ')'
                    sql = 'INSERT INTO %s (%s) VALUES %s' \
                        % (threeprimetable, colstring, valstring)

                    # print "B", sql

                    sqls.append(sql) # MS
                    #cursor.execute(sql)
                    #db.commit()

                if flag == 16:  # It's a primary alignment on the NEGATIVE strand
                    fiveprimetable = tablename + '_5prime'
                    threeprimetable = tablename + '_3prime'

                    # print "Qs", len(qualscores), int(align_info["q_stop"])
                    # --------------------------------------------------------------------------- five prime

                    valstring = '('
                    valstring += '%s,' % basenameid  # basename_id
                    valstring += '%s,' % refid  # refid
                    valstring += '%s,' % '1'  # alignnum
                    valstring += '%s,' % '0'  # covcount
                    valstring += "\'%s\'," % align_strand  # alignstrand
                    valstring += '%s,' % mapq  # score
                    valstring += '%s,' % align_info['q_start']  # seqpos
                    valstring += '%s,' % align_info['r_stop']  # refpos
                    valstring += "\'%s\'," % align_info['q_start_base']  # seqbase
                    valstring += "\'%s\'," % align_info['r_stop_base']  # refbase
                    valstring += '%s,' \
                        % qualscores[int(align_info['q_stop']) - 1]  # seqbasequal
                    valstring += '%s' % '7'  # cigarclass
                    valstring += ')'

                    # primes[tablename]['fiveprime']['string']=valstring

                    sql = 'INSERT INTO %s (%s) VALUES %s' \
                        % (fiveprimetable, colstring, valstring)

                    # print "B", sql

                    sqls.append(sql) # MS
                    #cursor.execute(sql)
                    #db.commit()

                    # --------------------------------------------------------------------------- three prime

                    valstring = '('
                    valstring += '%s,' % basenameid  # basename_id
                    valstring += '%s,' % refid  # refid
                    valstring += '%s,' % '1'  # alignnum
                    valstring += '%s,' % '0'  # covcount
                    valstring += "\'%s\'," % align_strand  # alignstrand
                    valstring += '%s,' % mapq  # score
                    valstring += '%s,' % align_info['q_stop']  # seqpos
                    valstring += '%s,' % align_info['r_start']  # refpos
                    valstring += "\'%s\'," % align_info['q_stop_base']  # seqbase
                    valstring += "\'%s\'," % align_info['r_start_base']  # refbase
                    valstring += '%s,' \
                        % qualscores[int(align_info['q_stop']) - 1]  # seqbasequal
                    valstring += '%s' % '7'  # cigarclass
                    valstring += ')'
                    sql = 'INSERT INTO %s (%s) VALUES %s' \
                        % (threeprimetable, colstring, valstring)

                    # print "B", sql


                    sqls.append(sql) # MS
                    #cursor.execute(sql)
                    #db.commit()

    #print "align_bwa insterting into database ...."
    #sys.stdout.flush()

    #n = len(sqls)
    #bar = mk_bar(n)
    #bar.start()
    for i,sql in enumerate(sqls): # MS
    #    bar.update(i)
        if args.verbose == "high": print s # MS
        try:
            cursor.execute(sql) # MS
        except:
            if args.verbose == "high" : debug()
            pass
    db.commit() # MS
    sqls = []  # MS
    #bar.finish()


# ---------------------------------------------------------------------------
'''

def do_bwa_align(
    args,
    ref_fasta_hash,
    seqid,
    fastqhash,
    basename,
    basenameid,
    dbname,
    db,
    ):
    cursor = db.cursor()

    # Op BAM Description
    # M 0 alignment match (can be a sequence match or mismatch)cur
    # I 1 insertion to the reference
    # D 2 deletion from the reference
    # N 3 skipped region from the reference
    # S 4 soft clipping (clipped sequences present in SEQ)
    # H 5 hard clipping (clipped sequences NOT present inSEQ)
    # P 6 padding (silent deletion from padded reference)
    # = 7 sequence match
    # X 8 sequence mismatch

    colstring = \
        'basename_id,refid,alignnum,covcount,alignstrand,score,seqpos,refpos,seqbase,refbase,seqbasequal,cigarclass'
    qualscores = fastqhash['quals']
    options = '-' + args.bwa_options.replace(',', ' -')

    # print options
    # read='>testread\nGGCATGACATAAACAAACGTACTTGCCTGTCTGATATATCTTCGGCGTCTTGATCGTAGTTATACGTCATCATAGTGCGGGCCGCGTATTTGTGTTGTCG'
    # cmd='bwa mem -x ont2d -T0 %s.bwa.index %s.fasta ' % (ref_fasta_hash[dbname]["prefix"], basename)
    # cmd='cat %s.fasta | bwa mem -x ont2d -T0 %s.bwa.index -' % (basename, ref_fasta_hash[dbname]["prefix"])

    read = '>%s \r\n%s' % (seqid, fastqhash['seq'])

    if args.verbose == "high":
        #read = read + '\r\n' + read # MS
        print read
        print "-"*80

    cmd = 'bwa mem -x ont2d %s %s -' % (options,
            ref_fasta_hash[dbname]['bwa_index'])

    if args.verbose == "high":
        print cmd; debug()

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.PIPE, shell=True)
    for read in streaming_reads:
        proc.stdin.write(read)
        out = proc.stdout.readline()
        #(out, err) = proc.communicate(input=read)
    #status = proc.wait()

    #print "BWA Error", err

    sam = out.encode('utf-8')
    samdata = sam.splitlines()

    if args.verbose == "high":
        for s in samdata: print s
        print "="*80

    sqls = [] # MS
    for line in samdata:
        if args.verbose == "high":
            debug()
            print line
            print "-"*80

        if not line.startswith('@'):
            if args.verbose == "high":
                    debug()
            line = line.strip('\n')
            record = line.split('\t')

            if args.verbose == "high":
                   print "RECORD", len(record)
                   debug()

            if record[2] == '*':
                print "Alignement failed:", line
                sys.stdout.flush()
            else:
                if args.verbose == "high":
                    debug()
                qname = record[0]
                flag = int(record[1])
                rname = record[2]
                refid = ref_fasta_hash[dbname]['refid'][rname]
                pos = int(record[3])
                mapq = int(record[4])
                cigar = record[5]
                rnext = record[6]
                pnext = record[7]
                tlen = int(record[8])
                seq = record[9]
                qual = record[10]
                n_m = record[11]
                m_d = record[12]
                a_s = record[13]
                x_s = record[14]

                align_strand = str()
                strand = str()
                if args.verbose == "high":
                    debug()

                if flag == 0 or flag == 2048:
                    strand = '+'
                    align_strand = 'F'

                if flag == 16 or flag == 2064:
                    strand = '-'
                    align_strand = 'R'

                tablename = str()
                if qname.endswith('2d'):
                    tablename = 'align_sam_basecalled_2d'
                if qname.endswith('complement'):
                    tablename = 'align_sam_basecalled_complement'
                #if qname.endswith('template'):
                else: # MS 01.06.16 for rrn called reas ....
                    tablename = 'align_sam_basecalled_template'
                if args.verbose == "high":
                    debug()
                sql = \
                    "INSERT INTO %s (basename_id,qname,flag,rname,pos,mapq,cigar,rnext,pnext,tlen,seq,qual,N_M,M_D,A_S,X_S) VALUES (%d,\'%s\',%d,\'%s\',%d,%d,\'%s\',\'%s\',\'%s\',%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')" \
                    % (
                    tablename,
                    basenameid,
                    qname,
                    flag,
                    rname,
                    pos,
                    mapq,
                    cigar,
                    rnext,
                    pnext,
                    tlen,
                    seq,
                    qual,
                    n_m,
                    m_d,
                    a_s,
                    x_s,
                    )

                if args.verbose == "high":
                        print sql; debug()

                sqls.append(sql) # MS
                #cursor.execute(sql)
                #db.commit()

                # print tablename

                # ---------------------------------------------------------------------------

                r_pos = int(record[3]) - 1
                readbases = list(record[9])

                # translate_cigar_mdflag_to_ref(cigar,m_d,r_pos,readbases)

                align_info = translate_cigar_mdflag_to_ref(cigar,
                        m_d, r_pos, readbases)

                # result={"q_start":q_start, "q_stop":q_stop, "q_start_base":r_array[0],"q_stop_base":r_array[-1], "r_start":r_start, "r_stop":r_stop, "r_start_base":r_array[0],"r_stop_base":r_array[-1]  }
                # --------------------------------------------------------------------------- do 5' 3' aligned read base position calc. ########

                tablename = 'last_align_' + qname.rsplit('.', 1)[1]

                # lo.pprint tablename

                if args.verbose == "high":
                    align_message = '%s\n\tAligned:%s:%s-%s (%s) ' \
                        % (qname, rname, align_info['r_start'],
                           align_info['r_stop'], strand)
                    print align_message

                # print line

                # ---------------------------------------------------------------------------I think this is the point we know a read is aligning.

                # print dbname,qname.split('.')[1]

                sql = 'UPDATE ' + dbname + '.' + qname.split('.')[-1] \
                    + ' SET align=\'1\' WHERE basename_id="%s" ' \
                    % basenameid  # ML

                #sqls.append(sql) # MS
                print sql
                cursor.execute(sql)
                db.commit()

                if flag == 0:  # so it's a primary alignment, POSITIVE strand
                    fiveprimetable = tablename + '_5prime'
                    threeprimetable = tablename + '_3prime'

                    # print "Qs", len(qualscores), int(align_info["q_stop"])
                    # --------------------------------------------------------------------------- five prime

                    valstring = '('
                    valstring += '%s,' % basenameid  # basename_id
                    valstring += '%s,' % refid  # refid
                    valstring += '%s,' % '1'  # alignnum
                    valstring += '%s,' % '0'  # covcount
                    valstring += "\'%s\'," % align_strand  # alignstrand
                    valstring += '%s,' % mapq  # score
                    valstring += '%s,' % align_info['q_start']  # seqpos
                    valstring += '%s,' % align_info['r_start']  # refpos
                    valstring += "\'%s\'," % align_info['q_start_base']  # seqbase
                    valstring += "\'%s\'," % align_info['r_start_base']  # refbase
                    valstring += '%s,' \
                        % qualscores[int(align_info['q_start']) - 1]  # seqbasequal
                    valstring += '%s' % '7'  # cigarclass
                    valstring += ')'

                    # primes[tablename]['fiveprime']['string']=valstring

                    sql = 'INSERT INTO %s (%s) VALUES %s' \
                        % (fiveprimetable, colstring, valstring)

                    if args.verbose == "high": print sql; debug()

                    sqls.append(sql) # MS
                    #cursor.execute(sql)
                    #db.commit()

                    # --------------------------------------------------------------------------- three prime

                    valstring = '('
                    valstring += '%s,' % basenameid  # basename_id
                    valstring += '%s,' % refid  # refid
                    valstring += '%s,' % '1'  # alignnum
                    valstring += '%s,' % '0'  # covcount
                    valstring += "\'%s\'," % align_strand  # alignstrand
                    valstring += '%s,' % mapq  # score
                    valstring += '%s,' % align_info['q_stop']  # seqpos
                    valstring += '%s,' % align_info['r_stop']  # refpos
                    valstring += "\'%s\'," % align_info['q_stop_base']  # seqbase
                    valstring += "\'%s\'," % align_info['r_stop_base']  # refbase
                    valstring += '%s,' \
                        % qualscores[int(align_info['q_stop']) - 1]  # seqbasequal
                    valstring += '%s' % '7'  # cigarclass
                    valstring += ')'
                    sql = 'INSERT INTO %s (%s) VALUES %s' \
                        % (threeprimetable, colstring, valstring)

                    if args.verbose == "high": print sql, debug()

                    sqls.append(sql) # MS
                    #cursor.execute(sql)
                    #db.commit()

                if flag == 16:  # It's a primary alignment on the NEGATIVE strand
                    fiveprimetable = tablename + '_5prime'
                    threeprimetable = tablename + '_3prime'

                    # print "Qs", len(qualscores), int(align_info["q_stop"])
                    # --------------------------------------------------------------------------- five prime

                    valstring = '('
                    valstring += '%s,' % basenameid  # basename_id
                    valstring += '%s,' % refid  # refid
                    valstring += '%s,' % '1'  # alignnum
                    valstring += '%s,' % '0'  # covcount
                    valstring += "\'%s\'," % align_strand  # alignstrand
                    valstring += '%s,' % mapq  # score
                    valstring += '%s,' % align_info['q_start']  # seqpos
                    valstring += '%s,' % align_info['r_stop']  # refpos
                    valstring += "\'%s\'," % align_info['q_start_base']  # seqbase
                    valstring += "\'%s\'," % align_info['r_stop_base']  # refbase
                    valstring += '%s,' \
                        % qualscores[int(align_info['q_stop']) - 1]  # seqbasequal
                    valstring += '%s' % '7'  # cigarclass
                    valstring += ')'

                    # primes[tablename]['fiveprime']['string']=valstring

                    sql = 'INSERT INTO %s (%s) VALUES %s' \
                        % (fiveprimetable, colstring, valstring)

                    if args.verbose == "high": print sql; debug()

                    sqls.append(sql) # MS
                    #cursor.execute(sql)
                    #db.commit()

                    # --------------------------------------------------------------------------- three prime

                    valstring = '('
                    valstring += '%s,' % basenameid  # basename_id
                    valstring += '%s,' % refid  # refid
                    valstring += '%s,' % '1'  # alignnum
                    valstring += '%s,' % '0'  # covcount
                    valstring += "\'%s\'," % align_strand  # alignstrand
                    valstring += '%s,' % mapq  # score
                    valstring += '%s,' % align_info['q_stop']  # seqpos
                    valstring += '%s,' % align_info['r_start']  # refpos
                    valstring += "\'%s\'," % align_info['q_stop_base']  # seqbase
                    valstring += "\'%s\'," % align_info['r_start_base']  # refbase
                    valstring += '%s,' \
                        % qualscores[int(align_info['q_stop']) - 1]  # seqbasequal
                    valstring += '%s' % '7'  # cigarclass
                    valstring += ')'
                    sql = 'INSERT INTO %s (%s) VALUES %s' \
                        % (threeprimetable, colstring, valstring)


                    if args.verbose == "high": debug; print sql


                    sqls.append(sql) # MS
                    #cursor.execute(sql)
                    #db.commit()

    #print "align_bwa insterting into database ...."
    #sys.stdout.flush()

    #n = len(sqls)
    #bar = mk_bar(n)
    #bar.start()
    for i,s in enumerate(sqls): # MS
    #    bar.update(i)
        if args.verbose == "high": print s # MS
        cursor.execute(s) # MS
    db.commit() # MS
    sqls = []  # MS
    #bar.finish()


# ---------------------------------------------------------------------------
'''
