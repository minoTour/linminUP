#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: align_dtw.py
# Purpose:
# Creation Date: 2015
# Last Modified: Wed Mar 30 13:56:20 2016
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import numpy as np
import sys
#from visual.graph import gcurve, color

# import sklearn.preprocessing

from mlpy import dtw_subsequence
#from cuDTW import dtw_subsequence
import MySQLdb
import h5py

from hdf5HashUtils import *

# z score

def scale(a):  # MS
    mu = np.mean(a, None)
    sigma = np.std(a)
    return (a - mu) / sigma

# ---------------------------------------------------------------------------

def mysql_load_from_hashes2(
    args, 
    cursorpre,
    tablename,
    data_hash,
    dbpre,
    ):
    cols = list()
    vals = list()
    for (colhead, entry) in data_hash.iteritems():
        if isinstance(entry, basestring):
            vals.append("'%s'" % entry)
        else:
            vals.append(str(entry))
    cols = ','.join(data_hash.keys())
    values = ','.join(vals)
    sql = 'INSERT INTO %s (%s) VALUES (%s) ' % (tablename, cols, values)

    if args.debug is True: print sql

    cursorpre.execute(sql)
    dbpre.commit()
    ids = cursorpre.lastrowid
    return ids


# ---------------------------------------------------------------------------
my_dtw_subsequence = dtw_subsequence 

'''
def my_dtw_subsequence(xs,ys):
	return dtw_subsequence(toInt(xs), toInt(ys))

def rnd(x): return int(round(x*1000,0))

def toInt(xs): return map(rnd, xs)
'''



# MS lines 74 - 163
def squiggle_align(args, squiggle, hashthang): 
    winSz = 256

    offset = 100
    squiggle = squiggle[offset:]

    if squiggle == []: 
	print "squiggle_align(): Error -- squiggle == [] "
	sys.exit()

   
    if args.debug is True: 
    	print "Squiggle len: ", len(squiggle), 
    	print "qryStartEnd: ", args.qryStartEnd

	# ?? ...
    if len(squiggle) >= 2*winSz \
		and args.qryStartEnd is True: 
	# Align just start and end of query...
	if args.debug is True: print "Align just start and end of query..."
    	res = squiggle_search3(args, winSz, squiggle, hashthang)
    else:
	# Align whole query ....
	if args.debug is True: print "Align whole query ...."
	res = squiggle_search2(args, squiggle, hashthang)

    return res
	


def squiggle_search3(args, winSz, squiggle, hashthang): 
    if args.debug is True:
	print "squiggle_search3 called..."
    #refLen = len(hashthang[ref]['Fprime'])

    # get result for start of query ..
    squiggleStart = squiggle[:winSz]
    qryStartResults = squiggle_search2(args, squiggleStart, hashthang)
    seqmatchname, distance, fr, rs, re, qs, qe = qryStartResults

    # get result for end of query ...
    squiggleEnd = squiggle[-winSz:]
    qryEndResults = squiggle_search2(args, squiggleEnd, hashthang)
    seqmatchname_, distance_, fr_, rs_, re_, qs_, qe_ = qryEndResults

    if args.debug is True:
    	print "Len squiggle:" + str(len(squiggle))
    	print "qryStartRes: " + str(qryStartResults[1:])
    	print "qryEndRes:   " + str(qryEndResults[1:])

    # TODO CHECK MATCH TEST LOGIC ...

    resultList = []

    # Matching on F only ...
    if( seqmatchname == seqmatchname_
	and fr=="F" 
	and fr == fr_

	and qs <= qe
	and qs_ <= qe_
	and rs <= re
	and rs_ <= re_
	# ... obviously

	and rs <= rs_ 
	and re <= rs_ # MS >= ??
	):
	  resultList = (seqmatchname
		, distance + distance_
		, fr
		, rs
		, re_
		, qs
		, qe_)
	  if args.debug is True:
	  	print "squiggle_seqrch3(): SUCCESS F ...."

    # Matching on R only ...
    elif( seqmatchname == seqmatchname_
	and fr=="R" 
	and fr == fr_

	and qs <= qe
	and qs_ <= qe_
	and rs <= re
	and rs_ <= re_
	# ... obviously

	and rs >= rs_ 
	and re >= rs_ 
	):
	  resultList = (seqmatchname
		, distance + distance_
		, fr
		, rs
		, re_
		, qs
		, qe_)
	  if args.debug is True:
	  	print "squiggle_seqrch3(): SUCCESS R ...."

    # Else FAIL ...
    else:
	if args.debug is True:
		print "squiggle_seqrch3(): FAIL ??...."
	# sys.exit()

    return resultList


def squiggle_search2(args, squiggle, hashthang):

    result = []
    if args.debug is True:
    	print 'Squiggle search called'

    # print "hashthang"+ hashthang

    for ref in hashthang:
	'''
	# MS -- NB sklearn breaks Ctrl^C from GUI 
	# so we use own z-score routines.
	# 
        # print "We're in the kmerhash loop"
        # print ref
        # print len(kmerhash2[id][ref]['F'])
        # queryfile=str(channel_id)+"_"+str(read_id)+"_query.bin"
        # -------- We are going to normalise this sequence
        # -------- with the sklearn preprocessing algorithm
        # -------- to see what happens.
        # queryarray = sklearn.preprocessing.scale(np.array(squiggle),axis=0,with_mean=True,with_std=False,copy=True)
	#
	'''

	# MS Maybe do this for whole squiggle ...  ??
        queryarray = scale(np.array(squiggle))  
			# ,axis=0,with_mean=True,with_std=False,copy=True)

	'''
	# MS -- Experimental qry scaling  by ref max.
	# Scale qry by max(ref)
	mx = np.max(refarray)
	#iqr = np.subtract(*np.percentile(refarray, [75, 25]))

	scalingFactor = mx # iqr # 3 # 1.2 # MS
	print "Scaling Factor: ", scalingFactor
	queryarray *= scalingFactor
	'''

	# --------------------------------------------------------
	# Forwards  ....

	refarray = hashthang[ref]['Fprime']


        (dist, cost, path) = my_dtw_subsequence(queryarray, refarray)
        result.append((
            dist, 	# distance
            ref, 	# seqmatchname
            'F', 	# for/rev
            path[1][0], # refstart
            path[1][-1],# refend
            path[0][0], # querystart
            path[0][-1],# queryend
            ))
	# --------------------------------------------------------
	# And the other way round ....

	refarray = hashthang[ref]['Rprime']

        (dist, cost, path) = my_dtw_subsequence(queryarray, refarray)
        result.append((
            dist,
            ref,
            'R',
            len(hashthang[ref]['Rprime']) - path[1][-1],
            len(hashthang[ref]['Rprime']) - path[1][0],
            path[0][0],
            path[0][-1],
            ))

	# --------------------------------------------------------

	'''
        # -------- We could correct for the reverse read here,
        # -------- so provide forward co-ordinates?
        # print "Ref len", len(kmerhash[ref]['Rprime'])
        # print "Reverse match coords", path[1][0],path[1][-1]
        # print "Corrected coords", (len(kmerhash[ref]['Rprime'])-path[1][-1]),(len(kmerhash[ref]['Rprime'])-path[1][0])
        # result.append((dist,ref,"R",path[1][0],path[1][-1],path[0][0],path[0][-1]))

    # -------- ('gi|10141003|gb|AF086833.2|', 368.20863807089216, 'R', 1658, 2038, 0, 1058)
    # -------- returning seqmatchname,distance,for/rev,refstart,refend,querystart,queryend
	'''

    #print 'Squiggle Search finished'
    srt =  sorted(result, key=lambda result: result[0] )[0]
    return srt[1], srt[0], srt[2], srt[3], srt[4], srt[5], srt[6]


# ---------------------------------------------------------------------------

def mp_worker((filename,kmerhashT,kmerhashC,time,rawbasename_id,db_name, args)):
    	if args.debug is True:
		print "mp_worker called ..."
	dbpre = MySQLdb.connect(host=args.dbhost, user=args.dbusername, passwd=args.dbpass, port=args.dbport)
	cursorpre = dbpre.cursor()
	if args.debug is True: 
		print "align_dtw: ",filename
		#print "kmerhashT",type(kmerhashT)
		#print kmerhashT
		#print "kmerhashC",type(kmerhashC)
		print "time",time
		print "raw_id",rawbasename_id
		print "db_name",db_name

	sql = "use %s" % (db_name)
	cursorpre.execute(sql)
	dbpre.commit()

	#print "**** Database name is ",db_name
	try:
		#if args.debug is True:
			#print "Read start time",readstarttime
			#print "Elapsed time since read=",(time.time()-readstarttime)
			#squiggle = extractsquig(data.events)
			#print data.events[0].start
			#result = 'bernard'
		hdf = h5py.File(filename, 'r')
			## Need to harvest the squiggles.
		for element in hdf['Analyses/EventDetection_000/Reads']:
			for thing in hdf['Analyses/EventDetection_000/Reads/'+element]:
					#Here we want to recover a list of means for the read.
				meansquiggle=list()
					#try:
	#			print 'Analyses/EventDetection_000/Reads/'+element+'/'+thing
				for but in hdf['Analyses/EventDetection_000/Reads/'+element+'/'+thing]:
					meansquiggle.append(float(but[2]))
					#print len(meansquiggle)
				read_id_fields = ['duration','hairpin_found','hairpin_event_index','read_number','scaling_used','start_mux','start_time',]
				read_info_hash =  make_hdf5_object_attr_hash(args, hdf['Analyses/EventDetection_000/Reads/'+element],read_id_fields)
		#			print read_info_hash['hairpin_found']
				if read_info_hash['hairpin_found']==1:
						#we need to split the list by the hairpin position and then map it
					#print "Hairpin found at",read_info_hash['hairpin_event_index']
					(seqmatchnameT,distanceT,frT,rsT,reT,qsT,qeT) = squiggle_align(args, meansquiggle[0:read_info_hash['hairpin_event_index']],kmerhashT)
	#				print (seqmatchnameT,distanceT,frT,rsT,reT,qsT,qeT)
						#print squiggle_align(args, meansquiggle[0:read_info_hash['hairpin_event_index']],kmerhash)
					(seqmatchnameC,distanceC,frC,rsC,reC,qsC,qeC) = squiggle_align(args, meansquiggle[read_info_hash['hairpin_event_index']:len(meansquiggle)],kmerhashC)
					#print (seqmatchnameC,distanceC,frC,rsC,reC,qsC,qeC)
					### If the forward and reverse reads map appropriately and overlap to the reference we upload template,complement and 2d. But what coordinate do we give for the 2D? Perhaps the overlap region?
					if (seqmatchnameC==seqmatchnameT and frT != frC and reC >= rsT and rsC <= reT):
						#print "Candidate"
						if (rsT < rsC):
							start = rsT
						else:
							start = rsC
						if (reT > reC):
							end = reT
						else:
							end = reC

						squiggle_hash=dict()
						squiggle_hash.update({'basename_id':rawbasename_id})
						#### HORRID FIX BUT IM TOO TIRED TO DO IT WELL
						squiggle_hash.update({'refid':'1'})
#						squiggle_hash.update({'refid':seqmatchnameT})
						squiggle_hash.update({'alignstrand':frT})
						squiggle_hash.update({'r_start':start})
						squiggle_hash.update({'q_start':qsT})
						squiggle_hash.update({'r_align_len':(end-start+1)})
						squiggle_hash.update({'q_align_len':(qeT-qsT+1)})
						mysql_load_from_hashes2(args, cursorpre,"pre_align_2d",squiggle_hash,dbpre)


					### If the forward and reverse reads do not map appropriately to the reference then we only upload the template and complement mappings - even if both are on the same strand?
					squiggle_hash=dict()
					squiggle_hash.update({'basename_id':rawbasename_id})
					#### HORRID FIX BUT IM TOO TIRED TO DO IT WELL
					squiggle_hash.update({'refid':'1'})
#					squiggle_hash.update({'refid':seqmatchnameT})
					squiggle_hash.update({'alignstrand':frT})
					squiggle_hash.update({'r_start':rsT})
					squiggle_hash.update({'q_start':qsT})
					squiggle_hash.update({'r_align_len':(reT-rsT+1)})
					squiggle_hash.update({'q_align_len':(qeT-qsT+1)})
					mysql_load_from_hashes2(args, cursorpre,"pre_align_template",squiggle_hash,dbpre)
					squiggle_hash=dict()
					squiggle_hash.update({'basename_id':rawbasename_id})
					#### HORRID FIX BUT IM TOO TIRED TO DO IT WELL
					squiggle_hash.update({'refid':'1'})
#					squiggle_hash.update({'refid':seqmatchnameT})
					squiggle_hash.update({'alignstrand':frC})
					squiggle_hash.update({'r_start':rsC})
					squiggle_hash.update({'q_start':qsC})
					squiggle_hash.update({'r_align_len':(reC-rsC+1)})
					squiggle_hash.update({'q_align_len':(qeC-qsC+1)})
					mysql_load_from_hashes2(args, cursorpre,"pre_align_complement",squiggle_hash,dbpre)

					### If the forward and reverse reads do not map appropriately to the reference then we only upload the template and complement mappings - even if both are on the same strand?

				else:
					if args.debug is True: 
						print "No Hairpin"

					(seqmatchnameT,distanceT,frT,rsT,reT,qsT,qeT) = squiggle_align(args, meansquiggle,kmerhashT)
					squiggle_hash=dict()
					squiggle_hash.update({'basename_id':rawbasename_id})
					#### HORRID FIX BUT IM TOO TIRED TO DO IT WELL
					squiggle_hash.update({'refid':'1'})
#					squiggle_hash.update({'refid':seqmatchnameT})
					squiggle_hash.update({'alignstrand':frT})
					squiggle_hash.update({'r_start':rsT})
					squiggle_hash.update({'q_start':qsT})
					squiggle_hash.update({'r_align_len':(reT-rsT+1)})
					squiggle_hash.update({'q_align_len':(qeT-qsT+1)})
					mysql_load_from_hashes2(args, cursorpre,"pre_align_template",squiggle_hash,dbpre)

		###In this case we just want to insert the result into the database depending on the orientation
		#squiggleres = squiggle_align(args, squiggle,channel_id,data.read_id,kmerhash,seqlen)
		#print squiggleres
		#result = go_or_no(squiggleres[0],squiggleres[2],squiggleres[3],seqlen)
		#print "result:",result


		hdf.close()
		#return result,channel_id,data.read_id,data.events[0].start,squiggleres
	except Exception, err:
		err_string="Time Warping Stuff : %s" % ( err)
		print >>sys.stderr, err_string
	if args.debug is True: print "align_dtw finished: ",filename
	return (filename)
