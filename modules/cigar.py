#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: cigar.py
# Purpose:
# Creation Date: 2015
# Last Modified: Wed, Oct 12, 2016 11:30:25 AM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import re


def translate_cigar_mdflag_to_ref(cigar, m_d, r_start, readbases):

    q_pos = 0
    r_pos = r_start
    r_array = list()
    q_array = list()

    cigparts = re.split('([A-Z])', cigar)
    cigsecs = [cigparts[x:x + 2] for x in xrange(0, len(cigparts) - 1, 2)]

    for cig in cigsecs:

        # print "cigar point:", cigarpoint

        cigartype = cig[1]
        cigarpartbasecount = int(cig[0])
        if cigartype is 'S':  # not aligned read section
            q_pos += cigarpartbasecount

        if cigartype is 'M':  # so its not a deletion or insertion. Its 0:M
            for q in xrange(q_pos, q_pos + cigarpartbasecount):
                q_array.append(readbases[q])
            for r in xrange(r_pos, r_pos + cigarpartbasecount):
                r_array.append('X')
            q_pos += cigarpartbasecount
            r_pos += cigarpartbasecount

        if cigartype is 'I':
            for q in xrange(q_pos, q_pos + cigarpartbasecount):
                q_array.append(readbases[q])
            for r in xrange(r_pos, r_pos + cigarpartbasecount):
                r_array.append('-')
            q_pos += cigarpartbasecount

        if cigartype is 'D':
            for q in xrange(q_pos, q_pos + cigarpartbasecount):
                q_array.append('-')
            for r in xrange(r_pos, r_pos + cigarpartbasecount):

                # rstring+=str(refbases[r])

                r_array.append('X')
            r_pos += cigarpartbasecount

    # print "QUERY:", ''.join(q_array)
    # print "REFF1:", ''.join(r_array)
    # ---------------------------------------------------------------------------

    for x in range(len(q_array)):
        if r_array[x] is not '-':
            if q_array[x] is not '-':
                r_array[x] = q_array[x]

    # print "REFF2:", ''.join(r_array)
    # ---------------------------------------------------------------------------

    mdparts = re.split('(\d+|MD:Z:)', m_d)
    a = 0
    for m in mdparts:
        if m.isdigit():
            for b in range(int(m)):
                if r_array[a + b] is '-':
                    while r_array[a + b] is '-':
                        a += 1
            a += int(m)
        else:

            if m is 'A' or m is 'T' or m is 'C' or m is 'G':
                if r_array[a] is '-':
                    while r_array[a] is '-':
                        a += 1
                r_array[a] = m

                # r_array[a]="o"

                a += 1
            else:

                if m.startswith('^'):
                    ins = list(m[1:])
                    for x in range(len(ins)):
                        r_array[a] = ins[x]

                        # r_array[a]='i'

                        a += 1

    # print "QUERY:", ''.join(q_array)
    # print "REFF3:", ''.join(r_array)
    # # get the first position of the query sequence that is aligned

    q_start = 0
    if cigsecs[0][1] is 'S' or cigsecs[0][1] is 'H':
        q_start = cigsecs[0][0]

    # ---------------------------------------------------------------------------

    q_stop = q_pos
    r_stop = r_pos
    result = {
        'q_start': int(q_start),
        'q_stop': int(q_stop),
        'q_start_base': r_array[0],
        'q_stop_base': r_array[-1],
        'r_start': int(r_start + 1),
        'r_stop': int(r_stop + 1),
        'r_start_base': r_array[0],
        'r_stop_base': r_array[-1],
        }
    return result


# ---------------------------------------------------------------------------
