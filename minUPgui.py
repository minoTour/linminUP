#!/usr/bin/python
# -*- coding: utf-8 -*-
# --------------------------------------------------
# File Name: gui.py
# Purpose:
# Creation Date: 04-11-2015
# Last Modified: Thu Mar  3 14:29:06 2016
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

from gooey import Gooey, GooeyParser
import argparse
import os
import sys
import json
import subprocess
import ctypes
import time
import psutil

import datetime
dateTime = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')

# Unbuffered IO
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)

# ------------------------------------------------------------------------------

pid = 0 # os.getpid() # MS -- !!! this breaks ctrl-c !!!


def run(cmd):
    global pid

    outF = open('log','w')
    outF.write(cmd+"\n")

    exit_code = 0
    print 'Run started ....'
    p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)

          # , bufsize=0)

    pid = p.pid
    outF.write("PID: " + str(pid))


    while p.poll() is None:
        out = p.stdout.read(1)
        sys.stdout.write(out)
	outF.write(out)

    outF.close()

    print ' '
    print 'Done!'
    time.sleep(1)

    

    sys.exit(1)  # Run stopped error...


def kill(pid):
    #s.kill(pid, signal.CTRL_C_EVENT)
    ctypes.windll.kernel32.GenerateConsoleCtrlEvent(0, pid)  # 0 => Ctrl-C


# ------------------------------------------------------------------------------

# Read params.csv

f = 'params.csv'
h = open(f, 'r')
xs = []
for line in h:
    if len(line) > 1:
        xs.append(line[:-1].split(','))
h.close()

settings = {}
try:
    with open('settings.json', 'r') as f:
        settings = json.load(f)
except:
    pass

lut = dict([(x[4], x[2]) for x in xs[1:]])


# ------------------------------------------------------------------------------

def mkWidget(parser, xs, settings):
    (
        omit,
        grpLabel,
        shrt,
        lng,
        desc,
        store,
        typ,
        target,
        req,
        dflt,
        options,
        details,
        ) = xs

    try:
        v = settings[desc]
    except:
        v = dflt

    if omit == '0':
        if typ == 'dir':
            parser.add_argument(
                '-' + shrt,
                '--' + lng,
                widget='DirChooser',
                required=req == 'TRUE',
                dest=desc,
                default=v,
                help=details,
                )
        if typ == 'file':
            parser.add_argument(
                '-' + shrt,
                '--' + lng,
                widget='FileChooser',
                dest=desc,
                default=v,
                help=details,
                )
        if typ == 'str' and shrt == 'cs':

        # Comment String -- set to date by default

            parser.add_argument(
                '-' + shrt,
                '--' + lng,
                type=str,
                dest=desc,
                default='"' + dateTime + '"',
                help=details,
                )
        if typ == 'str' and not shrt == 'cs':
            parser.add_argument(
                '-' + shrt,
                '--' + lng,
                type=str,
                dest=desc,
                default=v,
                help=details,
                )

        if typ == 'bool':
            parser.add_argument(  # , required=req=='TRUE'
                '-' + shrt,
                '--' + lng,
                action='store_true',
                dest=desc,
                default=False,
                help=details,
                )
        if typ == 'int':
            parser.add_argument(
                '-' + shrt,
                '--' + lng,
                type=int,
                dest=desc,
                default=v,
                help=details,
                )
        if typ == 'dropdown':
            parser.add_argument(
                '-' + shrt,
                '--' + lng,
                dest=desc,
                choices=options.split(';'),
                default=v,
                help=details,
                )
    return ()


def toStr(x):
    if x == None:
        return "''"

    # if isinstance(x[0], basestring): ....return '_'.join(x)

    if isinstance(x, basestring):
        return x
    else:
        return str(x)


def isActive((k, v)):
    return not v in ['None', 'none', 'FALSE', 'False', '-1']


def showParam((k, v)):
    if v == 'True':
        return k
    else:
        return ' '.join([k, toStr(v)])


def fixAligner((k, v)):
    if v == 'none':
        return (k, 'FALSE')
    if k == '-a':
        k = '-' + v
        v = ''
    return (k, v)


def fixAlignerOpts(aligner, (k, v)):
    if k == '-opts':
        if aligner == 'none':
            return (k, 'FALSE')
        k = '-' + aligner + 'opts'
    return (k, v)


# ------------------------------------------------------------------------------

@Gooey(
    program_name='minUP',
    advanced=1,
    language='english',
    show_config=True,
    default_size=(1000, 600),
    required_cols=2,
    optional_cols=3,
    dump_build_config=0,
    )
def main():

    print 'minUP GUI'
    desc = \
        'A program to analyse minION fast5 files in real-time or post-run.'
    print desc

    parser = GooeyParser(description=desc)
    for x in xs[1:]:
        if len(x) > 1:
            mkWidget(parser, x, settings)

  # This is updated every time to update the values of the variables...

    try:
        vs = vars(parser.parse_args())
        with open('settings.json', 'w') as f:

          # print json.dumps(vs, indent=4, sort_keys=True)

            f.write(json.dumps(vs, indent=4, sort_keys=True))
    except:
        return ()

  # Build a dict of the var:vals

    print '------------------'
    ps = []
    for k in vs:
        ps.append(('-' + lut[k], toStr(vs[k])))

    ps = map(fixAligner, ps)
    aligner = vs['Aligner_to_Use']
    ps = map(lambda o: fixAlignerOpts(aligner, o), ps)
    ps = sorted(filter(isActive, ps))
    params = ' '.join(map(showParam, ps))

  # cmd = 'ls /a'
  # cmd = 'c:\Python27\python.exe .\minup.v0.63.py ' +params

    cmd = '.\\minUP.exe ' + params  # + ' 2>&1'
    print cmd

    '''
    fl = open("cmd.sh", 'w')
    fl.write(cmd)
    fl.close()
    '''
    run(cmd)

# ------------------------------------------------------------------------------

if __name__ == '__main__':
    main()
    if KeyboardInterrupt:
        kill(pid)

	#time.sleep(3)
	#current_process = psutil.Process(os.getpid())
	#for child in current_process.children(recursive=True):
	#	child.kill()
    

