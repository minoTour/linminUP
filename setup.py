# --------------------------------------------------
# File Name: setup_0.63.py
# Purpose:
# Creation Date: 20-11-2015
# Last Modified: Wed, Jan 25, 2017  2:40:50 PM
# Author(s): The DeepSEQ Team, University of Nottingham UK
# Copyright 2015 The Author(s) All Rights Reserved
# Credits: 
# --------------------------------------------------

#!/usr/bin/python
# -*- coding: utf-8 -*-

from distutils.core import setup
import py2exe
import sys


sys.path.append('modules')

setup(console=['getmodels.py'
        , 'minUPgui.py'
        , 'mincontrol.py'
        , 'mincontrol_dev.py'
        , 'minUP.py'
        ],
    options = {'py2exe': 
    { 'includes': 
            [ 'h5py.*'
            , "psutil.*"
            , 'urlparse.*'
            , 'cython.*'
            , 'appdirs.*'
            , 'packaging.*'
            , 'twisted.*'
        ]
    , 'dll_excludes': 
        [ 'MSVCP90.dll'
        , 'libgobject-2.0-0.dll'
        , 'libglib-2.0-0.dll'
        , 'libgthread-2.0-0.dll'
        ]
     , 'excludes': 
        [ 'IPython.*'
        , 'tcl.*'
        , 'Tkinter.*'
        , 'scipy.*'
        ]
    }})  


