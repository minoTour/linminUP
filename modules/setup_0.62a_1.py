#!/usr/bin/python
# -*- coding: utf-8 -*-
from distutils.core import setup
import py2exe

# import scipy
# import scipy.linalg.cython_blas
# import scipy.sparse.csgraph._validation
# import mlpy
# import scipy.special

ver = '0.62a'

setup(console=['gui.py', 'mincontrol.py', 'minup.v' + ver + '.py'],
	options = {'py2exe':  \
	{ 'bundle_files': 1 \
	, 'compressed': True \
	, 'includes': [ 'h5py.*' \
    		, 'cython.*' \
    		, 'scipy.linalg.cython_blas' \
    		, 'scipy.linalg.cython_lapack' \
    		, 'scipy.sparse.csgraph._validation' \
    		, 'mlpy.*'] \
	, 'dll_excludes': [ 'MSVCP90.dll' \
		, 'libgobject-2.0-0.dll' \
		, 'libglib-2.0-0.dll' \
		, 'libgthread-2.0-0.dll'] \
        , 'excludes': ['scipy.*' \
		, 'IPython' \
		, 'Tkinter' \
		, 'tcl'] \
	}})  


# ,"scipy.special.*","scipy.linalg.*","scipy.sparse.csgraph._validation"
                                                                 # , "sklearn.*"]
                                                                 # "h5py.*"
                                                                 # , "scipy.special.*"
                                                                 # , "scipy.linalg.*"
                                                                 # , "scipy.special._ufuncs_cxx"
                                                                 # , "scipy.linalg.cython_lapack"
                                                                 # ]

exit()

