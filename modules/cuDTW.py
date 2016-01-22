# --------------------------------------------------
# File Name: cuDTW.py
# Purpose:
# Creation Date: 13-10-2015
# Last Modified: Mon Nov 23 20:09:25 2015
# Author(s): Mike Stout 
# Copyright 2015 The Author(s) All Rights Reserved
# Credits:
# --------------------------------------------------

import sys
import numpy as np
from visual.graph import *

#------------------------------------------------------------------------------
# CUDA STUFF

import pycuda.driver as drv
import pycuda.autoinit
from pycuda.elementwise import ElementwiseKernel
import pycuda.gpuarray as gpuarray

devId = 0 
dev = drv.Device(devId)
cxt = dev.make_context()

def togpu(a):
        return gpuarray.to_gpu(np.array(a, dtype=np.float32))

def togpuint(a):
        return gpuarray.to_gpu(np.array(a, dtype=np.int16))

#------------------------------------------------------------------------------
# Create GPU arrays ....

qrySz = 32 # 128
refSz = 2**12 

sz = qrySz * refSz
dist = togpu(xrange(sz))*0
print len(dist)
cost = dist*0
#path = dist*0
px = togpuint(xrange(qrySz))*0
py = togpuint(xrange(refSz))*0

def initGPUArrays(h,w):
	dist = togpu(xrange(h*w))*0 
	cost = dist*0

	# Worst case path ...
	px = togpuint(xrange(w*10))*0
	py = togpuint(xrange(h*10))*0

#------------------------------------------------------------------------------
# CUDA Kernels

# OK Here we go ....
# Based on: http://jeremykun.com/2012/07/25/dynamic-time-warping/


cuDistInt = ElementwiseKernel(
   "int *dist, int *qry, int *ref, int w,  int h",
   "int row = i % w; \
    int col = (i / w) % h; \
        \
    int x = qry[row] - ref[col]; \
    dist[i] = abs(x); \
    ",
   "cuDistInt")

cuDTW_init_int = ElementwiseKernel(
   "int *cost, int *dist, int w, int h",
   "int row = i / w; \
    int col = i % w; \
    if (row==0)  \
	for (int j=0; j<=col; j++) \
		cost[i] += dist[row*w + j]; \
    if (col==0) \
	for (int j=0; j<=row; j++) \
		cost[i] += dist[j*w + col]; \
    ",
   "cuDTW_init_int")

cuDTW_int = ElementwiseKernel(
   "int *cost, int *dist, int w, int h",
   "int row = i / w; \
    int col = i % w; \
    int costDown = 0; \
    int costLeft = 0; \
    int costDiag = 0; \
    int choice = 0; \
	\
    for (int j=1; j<=row; j++) { \
     for (int k=1; k<=col; k++) { \
	\
    	costLeft = cost[ ((j-1)*w) + k ]; \
    	costDown = cost[ (j*w) + (k-1) ]; \
    	costDiag = cost[ ((j-1)*w) + (k-1) ]; \
    	choice = min( costDown, min( costLeft , costDiag)); \
		\
    	cost[j*w +k] = choice + dist[j*w + k]; \
    }}",
   "cuDTW_int")

cuLCP_int = ElementwiseKernel(
   "int *px, int *py, int *cost, int w, int h, int tau",
   "int k = i%w; \
    int j = h-1; \
    int costDown = 0; \
    int costLeft = 0; \
    int costDiag = 0; \
    int choice = 0; \
    px[i] = k; \
    py[i] = j; \
    while (k>=0 && j>=0) { \
    	costLeft = cost[ (j*w) + (k-1) ]; \
    	costDown = cost[ ((j-1)*w) + k ]; \
    	costDiag = cost[ ((j-1)*w) + (k-1) ]; \
		\
    	choice = min( costDown, min( costLeft , costDiag)); \
		\
    	if (choice == costDiag) { k-=1; j-=1; } \
        else { if (choice == costDown) j-=1; \
    	  else if (choice == costLeft) k-=1; }\
     	if (i==j) { px[i]=k; py[i]=j; }; \
     }",
   "cuLCP_int")
	#if (cost[j*w] < tau) path[j*w +k] = 1; \

#------------------------------------------------------------------------------
cuDist = ElementwiseKernel(
   "float *dist, float *qry, float *ref, int w,  int h",
   "int row = i % w; \
    int col = (i / w) % h; \
        \
    float x = qry[row] - ref[col]; \
    dist[i] = abs(x); \
    ",
   "cuDist")

cuDTW_init = ElementwiseKernel(
   "float *cost, float *dist, int w, int h",
   "int row = i % h; \
    int col = i / h; \
    if (row==0)  \
	for (int j=0; j<=col; j++) \
		cost[i] += dist[row*w + j]; \
    if (col==0) \
	for (int j=0; j<=row; j++) \
		cost[i] += dist[j*w + col]; \
    ",
   "cuDTW_init")

cuDTW = ElementwiseKernel(
   "float *cost, float *dist, int w, int h",
   "int row = i % h; \
    int col = i / h; \
    float costDown = 0.0; \
    float costLeft = 0.0; \
    float costDiag = 0.0; \
    float choice = 0.0; \
	\
    for (int j=1; j<=row; j++) { \
     for (int k=1; k<=col; k++) { \
	\
    	costLeft = cost[ ((j-1)*w) + k ]; \
    	costDown = cost[ (j*w) + (k-1) ]; \
    	costDiag = cost[ ((j-1)*w) + (k-1) ]; \
    	choice = min( costDown, min( costLeft , costDiag)); \
		\
    	cost[j*w +k] = choice + dist[j*w + k]; \
    }}",
   "cuDTW")

cuLCP = ElementwiseKernel(
   "int *px, int *py, float *cost, int w, int h, float tau",
   "int k = i%w; \
    int j = h-1; \
    float costDown = 0.0; \
    float costLeft = 0.0; \
    float costDiag = 0.0; \
    float choice = 0.0; \
    px[i] = k; \
    py[i] = j; \
    while (k>=0 && j>=0) { \
    	costLeft = cost[ (j*w) + (k-1) ]; \
    	costDown = cost[ ((j-1)*w) + k ]; \
    	costDiag = cost[ ((j-1)*w) + (k-1) ]; \
		\
    	choice = min( costDown, min( costLeft , costDiag)); \
		\
    	if (choice == costDiag) { k-=1; j-=1; } \
        else { if (choice == costDown) j-=1; \
    	  else if (choice == costLeft) k-=1; }\
     	if (i==j) { px[i]=k; py[i]=j; }; \
     }",
   "cuLCP")
	#if (cost[j*w] < tau) path[j*w +k] = 1; \

#------------------------------------------------------------------------------

def findDTW(x,y, tau):
	h = len(x) # qry size
	w = len(y) # ref size
	print (h,w)
	initGPUArrays(h,w)
	#print px
	#print py
	cuDist(dist, x, y, w, h) 
	cuDTW_init(cost*0, dist, w, h) 
	cuDTW(cost, dist, w, h) 
	cuLCP(px, py, cost, w, h, tau) 
	print px
	print py
	print '-'*80

'''
def reset(f1):
	x=len(f1.pos())
	f1.plot(pos=(x,0))
	f1.plot(pos=(0,0), color=color.white)
'''
def toPoints(xs): return zip(xrange(len(xs)), xs)

qryCurve = gcurve(color=color.cyan)
refCurve = gcurve(color=color.red)

def dtw_subsequence(_qry, _ref):
	_ref = _ref[:refSz]
	_qry = _qry[:qrySz]
	
	if len(refCurve.gcurve.pos) is 0: refCurve.plot(pos=toPoints(_ref))

	qryCurve.gcurve.pos=[]
	qryCurve.plot(pos=toPoints(_qry))
	
	qry = togpu(_qry)
	
	#for _ in xrange(10): _ref = np.hstack([_ref,_ref])
	#print len(_ref)
	ref = togpu(_ref)
	#print ref 

	tau = 1. # threshold

	findDTW(qry, ref, tau)
	'''
	#_path = path.get()
	#print path 
	_cost = cost.get()

	_px = px.get()
	_py = py.get()
	return (0, _cost, (_px, _py) )
	'''
	#cxt.pop()

#def dtw_subsequence(_qry, _ref, f1):
#	return cu_dtw_subsequence( _qry,  _ref, f1)

#if KeyboardInterrupt: 
while 1:
	_qry = np.random.uniform(-1,1,qrySz)
	_ref = np.random.uniform(-1,1,refSz)

	dtw_subsequence(_qry, _ref)

cxt.pop
