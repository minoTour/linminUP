import MySQLdb
import time
import os
import sys
import psutil
import ctypes


def terminateMinup(args, dbcheckhash, oper, minup_version):
    sys.stdout.flush()

    # Sign off any mySQL connections ...
    exitGracefully(args, dbcheckhash, minup_version) 

    print "terminating sub-processes...."

    pid = os.getpid() 

    # Tell minup to terminate
    if oper == "windows":
    	# -- sending minup pid a Ctrl-C signal 
    	# -- this also cleanly closes subprocesses and threads ....
    	ctypes.windll.kernel32.GenerateConsoleCtrlEvent(0, pid) # 0 => Ctrl-C
    else:
	process = psutil.Process(pid)
	for proc in process.children(recursive=True):
                proc.kill()
	process.kill()


    print 'finished.'
    sys.stdout.flush()
    sys.exit(1)

def terminateSubProcesses(args, dbcheckhash, oper, minup_version):
    print "terminating sub-processes...."
    sys.stdout.flush()

    pid = os.getpid() 
    process = psutil.Process(pid)
    for proc in process.children(recursive=True):
      	if oper == "windows":
	  pid_ = proc.as_dict(attrs=['pid'])['pid']
	  # 0 => Ctrl-C
      	  ctypes.windll.kernel32.GenerateConsoleCtrlEvent(0, pid_ )  
    	else: proc.kill()


def terminateMinControl(args, dbcheckhash, oper, minup_version):
    print "terminating sub-processes...."
    sys.stdout.flush()

    pid = os.getpid() 
    process = psutil.Process(pid)
    for proc in process.children(recursive=True):
     if proc.name() == "mincontrol.exe":
        proc.kill()
    '''
      	if oper == "windows":
	  pid_ = proc.as_dict(attrs=['pid'])['pid']
	  # 0 => Ctrl-C
      	  ctypes.windll.kernel32.GenerateConsoleCtrlEvent(0, pid_ )  
    	else: proc.kill()
    '''



def exitGracefully(args, dbcheckhash, minup_version):

    sys.stdout.flush()
  # if dbname is not None:
	#                #print "dbname", dbname

    for name in dbcheckhash['dbname'].keys():
        dba = MySQLdb.connect(host=args.dbhost, user=args.dbusername,
                              passwd=args.dbpass, port=args.dbport,
                              db=name)
        cur = dba.cursor()
        print 'setting %s to an inactive run' % name
        sql = \
            'UPDATE Gru.minIONruns SET activeflag=\'0\' WHERE runname="%s" ' \
            % name
        cur.execute(sql)
        dba.commit()

        try: runindex = dbcheckhash['runindex'][name] # MS .. 
	except: 
		print "exitGracefully(): line 26, dbcheckhash, key error: " + name
		sys.stdout.flush()
		return() # #sys.exit(1)

        finish_time = time.strftime('%Y-%m-%d %H:%M:%S')
        comment_string = 'minUp version %s finished' % minup_version
        sql = \
            "INSERT INTO Gru.comments (runindex,runname,user_name,comment,name,date) VALUES (%s,'%s','%s','%s','%s','%s') " \
            % (
            runindex,
            name,
            args.minotourusername,
            comment_string,
            args.dbusername,
            finish_time,
            )
        cur.execute(sql)
        dba.commit()

        with open(dbcheckhash['logfile'][name], 'a') as logfilehandle:
            logfilehandle.write('minup finished at:\t%s:\tset to inactive gracefully%s'
                                 % (finish_time, os.linesep))
            logfilehandle.close()
        dba.close()
    sys.stdout.flush()




