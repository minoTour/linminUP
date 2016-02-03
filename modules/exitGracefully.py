import MySQLdb
import time
import os
import sys


def exitGracefully(args, dbcheckhash, minup_version):

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
		"exitGracefully(): line 26, dbcheckhash, key error: " + name
		sys.exit(-1)

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




