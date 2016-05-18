import threading
import time

from align_bwa import do_bwa_align2

class BwaClass():
    """ Threading example class

    The run() method will be started and it will run in the background
    until the application exits.
    """

    def __init__(self, readnumber,timeout):
        """ Constructor

        :type interval: int
        :param interval: Check interval, in seconds
        """
        print "initialising bwa aligner"
        self.processingdictionary=dict()
        self.readnumber = readnumber
        self.timeout = timeout
        self.todo = 0
        self.done = 0
        self.dbname = ""
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

    def setjob(self,
                args,
                ref_fasta_hash,
                connection_pool,
                fastqhash,
                basename,
                basenameid,
                dbname,
            ):
        self.processingdictionary[basenameid]=dict()
        self.processingdictionary[basenameid]["args"]=args
        self.processingdictionary[basenameid]["ref_fasta_hash"]=ref_fasta_hash
        self.processingdictionary[basenameid]["connections"]=connection_pool
        self.processingdictionary[basenameid]["fastqhash"]=fastqhash
        self.processingdictionary[basenameid]["basename"]=basename
        self.processingdictionary[basenameid]["dbname"]=dbname
        self.processingdictionary[basenameid]["time"]=time.time()
        self.todo += 1
        #self.dbname = dbname
        #self.job = task
        #self.color = color

    def run(self):
        """ Method that runs forever """
        timecheck = time.time()
        dbnamecheck = self.dbname
        while True:
            currenttime = time.time()
            #if (len(self.processingdictionary) > 0):
            print "(BWA) Reads to Align:",self.todo,"Aligned:",self.done
            #if self.dbname != dbnamecheck:
            #    print "DATABASE NAME CHANGE"
            if (len(self.processingdictionary) > 0 ) and ((len(self.processingdictionary) > self.readnumber) or (time.time() - timecheck > self.timeout) or (self.dbname != dbnamecheck)):
                ##We want to make a dictionary of shizzle to pass to our new bwa aligner algorithm
                #print "Trying to align"
                mydicttopass=dict()
                custargs = dict()
                count = 0
                tempdbname = ""
                for k,v in self.processingdictionary.items():
                    custargs = v["args"]
                    tempdbname = v["dbname"]
                    if (str(v["dbname"]) == str(dbnamecheck) or dbnamecheck is "") and (count <= self.readnumber):
                        #print "yep"
                    #if  count <= self.readnumber:
                        for seqid in v["fastqhash"].keys():
                            mydicttopass[seqid]=dict()
                            d = 0
                            mydicttopass[seqid]["basename_id"]=k
                            mydicttopass[seqid]["ref_fasta_hash"]=v["ref_fasta_hash"]
                            mydicttopass[seqid]["fastqdata"]=v["fastqhash"][seqid]
                            mydicttopass[seqid]["basename"]=v["basename"]
                            mydicttopass[seqid]["dbname"]=v["dbname"]
                            mydicttopass[seqid]["db"]=v["connections"][d]
                            d += 1
                        del self.processingdictionary[k]
                        #print "deleting"
                        self.todo -= 1
                        self.done += 1
                        dbnamecheck = v["dbname"]
                        count += 1
                    else:
                        #print "nope"
                        #print "Escaping Loop"
                        break
                dbnamecheck = tempdbname
                self.dbname = dbnamecheck
                if len(mydicttopass) > 0:
                    do_bwa_align2(custargs,mydicttopass)
                timecheck = time.time()


            if time.time() - currenttime < 5:
                time.sleep(2)
