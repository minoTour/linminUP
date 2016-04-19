#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import re
import time
import errno
from socket import error as socket_error
import threading
import MySQLdb
import configargparse
import urllib2
import json
from ws4py.client.threadedclient import WebSocketClient
from thrift import Thrift
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol


# import memcache

import hashlib

# Unbuffered IO
# sys.stdin = os.fdopen(sys.stdin.fileno(), 'w', 0) # MS
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)  # MS
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', 0)  # MS



config_file = script_dir = os.path.dirname(os.path.realpath('__file__'
        )) + '/' + 'minup_posix.config'
parser = \
    configargparse.ArgParser(description='interaction: A program to provide real time interaction for minION runs.'
                             , default_config_files=[config_file])
parser.add(
    '-dbh',
    '--mysql-host',
    type=str,
    dest='dbhost',
    required=False,
    default='localhost',
    help="The location of the MySQL database. default is 'localhost'.",
    )
parser.add(
    '-dbu',
    '--mysql-username',
    type=str,
    dest='dbusername',
    required=True,
    default=None,
    help='The MySQL username with create & write privileges on MinoTour.'
        ,
    )
parser.add(
    '-dbp',
    '--mysql-port',
    type=int,
    dest='dbport',
    required=False,
    default=3306,
    help="The MySQL port number, else the default port '3306' is used."
        ,
    )
parser.add(
    '-pw',
    '--mysql-password',
    type=str,
    dest='dbpass',
    required=True,
    default=None,
    help='The password for the MySQL username with permission to upload to MinoTour.'
        ,
    )
parser.add(
    '-db',
    '--db-name',
    type=str,
    dest='dbname',
    required=True,
    default=None,
    help='The database being monitored.',
    )
parser.add(
    '-pin',
    '--security-pin',
    type=str,
    dest='pin',
    required=True,
    default=None,
    help='This is a security feature to prevent unauthorised remote control of a minION device. You need to provide a four digit pin number which must be entered on the website to remotely control the minION.'
        ,
    )
parser.add(
    '-ip',
    '--ip-address',
    type=str,
    dest='ip',
    required=True,
    default=None,
    help='The IP address of the minKNOW machine.',
    )
parser.add(
    '-v',
    '--verbose',
    action='store_true',
    help='Display debugging information.',
    default=False,
    dest='verbose',
    )

args = parser.parse_args()

version = '0.2'  # 9th June 2015

### test which version of python we're using

###Machine to connect to address

ipadd = args.ip


def _urlopen(url, *args):
    """Open a URL, without using a proxy for localhost.

    While the no_proxy environment variable or the Windows "Bypass proxy

    server for local addresses" option should be set in a normal proxy

    configuration, the latter does not affect requests by IP address. This

    is apparently "by design" (http://support.microsoft.com/kb/262981).

    This method wraps urllib2.urlopen and disables any set proxy for

    localhost addresses.

    """

    try:

        host = url.get_host().split(':')[0]
    except AttributeError:

        host = urlparse.urlparse(url).netloc.split(':')[0]

    import socket

    # NB: gethostbyname only supports IPv4

    # this works even if host is already an IP address

    addr = socket.gethostbyname(host)

    #if addr.startswith('127.'):

    #    return _no_proxy_opener.open(url, *args)
    #else:

    return urllib2.urlopen(url, *args)


def execute_command_as_string(data, host=None, port=None):

    host_name = host

    port_number = port

    url = 'http://%s:%s%s' % (host_name, port_number, '/jsonrpc')

        # print url

    req = urllib2.Request(url, data=data,
                          headers={'Content-Length': str(len(data)),
                          'Content-Type': 'application/json'})

    try:
        f = _urlopen(req)
    except Exception, err:
        err_string = \
            'Fail to initialise mincontrol. Likely reasons include minKNOW not running, the wrong IP address for the minKNOW server or firewall issues.'
        print err_string, err
    json_respond = json.loads(f.read())

    f.close()

    return json_respond

class DummyClient(WebSocketClient):
    infodict = {}

    def opened(self):
        print "Connection Success"
        #print "Trying \"engine_states\":\"1\",\"channel_states\":\"1\",\"multiplex_states\":\"1\""
        #self.send(json.dumps({'engine_states':'1','channel_states':'1','multiplex_states':'1'}))
        self.send(json.dumps({'engine_states':'1','channel_states':'1'}))
        #self.send(json.dumps({'engine_states':'1'}))
        #self.send(transport.getvalue(), binary=True)
    #    def data_provider():
    #        for i in range(1, 200, 25):
    #            yield "#" * i

    #    self.send(data_provider())

    #    for i in range(0, 200, 25):
    #       print i
    #       self.send("*" * i)

    def closed(self, code, reason=None):
        print "Closed down", code, reason

    def received_message(self, m):
        if not m.is_binary:
            #print "Non binary message"
            #print type(m)
            json_object = json.loads(str(m))
            for element in json_object:
                #print element
                if json_object[element] == "error":
                    continue

                if json_object[element] != "null" and json_object[element] != "error":
                    for bit in json_object[element]:
                        try:
                            if bit in self.infodict:
                                #print "Value Exists"
                                if self.infodict[bit] != json_object[element][bit]:
                                    #queryupdate = "INSERT into messages (message,target,param1,complete) VALUES ('%s', 'details','%s','1')" % (bit,json_object[element][bit])
                                    queryupdate = "UPDATE messages set param1 = '%s' where message = '%s'" %(json_object[element][bit],bit)
                                    #print "Update Value"
                                    if args.verbose is True:
                                        print queryupdate
                                    self.infodict[bit] = json_object[element][bit]
                                    cursor2.execute(queryupdate)
                                    db2.commit()
                                #else:
                                #    print "Value Unchanged",
                            else:
                                #print element, bit, json_object[element][bit]
                                self.infodict[bit]=json_object[element][bit]
                                queryupdate = "INSERT into messages (message,target,param1,complete) VALUES ('%s', 'details','%s','1')" % (bit,json_object[element][bit])
                                cursor2.execute(queryupdate)
                                db2.commit()
                        except:
                            pass
                            #print "d'oh"
                #else:
                #   print "null"
            #print json_object
            #self.close()
            return
        transport = TTransport.TMemoryBuffer(m.data)
        protocol = TCompactProtocol.TCompactProtocol(transport)
        print "Binary Message"
        #self.close()
        #msg = es.ServerMessage()
        #msg.read(protocol)
        #self.received_server_message(msg)

def update_run_scripts():
    get_scripts = \
        '{"id":"1", "method":"get_script_info_list","params":{"state_id":"status"}}'
    results = execute_command_as_string(get_scripts, ipadd, 8000)

    # print get_scripts
    # for key in results.keys():
    #    print "mincontrol:", key, results[key]

    scriptlist = list()
    for element in results['result']:

        # print "Element", results["result"][element]

        for item in results['result'][element]:

            # print "Item", item
            # print item["name"]

            scriptlist.append("('runscript','all','" + item['name']
                              + "',1)")
    recipelist = ','.join(scriptlist)
    sqlinsert = \
        'insert into messages (message,target,param1,complete) VALUES %s' \
        % recipelist
    if args.verbose is True:
        print sqlinsert
    cursor.execute(sqlinsert)
    db.commit()


def send_message(message):
    message_to_send = \
        '{"id":"1", "method":"user_message","params":{"content":"%s"}}' \
        % message
    results = execute_command_as_string(message_to_send, ipadd, 8000)
    return results


def run_analysis():
    keeprunning = 1
    status = \
        '{"id":"1", "method":"get_engine_state","params":{"state_id":"status"}}'
    dataset = \
        '{"id":"1", "method":"get_engine_state","params":{"state_id":"data_set"}}'
    startmessagenew = \
        '{"id":"1", "method":"user_message","params":{"content":"minoTour is now interacting with your run. This is done at your own risk. To stop minoTour interaction with minKnow disable upload of read data to minoTour."}}'
    startmessage = \
        'minoTour is now interacting with your run. This is done at your own risk. To stop minoTour interaction with minKnow disable upload of read data to minoTour.'
    testmessage = 'minoTour is checking communication status.'

    # testmessage = '{"id":"1", "method":"set_engine_state","params":{"state_id":"user_message","value":"minoTour is checking communication status."}}'

    incmessage = 'minoTour shifted the bias voltage by +10 mV.'
    decmessage = 'minoTour shifted the bias voltage by -10 mV.'
    startrun = \
        '{"id":"1", "method":"start_script","params":{"name":"MAP_Lambda_Burn_In_Run_SQK_MAP005.py"}}'
    stoprun = '{"id":"1", "method":"stop_experiment","params":"null"}'
    stopprotocol = \
        '{"id":"1", "method":"stop_script","params":{"name":"MAP_48Hr_Sequencing_Run.py"}}'
    startrunmessage = 'minoTour sent a remote run start command.'

    stoprunmessage = 'minoTour sent a remote run stop command.'
    biasvoltageget = \
        '{"id":"1","method":"board_command_ex","params":{"command":"get_bias_voltage"}}'

    bias_voltage_gain = \
        '{"id":"1","method":"get_engine_state","params":{"state_id":"bias_voltage_gain"}}'

    bias_voltage_set = \
        '{"id":"1","method":"board_command_ex","params":{"command":"set_bias_voltage","parameters":"-120"}}'

    machine_id = \
        '{"id":"1","method":"get_engine_state","params":{"state_id":"machine_id"}}'
    machine_name = \
        '{"id":"1","method":"get_engine_state","params":{"state_id":"machine_name"}}'
    sample_id = \
        '{"id":"1","method":"get_engine_state","params":{"state_id":"sample_id"}}'
    user_error = \
        '{"id":"1","method":"get_engine_state","params":{"state_id":"user_error"}}'
    sequenced_res = \
        '{"id":"1","method":"get_engine_state","params":{"state_id":"sequenced"}}'
    yield_res = \
        '{"id":"1","method":"get_engine_state","params":{"state_id":"yield"}}'
    current_script = \
        '{"id":"1","method":"get_engine_state","params":{"state_id":"current_script"}}'

    get_scripts = \
        '{"id":"1", "method":"get_script_info_list","params":{"state_id":"status"}}'

    sqldelete = 'delete from messages'
    cursor.execute(sqldelete)
    db.commit()

    try:
        update_run_scripts()

        # results = execute_command_as_string(get_scripts,ipadd,8000)
        # print get_scripts
        # for key in results.keys():
        #    print "mincontrol:", key, results[key]
        # scriptlist=list()
        # for element in results["result"]:
            # print "Element", results["result"][element]
        #    for item in results["result"][element]:
                # print "Item", item
                # print item["name"]
        #        scriptlist.append(("('runscript','all','"+item["name"]+"',1)"))
        # recipelist = ",".join(scriptlist)
        # sqlinsert = "insert into messages (message,target,param1,complete) VALUES %s" % (recipelist)
        # print sqlinsert
        # cursor.execute(sqlinsert)
        # db.commit()
        # print results["result"]

        results = execute_command_as_string(dataset, ipadd, 8000)

        # print dataset

        for key in results.keys():
            print 'mincontrol:', key, results[key]
        results = execute_command_as_string(startmessagenew, ipadd,
                8000)

        # print startmessagenew

        for key in results.keys():
            print 'mincontrol:', key, results[key]
    except Exception, err:

        # sys.exit()

        print >> sys.stderr, err

    # # We're going to collect some data about the minKNOW installation that we are connecting to:

    try:
        pininsert = \
            "insert into messages (message,target,param1,complete) VALUES ('pin','all','%s','1')" \
            % hashlib.md5(args.pin).hexdigest()
        cursor.execute(pininsert)
        db.commit()
        statusis = execute_command_as_string(status, ipadd, 8000)
        datasetis = execute_command_as_string(dataset, ipadd, 8000)
        machineid = execute_command_as_string(machine_id, ipadd, 8000)
        machinename = execute_command_as_string(machine_name, ipadd,
                8000)
        sampleid = execute_command_as_string(sample_id, ipadd, 8000)
        usererror = execute_command_as_string(user_error, ipadd, 8000)
        sequenced = execute_command_as_string(sequenced_res, ipadd,
                8000)
        yieldres = execute_command_as_string(yield_res, ipadd, 8000)
        currentscript = execute_command_as_string(current_script,
                ipadd, 8000)
        sqlinsert = \
            "insert into messages (message,target,param1,complete) VALUES ('Status','all','%s','1'),('Dataset','all','%s','1'),('Functioning','all','1','1'),('machinename','all','%s','1'),('sampleid','all','%s','1'),('usererror','all','%s','1'),('sequenced','all','%s','1'),('yield','all','%s','1'),('currentscript','all','%s','1')" \
            % (
            statusis['result'],
            datasetis['result'],
            machinename['result'],
            sampleid['result'],
            usererror['result'],
            sequenced['result'],
            yieldres['result'],
            currentscript['result'],
            )
        cursor.execute(sqlinsert)
        db.commit()
        biasresultmessage = execute_command_as_string(biasvoltageget,
                ipadd, 8000)
        biasvoltageoffset = \
            execute_command_as_string(bias_voltage_gain, ipadd, 8000)
        curr_voltage = int(biasresultmessage['result']['bias_voltage']) \
            * int(biasvoltageoffset['result'])
        sqlvoltage = \
            "INSERT into messages (message,target,param1,complete) VALUES ('biasvoltage', 'all','%s','1')" \
            % curr_voltage

        # print sqlvoltage

        cursor.execute(sqlvoltage)
        db.commit()
    except Exception, err:

        print >> sys.stderr, err

                #    sys.exit()
    # print "We're in bad boy"

    while keeprunning == 1:

        # ##Background updates

        statusis = execute_command_as_string(status, ipadd, 8000)

        # print status

        datasetis = execute_command_as_string(dataset, ipadd, 8000)

        # print dataset

        machineid = execute_command_as_string(machine_id, ipadd, 8000)

        # print machine_id

        machinename = execute_command_as_string(machine_name, ipadd,
                8000)

        # print machine_name

        sampleid = execute_command_as_string(sample_id, ipadd, 8000)

        # print sampleid

        usererror = execute_command_as_string(user_error, ipadd, 8000)

        # print usererror

        sequenced = execute_command_as_string(sequenced_res, ipadd,
                8000)

        # print sequenced_res

        yieldres = execute_command_as_string(yield_res, ipadd, 8000)

        # print yield_res

        currentscript = execute_command_as_string(current_script,
                ipadd, 8000)

        # print current_script

        sqlupdate = \
            "UPDATE messages set param1 = '%s' where message='Status'" \
            % statusis['result']
        cursor.execute(sqlupdate)
        sqlupdate = \
            "UPDATE messages set param1 = '%s' where message='Dataset'" \
            % datasetis['result']
        cursor.execute(sqlupdate)
        sqlupdate = \
            "UPDATE messages set param1 = '%s' where message='machinename'" \
            % machinename['result']
        cursor.execute(sqlupdate)
        sqlupdate = \
            "UPDATE messages set param1 = '%s' where message='sampleid'" \
            % sampleid['result']
        cursor.execute(sqlupdate)
        sqlupdate = \
            "UPDATE messages set param1 = '%s' where message='usererror'" \
            % usererror['result']
        cursor.execute(sqlupdate)
        sqlupdate = \
            "UPDATE messages set param1 = '%s' where message='sequenced'" \
            % sequenced['result']
        cursor.execute(sqlupdate)
        sqlupdate = \
            "UPDATE messages set param1 = '%s' where message='yield'" \
            % yieldres['result']
        cursor.execute(sqlupdate)
        sqlupdate = \
            "UPDATE messages set param1 = '%s' where message='currentscript'" \
            % currentscript['result']
        cursor.execute(sqlupdate)
        db.commit()

        biasresultmessage = execute_command_as_string(biasvoltageget,
                ipadd, 8000)
        biasvoltageoffset = \
            execute_command_as_string(bias_voltage_gain, ipadd, 8000)
        curr_voltage = int(biasresultmessage['result']['bias_voltage']) \
            * int(biasvoltageoffset['result'])
        sqlvoltage = \
            "UPDATE messages set param1 = '%s' where message='biasvoltage'" \
            % curr_voltage

        # print sqlvoltage

        cursor.execute(sqlvoltage)
        db.commit()

        # print "Checking commands"

        sqlstart = 'SELECT * FROM interaction where complete != 1'
        cursor.execute(sqlstart)
        db.commit()

        # print "Executing fresh query"
        # print cursor.rowcount
        # for x in xrange(0,cursor.rowcount):

        rows = cursor.fetchall()
        for row in rows:
            print row[0], '-->', row[1], '-->', row[2], '-->', row[3], \
                '-->', row[4], '-->', row[5]
            if row[1] == 'start':
                print 'Starting the minION device.'
                try:
                    startruncustom = \
                        '{"id":"1", "method":"start_script","params":{"name":"' \
                        + row[3] + '.py"}}'
                    startresult = \
                        execute_command_as_string(startruncustom,
                            ipadd, 8000)
                    startresultmessage = send_message(startrunmessage)
                except Exception, ett:
                    print >> sys.stderr, err
                print 'minION device started.'
                sqlstart = \
                    'UPDATE interaction SET complete=1 WHERE job_index="%s" ' \
                    % row[0]
                cursor.execute(sqlstart)
                db.commit()
            elif row[1] == 'stop':

                # break
                # except Exception, err:
                #    print >>sys.stderr, err
                #    sys.exit()

                print 'Stopping the minION device.'
                try:
                    stopresult = execute_command_as_string(stoprun,
                            ipadd, 8000)
                    stopprotocolresult = \
                        execute_command_as_string(stopprotocol, ipadd,
                            8000)
                    stopresultmessage = send_message(stoprunmessage)
                except Exception, err:
                    print >> sys.stderr, err

                print 'minION device stopped.'
                sqlstop = \
                    'UPDATE interaction SET complete=1 WHERE job_index="%s" ' \
                    % row[0]
                cursor.execute(sqlstop)
                db.commit()
            elif row[1] == 'setsamplename':

                # break
                # except Exception, err:
                #    print >>sys.stderr, err
                #    sys.exit()

                print 'Setting the sample name to ' + row[3] + '.'
                try:
                    set_sample_id = \
                        '{"id":"1","method":"set_engine_state","params":{"state_id":"sample_id","value":"' \
                        + row[3] + '"}}'
                    set_sample_id_result = \
                        execute_command_as_string(set_sample_id, ipadd,
                            8000)
                    startresultmessage = \
                        send_message('minoTour renamed the run to '
                            + row[3])
                except Exception, ett:
                    print >> sys.stderr, err
                print 'Run has been renamed to ' + row[3] + '.'
                sqlstart = \
                    'UPDATE interaction SET complete=1 WHERE job_index="%s" ' \
                    % row[0]
                cursor.execute(sqlstart)
                db.commit()
            elif row[1] == 'test':

                # break
                # except Exception, err:
                #    print >>sys.stderr, err
                #    sys.exit()

                print 'Sending a test message to minKNOW.'
                try:
                    testresultmessage = send_message(testmessage)
                except Exception, ett:
                    print >> sys.stderr, err
                print 'Test message sent.'
                sqlstop = \
                    'UPDATE interaction SET complete=1 WHERE job_index="%s" ' \
                    % row[0]
                cursor.execute(sqlstop)
                db.commit()
            elif row[1] == 'biasvoltageget':

                print 'Fetching Bias Voltage'
                try:
                    biasresultmessage = \
                        execute_command_as_string(biasvoltageget,
                            ipadd, 8000)
                    biasvoltageoffset = \
                        execute_command_as_string(bias_voltage_gain,
                            ipadd, 8000)
                    curr_voltage = int(biasresultmessage['result'
                            ]['bias_voltage']) \
                        * int(biasvoltageoffset['result'])
                    sqlvoltage = \
                        "UPDATE messages set param1 = '%s' where message='biasvoltage'" \
                        % curr_voltage

                    # print sqlvoltage

                    cursor.execute(sqlvoltage)
                    db.commit()
                except Exception, err:

                    # print curr_voltage
                    # biasvoltagereset = execute_command_as_string(bias_voltage_set,ipadd,8000)
                    # biasresultmessage2 = execute_command_as_string(biasvoltageget,ipadd,8000)
                    # biasvoltageoffset2 = execute_command_as_string(bias_voltage_gain,ipadd,8000)

                    print >> sys.stderr, err
                    continue
                print biasresultmessage
                print biasvoltageoffset

                # print biasvoltagereset
                # print biasresultmessage2
                # print biasvoltageoffset2

                sqlstop = \
                    'UPDATE interaction SET complete=1 WHERE job_index="%s" ' \
                    % row[0]
                cursor.execute(sqlstop)
                db.commit()
            elif row[1] == 'biasvoltageinc':
                print 'Incrementing Bias Voltage'
                try:
                    biasresultmessage = \
                        execute_command_as_string(biasvoltageget,
                            ipadd, 8000)
                    biasvoltageoffset = \
                        execute_command_as_string(bias_voltage_gain,
                            ipadd, 8000)
                    curr_voltage = int(biasresultmessage['result'
                            ]['bias_voltage']) \
                        * int(biasvoltageoffset['result']) + 10
                    bias_voltage_inc = \
                        '{"id":"1","method":"board_command_ex","params":{"command":"set_bias_voltage","parameters":"%s"}}' \
                        % curr_voltage
                    biasvoltagereset = \
                        execute_command_as_string(bias_voltage_inc,
                            ipadd, 8000)
                    biasresultmessage = \
                        execute_command_as_string(biasvoltageget,
                            ipadd, 8000)
                    biasvoltageoffset = \
                        execute_command_as_string(bias_voltage_gain,
                            ipadd, 8000)
                    curr_voltage = int(biasresultmessage['result'
                            ]['bias_voltage']) \
                        * int(biasvoltageoffset['result'])
                    sqlvoltage = \
                        "UPDATE messages set param1 = '%s' where message='biasvoltage'" \
                        % curr_voltage

                    # print sqlvoltage

                    cursor.execute(sqlvoltage)
                    db.commit()
                    incresultmessage = send_message(incmessage)
                except Exception, err:
                    print >> sys.stderr, err
                    continue
                sqlstop = \
                    'UPDATE interaction SET complete=1 WHERE job_index="%s" ' \
                    % row[0]
                cursor.execute(sqlstop)
                db.commit()
            elif row[1] == 'biasvoltagedec':
                print 'Decreasing Bias Voltage'
                try:
                    biasresultmessage = \
                        execute_command_as_string(biasvoltageget,
                            ipadd, 8000)
                    biasvoltageoffset = \
                        execute_command_as_string(bias_voltage_gain,
                            ipadd, 8000)
                    curr_voltage = int(biasresultmessage['result'
                            ]['bias_voltage']) \
                        * int(biasvoltageoffset['result']) - 10
                    bias_voltage_dec = \
                        '{"id":"1","method":"board_command_ex","params":{"command":"set_bias_voltage","parameters":"%s"}}' \
                        % curr_voltage
                    biasvoltagereset = \
                        execute_command_as_string(bias_voltage_dec,
                            ipadd, 8000)
                    biasresultmessage = \
                        execute_command_as_string(biasvoltageget,
                            ipadd, 8000)
                    biasvoltageoffset = \
                        execute_command_as_string(bias_voltage_gain,
                            ipadd, 8000)
                    curr_voltage = int(biasresultmessage['result'
                            ]['bias_voltage']) \
                        * int(biasvoltageoffset['result'])
                    sqlvoltage = \
                        "UPDATE messages set param1 = '%s' where message='biasvoltage'" \
                        % curr_voltage

                    # print sqlvoltage

                    cursor.execute(sqlvoltage)
                    db.commit()
                    decresultmessage = send_message(decmessage)
                except Exception, err:
                    print >> sys.stderr, err
                    continue
                sqlstop = \
                    'UPDATE interaction SET complete=1 WHERE job_index="%s" ' \
                    % row[0]
                cursor.execute(sqlstop)
                db.commit()
            else:

                print "We don't know what to do here"
                break

        # db.commit()

        time.sleep(3)
    print '...unblock loop ended. Connection closed.'


if __name__ == '__main__':

    # A few extra bits here to automatically reconnect if the server goes down
    # and is brought back up again.
    wsip = "ws://"+ args.ip + ":9000/"
    #ws = DummyClient('ws://127.0.0.1:9000/')
    #sys.exit()
    ws = DummyClient(wsip)
    ws.connect()


    try:
        db = MySQLdb.connect(host=args.dbhost, user=args.dbusername,
                             passwd=args.dbpass, port=args.dbport,
                             db=args.dbname)
        db2 = MySQLdb.connect(host=args.dbhost, user=args.dbusername,
                             passwd=args.dbpass, port=args.dbport,
                             db=args.dbname)
        cursor = db.cursor()
        cursor2 = db2.cursor()
    except Exception, err:
        print >> sys.stderr, "Can't connect to MySQL: %s" % err
        sys.exit()
    try:
        while 1:
            try:
                run_analysis()
                ws.run_forever()
            except socket_error, serr:
                if serr.errno != errno.ECONNREFUSED:
                    raise serr
                print 'Hanging around, waiting for the server...'
                time.sleep(5)  # Wait a bit and try again
    except (KeyboardInterrupt, SystemExit):
        ws.close()
        print 'stopped mincontrol.'
        time.sleep(1)
        sys.exit()
