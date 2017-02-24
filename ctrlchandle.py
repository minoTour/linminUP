import signal
import time

def handler(signum,frame):
    print 'Here you go'
    
signal.signal(signal.SIGINT, handler)

print "gonna sleep"
time.sleep(10)