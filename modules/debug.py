import inspect
from subprocess import call
import sys


def die():
    call(["cmd","/c /cygwin64/bin/bash.exe /home/Admin/bin/zap pyth"])
    sys.exit()


def debug():
        sys.stdout.flush()
        cf = inspect.currentframe()
        filename = inspect.getframeinfo(cf).filename
        currentF = inspect.stack()[1][3]
        parentF = inspect.stack()[2][3]
        print  ">> line", inspect.currentframe().f_back.f_lineno, \
                        ".", currentF, ".", parentF # , ".", filename

        sys.stdout.flush()
        #die()



        

