#-*- coding:utf8 -*-
#!/usr/bin/env python


########################################################################
#author:liutaihua
#email: defage@gmail.com
#
#########################################################################
"""
Usage:
    [-h|--help] [-H|--host] [-p|--port] [-f|--file]

-H|--host    the scribe host
-p|--port    the scribe port
-f|--file    the log file

example:
    python client -H 127.0.0.1 -p 1463 -f /tmp/test.log
"""

import sys
import time
import getopt

from scribe import scribe
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol




#host = '127.0.0.1'
#port = 1463




def getLocalIp():
    from socket import socket, SOCK_DGRAM, AF_INET
    s = socket(AF_INET, SOCK_DGRAM)
    s.connect(('8.8.8.8',0))
    LocalIp = s.getsockname()[0]
    s.close()
    return LocalIp


def main(argv):
    if not argv:
        usage()
        sys.exit()
    try:
        opts, args = getopt.getopt(argv, "hH:p:f:", ["help", "host=","port=","logfile="])
    except getopt.GetoptError,err:
        print err
        usage()
        sys.exit()
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-H", "--host"):
            host = arg
        elif opt in ("-p", "--port"):
            port = arg
        elif opt in ("-f", "--file"):
            logfile = arg
        else:
            pass
            usage()
            sys.exit()

    socket = TSocket.TSocket(host=host, port=port)
    transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
    client = scribe.Client(iprot=protocol, oprot=protocol)
    transport.open()
    file = open(logfile,'r')
    file.seek(0,2)
    while True:
        where = file.tell()
        line = file.readline()
        if not line:
    	    time.sleep(1)
	    file.seek(where)
        else:
	    print line, # already has newline
	    log_entry = scribe.LogEntry(category=getLocalIp(), message=line)
	    result = client.Log(messages=[log_entry])
	    #transport.close()
def usage():
    print __doc__

if __name__ == "__main__":
    main(sys.argv[1:])
