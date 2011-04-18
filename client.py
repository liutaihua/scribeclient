#-*- coding:utf8 -*-
#!/usr/bin/env python
########################################################################
#author:liutaihua
#email: defage@gmail.com
#
#########################################################################


import os, sys
import time
import syslog
import threading
from defage.daemon import Daemon

from  defage.pyinotify import  WatchManager, Notifier, ProcessEvent, IN_DELETE, IN_CREATE,IN_MODIFY

from scribe import scribe
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol



host = '10.241.12.74'
port = 1463

def getLocalIp():
    from socket import socket, SOCK_DGRAM, AF_INET
    s = socket(AF_INET, SOCK_DGRAM)
    s.connect(('8.8.8.8',0))
    LocalIp = s.getsockname()[0]
    s.close()
    return LocalIp


class TailThread(threading.Thread):
    def __init__(self, host, port, handleFile):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.handleFile= handleFile

    def run(self):
        category_name = "[" + getLocalIp() +  "]" + "[" + self.handleFile.split('/')[-2] + "]" + "[" + self.handleFile.split('/')[-1] + "]"
        socket = TSocket.TSocket(host=self.host, port=self.port)
        transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
        client = scribe.Client(iprot=protocol, oprot=protocol)
        file = open(self.handleFile,'r')
        file.seek(0,2)

        transport.open()
        while True:
            where = file.tell()
            line = file.readline()
            if not line:
                time.sleep(1)
                file.seek(where)
            else:
                print line, # already has newline
                syslog.syslog(line)
                log_entry = scribe.LogEntry(category=category_name, message=line)
                result = client.Log(messages=[log_entry])
                #transport.close()

class EventHandler(ProcessEvent):
    def process_IN_CREATE(self, event):
        print   "Create file: %s "  %   os.path.join(event.path, event.name)
        thread = TailThread(host, port, handleFile=os.path.join(event.path, event.name))
        thread.start()

    def process_IN_DELETE(self, event):
        print   "Delete file: %s "  %   os.path.join(event.path, event.name)
    
    def process_IN_MODIFY(self, event):
        print   "Modify file: %s "  %   os.path.join(event.path, event.name)
        thread = TailThread(host, port, handleFile=os.path.join(event.path, event.name))
        thread.start()
    

class MyDaemon(Daemon):
    def run(self):
        syslog.openlog('scribeClient',syslog.LOG_PID)
        path = '/opt/logs/nginx/'
        wm = WatchManager() 
        mask = IN_DELETE | IN_CREATE |IN_MODIFY
        notifier = Notifier(wm, EventHandler())
        wm.add_watch(path, mask,rec=True)
        #print 'now starting monitor %s'%(path)
        while True:
            try:
                notifier.process_events()
                if notifier.check_events():
                    notifier.read_events()
            except KeyboardInterrupt:
                notifier.stop()
                break

if __name__ == "__main__":
        daemon = MyDaemon('/tmp/daemon.pid')
        if len(sys.argv) == 2:
                if 'start' == sys.argv[1]:
                        daemon.start()
                elif 'stop' == sys.argv[1]:
                        daemon.stop()
                elif 'restart' == sys.argv[1]:
                        daemon.restart()
                else:
                        print "Unknown command"
                        sys.exit(2)
                sys.exit(0)
        else:
                print "usage: %s start|stop|restart" % sys.argv[0]
                sys.exit(2)


