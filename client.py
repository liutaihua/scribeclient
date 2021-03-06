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



host = '10.127.26.241'
port = 1463
logFileFormat = '.log'
#domain = ['reg','www','cas','reguser','login']
domain = []

def getLocalIp():
    from socket import socket, SOCK_DGRAM, AF_INET
    s = socket(AF_INET, SOCK_DGRAM)
    s.connect(('8.8.8.8',0))
    LocalIp = s.getsockname()[0]
    s.close()
    return LocalIp

class TailThread(threading.Thread):
    def __init__(self, handleFile):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.handleFile= handleFile
        #self.last_seek = None

    def run(self):
        category_name = getLocalIp() + "%" + self.handleFile.split('/')[-2]
        socket = TSocket.TSocket(host=self.host, port=self.port)
        transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
        client = scribe.Client(iprot=protocol, oprot=protocol)

        while True:
            try:
                transport.open()
            except Exception, e:
                print e
                syslog.syslog("%s\n"%e)
                time.sleep(10)
            else:break
    
        if self.handleFile.endswith(logFileFormat):
            try:
                file = open(self.handleFile,'r')
            except Exception, e:
                syslog.syslog("%s\n"%e)
                sys.exit(2)

            file.seek(0,2)
            max_wait = 0
            while True:
                where = file.tell()
                line = file.readline().split('\n')[0]
                if not line:
                    if not os.path.exists(self.handleFile):
                        sys.exit(2)
                    else:
                        time.sleep(1)
                        max_wait += 1
                        file.seek(where)
                        if max_wait >= 10:
                            new_md5 = commands.getoutput("md5sum %s"%self.handleFile).split()[0]
                            try:
                                file.close()
                                file = open(self.handleFile)
                            except Exception, e:
                                syslog.syslog("file have to flushed, reopen it: %s"%e)
                            else:
                                try:
                                    t = old_md5
                                except Exception,e:
                                    file.seek(0,2)
                                else:
                                    if new_md5 == old_md5:
                                        file.seek(0,2)
                                max_wait = 0
                else:
                    old_md5 = commands.getoutput("md5sum %s"%self.handleFile).split()[0]
                    max_wait = 0
                    try:
                        log_entry = scribe.LogEntry(category=category_name, message=line)
                        result = client.Log(messages=[log_entry])
                        #transport.close()
                    except Exception, e:
                        syslog.syslog("disconnect from scribe server,error: %s\n"%e)
                        while True:
                            try:
                                transport.open()
                                log_entry = scribe.LogEntry(category=category_name, message=line)
                                result = client.Log(messages=[log_entry])
                            except Exception, e:
                                syslog.syslog("can't conn scribe server.")
                                time.sleep(3)
                            else: break
                        file.seek(where)
        else:
            sys.exit(2)

class EventHandler(ProcessEvent):
    def process_IN_CREATE(self, event):
        #syslog.syslog("Create file: %s "  %   os.path.join(event.path, event.name))
        thread = TailThread(handleFile=os.path.join(event.path, event.name))
        thread.start()

    def process_IN_DELETE(self, event):
        #print   "Delete file: %s "  %   os.path.join(event.path, event.name)
        pass
    
    def process_IN_MODIFY(self, event):
        #syslog.syslog("Modify file: %s "  %   os.path.join(event.path, event.name))
        #thread = TailThread(host, port, handleFile=os.path.join(event.path, event.name))
        #thread.start()
        pass
    

class MyDaemon(Daemon):
    def run(self):
        path = '/opt/logs/nginx/'
        watch_path = map(lambda x:os.path.join(path, x),filter(lambda x:x!="nginx", [i for i in os.listdir(path)]))
        scan = None
        syslog.openlog('scribeClient',syslog.LOG_PID)

        wm = WatchManager() 
        mask = IN_DELETE | IN_CREATE |IN_MODIFY
        notifier = Notifier(wm, EventHandler())
        wm.add_watch(watch_path, mask,rec=True)
        #print 'now starting monitor %s'%(path)
        while True:
            if not scan:
                for root, dirs, files in os.walk(path):
                    for file in files:
                        if domain:
                            if os.path.join(root,file).split('/')[-2] in domain:
                                thread = TailThread(os.path.join(root,file))
                                thread.start()
                        elif os.path.join(root,file).split('/')[4] not in ["nginx", "archive"]:
                            thread = TailThread(os.path.join(root,file))
                            thread.start()
                scan = 'complete'
            try:
                notifier.process_events()
                if notifier.check_events():
                    notifier.read_events()
            except KeyboardInterrupt:
                notifier.stop()
                break

if __name__ == "__main__":
	daemon = MyDaemon('/var/run/scribeclient.pid')
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
