import os, sys
import time
import syslog
import threading
from defage.daemon import Daemon

from  defage.pyinotify import  WatchManager, Notifier, ProcessEvent, IN_DELETE, IN_CREATE,IN_MODIFY
from defage import pyinotify

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
    def __init__(self, handleFile):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.handleFile= handleFile
        self.w = None

    def run(self):
        category_name = getLocalIp() + "%" + self.handleFile.split('/')[-2] + "%" + self.handleFile.split('/')[-1]
        socket = TSocket.TSocket(host=self.host, port=self.port)
        transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False, strictWrite=False)
        client = scribe.Client(iprot=protocol, oprot=protocol)
        print self.handleFile

        try:
            transport.open()
        except Exception, e:
            syslog.syslog("%s\n"%e)
            sys.exit(2)

        while True:
            try:
                file = open(self.handleFile,'r')
            except Exception, e:
                print e
                sys.exit(2)
            file.seek(0,2)
            if self.w:
                file.seek(self.w)
            #else:
            #    file.seek(0,2)
            where = file.tell()
            line = file.readline()
            if not line:
                time.sleep(1)
                file.seek(where)
            else:
                print line, # already has newline
                log_entry = scribe.LogEntry(category=category_name, message=line)
                print "fuck is: ", log_entry
                result = client.Log(messages=[log_entry])
                self.w = file.tell()
            file.close()


for root,dirs, files in os.walk(sys.argv[1]):
    for file in files:
        thread = TailThread(os.path.join(root,file))
        thread.start()

wm = pyinotify.WatchManager()
mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE | pyinotify.IN_MODIFY

class EventHandler(pyinotify.ProcessEvent):
    def process_IN_CREATE(self, event):
        print "Creating: ", event.pathname
        thread = TailThread(handleFile=os.path.join(event.path, event.name))
        thread.start()

    def process_IN_DELETE(self, event):
        print "Deleting: ", event.pathname
        #thread = TailThread(handleFile=os.path.join(event.path, event.name))
        #thread.start()


    def process_IN_MODIFY(self, event):
        print "Modify: ", event.pathname
        #thread = TailThread(handleFile=os.path.join(event.path, event.name))
        #thread.start()

handler = EventHandler()
notifier = pyinotify.Notifier(wm, handler)
wdd = wm.add_watch(sys.argv[1], mask, rec=True)

notifier.loop()
