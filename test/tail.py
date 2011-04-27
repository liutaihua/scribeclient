import os
import sys
import time
import threading

from defage import pyinotify
import sys




class TailThread(threading.Thread):
    def __init__(self, handleFile):
        threading.Thread.__init__(self)
        #self.host = host
        #self.port = port
        self.handleFile= handleFile
        self.w = None

    def run(self):
        while True:
            try:
                file = open(self.handleFile,'r')
            except Exception, e:
                print e
                sys.exit(2)
            if self.w:
                file.seek(self.w)
            where = file.tell()
            line = file.readline()
            if not line:
                time.sleep(1)
                file.seek(where)
            else:
                print line, # already has newline
                self.w = file.tell()
		#transport.close()
        file.close()



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
