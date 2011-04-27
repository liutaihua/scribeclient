#!/usr/bin/python
from defage import pyinotify
import sys

wm = pyinotify.WatchManager()
mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE | pyinotify.IN_MODIFY

class EventHandler(pyinotify.ProcessEvent):
    def process_IN_CREATE(self, event):
        print "Creating: ", event.pathname

    def process_IN_DELETE(self, event):
        print "Deleting: ", event.pathname
    def process_IN_MODIFY(self, event):
        print "Modify: ", event.pathname

handler = EventHandler()
notifier = pyinotify.Notifier(wm, handler)
wdd = wm.add_watch(sys.argv[1], mask, rec=True)

notifier.loop()
