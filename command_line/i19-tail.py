from __future__ import absolute_import, division, print_function

# LIBTBX_SET_DISPATCHER_NAME i19.tail

import os
import re
import select
import sys
import threading
import time
import procrunner
from pprint import pprint

start = time.time()

def ensure_we_are_in_visit_directory():
  current_path = os.getcwd()
  m = re.match(r'(/dls/[^/]+/data/[0-9]+/[^/]+)(/.*)?$', current_path)
  if not m:
    sys.exit("This script must be run inside a visit directory. %s is not a visit directory." % current_path)

  visit_dir = m.group(1)
  print("Looking for i19 screen logs for visit directory", visit_dir)
  os.chdir(visit_dir)

  if not os.path.exists('tmp'):
    sys.exit("This visit directory does not have a 'tmp' directory. Can't run.")
  os.chdir('tmp')
  if not os.path.exists('zocalo'):
    print("This visit directory does not yet have a 'tmp/zocalo' directory.")
    print("Waiting up to 10 minutes for first data collection...")
    while not os.path.exists('zocalo') and time.time() < (start + 600): # Limit waiting to 10 minutes
      time.sleep(10)
    if not os.path.exists('zocalo'):
      sys.exit("There is no live data processing in this visit.")
  os.chdir('zocalo')

def header(text):
  print()
  print(text)
  print('=' * len(text))

ensure_we_are_in_visit_directory()
base_directory = os.getcwd()

seen_dirs = {}
most_recent_dir = (None, 0)

def recursively_find_most_current_directory(base):
  global most_recent_dir
  entries = os.listdir(base)
  if 'i19.screen' in entries:
    return os.path.join(base, 'i19.screen')

  print(base, ':', entries)
  dir_ages = { directory: os.path.getmtime(directory)
               for directory in (os.path.join(base, entry) for entry in entries) }
  newest_entry = (None, 0)
  for entry in entries:
    directory = os.path.join(base, entry)
    last_modification = os.path.getmtime(directory)
    if seen_dirs.get(directory, 0) < last_modification:
      seen_dirs[directory] = last_modification
      if last_modification > newest_entry[1]:
        newest_entry = (directory, last_modification)
  if newest_entry[1] > most_recent_dir[1]:
    most_recent_dir = newest_entry
    return recursively_find_most_current_directory(most_recent_dir[0])

class tail_log(threading.Thread):
  def __init__(self, path):
    threading.Thread.__init__(self)
    self.daemon = True
    header(os.path.dirname(os.path.dirname(path)))
    self._fh = open(os.path.join(path, 'i19.screen.log'))
    self._closing = False
    self.start()

  def close(self):
    self._closing = True

  def run(self):
    la = procrunner._LineAggregator(print_line=True, callback=None)
    while not self._closing:
      if select.select([self._fh], [], [], 0.1)[0]:
        char = self._fh.read(1)
        if char:
          la.add(char)
      else:
        if self._closing: break
    la.flush()
    print("\nTHREAD TERMINATED!\n")

active_tail = None
next_i19_log = None
try:
  while time.time() < start + (24 * 3600): # Set up a 24hr runtime limit
    pprint(threading.enumerate())
    if not next_i19_log:
      next_i19_log = recursively_find_most_current_directory(base_directory)
    if next_i19_log:
      if active_tail:
        active_tail.close()
        active_tail = None
      if os.path.exists(os.path.join(next_i19_log, 'i19.screen.log')):
        active_tail = tail_log(next_i19_log)
        next_i19_log = None
    time.sleep(5)
except KeyboardInterrupt:
  print()
