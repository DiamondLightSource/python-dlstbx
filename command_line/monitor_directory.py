# Debugging tool for https://jira.diamond.ac.uk/browse/SCI-7634

from __future__ import absolute_import, division, print_function

import dlstbx
import logging
import os
import sys
import time
from datetime import datetime
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from optparse import SUPPRESS_HELP, OptionParser

if __name__ == '__main__':
  parser = OptionParser()
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-i", "--interval", dest="interval", default=1, type="float",
                    help="Check directory every this many seconds.")
  parser.add_option("-p", "--path", dest="path", default='.', type="str",
                    help="Log any files appearing in this directory.")
  (options, args) = parser.parse_args(sys.argv[1:])

  # Set up logging
  log = logging.getLogger('dlstbx.SCI-7646')
  log.setLevel(logging.DEBUG)

  # Enable logging to graylog
  dlstbx.enable_graylog()

  # Enable logging to console
  console = ColorStreamHandler()
  console.setLevel(logging.DEBUG)
  log.addHandler(console)

  # Prefix timestamp to console output
  console_print = console.format
  def prefix_time(*args, **kwargs):
    return "[{1:%H:%M:%S.%f}] {0}".format(console_print(*args, **kwargs), datetime.now())
  console.format = prefix_time

  # Look for files
  seen_files = set()
  basepath = options.path
  absolute_path = os.path.abspath(basepath)
  try:
    while True:
      for filename in os.listdir(basepath):
        if filename not in seen_files:
          seen_files.add(filename)
          log.debug(
              os.path.join(absolute_path, filename),
              extra={
                  'watch_interval': options.interval,
                  'watch_path': absolute_path,
                  'watch_filename': filename,
              },
          )
      time.sleep(options.interval)
  except KeyboardInterrupt:
    print()
