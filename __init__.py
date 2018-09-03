from __future__ import absolute_import, division, print_function

import sys

def enable_graylog(host='graylog2.diamond.ac.uk', port=12208):
  '''Central function to set up graylog handler in logging module.'''
  import logging
  try:
    import graypy
  except ImportError:
    logging.getLogger('dlstbx').warn(
        'Could not enable logging to graylog: python module graypy missing')
    return

  # Monkeypatch graypy for proper log level support
  class PythonLevelToSyslogConverter(object):
    @staticmethod
    def get(level, _):
      if level < 20:   return 7 # DEBUG
      elif level < 25: return 6 # INFO
      elif level < 30: return 5 # NOTICE
      elif level < 40: return 4 # WARNING
      elif level < 50: return 3 # ERROR
      elif level < 60: return 2 # CRITICAL
      else:            return 1 # ALERT
  graypy.handler.SYSLOG_LEVELS = PythonLevelToSyslogConverter()

  # Create and enable graylog handler
  graylog = graypy.GELFHandler(host, port, level_names=True)
  logger = logging.getLogger()
  logger.addHandler(graylog)

  # Return the handler, which may be useful to attach filters to it.
  return graylog

class Buck():
  '''A buck, which can be passed.'''
  def __init__(self, name='Buck'):
    self._name = name

  def _debuck(self, frame):
    references = [var for var in frame if frame[var] == self]
    for ref in references:
      del frame[ref]

  def Pass(self):
    try:
      raise Exception()
    except Exception:
      self._debuck(sys.exc_info()[2].tb_frame.f_back.f_locals)
      print("...aand it's gone.")

  def __repr__(self):
    try:
      raise Exception()
    except Exception:
      self._debuck(sys.exc_info()[2].tb_frame.f_back.f_locals)
      return("<%s instance at %s...aand it's gone>" % (self._name, hex(id(self))[:-1]))
