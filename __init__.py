from __future__ import absolute_import, division

def enable_graylog():
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
  graylog = graypy.GELFHandler('cs04r-sc-serv-14.diamond.ac.uk', 12201, \
                               level_names=True)
  logger = logging.getLogger()
  logger.addHandler(graylog)
