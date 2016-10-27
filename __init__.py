from __future__ import absolute_import, division

def enable_graylog():
  '''Central function to set up graylog handler in logging module.'''
  import logging
  logger = logging.getLogger()
  try:
    import graypy
    graylog = graypy.GELFHandler('cs04r-sc-serv-14.diamond.ac.uk', 12201, \
                                 level_names=True)
    logger.addHandler(graylog)
  except ImportError:
    logging.getLogger('dlstbx').warn(
        'Could not enable logging to graylog: python module graypy missing')

