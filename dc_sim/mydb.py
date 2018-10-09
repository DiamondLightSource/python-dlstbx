from __future__ import absolute_import, division, print_function

import ispyb
import logging

log = logging.getLogger()

class DB:
  def __init__(self, *args, **kwargs):
    '''All parameters are ignored.'''
    self.i = ispyb.open('/dls_sw/dasc/mariadb/credentials/ispyb_scripts.cfg')
    self.cursor = self.i.create_cursor()

  def doQuery(self, querystr):
    log.debug("DB: %s" % querystr)
    try:
      ret = self.cursor.execute(querystr)
    except:
      log.exception("DB: exception running sql statement :-(")
      raise
    try:
      return self.cursor.fetchall()
    except:
      log.exception("DB: exception fetching cursor :-(")
      raise
