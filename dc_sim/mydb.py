from __future__ import absolute_import, division, print_function

import logging

import ispyb

log = logging.getLogger('dlstbx.dc_sim')

class DB:
  def __init__(self):
    self.i = ispyb.open('/dls_sw/dasc/mariadb/credentials/ispyb_scripts.cfg')
    self.cursor = self.i.create_cursor()

  def doQuery(self, querystr):
    cursor = self.i.create_cursor()
    log.debug("DB: %s", querystr)
    try:
      cursor.execute(querystr)
      return cursor.fetchall()
    finally:
      cursor.close()
