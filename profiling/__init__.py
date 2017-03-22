#!/usr/bin/python
#
# API for access to the zocalo profiling database, which includes DLS
# infrastructure status information.
#
# Dependencies:
#
#   dials.python -m pip install mysql-connector
#

import json
import os

class database(object):
  def __init__(self):
    _secret_configuration = '/dls_sw/apps/zocalo/secrets/sql-zocalo-profiling.json'
    _secret_ingredients = json.load(open(_secret_configuration, 'r'))

    try:
      import mysql.connector
    except ImportError:
      raise ImportError('MySQL connector module not found. Run python -m pip install mysql-connector')

    self.conn = mysql.connector.connect(
        host=_secret_ingredients['host'],
        port=_secret_ingredients['port'],
        user=_secret_ingredients['user'],
        password=_secret_ingredients['passwd'],
        database=_secret_ingredients['db'])
    self._cursor = self.conn.cursor(dictionary=True)

  def __del__(self):
    if hasattr(self, 'conn') and self.conn:
      self.conn.close()

  def cursor(self):
    return self._cursor

  def execute(self, query, parameters=None):
    cursor = self.cursor()
    if parameters:
      if isinstance(parameters, (basestring, int, long)):
        parameters = (parameters,)
      cursor.execute(query, parameters)
    else:
      cursor.execute(query)
    results = [result for result in cursor]
    return results

  def commit(self):
    self.conn.commit()

  def get_infrastructure_status(self):
    status = self.execute('SELECT * FROM infrastructure_status;')
    for s in status:
      if s['Level'] < 10:
        s['Group'] = 'Information'
      elif s['Level'] < 20:
        s['Group'] = 'Warning'
      else:
        s['Group'] = 'Error'
    return status

if __name__ == '__main__':
  import pprint
  pp = pprint.PrettyPrinter(indent=2).pprint

  db = database()
  pp(db.get_infrastructure_status())
