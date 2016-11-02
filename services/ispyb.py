#!/usr/bin/python
#
# Temporary API to ISPyB while I wait for a proper one using stored procedures
# - beware here be dragons, written by a hacker who is not a database wonk.
#
# Dependencies:
#
#   dials.python -m pip install mysql
#

import MySQLdb.connections as mysql
import json

sauce = '/dls_sw/apps/mx-scripts/plum-duff/secret_ingredient.json'

secret_ingredients = json.load(open(sauce, 'r'))

class ispyb(object):
  def __init__(self):
    self.conn = mysql.Connection(host=secret_ingredients['host'],
                                 port=secret_ingredients['port'],
                                 user=secret_ingredients['user'],
                                 passwd=secret_ingredients['passwd'],
                                 db=secret_ingredients['db'])

    # gather information on tables so we can map the data structures
    # back to named tuples / dictionaries in the results
    tables = ['DataCollection', 'DataCollectionGroup']
    self.columns = { }
    cursor = self.conn.cursor()
    for table in tables:
      query = 'describe %s;' % table
      cursor.execute(query)
      columns = []
      for record in cursor:
        name = record[0]
        columns.append(name)
      self.columns[table] = columns

    self._cursor = self.conn.cursor()

  def cursor(self):
    return self._cursor

  def execute(self, query):
    cursor = self.cursor()
    cursor.execute(query)
    results = [result for result in cursor]
    return results

  def get_dc_info(self, dc_id):
    results = self.execute('select * from DataCollection where '
                           'datacollectionid="%d";' % dc_id)
    labels = self.columns['DataCollection']
    result = { }
    for l, r in zip(labels, results[0]):
      result[l] = r
    return result

  def get_dc_group(self, dc_id):
    groups = self.execute('select dataCollectionGroupId from DataCollection '
                          'where datacollectionid="%d";' % dc_id)
    assert(len(groups) == 1)
    group = groups[0][0]
    matches = self.execute('select datacollectionid from DataCollection '
                           'where dataCollectionGroupId="%d";' % group)
    assert(len(matches) >= 1)
    dc_ids = [m[0] for m in matches]
    return dc_ids

  def get_matching_folder(self, dc_id):
    folders = self.execute('select imageDirectory from DataCollection '
                           'where datacollectionid="%d";' % dc_id)
    assert(len(folders) == 1)
    folder = folders[0][0]
    matches = self.execute('select datacollectionid from DataCollection '
                           'where imageDirectory="%s";' % folder)
    assert(len(matches) >= 1)
    dc_ids = [m[0] for m in matches]
    return dc_ids

def test():
  i = ispyb()
  dc_id = 1397955
  res = i.get_dc_info(dc_id)
  # this was not recorded as a data collection group
  whole_group = i.get_dc_group(dc_id)
  assert(len(whole_group) == 1)
  # however there are four data collections
  whole_group = i.get_matching_folder(dc_id)
  assert(len(whole_group) == 4)
  print 'OK'

if __name__ == '__main__':
  test()
