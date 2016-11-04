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
import os

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

  def __del__(self):
    self.conn.close()

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
    # someone should learn how to use SQL JOIN here
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
    # someone should learn how to use SQL JOIN here
    folders = self.execute('select imageDirectory from DataCollection '
                           'where datacollectionid="%d";' % dc_id)
    assert(len(folders) == 1)
    folder = folders[0][0]
    matches = self.execute('select datacollectionid from DataCollection '
                           'where imageDirectory="%s";' % folder)
    assert(len(matches) >= 1)
    dc_ids = [m[0] for m in matches]
    return sorted(dc_ids)

  def get_matching_sample_and_session(self, dc_id):
    result = self.execute(
      'select actualsamplebarcode,sessionid,blsampleid from DataCollection '
      'where datacollectionid="%d";' % dc_id)
    assert len(result) == 1
    barcode, session, sample = result[0]
    if barcode and barcode != 'NR':
      matches = self.execute('select datacollectionid from DataCollection '
                             'where sessionid="%d" and barcode="%s";' % \
                               (session, barcode))
    else:
      matches = self.execute('select datacollectionid from DataCollection '
                             'where sessionid="%d" and blsampleid="%d";' % \
                               (session, sample))

    assert(len(matches) >= 1)
    dc_ids = [m[0] for m in matches]
    return sorted(dc_ids)

  def dc_info_to_filename(self, dc_info):
    template = dc_info['fileTemplate']
    directory = dc_info['imageDirectory']
    start = dc_info['startImageNumber']
    number = dc_info['numberOfImages']
    end = start + number - 1
    fmt = '%%0%dd' % template.count('#')
    prefix = template.split('#')[0]
    suffix = template.split('#')[-1]
    first_image = os.path.join(directory, '%s%s%s' %
                               (prefix, fmt % start, suffix))
    return first_image

  def dc_info_to_start_end(self, dc_info):
    start = dc_info['startImageNumber']
    number = dc_info['numberOfImages']
    end = start + number - 1
    return start, end

  def dc_info_is_grid_scan(self, dc_info):
    if dc_info['numberOfImages'] > 1 and dc_info['axisRange'] == 0.0:
      return True
    return False

  def dc_info_is_screening(self, dc_info):
    if dc_info['numberOfImages'] == 1:
      return True
    if dc_info['numberOfImages'] > 1 and dc_info['overlap'] != 0.0:
      return True
    return False

  def dc_info_is_rotation_scan(self, dc_info):
    if dc_info['overlap'] == 0.0 and dc_info['axisRange'] > 0:
      return True
    return False

  def classify_dc(self, dc_info):
    return {'grid':self.dc_info_is_grid_scan(dc_info),
            'screen':self.dc_info_is_screening(dc_info),
            'rotation':self.dc_info_is_rotation_scan(dc_info)}

  def data_folder_to_visit(self, directory):
    '''Extract visit directory, assumes the path structure goes something
    like /dls/${beamline}/data/${year}/${visit} - 2016/11/03 this is a
    valid assumption'''

    return os.sep.join(directory.split(os.sep)[:6]).strip()

  def dc_info_to_working_directory(self, dc_info, taskname=None):
    template = dc_info['fileTemplate']
    directory = dc_info['imageDirectory']
    visit = self.data_folder_to_visit(directory)
    rest = directory.replace(visit, '')
    if taskname is None:
      return os.sep.join([visit, 'tmp', rest, template.split('#')[0]]).replace(
        2*os.sep, os.sep)
    else:
      import uuid
      root = os.sep.join([visit, 'tmp', rest, template.split('#')[0]]).replace(
        2*os.sep, os.sep)
      return os.path.join(root, '%s-%s' % (taskname, uuid.uuid4()))

  def dc_info_to_results_directory(self, dc_info, taskname=None):
    template = dc_info['fileTemplate']
    directory = dc_info['imageDirectory']
    visit = self.data_folder_to_visit(directory)
    rest = directory.replace(visit, '')
    if taskname is None:
      return os.sep.join(
        [visit, 'processed', rest, template.split('#')[0]]).replace(
        2*os.sep, os.sep)
    else:
      import uuid
      root = os.sep.join(
        [visit, 'processed', rest, template.split('#')[0]]).replace(
        2*os.sep, os.sep)
      run = 0
      while os.path.exists(os.path.join(root, '%s-%d' % (taskname, run))):
        run += 1
      return os.path.join(root, '%s-%d' % (taskname, run))

def test():
  i = ispyb()
  dc_id = 1397955
  dc_info = i.get_dc_info(dc_id)
  # this was not recorded as a data collection group
  whole_group = i.get_dc_group(dc_id)
  assert(len(whole_group) == 1)
  # however there are four data collections
  whole_group = i.get_matching_folder(dc_id)
  assert(len(whole_group) == 4)
  for dc_id in whole_group:
    dc_info = i.get_dc_info(dc_id)
  print 'OK'

def ispyb_magic(message, parameters):
  '''Do something to work out what to do with this data...'''

  if not 'ispyb_dcid' in parameters:
    return message, parameters

  i = ispyb()
  dc_id = parameters['ispyb_dcid']

  dc_info = i.get_dc_info(dc_id)
  dc_class = i.classify_dc(dc_info)
  start, end = i.dc_info_to_start_end(dc_info)
  parameters['image'] = '%s:%d:%d' % (i.dc_info_to_filename(dc_info),
                                      start, end)
  parameters['working_directory'] = i.dc_info_to_working_directory(dc_info)
  parameters['results_directory'] = i.dc_info_to_results_directory(dc_info)

  if dc_class['grid']:
    message['default_recipe'] = ['per_image_analysis']
    return message, parameters

  if dc_class['screen']:
    message['default_recipe'] = ['per_image_analysis', 'strategy']
    return message, parameters

  assert(dc_class['rotation'])

  related_dcs = i.get_dc_group(dc_id)
  related_dcs.extend(i.get_matching_folder(dc_id))
  related_dcs.extend(i.get_matching_sample_and_session(dc_id))

  related = list(sorted(set(related_dcs)))

  other_dc_info = { }

  related_images = []

  parameters['images'] = ''

  for dc in related:

    # FIXME logic: should this exclude dc > dc_id?
    if dc == dc_id:
      continue

    info = i.get_dc_info(dc)
    other_dc_class = i.classify_dc(info)
    if other_dc_class['rotation']:
      start, end = i.dc_info_to_start_end(info)

      related_images.append('%s:%d:%d' % (i.dc_info_to_filename(info),
                                          start, end))

    parameters['images'] = ','.join(related_images)

  message['default_recipe'] = ['per_image_analysis', 'fast_dp', 'xia2',
                               'multi_xia2']

  return message, parameters

def work(dc_ids):

  for dc_id in dc_ids:
    message = { }
    parameters = {'ispyb_dcid': dc_id}
    message, parameters = ispyb_magic(message, parameters)
    print message
    print parameters

if __name__ == '__main__':
  import sys
  if len(sys.argv) == 1:
    test()
  else:
    work(map(int, sys.argv[1:]))
