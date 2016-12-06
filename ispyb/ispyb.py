#!/usr/bin/python
#
# Temporary API to ISPyB while I wait for a proper one using stored procedures
# - beware here be dragons, written by a hacker who is not a database wonk.
#
# Dependencies:
#
#   dials.python -m pip install mysql-connector
#

import mysql.connector
import json
import os

sauce = '/dls_sw/apps/zocalo/secrets/ispyb-login.json'

secret_ingredients = json.load(open(sauce, 'r'))

class ispyb(object):
  def __init__(self):
    self.conn = mysql.connector.connect(
        host=secret_ingredients['host'],
        port=secret_ingredients['port'],
        user=secret_ingredients['user'],
        password=secret_ingredients['passwd'],
        database=secret_ingredients['db'])

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

  def find_dc_id(self, directory):
    results = self.execute('select datacollectionid from DataCollection where '
                           'imagedirectory=%s;', directory)
    ids = [result[0] for result in results]
    return ids

  def get_dc_info(self, dc_id):
    results = self.execute('select * from DataCollection where '
                           'datacollectionid=%s;', dc_id)
    labels = self.columns['DataCollection']
    result = { }
    for l, r in zip(labels, results[0]):
      result[l] = r
    return result

  def get_pia_results(self, dc_id):
    results = self.execute(
      'select imagenumber, spottotal from '
      'ImageQualityIndicators where datacollectionid=%s;', dc_id)
    return results

  def get_dc_group(self, dc_id):
    # someone should learn how to use SQL JOIN here
    groups = self.execute('select dataCollectionGroupId from DataCollection '
                          'where datacollectionid=%s;', dc_id)
    assert(len(groups) == 1)
    group = groups[0][0]
    matches = self.execute('select datacollectionid from DataCollection '
                           'where dataCollectionGroupId=%s;', group)
    assert(len(matches) >= 1)
    dc_ids = [m[0] for m in matches]
    return dc_ids

  def get_space_group(self, dc_id):
    samples = self.execute('select blsampleid from DataCollection '
                           'where datacollectionid=%s;', dc_id)
    assert len(samples) == 1
    if samples[0][0] is None:
      return None
    sample = samples[0][0]
    crystals = self.execute('select crystalid from BLSample where '
                            'blsampleid=%s;', sample)

    if crystals[0][0] is None:
      return None

    crystal = crystals[0][0]

    spacegroups = self.execute('select spacegroup from Crystal where '
                               'crystalid=%s;', crystal)

    return spacegroups[0][0]

  def get_space_group_and_cell(self, dc_id):
    samples = self.execute('select blsampleid from DataCollection '
                           'where datacollectionid=%s;', dc_id)
    assert len(samples) == 1
    if samples[0][0] is None:
      return None, []

    sample = samples[0][0]
    crystals = self.execute('select crystalid from BLSample where '
                            'blsampleid=%s;', sample)

    if crystals[0][0] is None:
      return None, []

    crystal = crystals[0][0]

    spacegroups = self.execute('select spacegroup from Crystal where '
                               'crystalid=%s;', crystal)

    spacegroup = spacegroups[0]

    cells = self.execute(
      'select cell_a,cell_b,cell_c,cell_alpha,cell_beta,cell_gamma '
      'from Crystal where crystalid=%s;', crystal)

    cell = cells[0]

    return spacegroup, cell

  def get_matching_dcids_by_folder(self, dc_id):
    matches = self.execute('SELECT datacollectionid FROM DataCollection '
                           'WHERE imageDirectory=(SELECT imageDirectory FROM DataCollection '
                           'WHERE datacollectionid=%s);', dc_id)
    assert(len(matches) >= 1)
    dc_ids = [m[0] for m in matches]
    return sorted(dc_ids)

  def get_matching_dcids_by_sample_and_session(self, dc_id):
    result = self.execute(
      'select actualsamplebarcode,sessionid,blsampleid from DataCollection '
      'where datacollectionid=%s;', dc_id)
    assert len(result) == 1
    barcode, session, sample = result[0]
    matches = []
    if barcode and barcode != 'NR':
      matches = self.execute('select datacollectionid from DataCollection '
                             'where sessionid=%s and barcode=%s;', \
                               (session, barcode))
      assert(len(matches) >= 1)
    elif sample:
      matches = self.execute('select datacollectionid from DataCollection '
                             'where sessionid=%s and blsampleid=%s;', \
                               (session, sample))
      assert(len(matches) >= 1)
    dc_ids = [m[0] for m in matches]
    return sorted(dc_ids)

  def dc_info_to_filename(self, dc_info, image_number=None):
    template = dc_info['fileTemplate']
    directory = dc_info['imageDirectory']
    start = dc_info['startImageNumber']
    fmt = '%%0%dd' % template.count('#')
    prefix = template.split('#')[0]
    suffix = template.split('#')[-1]
    if image_number is None:
      return os.path.join(directory, '%s%s%s' % (prefix, fmt % start, suffix))
    return os.path.join(directory, '%s%s%s' % (prefix, fmt % image_number,
                                               suffix))

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

    if False:
      import uuid
      return os.path.join('/', 'dls', 'tmp', str(uuid.uuid4()))

    if taskname is None:
      return os.sep.join([visit, 'tmp', 'zocalo', rest,
                          template.split('#')[0]]).replace(2*os.sep, os.sep)
    else:
      import uuid
      root = os.sep.join([visit, 'tmp', 'zocalo', rest,
                          template.split('#')[0]]).replace(2*os.sep, os.sep)
      return os.path.join(root, '%s-%s' % (taskname, uuid.uuid4()))

  def dc_info_to_results_directory(self, dc_info, taskname=None):
    template = dc_info['fileTemplate']
    directory = dc_info['imageDirectory']
    visit = self.data_folder_to_visit(directory)
    rest = directory.replace(visit, '')
    if taskname is None:
      return os.sep.join(
        [visit, 'processed', 'zocalo', rest, template.split('#')[0]]).replace(
        2*os.sep, os.sep)
    else:
      root = os.sep.join(
        [visit, 'processed', 'zocalo', rest, template.split('#')[0]]).replace(
        2*os.sep, os.sep)
      run = 0
      while os.path.exists(os.path.join(root, '%s-%d' % (taskname, run))):
        run += 1
      return os.path.join(root, '%s-%d' % (taskname, run))

  def wrap_stored_procedure_insert_program(self, values):
    # this wraps a stored procedure I think - which should be a good thing
    # FIXME any documentation for what values should contain?!
    result = self.execute('select ispub.upsert_program_run(%s)' % \
                            ','.join([str(v) for v in values]))
    # etc? do I need to return anything?
    # probably

  def insert_screening_results(self, dc_id, values):
    keys = (
      'dataCollectionId', 'programVersion', 'shortComments', 'mosaicity',
      'spacegroup', 'unitCell_a', 'unitCell_b', 'unitCell_c',
      'unitCell_alpha', 'unitCell_beta', 'unitCell_gamma',
      'comments', 'wedgeNumber', 'numberOfImages', 'completeness', 'resolution',
      'axisStart', 'axisEnd', 'oscillationRange', 'numberOfImages', 'completeness',
      'resolution', 'rotationAxis', 'exposureTime', 'transmission',
    )
    for k in keys:
      assert k in values, k
      self.execute('SET @%s="%s";' % (k, values[k]))

    #-- Insert characterisation
    self.execute('insert into Screening (dataCollectionId, programVersion, comments, shortComments) values ('
                 '@dataCollectionId, @programVersion, @comments, @shortComments);')
    self.execute('SET @scrId = LAST_INSERT_ID();')
    self.execute('insert into ScreeningOutput (screeningId, mosaicity) values ('
                 '@scrId, @mosaicity);')
    self.execute('SET @scrOutId = LAST_INSERT_ID();')
    self.execute('insert into ScreeningOutputLattice (screeningOutputId, spacegroup,'
                 'unitCell_a,unitCell_b,unitCell_c,unitCell_alpha,unitCell_beta,unitCell_gamma) values ('
                 '@scrOutId, @spacegroup,'
                 '@unitCell_a, @unitCell_b, @unitCell_c, @unitCell_alpha, @unitCell_beta, @unitCell_gamma'
                 ');')

    #-- Insert strategy
    self.execute('insert into ScreeningStrategy (screeningOutputId, program) values ('
                '@scrOutId, @program'
                ');')
    self.execute('SET @scrStratId = LAST_INSERT_ID();')

    self.execute('insert into ScreeningStrategyWedge (screeningStrategyId, wedgeNumber, numberOfImages,'
                 'completeness, resolution) values ('
                 '@scrStratId, 1, @numberOfImages, @completeness, @resolution'
                 ');')

    self.execute('SET @scrStratWId = LAST_INSERT_ID();')

    self.execute('insert into ScreeningStrategySubWedge (screeningStrategyWedgeId,'
                 'axisStart, axisEnd, oscillationRange, numberOfImages, completeness,'
                 'resolution, rotationAxis, exposureTime, transmission) values ('
                 '@scrStratWId, @axisStart, @axisEnd, @oscillationRange,'
                 '@numberOfImages, @completeness, @resolution, @rotationAxis,'
                 '@exposureTime, @transmission'
                 ');')
    self.commit()

  def get_screening_results(self, dc_id, columns=None):
    if columns is not None:
      select_str = ', '.join(c for c in columns)
    else:
      select_str = '*'
    results = self.execute('''
SELECT %s
FROM Screening
INNER JOIN ScreeningOutput
ON Screening.screeningID = ScreeningOutput.screeningID
INNER JOIN ScreeningStrategy
ON ScreeningOutput.screeningoutputID = ScreeningStrategy.screeningoutputID
INNER JOIN ScreeningStrategyWedge
ON ScreeningStrategy.screeningstrategyID = ScreeningStrategyWedge.screeningstrategyID
INNER JOIN ScreeningStrategySubWedge
ON ScreeningStrategyWedge.screeningStrategyWedgeId = ScreeningStrategySubWedge.screeningStrategyWedgeId
INNER JOIN ScreeningOutputLattice
ON ScreeningOutput.screeningOutputId = ScreeningOutputLattice.screeningOutputId
WHERE Screening.datacollectionid=%s
;
''' %(select_str, dc_id)
    )
    return results

def ispyb_filter(message, parameters):
  '''Do something to work out what to do with this data...'''

  if not 'ispyb_dcid' in parameters:
    return message, parameters

  # FIXME put in here logic to check input if set i.e. if dc_id==0 then check
  # files exist; if image already set check they exist, ...

  i = ispyb()
  dc_id = parameters['ispyb_dcid']

  dc_info = i.get_dc_info(dc_id)
  dc_class = i.classify_dc(dc_info)
  start, end = i.dc_info_to_start_end(dc_info)
  parameters['ispyb_image'] = '%s:%d:%d' % (i.dc_info_to_filename(dc_info),
                                            start, end)
  parameters['ispyb_working_directory'] = i.dc_info_to_working_directory(
    dc_info)
  parameters['ispyb_results_directory'] = i.dc_info_to_results_directory(
    dc_info)

  if dc_class['grid']:
    message['default_recipe'] = ['per_image_analysis']
    return message, parameters

  if dc_class['screen']:
    message['default_recipe'] = ['per_image_analysis', 'strategy']
    return message, parameters

  assert(dc_class['rotation'])

  related_dcs = i.get_dc_group(dc_id)
  related_dcs.extend(i.get_matching_dcids_by_folder(dc_id))
  related_dcs.extend(i.get_matching_dcids_by_sample_and_session(dc_id))

  related = list(sorted(set(related_dcs)))

  other_dc_info = { }

  parameters['ispyb_space_group'] = i.get_space_group(dc_id)

  related_images = []

  parameters['ispyb_images'] = ''

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

    parameters['ispyb_images'] = ','.join(related_images)

  message['default_recipe'] = ['per_image_analysis', 'fast_dp', 'xia2',
                               'multi_xia2']

  return message, parameters

def work(dc_ids):

  import pprint

  pp = pprint.PrettyPrinter(indent=2)

  i = ispyb()

  for dc_id in dc_ids:
    print i.get_space_group_and_cell(dc_id)
    message = { }
    parameters = {'ispyb_dcid': dc_id}
    message, parameters = ispyb_filter(message, parameters)

    pp.pprint('Message:')
    pp.pprint(message)
    pp.pprint('Parameters:')
    pp.pprint(parameters)

if __name__ == '__main__':
  import sys
  if len(sys.argv) == 1:
    raise RuntimeError, 'for this mode of testing pass list of DCID on CL'
  else:
    work(map(int, sys.argv[1:]))
