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

# convenience functions
def _clean_(path):
  return path.replace(2*os.sep, os.sep)

def _prefix_(template):
  return template.split('#')[0]

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

  def get_container_type(self, dc_id):
    samples = self.execute('select blsampleid from DataCollection '
                           'where datacollectionid=%s;', dc_id)
    assert len(samples) == 1
    if samples[0][0] is None:
      return None
    sample = samples[0][0]
    containers = self.execute('select containerid from BLSample where '
                              'blsampleid=%s;', sample)
    assert len(containers) == 1
    if containers[0][0] is None:
      return None
    container_id = containers[0][0]
    container_type = self.execute('select containertype from Container where '
                                  'containerid=%s;', container_id)
    assert len(container_type) == 1
    return container_type[0][0]

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

  def dc_info_to_filename_pattern(self, dc_info):
    template = dc_info['fileTemplate']
    fmt = '%%0%dd' % template.count('#')
    prefix = template.split('#')[0]
    suffix = template.split('#')[-1]
    return prefix + fmt + suffix

  def dc_info_to_filename(self, dc_info, image_number=None):
    template = self.dc_info_to_filename_pattern(dc_info)
    directory = dc_info['imageDirectory']
    if image_number:
      return os.path.join(directory, template % image_number)
    return os.path.join(directory, template % dc_info['startImageNumber'])

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

  def dc_info_to_working_directory(self, dc_info, taskname):
    import uuid
    prefix = _prefix_(dc_info['fileTemplate'])
    directory = dc_info['imageDirectory']
    visit = self.data_folder_to_visit(directory)
    rest = directory.replace(visit, '')
    root = _clean_(os.sep.join([visit, 'tmp', 'zocalo', rest, prefix]))
    return os.path.join(root, '%s-%s' % (taskname, str(uuid.uuid4())))

  def dc_info_to_results_directory(self, dc_info, taskname):
    import uuid
    prefix = _prefix_(dc_info['fileTemplate'])
    directory = dc_info['imageDirectory']
    visit = self.data_folder_to_visit(directory)
    rest = directory.replace(visit, '')
    root = _clean_(os.sep.join([visit, 'processed', rest, prefix]))
    return os.path.join(root, '%s-%s' % (taskname, str(uuid.uuid4())))

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
      'rankingResolution', 'rotationAxis', 'exposureTime', 'transmission',
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
    self.execute('insert into ScreeningStrategy (screeningOutputId, program, rankingResolution) values ('
                '@scrOutId, @program, @rankingResolution'
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
    sql_str = '''
SELECT %s
FROM Screening
INNER JOIN ScreeningOutput
ON Screening.screeningID = ScreeningOutput.screeningID
INNER JOIN ScreeningStrategy
ON ScreeningOutput.screeningOutputID = ScreeningStrategy.screeningOutputID
INNER JOIN ScreeningStrategyWedge
ON ScreeningStrategy.screeningStrategyID = ScreeningStrategyWedge.screeningStrategyID
''' %select_str
    if columns is not None:
      for c in columns:
        if c.startswith('ScreeningStrategySubWedge'):
          sql_str += '''\
INNER JOIN ScreeningStrategySubWedge
ON ScreeningStrategyWedge.screeningStrategyWedgeID = ScreeningStrategySubWedge.screeningStrategyWedgeID
'''
          break
      for c in columns:
        if c.startswith('ScreeningOutputLattice'):
          sql_str += '''\
INNER JOIN ScreeningOutputLattice
ON ScreeningOutput.screeningOutputID = ScreeningOutputLattice.screeningOutputID
'''
          break
    sql_str += '''\
WHERE Screening.dataCollectionID=%s
;
''' %dc_id
    results = self.execute(sql_str)
    field_names = [i[0] for i in self._cursor.description]
    return field_names, results

  def get_processing_statistics(self, dc_ids, columns=None, statistics_type='overall'):
    assert statistics_type in ('outerShell', 'innerShell', 'overall')
    if columns is not None:
      select_str = ', '.join(c for c in columns)
    else:
      select_str = '*'
    sql_str = '''
SELECT %s
FROM AutoProcIntegration
INNER JOIN AutoProcProgram
ON AutoProcIntegration.autoProcProgramId = AutoProcProgram.autoProcProgramId
INNER JOIN AutoProcScaling_has_Int
ON AutoProcIntegration.autoProcIntegrationId = AutoProcScaling_has_Int.autoProcIntegrationId
INNER JOIN AutoProcScaling
ON AutoProcScaling_has_Int.autoProcScalingId = AutoProcScaling.autoProcScalingId
INNER JOIN AutoProcScalingStatistics
ON AutoProcScaling.autoProcScalingId = AutoProcScalingStatistics.autoProcScalingId
WHERE AutoProcIntegration.dataCollectionId IN (%s) AND scalingStatisticsType='%s'
;
''' %(select_str, ','.join(str(i) for i in dc_ids), statistics_type)
    results = self.execute(sql_str)
    field_names = [i[0] for i in self._cursor.description]
    return field_names, results

  def insert_alignment_result(self, values):
    keys = ('dataCollectionId', 'program', 'shortComments', 'comments', 'phi')
    for k in keys:
      assert k in values, k
      self.execute('SET @%s="%s";' % (k, values[k]))
    self.execute('insert into Screening (dataCollectionId, programVersion, comments, shortComments) values ('
                 '@dataCollectionId, @program, @comments, @shortComments'
                 ');')
    self.execute('SET @scrId=LAST_INSERT_ID();')
    self.execute('insert into ScreeningOutput (screeningId) values (@scrId);')
    self.execute('SET @scrOutId = LAST_INSERT_ID();')
    self.execute('insert into ScreeningStrategy (screeningOutputId, program) values ('
                 '@scrOutId, @program'
                 ');')
    self.execute('SET @scrStratId = LAST_INSERT_ID();')
    if 'chi' in values:
      self.execute('SET @chi="%s";' % (values['chi']))
      self.execute('insert into ScreeningStrategyWedge (screeningStrategyId, chi, phi) values ('
                   '@scrStratId, @chi, @phi'
                   ');')
    elif 'kappa' in values:
      self.execute('SET @kappa="%s";' % (values['kappa']))
      self.execute('insert into ScreeningStrategyWedge (screeningStrategyId, kappa, phi) values ('
                   '@scrStratId, @kappa, @phi'
                   ');')
    else:
      raise RuntimeError('chi or kappa values must be provided')
    self.commit()


def ispyb_filter(message, parameters):
  '''Do something to work out what to do with this data...'''

  if not 'ispyb_dcid' in parameters:
    return message, parameters

  # FIXME put in here logic to check input if set i.e. if dc_id==0 then check
  # files exist; if image already set check they exist, ...

  i = ispyb()
  dc_id = parameters['ispyb_dcid']

  dc_info = i.get_dc_info(dc_id)
  parameters['ispyb_dc_info'] = dc_info
  dc_class = i.classify_dc(dc_info)
  parameters['ispyb_dc_class'] = dc_class
  start, end = i.dc_info_to_start_end(dc_info)
  parameters['ispyb_image_first'] = start
  parameters['ispyb_image_last'] = end
  parameters['ispyb_image_pattern'] = i.dc_info_to_filename_pattern(dc_info)
  parameters['ispyb_image'] = '%s:%d:%d' % (i.dc_info_to_filename(dc_info),
                                            start, end)
  parameters['ispyb_working_directory'] = i.dc_info_to_working_directory(
    dc_info, 'ispyb')
  parameters['ispyb_results_directory'] = i.dc_info_to_results_directory(
    dc_info, 'ispyb')

  if dc_class['grid']:
    message['default_recipe'] = ['per-image-analysis-gridscan']
    return message, parameters

  if dc_class['screen']:
    message['default_recipe'] = ['per-image-analysis-rotation', 'strategy-edna', 'strategy-mosflm', 'strategy-xia2']
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

  message['default_recipe'] = ['per-image-analysis-rotation', 'processing-fast-dp', 'processing-xia2-3dii', 'processing-xia2-dials',
                               'processing-multi-xia2']

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
