from __future__ import absolute_import, division, print_function

import json
import logging
import os
import uuid

import ispyb
import ispyb.exception
import mysql.connector  # installed by ispyb

# Temporary API to ISPyB while I wait for a proper one using stored procedures
# - beware here be dragons, written by a hacker who is not a database wonk.

with open('/dls_sw/apps/zocalo/secrets/ispyb-login.json', 'r') as sauce:
  secret_ingredients = json.load(sauce)

# convenience functions
def _clean_(path):
  return path.replace(2*os.sep, os.sep)

def _prefix_(template):
  if not template:
    return template
  return template.split('#')[0]

def _ispyb_api():
  if not hasattr(_ispyb_api, 'instance'):
    setattr(_ispyb_api, 'instance',
        ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg'))
  return _ispyb_api.instance

future_enabled = False
def _enable_future():
  global future_enabled
  if future_enabled: return
  import ispyb.model.__future__
  ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
  future_enabled = True

class ispybtbx(object):
  def __init__(self):
    self.legacy_init()

    self.log = logging.getLogger('dlstbx.ispybtbx')
    self.log.debug('ISPyB objects set up')

  def __call__(self, message, parameters):
    reprocessing_id = parameters.get('ispyb_reprocessing_id', parameters.get('ispyb_process'))
    if reprocessing_id:
      parameters['ispyb_process'] = reprocessing_id
      try:
        rp = _ispyb_api().get_processing_job(reprocessing_id)
        parameters['ispyb_images'] = ','.join(
            "%s:%d:%d" % (
                sweep.data_collection.file_template_full_python % sweep.start if '%' in sweep.data_collection.file_template_full_python else sweep.data_collection.file_template_full_python,
                sweep.start,
                sweep.end,
            )
            for sweep in rp.sweeps
        )
        parameters['ispyb_reprocessing_parameters'] = {
            k: v.value for k, v in dict(rp.parameters).items()
        }
      except ispyb.exception.ISPyBNoResultException:
        self.log.warning("Reprocessing ID %s not found", str(reprocessing_id))
    return message, parameters

  def get_gridscan_info(self, dcgid):
    '''Extract GridInfo table contents for a DC group ID.'''
    newgrid = _ispyb_api().get_data_collection_group(dcgid).gridinfo
    if not newgrid:
      return {} # This is no grid scan.
    return {
        'steps_x': newgrid.steps_x,
        'steps_y': newgrid.steps_y,
        'dx_mm': newgrid.dx_mm,
        'dy_mm': newgrid.dy_mm,
        'orientation': newgrid.orientation,
        'snaked': newgrid.snaked,
        'snapshot_offsetXPixel': newgrid.snapshot_offset_pixel_x,
        'snapshot_offsetYPixel': newgrid.snapshot_offset_pixel_y,
#       'recordTimeStamp': newgrid.timestamp,
        'gridInfoId': newgrid.id,
        'pixelsPerMicronX': newgrid.pixels_per_micron_x,
        'pixelsPerMicronY': newgrid.pixels_per_micron_y,
        'dataCollectionGroupId': newgrid.dcgid,
    }

  def legacy_init(self):
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
    if results:
      for l, r in zip(labels, results[0]):
        result[l] = r
    return result

  def get_beamline_from_dcid(self, dc_id):
    results = self.execute('SELECT bs.beamlineName FROM BLSession bs INNER JOIN DataCollectionGroup dcg ON dcg.sessionId = bs.sessionId INNER JOIN DataCollection dc ON dc.dataCollectionGroupId = dcg.dataCollectionGroupId WHERE dc.dataCollectionId = %s;' % str(dc_id))
    if not results:
      return None
    assert(len(results) == 1)
    result = results[0][0]
    return result

  def get_pia_results(self, dc_ids, columns=None):
    if columns is not None:
      select_str = ', '.join(c for c in columns)
    else:
      select_str = '*'
    sql_str = '''
SELECT %s
FROM ImageQualityIndicators
WHERE ImageQualityIndicators.dataCollectionId IN (%s)
;
''' %(select_str, ','.join(str(i) for i in dc_ids))
    results = self.execute(sql_str)
    field_names = [i[0] for i in self._cursor.description]
    return field_names, results

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

  def get_edge_data(self, dc_id):

    def __energy_offset(row):
      energy = 12398.42 / row['wavelength']
      pk_energy = row['peakenergy']
      if_energy = row['inflectionenergy']

      return min(abs(pk_energy - energy),
                 abs(if_energy - energy))

    def __select_edge_position(wl, peak, infl):
      e0 = 12398.42 / wl
      if e0 > peak + 30.:
          return 'hrem'
      if e0 < infl - 30.:
          return 'lrem'
      if abs(e0 - infl) < abs(e0 - peak):
          return 'infl'
      return 'peak'

    s = '''SELECT
    EnergyScan.energyscanid,
    EnergyScan.element,
    EnergyScan.peakenergy,
    EnergyScan.peakfprime,
    EnergyScan.peakfdoubleprime,
    EnergyScan.inflectionenergy,
    EnergyScan.inflectionfprime,
    EnergyScan.inflectionfdoubleprime,
    DataCollection.wavelength,
    BLSample.blsampleid as dcidsampleid,
    BLSampleProtein.blsampleid as protsampleid
FROM
    DataCollection
        INNER JOIN
    BLSample ON BLSample.blsampleid = DataCollection.blsampleid
        INNER JOIN
    Crystal ON Crystal.crystalid = BLSample.crystalid
        INNER JOIN
    Protein ON Protein.proteinid = Crystal.proteinid
        INNER JOIN
    Crystal CrystalProtein ON Protein.proteinid = CrystalProtein.proteinid
        INNER JOIN
    BLSample BLSampleProtein ON CrystalProtein.crystalid = BLSampleProtein.crystalid
        INNER JOIN
    EnergyScan ON DataCollection.sessionid = EnergyScan.sessionid
        AND BLSampleProtein.blsampleid = EnergyScan.blsampleid
WHERE
    DataCollection.datacollectionid = %s
        AND EnergyScan.element IS NOT NULL
'''
    labels = ('energyscanid',
              'element',
              'peakenergy',
              'peakfprime',
              'peakfdoubleprime',
              'inflectionenergy',
              'inflectionfprime',
              'inflectionfdoubleprime',
              'wavelength',
              'dcidsampleid',
              'protsampleid')
    all_rows = [dict(zip(labels, r)) for r in self.execute(s, dc_id)]
    rows = [r for r in all_rows if r['dcidsampleid'] == r['protsampleid']]
    if not rows:
      rows = all_rows
    try:
      energy_scan = min(rows, key=__energy_offset)
      edge_position = __select_edge_position(energy_scan['wavelength'],
                                             energy_scan['peakenergy'],
                                             energy_scan['inflectionenergy'])
      res = {'energyscanid' : energy_scan['energyscanid'],
             'atom_type' : energy_scan['element'],
             'edge_position' : edge_position,
             }
      if edge_position == 'peak':
        res.update({'fp': energy_scan['peakfprime'],
                    'fpp': energy_scan['peakfdoubleprime']})
      else:
        if edge_position == 'infl':
            res.update({'fp': energy_scan['inflectionfprime'],
                        'fpp': energy_scan['inflectionfdoubleprime']})
    except:
        res = {}
    return res

  def get_sequence(self, dc_id):

    s = '''SELECT
    Protein.sequence
FROM
    DataCollection
        INNER JOIN
    BLSample ON BLSample.blsampleid = DataCollection.blsampleid
        INNER JOIN
    Crystal ON Crystal.crystalid = BLSample.crystalid
        INNER JOIN
    Protein ON Protein.proteinid = Crystal.proteinid
WHERE
    DataCollection.datacollectionid = %s
'''
    row = self.execute(s, dc_id)
    try:
      seq = row[0][0]
    except:
      seq = None
    return seq

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
    template = dc_info.get('fileTemplate')
    if not template:
      return None
    if '#' not in template:
      return template
    fmt = '%%0%dd' % template.count('#')
    prefix = template.split('#')[0]
    suffix = template.split('#')[-1]
    return prefix + fmt + suffix

  def dc_info_to_filename(self, dc_info, image_number=None):
    directory = dc_info['imageDirectory']
    template = self.dc_info_to_filename_pattern(dc_info)
    if '%' not in template:
      return os.path.join(directory, template)
    if image_number:
      return os.path.join(directory, template % image_number)
    if dc_info['startImageNumber']:
      return os.path.join(directory, template % dc_info['startImageNumber'])
    return None

  def dc_info_to_start_end(self, dc_info):
    start = dc_info.get('startImageNumber')
    number = dc_info.get('numberOfImages')
    if start is None or number is None:
      end = None
    else:
      end = start + number - 1
    return start, end

  def dc_info_is_grid_scan(self, dc_info):
    number_of_images = dc_info.get('numberOfImages')
    axis_range = dc_info.get('axisRange')
    if number_of_images is None or axis_range is None:
      return None
    return number_of_images > 1 and axis_range == 0.0

  def dc_info_is_screening(self, dc_info):
    if dc_info.get('numberOfImages') == None:
      return None
    if dc_info['numberOfImages'] == 1:
      return True
    if dc_info['numberOfImages'] > 1 and dc_info['overlap'] != 0.0:
      return True
    return False

  def dc_info_is_rotation_scan(self, dc_info):
    overlap = dc_info.get('overlap')
    axis_range = dc_info.get('axisRange')
    if overlap is None or axis_range is None:
      return None
    return overlap == 0.0 and axis_range > 0

  def classify_dc(self, dc_info):
    return {'grid':self.dc_info_is_grid_scan(dc_info),
            'screen':self.dc_info_is_screening(dc_info),
            'rotation':self.dc_info_is_rotation_scan(dc_info)}

  def data_folder_to_visit(self, directory):
    '''Extract visit directory, assumes the path structure goes something
    like /dls/${beamline}/data/${year}/${visit} - 2016/11/03 this is a
    valid assumption'''

    return os.sep.join(directory.split(os.sep)[:6]).strip()

  def dc_info_to_working_directory(self, dc_info):
    prefix = _prefix_(dc_info.get('fileTemplate'))
    if not prefix: return None
    directory = dc_info['imageDirectory']
    visit = self.data_folder_to_visit(directory)
    rest = directory.replace(visit, '')
    root = _clean_(os.sep.join([visit, 'tmp', 'zocalo', rest, prefix]))
    return os.path.join(root, dc_info['uuid'])

  def dc_info_to_results_directory(self, dc_info):
    prefix = _prefix_(dc_info.get('fileTemplate'))
    if not prefix: return None
    directory = dc_info['imageDirectory']
    visit = self.data_folder_to_visit(directory)
    rest = directory.replace(visit, '')
    root = _clean_(os.sep.join([visit, 'processed', rest, prefix]))
    return os.path.join(root, dc_info['uuid'])

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

  def get_screening_results(self, dc_ids, columns=None):
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
WHERE Screening.dataCollectionID IN (%s)
;
''' %','.join(str(i) for i in dc_ids)
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
INNER JOIN AutoProcProgramAttachment
ON AutoProcProgram.autoProcProgramId = AutoProcProgramAttachment.autoProcProgramId
INNER JOIN AutoProcScaling_has_Int
ON AutoProcIntegration.autoProcIntegrationId = AutoProcScaling_has_Int.autoProcIntegrationId
INNER JOIN AutoProcScaling
ON AutoProcScaling_has_Int.autoProcScalingId = AutoProcScaling.autoProcScalingId
INNER JOIN AutoProcScalingStatistics
ON AutoProcScaling.autoProcScalingId = AutoProcScalingStatistics.autoProcScalingId
INNER JOIN AutoProc
ON AutoProcScaling.autoProcId = AutoProc.autoProcId
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

  def insert_fastep_phasing_results(self, values):
    self.execute('SET @recordTimeStamp = CURRENT_TIMESTAMP');
    self.execute('INSERT INTO PhasingAnalysis'
                  '(recordTimeStamp)'
                  'VALUES'
                  '(@recordTimeStamp);')
    self.execute('SET @phasingAnalysisId = LAST_INSERT_ID();')
    self.execute('INSERT INTO PhasingProgramRun'
                 '(phasingCommandLine, phasingPrograms, phasingStatus)'
                 'VALUES'
                 '(%(phasingCommandLine)s, %(phasingPrograms)s, %(phasingStatus)s);',
                 values['PhasingProgramRun'])
    self.execute('SET @phasingProgramRunId = LAST_INSERT_ID();')
    self.execute('INSERT INTO PhasingProgramAttachment'
                 '(phasingProgramRunId, fileType, fileName, filePath, recordTimeStamp)'
                 'VALUES'
                 '(@phasingProgramRunId, %(fileType)s, %(fileName)s, %(filePath)s, @recordTimeStamp);',
                 values['PhasingProgramAttachment'])
    self.execute('INSERT INTO Phasing'
                 '(phasingAnalysisId, phasingProgramRunId, spaceGroupId, method, solventContent, enantiomorph, lowRes, highRes, recordTimeStamp)'
                 'VALUES'
                 '(@phasingAnalysisId, @phasingProgramRunId, %(spaceGroupId)s, %(method)s, %(solventContent)s, %(enantiomorph)s, %(lowRes)s, %(highRes)s, @recordTimeStamp);',
                 values['Phasing'])
    self.execute('INSERT INTO PreparePhasingData'
                 '(phasingAnalysisId, phasingProgramRunId, spaceGroupId, lowRes, highRes, recordTimeStamp)'
                 'VALUES'
                 '(@phasingAnalysisId, @phasingProgramRunId, %(spaceGroupId)s, %(lowRes)s, %(highRes)s, @recordTimeStamp);',
                 values['PreparePhasingData'])
    self.execute('INSERT INTO SubstructureDetermination'
                 '(phasingAnalysisId, phasingProgramRunId, spaceGroupId, method, lowRes, highRes, recordTimeStamp)'
                 'VALUES'
                 '(@phasingAnalysisId, @phasingProgramRunId, %(spaceGroupId)s, %(method)s, %(lowRes)s, %(highRes)s, @recordTimeStamp);',
                 values['SubstructureDetermination'])
    self.execute('INSERT INTO Phasing_has_Scaling'
                 '(phasingAnalysisId, autoProcScalingId, recordTimeStamp)'
                 'VALUES'
                 '(@phasingAnalysisId, %s, @recordTimeStamp);', (values['autoProcScalingId'],))
    self.execute('SET @phasingHasScalingId = LAST_INSERT_ID();')
    for stats in values['phasingStatistics']:
      self.execute('INSERT INTO PhasingStatistics'
                   '(phasingHasScalingId1, numberOfBins, binNumber, lowRes, highRes, metric, statisticsValue, nReflections, recordTimeStamp)'
                   'VALUES'
                   '(@phasingHasScalingId, %(numberOfBins)s, %(binNumber)s, %(lowRes)s, %(highRes)s, %(metric)s, %(statisticsValue)s, %(nReflections)s, @recordTimeStamp);',
                   stats)
    self.commit()

  def get_visit_name_from_dcid(self, dc_id):
    sql_str = '''
SELECT proposalcode, proposalnumber, visit_number
FROM BLSession bs
INNER JOIN DataCollectionGroup dcg
ON dcg.sessionId = bs.sessionId
INNER JOIN DataCollection dc
ON dc.dataCollectionGroupId = dcg.dataCollectionGroupId
INNER JOIN Proposal p
ON bs.proposalId = p.proposalId
WHERE dc.dataCollectionId='%s'
;''' % str(dc_id)
    results = self.execute(sql_str)
    assert len(results) == 1, len(results)
    assert len(results[0]) == 3, results[0]
    proposal_code, proposal_number, visit_number = results[0]
    return proposal_code, proposal_number, visit_number

  def get_bl_sessionid_from_visit_name(self, visit_name):
    import re
    m = re.match(r'([a-z][a-z])([\d]+)[-]([\d]+)', visit_name)
    assert m is not None
    assert len(m.groups()) == 3
    proposal_code, proposal_number, visit_number = m.groups()
    sql_str = '''
SELECT sessionId
FROM BLSession bs
INNER JOIN Proposal p
ON bs.proposalId = p.proposalId
WHERE p.proposalcode='%s' and p.proposalnumber='%s' and bs.visit_number='%s'
;''' %(proposal_code, proposal_number, visit_number)
    results = self.execute(sql_str)
    assert len(results) == 1
    return results[0][0]

def ispyb_filter(message, parameters):
  '''Do something to work out what to do with this data...'''

  i = ispybtbx()

  message, parameters = i(message, parameters)

  processingjob_id = parameters.get('ispyb_reprocessing_id', parameters.get('ispyb_process'))
  if processingjob_id:
    parameters['ispyb_processing_job'] = _ispyb_api().get_processing_job(processingjob_id)
    if not 'ispyb_dcid' in parameters:
      parameters['ispyb_dcid'] = parameters['ispyb_processing_job'].DCID

  if not 'ispyb_dcid' in parameters:
    return message, parameters

  # FIXME put in here logic to check input if set i.e. if dc_id==0 then check
  # files exist; if image already set check they exist, ...

  dc_id = parameters['ispyb_dcid']

  dc_info = i.get_dc_info(dc_id)
  dc_info['uuid'] = parameters.get('guid') or str(uuid.uuid4())
  parameters['ispyb_beamline'] = i.get_beamline_from_dcid(dc_id)
  parameters['ispyb_dc_info'] = dc_info
  dc_class = i.classify_dc(dc_info)
  parameters['ispyb_dc_class'] = dc_class
  start, end = i.dc_info_to_start_end(dc_info)
  if dc_class['grid'] and dc_info['dataCollectionGroupId']:
    try:
      gridinfo = i.get_gridscan_info(dc_info['dataCollectionGroupId'])
      if gridinfo:
        # FIXME: timestamps can not be JSON-serialized
        if 'recordTimeStamp' in gridinfo:
          del(gridinfo['recordTimeStamp'])
        parameters['ispyb_dc_info']['gridinfo'] = gridinfo
    except ispyb.exception.ISPyBNoResultException:
      pass
  parameters['ispyb_image_first'] = start
  parameters['ispyb_image_last'] = end
  parameters['ispyb_image_template'] = dc_info.get('fileTemplate')
  parameters['ispyb_image_directory'] = dc_info.get('imageDirectory')
  parameters['ispyb_image_pattern'] = i.dc_info_to_filename_pattern(dc_info)
  if not parameters.get('ispyb_image') and start is not None and end is not None:
    parameters['ispyb_image'] = '%s:%d:%d' % (i.dc_info_to_filename(dc_info),
                                              start, end)
  parameters['ispyb_working_directory'] = i.dc_info_to_working_directory(dc_info)
  parameters['ispyb_results_directory'] = i.dc_info_to_results_directory(dc_info)

  if 'ispyb_processing_job' in parameters and \
      parameters['ispyb_processing_job'].recipe and \
      not message.get('recipes') and \
      not message.get('custom_recipe'):
    # Prefix recipe name coming from ispyb/synchweb with 'ispyb-'
    message['recipes'] = [ 'ispyb-' + parameters['ispyb_processing_job'].recipe ]
    return message, parameters

  if dc_class['grid']:
    if parameters['ispyb_beamline'] == 'i02-2':
      message['default_recipe'] = ['archive-nexus', 'vmxi-spot-counts-per-image']
    else:
      message['default_recipe'] = ['per-image-analysis-gridscan']
    return message, parameters

  if dc_class['screen']:
    message['default_recipe'] = ['per-image-analysis-rotation', 'strategy-edna', 'strategy-mosflm']
    parameters['ispyb_images'] = ''
    return message, parameters

  if not dc_class['rotation']:
    # possibly EM dataset
    message['default_recipe'] = [ ]
    return message, parameters

  # for the moment we do not want multi-xia2 for /dls/mx i.e. VMXi
  # beware if other projects start using this directory structure will
  # need to be smarter here...

  if parameters['ispyb_image_directory'].startswith('/dls/mx'):
    related = []

  else:
    related_dcs = i.get_dc_group(dc_id)
    #related_dcs.extend(i.get_matching_dcids_by_folder(dc_id))
    #related_dcs.extend(i.get_matching_dcids_by_sample_and_session(dc_id))

    related = list(sorted(set(related_dcs)))

  other_dc_info = { }

  parameters['ispyb_space_group'] = i.get_space_group(dc_id)

  related_images = []

  if not parameters.get('ispyb_images'):
   # may have been set via __call__ for reprocessing jobs
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

  message['default_recipe'] = [
      'per-image-analysis-rotation',
      'processing-autoproc',
      'processing-fast-dp',
      'processing-rlv',
      'processing-xia2-3dii',
      'processing-xia2-dials',
      'processing-xia2-dials-full',
  ]

  if parameters['ispyb_beamline'] == 'i02-2':
    message['default_recipe'] = [
        'archive-nexus',
        'processing-fast-dp',
        'processing-xia2-dials',
        'processing-xia2-dials-full',
        'vmxi-per-image-analysis',
    ]

  if parameters['ispyb_images']:
    message['default_recipe'].append('processing-multi-xia2-dials')
    message['default_recipe'].append('processing-multi-xia2-3dii')

  return message, parameters

def work(dc_ids):

  import pprint

  pp = pprint.PrettyPrinter(indent=2)

  i = ispybtbx()

  for dc_id in dc_ids:
    print(i.get_space_group_and_cell(dc_id))
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
    raise RuntimeError('for this mode of testing pass list of DCID on CL')
  else:
    work(map(int, sys.argv[1:]))
