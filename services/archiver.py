from __future__ import absolute_import, division

import errno
import itertools
import os
import os.path
import xml.etree.cElementTree as ET
from datetime import datetime

import workflows.recipe
from workflows.services.common_service import CommonService

class DLSArchiver(CommonService):
  '''A service that generates dropfiles for data collections.'''

  # Human readable service name
  _service_name = "DLS Archiver"

  # Logger name
  _logger_name = 'dlstbx.services.archiver'

  def initializing(self):
    '''Subscribe to the archiver queue. Received messages must be
       acknowledged.'''
    self.log.info("Archiver starting")
    workflows.recipe.wrap_subscribe(
        self._transport, 'archive.pattern',
        self.archive_dcid, acknowledgement=True, log_extender=self.extend_log)

  @staticmethod
  def rangifier(numbers):
    '''Convert lists into lists of ranges. Copied from are_all_images_there.py'''
    ranges = lambda l:map(lambda x:(x[0][1], x[-1][1]),
                          map(lambda (x,y):list(y), itertools.groupby(enumerate(l),
                                                                      lambda (x,y):x-y)))
    return list(ranges(numbers))

  def archive_dcid(self, rw, header, message):
    '''Archive collected datafiles connected to a data collection.'''

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header, transaction=txn)

    # Extract the recipe
    subrecipe = rw.recipe_step
    self.log.info("Attempting to archive %s", subrecipe['parameters']['pattern'])

    settings = subrecipe['parameters'].copy()
    if isinstance(message, dict):
      for field in ('multipart', 'pattern-start'):
        if 'archive-' + field in message:
          settings[field] = message['archive-' + field]

    file_range_limit = int(settings.get('limit-files', 0))

    filepaths = subrecipe['parameters']['pattern'].split('/')
    _, _, beamline, _, _, visit_id = filepaths[0:6]
    visit_id_u = visit_id.upper()

    icat = ET.Element('icat')
    icat.set('version', '1.0 RC6')
    icat.set('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
    icat.set('xsi:noNamespaceSchemaLocation', 'icatXSD.xsd')
    study = ET.SubElement(icat, 'study')
    investigation = ET.SubElement(study, 'investigation')
    ET.SubElement(investigation, 'inv_number').text = visit_id_u.split('-')[0]
    ET.SubElement(investigation, 'visit_id').text = visit_id_u
    ET.SubElement(investigation, 'instrument').text = beamline
    ET.SubElement(investigation, 'title').text = 'dont need it'
    ET.SubElement(investigation, 'inv_type').text = 'experiment'

    dataset = ET.SubElement(investigation, 'dataset')
    ET.SubElement(dataset, 'name').text = '/'.join(filepaths[6:-1]) or 'topdir'
    ET.SubElement(dataset, 'dataset_type').text = 'EXPERIMENT_RAW'
    ET.SubElement(dataset, 'description').text = 'unknown'

    message_out = { 'success': 0, 'failed': 0 }
    files_not_found = []
    for x in range(int(settings['pattern-start']), int(settings['pattern-end']) + 1):
      if file_range_limit and message_out['success'] >= file_range_limit:
        # Test for limit at beginning, not end, so >= 1 file remains
        self.log.info("Reached dropfile limit of %d entries, splitting job.", file_range_limit)
        # limit reached - bail out
        if not settings.get('multipart'):
          settings['multipart'] = 1
        rw.checkpoint({
            'archive-multipart': settings['multipart'] + 1,
            'archive-pattern-start': x,
          }, transaction=txn)
        break

      filename = subrecipe['parameters']['pattern'] % x

      try:
        stat = os.stat(filename)
      except OSError, e:
        if e.errno == errno.ENOENT:
          files_not_found.append(filename)
        else:
          # Report all missing files as warnings unless recipe says otherwise
          if rw.recipe_step['parameters'].get('log-file-warnings-as-info'):
            self.log.info("Could not archive %s", filename, exc_info=True)
          else:
            self.log.warning("Could not archive %s", filename, exc_info=True)
        message_out['failed'] += 1
        continue
      self.log.debug("Archiving %s", filename)
      df = ET.SubElement(dataset, 'datafile')
      ET.SubElement(df, 'name').text = filename.split('/')[-1]
      ET.SubElement(df, 'location').text = filename
      ET.SubElement(df, 'description').text = 'unknown'
      ET.SubElement(df, 'datafile_version').text = '1.0'
      ET.SubElement(df, 'datafile_create_time').text = \
        datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%dT%H:%M:%S")
      ET.SubElement(df, 'datafile_modify_time').text = \
        datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%dT%H:%M:%S")
        # both are set to time of last modification
      ET.SubElement(df, 'file_size').text = str(stat.st_size)
      message_out['success'] += 1
    if files_not_found:
      self.log.info("The following files were not found:\n%s", "\n".join(files_not_found))
    self.log.info("%d files archived", message_out['success'])
    if message_out['failed']:
      if rw.recipe_step['parameters'].get('log-summary-warning-as-info'):
        self.log.info("Failed to archive %d files", message_out['failed'])
      else:
        self.log.warning("Failed to archive %d files", message_out['failed'])

    def indent(elem, level=0):
      i = "\n" + level*"  "
      if len(elem):
        if not elem.text or not elem.text.strip():
          elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
          elem.tail = i
        for elem in elem:
          indent(elem, level+1)
        if not elem.tail or not elem.tail.strip():
          elem.tail = i
      else:
        if level and (not elem.tail or not elem.tail.strip()):
          elem.tail = i
    indent(icat)

    xml_string = '<?xml version="1.0" ?>\n' + ET.tostring(icat)
    dropfile = subrecipe['parameters'].get('dropfile')
    if dropfile == '{dropfile_override}':
      dropfile = None
    if not dropfile and all(k in subrecipe['parameters'] for k in ('dropfile-dir', 'dropfile-filename')):
      dropfile = os.path.join(subrecipe['parameters']['dropfile-dir'], subrecipe['parameters']['dropfile-filename'])
    if dropfile:
      timestamp = datetime.strftime(datetime.now(), "%Y%m%d-%H%M%S")
      multipart_label = '-' + str(settings['multipart']) if settings.get('multipart') else ''
      dropfile = dropfile.format(visit_id=visit_id, beamline=beamline, timestamp=timestamp, multipart=multipart_label)
      if message_out['success']:
        with open(dropfile, 'w') as fh:
          fh.write(xml_string)
        self.log.info("Written dropfile XML to %s", dropfile)
      else:
        self.log.info("Skipped writing empty dropfile XML to %s", dropfile)
    message_out['xml'] = xml_string

    rw.set_default_channel('dropfile')
    rw.send_to('dropfile', message_out, transaction=txn)

    self._transport.transaction_commit(txn)
    self.log.info("Done.")
