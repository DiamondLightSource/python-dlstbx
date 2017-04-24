from __future__ import absolute_import, division
from datetime import datetime
import errno
import os
import os.path
from workflows.recipe import Recipe
from workflows.services.common_service import CommonService
import xml.etree.cElementTree as ET
from xml.etree.cElementTree import Element, ElementTree

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
    self._transport.subscribe('archive.pattern',
                              self.archive_dcid,
                              acknowledgement=True)

  def archive_dcid(self, header, message):
    '''Archive collected datafiles connected to a data collection.'''

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header, transaction=txn)

    # Extract the recipe
    current_recipe = Recipe(header['recipe'])
    current_recipepointer = int(header['recipe-pointer'])
    subrecipe = current_recipe[current_recipepointer]

    self.log.info("Attempting to archive %s", subrecipe['parameters']['pattern'])

    # List files to archive
    files = [ subrecipe['parameters']['pattern'] % x
              for x in range(int(subrecipe['parameters']['pattern-start']),
                             int(subrecipe['parameters']['pattern-end']) + 1) ]

    filepaths = subrecipe['parameters']['pattern'].split('/')
    _, _, beamline, _, _, visit_id = filepaths[0:6]
    visit_id_u = visit_id.upper()

    icat = Element('icat')
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
    for f in files:
      try:
        stat = os.stat(f)
      except OSError, e:
        if e.errno == errno.ENOENT:
          files_not_found.append(f)
        else:
          self.log.warn("Could not archive %s", f, exc_info=True)
        message_out['failed'] += 1
        continue
      self.log.debug("Archiving %s", f)
      df = ET.SubElement(dataset, 'datafile')
      ET.SubElement(df, 'name').text = f.split('/')[-1]
      ET.SubElement(df, 'location').text = f
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
      self.log.warn("Failed to archive %d files", message_out['failed'])

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
      dropfile = dropfile.format(visit_id=visit_id, beamline=beamline, timestamp=timestamp)
      if message_out['success']:
        with open(dropfile, 'w') as fh:
          fh.write(xml_string)
        self.log.info("Written dropfile XML to %s", dropfile)
      else:
        self.log.info("Skipped writing empty dropfile XML to %s", dropfile)
    message_out['xml'] = xml_string

    if subrecipe['output']:
      if not isinstance(subrecipe['output'], list):
        subrecipe['output'] = [ subrecipe['output'] ]
      for destination in subrecipe['output']:
        header['recipe-pointer'] = destination

    message_headers = { 'recipe': header['recipe'] }
    self.notify(current_recipe, subrecipe['output'], message_headers, message_out, txn)

    self._transport.transaction_commit(txn)
    self.log.info("Done.")

  def notify(self, recipe, destinations, header, message, txn):
    '''Send notifications to selected output channels.'''
    if destinations is None:
      return
    if not isinstance(destinations, list):
      destinations = [ destinations ]
    for destination in destinations:
      header['recipe-pointer'] = destination
      if recipe[destination].get('queue'):
        self._transport.send(
            recipe[destination]['queue'],
            message, headers=header,
            transaction=txn)
      if recipe[destination].get('topic'):
        self._transport.broadcast(
            recipe[destination]['topic'],
            message, headers=header,
            transaction=txn)
