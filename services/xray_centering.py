from __future__ import absolute_import, division, print_function

import errno
import json
import os
import threading
import time
import sys

import dlstbx.util.xraycentering
import workflows.recipe
from workflows.services.common_service import CommonService

class DLSXRayCentering(CommonService):
  '''A service to aggregate per-image-analysis results and identify an X-ray
     centering solution for a data collection.'''

  _service_name = "DLS X-Ray Centering"
  _logger_name = 'dlstbx.services.xray-centering'

  def initializing(self):
    '''Try to exclusively subscribe to the x-ray centering queue. Received messages must be acknowledged.
       Exclusive subscription enables a single process to do the 'reduce' step, aggregating many messages
       that belong together.
    '''
    self.log.info("X-Ray centering service starting up")

    self._centering_data = {}
    self._centering_lock = threading.Lock()

    self._register_idle(60, self.garbage_collect)
    workflows.recipe.wrap_subscribe(self._transport, 'reduce.xray_centering',
        self.add_pia_result, acknowledgement=True, exclusive=True, log_extender=self.extend_log)
#   need to restart AMQ server to enable reduce policy

  def garbage_collect(self):
    '''Throw away partial scan results after a while.'''
#   mostly pointless until reduce policy in place
#   self.log.debug('Garbage collect')

#    with self._centering_lock:
    # for dcid in centering_data:
    #   if last_seen > 15 minutes:
    #     transaction.start()
    #     ack_all_messages()
    #     send_something_to_abort_output()
    #     transaction.commit()
    #     remove dcid
    #     # could do this partially outside of lock, but probably does not matter

  def add_pia_result(self, rw, header, message):
    '''Process incoming PIA result.'''

    parameters = rw.recipe_step.get('parameters', None)
    if not parameters or not parameters.get('dcid'):
      self.log.error('X-ray centering service called without recipe parameters')
      rw.transport.nack(header)
      return
    gridinfo = rw.recipe_step.get('gridinfo', None)
    if not gridinfo or not isinstance(gridinfo, dict):
      print(gridinfo)
      self.log.error('X-ray centering service called without grid information')
      sys.exit(1)
      rw.transport.nack(header)
      return
    dcid = int(parameters['dcid'])

    if not message or not message.get('file-number') or message.get('n_spots_total') is None:
      self.log.error('X-ray centering service called without valid payload')
      rw.transport.nack(header)
      return
    file_number = message['file-number']
    spots_count = message['n_spots_total']

    with self._centering_lock:
      if dcid in self._centering_data:
        cd = self._centering_data[dcid]
      else:
        cd = {
            'steps_x': gridinfo.get('steps_x'),
            'steps_y': gridinfo.get('steps_y'),
            'images_seen': 0,
            'headers': [],
            'data': [],
        }
        cd['image_count'] = cd['steps_x'] * cd['steps_y']
        self._centering_data[dcid] = cd
        self.log.info('First record arrived for X-ray centering on DCID {dcid}, '
                      '{cd[steps_x]} x {cd[steps_y]} grid, {cd[image_count]} images in total'.format(
                          dcid=dcid, cd=cd))

#     Correct way of handling this requires reduce policy to be in place. x_x
#     cd['headers'].append(header)
      rw.transport.ack(header)

      cd['images_seen'] += 1
      cd['last_activity'] = time.time()
      self.log.debug('Received PIA result for DCID %d image %d, %d of %d expected results',
          dcid, file_number, cd['images_seen'], cd['image_count'])
      cd['data'].append((file_number, spots_count))

      if cd['images_seen'] == cd['image_count']:
        self.log.info('All records arrived for X-ray centering on DCID %d', dcid)
        result, output = dlstbx.util.xraycentering.main(
          cd['data'],
          numBoxesX=cd['steps_x'],
          numBoxesY=cd['steps_y'],
          snaked=bool(gridinfo.get('snaked')),
          boxSizeXPixels=1000 * gridinfo['dx_mm'] / gridinfo['pixelsPerMicronX'],
          boxSizeYPixels=1000 * gridinfo['dy_mm'] / gridinfo['pixelsPerMicronY'],
          topLeft=(float(gridinfo.get('snapshot_offsetXPixel')),
                   float(gridinfo.get('snapshot_offsetYPixel'))),
        )

        # Write result file
        if parameters.get('output'):
          self.log.info('Writing X-Ray centering results for DCID %d to %s', dcid, parameters['output'])
          path = os.path.dirname(parameters['output'])
          try:
            os.makedirs(path)
          except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
              pass
            else:
              raise
          with open(parameters['output'], 'w') as fh:
            json.dump(result, fh, sort_keys=True)

#       transaction.start()
#       ack_all_messages()

        # Send results onwards
        rw.set_default_channel('success')
        rw.send_to('success', result) # transaction=txn)
#       transaction.commit()

        print(result)
        print(output)

        del self._centering_data[dcid]

#   if last_garbage_collection > 60 seconds:
#     self.garbage_collect()
