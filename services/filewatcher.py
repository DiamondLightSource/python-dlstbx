from __future__ import absolute_import, division, print_function

import os.path
import time

import workflows.recipe
from workflows.services.common_service import CommonService

def is_file_selected(file_number, selection, total_files):
  '''Checks if item number 'file_number' is in a list of 'selection'
     evenly spread out items out of a list of 'total_files' items,
     without constructing the full list of selected items.

     :param: file_number: positive number between 1 and total_files
     :param: selection: number of files to be selected out of total_files
     :param: total_files: number of total files
     :return: True if file_number would be selected, False otherwise.
  '''
  return total_files <= selection or \
      file_number in (
          total_files,
          1 + round(file_number * (selection-1) // total_files) * total_files // (selection-1),
      )

class DLSFileWatcher(CommonService):
  '''A service that waits for files to arrive on disk and notifies interested
     parties when they do, or don't.'''

  # Human readable service name
  _service_name = "DLS Filewatcher"

  # Logger name
  _logger_name = 'dlstbx.services.filewatcher'

  def initializing(self):
    '''Subscribe to the filewatcher queue. Received messages must be
       acknowledged.'''
    self.log.info("Filewatcher starting")
    workflows.recipe.wrap_subscribe(
        self._transport, 'filewatcher',
        self.watch_files, acknowledgement=True, log_extender=self.extend_log)

  def watch_files(self, rw, header, message):
    '''Check for presence of files.'''

    # Check if message body contains partial results from a previous run
    status = { 'seen-files': 0, 'start-time': time.time() }
    if isinstance(message, dict):
      status.update(message.get('filewatcher-status', {}))

    # List files to wait for
    pattern = rw.recipe_step['parameters']['pattern']
    pattern_start = int(rw.recipe_step['parameters']['pattern-start'])
    filecount = int(rw.recipe_step['parameters']['pattern-end']) - pattern_start + 1

    # Sanity check received message
    try:
      pattern % 0
    except TypeError:
      self.log.error("Rejecting message with non-conforming pattern string: %s", pattern)
      rw.transport.nack(header)
      return

    # Conditionally acknowledge receipt of the message
    txn = rw.transport.transaction_begin()
    rw.transport.ack(header, transaction=txn)

    # Identify selections to notify for
    selections = [ k for k in rw.recipe_step['output'].iterkeys()
                   if isinstance(k, basestring) and k.startswith('select-') ]
    selections = { int(k[7:]): k for k in selections }

    # Look for files
    files_found = 0
    while status['seen-files'] < filecount and \
          files_found < rw.recipe_step['parameters'].get('burst-limit', 100) and \
          os.path.isfile(pattern % (pattern_start + status['seen-files'])):
      filename = pattern % (pattern_start + status['seen-files'])
      notification_record = {
          'file': filename,
          'file-number': status['seen-files'] + 1,
          'file-pattern-index': pattern_start + status['seen-files'],
      }

      files_found += 1
      status['seen-files'] += 1

      # Notify for first file
      if status['seen-files'] == 1:
        rw.send_to('first', notification_record, transaction=txn)

      # Notify for every file
      rw.send_to('every', notification_record, transaction=txn)

      # Notify for last file
      if status['seen-files'] == filecount:
        rw.send_to('last', notification_record, transaction=txn)

      # Notify for nth file
      rw.send_to(status['seen-files'], notification_record, transaction=txn)
      rw.send_to(str(status['seen-files']), notification_record, transaction=txn)

      # Notify for selections
      for m, dest in selections.iteritems():
        if is_file_selected(status['seen-files'], m, filecount):
          rw.send_to(dest, notification_record, transaction=txn)

    # Are we done?
    if status['seen-files'] == filecount:
      # Happy days

      self.log.debug(
          "%d files found for %s with indices %d-%d (all %d files found)",
          files_found,
          pattern,
          pattern_start + status['seen-files'] - files_found,
          pattern_start + status['seen-files'] - 1,
          filecount,
      )

      extra_log = { "delay": time.time()-status['start-time'] }
      if rw.recipe_step['parameters'].get('expected-per-image-delay'):
        # Estimate unexpected delay
        expected_delay = float(rw.recipe_step['parameters']['expected-per-image-delay']) * filecount
        extra_log['unexpected_delay'] = max(0, extra_log['delay'] - expected_delay)

      self.log.info("All %d files found for %s after %.1f seconds.",
        filecount,
        rw.recipe_step['parameters']['pattern'],
        time.time()-status['start-time'],
        extra=extra_log)

      rw.send_to('any', {
                    'files-expected': filecount,
                    'files-seen': status['seen-files'],
                 }, transaction=txn)
      rw.send_to('finally', {
                    'files-expected': filecount,
                    'files-seen': status['seen-files'],
                    'success': True,
                 }, transaction=txn)

      self._transport.transaction_commit(txn)
      return

    message_delay = rw.recipe_step['parameters'].get('burst-wait')
    if files_found == 0:
      # If no files were found, check timeout conditions.
      if status['seen-files'] == 0:
        # For first file: relevant timeout is 'timeout-first', with fallback 'timeout', with fallback 1 hour
        timeout = rw.recipe_step['parameters'].get('timeout-first', rw.recipe_step['parameters'].get('timeout', 3600))
        timed_out = (status['start-time'] + timeout) < time.time()
      else:
        # For subsequent files: relevant timeout is 'timeout', with fallback 1 hour
        timeout = rw.recipe_step['parameters'].get('timeout', 3600)
        timed_out = (status['last-seen'] + timeout) < time.time()
      if timed_out:
        # File watch operation has timed out.

        # Report all timeouts as warnings unless the recipe specifies otherwise
        timeoutlog = self.log.warning
        if rw.recipe_step['parameters'].get('log-timeout-as-info'):
          timeoutlog = self.log.info

        timeoutlog("Filewatcher for %s timed out after %.1f seconds (%d files found, nothing seen for %.1f seconds)",
          rw.recipe_step['parameters']['pattern'],
          time.time()-status['start-time'],
          status['seen-files'],
          time.time()-status.get('last-seen', status['start-time']))

        # Notify for timeout
        rw.send_to('timeout', {
                        'file': pattern % (pattern_start + status['seen-files']),
                        'file-number': status['seen-files'] + 1,
                        'file-pattern-index': pattern_start + status['seen-files'],
                        'success': False }, transaction=txn)
        # Notify for 'any' target if any file was seen
        if status['seen-files']:
          rw.send_to('any', {
                  'files-expected': filecount,
                  'files-seen': status['seen-files'],
              }, transaction=txn)

        # Notify for 'finally' outcome
        rw.send_to('finally', {
                      'files-expected': filecount,
                      'files-seen': status['seen-files'],
                      'success': False }, transaction=txn)
        # Stop processing message
        self._transport.transaction_commit(txn)
        return

      # If no timeouts are triggered, set a minimum waiting time.
      if message_delay:
        message_delay = max(1, message_delay)
      else:
        message_delay = 1
      self.log.debug(
          ("No further files found for {pattern} after a total time of {time:.1f} seconds\n"
          "{files_seen} of {files_total} files seen so far").format(
             time=time.time()-status['start-time'],
         pattern=rw.recipe_step['parameters']['pattern'],
         files_seen=status['seen-files'], files_total=filecount))
    else:
      # Otherwise note last time progress was made
      status['last-seen'] = time.time()
      self.log.info("%d files found for %s with indices %d-%d (total: %d out of %d) within %.1f seconds",
        files_found,
        rw.recipe_step['parameters']['pattern'],
        pattern_start + status['seen-files'] - files_found,
        pattern_start + status['seen-files'] - 1,
        status['seen-files'], filecount,
        time.time()-status['start-time'])

    # Send results to myself for next round of processing
    rw.checkpoint(
        { 'filewatcher-status': status },
        delay=message_delay,
        transaction=txn)
    rw.transport.transaction_commit(txn)
