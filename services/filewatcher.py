from __future__ import absolute_import, division
import os.path
import time
import workflows.recipe
from workflows.services.common_service import CommonService

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
        self.watch_files, acknowledgement=True)

  def watch_files(self, rw, header, message):
    '''Check for presence of files.'''

    # Conditionally acknowledge receipt of the message
    txn = rw.transport.transaction_begin()
    rw.transport.ack(header, transaction=txn)

    # Check if message body contains partial results from a previous run
    status = { 'seen-files': 0, 'start-time': time.time() }
    if isinstance(message, dict):
      status.update(message.get('filewatcher-status', {}))

    # List files to wait for
    files = [ rw.recipe_step['parameters']['pattern'] % x
              for x in range(int(rw.recipe_step['parameters']['pattern-start']),
                             int(rw.recipe_step['parameters']['pattern-end']) + 1) ]
    filecount = len(files)

    self.log.debug("Waiting %.1f seconds for %s\n%d of %d files seen so far",
        time.time()-status['start-time'],
        rw.recipe_step['parameters']['pattern'],
        status['seen-files'], filecount)

    # Identify selections to notify for
    selections = [ k for k in rw.recipe_step['output'].iterkeys()
                   if isinstance(k, basestring) and k.startswith('select-') ]
    selections = { int(k[7:]): k for k in selections }

    # Check if a minimum-wait time is set, and wait accordingly
    # (but no more than 3 seconds)
    # can be removed in the future
    if status.get('min-wait', 0) > time.time():
      timeout = max(0, min(3, status['min-wait'] - time.time()))
      self.log.debug("Waiting %.1f seconds", timeout)
      time.sleep(timeout)

    # Look for files
    files_found = 0
    while status['seen-files'] < filecount and \
          files_found < rw.recipe_step['parameters'].get('burst-limit', 100) and \
          os.path.isfile(files[status['seen-files']]):
      filename = files[status['seen-files']]
      self.log.debug("Found %s", filename)

      files_found += 1
      status['seen-files'] += 1

      # Notify for first file
      if status['seen-files'] == 1:
        rw.send_to('first', { 'file': filename }, transaction=txn)

      # Notify for every file
      rw.send_to('every', { 'file': filename }, transaction=txn)

      # Notify for last file
      if status['seen-files'] == filecount:
        rw.send_to('last', { 'file': filename }, transaction=txn)

      # Notify for nth file
      rw.send_to(status['seen-files'], { 'file': filename }, transaction=txn)
      rw.send_to(str(status['seen-files']), { 'file': filename }, transaction=txn)

      # Notify for selections
      for m, dest in selections.iteritems():
        if status['seen-files'] in (
            filecount,
            1 + round(status['seen-files'] * (m-1) // filecount) \
                * filecount // (m-1)):
          rw.send_to(dest, { 'file': filename }, transaction=txn)

    # Are we done?
    if status['seen-files'] == filecount:
      # Happy days
      self.log.info("All %d files found for %s after %.1f seconds.",
        filecount,
        rw.recipe_step['parameters']['pattern'],
        time.time()-status['start-time'])

      # Notify for 'finally' outcome
      rw.send_to('finally', {
                    'files-expected': filecount,
                    'files-seen': status['seen-files'],
                    'success': True,
                 }, transaction=txn)

      self._transport.transaction_commit(txn)
      return

    message_delay = None
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

        timeoutlog = self.log.warn
        # Normally report all timeouts as warnings. If the warning is caused by
        # a system test then downgrade it to information level.
        if rw.recipe_step['parameters']['pattern'].startswith('/dls/tmp/dlstbx') and ( \
            ('tst_semi_' in rw.recipe_step['parameters']['pattern'] and status['seen-files'] == 1) or \
            ('tst_fail_' in rw.recipe_step['parameters']['pattern'] and status['seen-files'] == 0)):
          timeoutlog = self.log.info

        timeoutlog("Filewatcher for %s timed out after %.1f seconds (%d files found, nothing seen for %.1f seconds)",
          rw.recipe_step['parameters']['pattern'],
          time.time()-status['start-time'],
          status['seen-files'],
          time.time()-status.get('last-seen', status['start-time']))

        # Notify for timeout
        rw.send_to('timeout', {
                        'file': files[status['seen-files']],
                        'success': False }, transaction=txn)
        # Notify for 'finally' outcome
        rw.send_to('finally', {
                      'files-expected': filecount,
                      'files-seen': status['seen-files'],
                      'success': False }, transaction=txn)
        # Stop processing message
        self._transport.transaction_commit(txn)
        return

      # If no timeouts are triggered, set a minimum waiting time.
      status['min-wait'] = time.time() + 1 # can be removed in the future
      message_delay = 1
      self.log.debug("No files found this time")
    else:
      # Otherwise note last time progress was made
      status['last-seen'] = time.time()
      self.log.info("%d files found for %s (total: %d out of %d) within %.1f seconds",
        files_found,
        rw.recipe_step['parameters']['pattern'],
        status['seen-files'], filecount,
        time.time()-status['start-time'])

    # Send results to myself for next round of processing
    rw.checkpoint(
        { 'filewatcher-status': status },
        delay=message_delay,
        transaction=txn)
    rw.transport.transaction_commit(txn)
