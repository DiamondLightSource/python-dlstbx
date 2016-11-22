from __future__ import absolute_import, division
import json
import os.path
import time
from workflows.recipe import Recipe
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
    import logging
    self.log.setLevel(logging.DEBUG)
    self.log.info("Filewatcher starting")
    self._transport.subscribe('filewatcher',
                              self.watch_files,
                              acknowledgement=True)

  def notify(self, recipe, destinations, header, filename, txn):
    '''Send file-found notifications to selected output channels.'''
    if destinations is None:
      return
    if not isinstance(destinations, list):
      destinations = [ destinations ]
    for destination in destinations:
      header['recipe-pointer'] = destination
      if recipe[destination].get('queue'):
        self._transport.send(
            recipe[destination]['queue'],
            { 'file': filename }, headers=header,
            transaction=txn)
      if recipe[destination].get('topic'):
        self._transport.broadcast(
            recipe[destination]['topic'],
            { 'file': filename }, headers=header,
            transaction=txn)

  def watch_files(self, header, message):
    '''Check for presence of files.'''

    # Conditionally acknowledge receipt of the message
    txn = self._transport.transaction_begin()
    self._transport.ack(header['message-id'], transaction=txn)

    # Extract the recipe
    current_recipe = Recipe(header['recipe'])
    current_recipepointer = int(header['recipe-pointer'])
    subrecipe = current_recipe[current_recipepointer]

    # Check if message body contains partial results from a previous run
    status = { 'seen-files': 0, 'start-time': time.time() }
    if isinstance(message, dict):
      status.update(message.get('filewatcher-status', {}))

    # List files to wait for
    files = [ subrecipe['parameters']['pattern'] % x
              for x in range(subrecipe['parameters']['pattern-start'],
                             subrecipe['parameters']['pattern-end'] + 1) ]
    filecount = len(files)

    self.log.info("Waiting %.1f seconds for %s\n%d of %d files seen so far",
        time.time()-status['start-time'],
        subrecipe['parameters']['pattern'],
        status['seen-files'], filecount)

    # Identify selections to notify for
    selections = [ k for k in subrecipe['output'].iterkeys()
                   if isinstance(k, basestring) and k.startswith('select-') ]
    selections = { int(k[7:]): k for k in selections }

    # Check if a minimum-wait time is set, and wait accordingly
    # (but no more than 3 seconds)
    if status.get('min-wait', 0) > time.time():
      timeout = max(0, min(3, status['min-wait'] - time.time()))
      self.log.debug("Waiting %.1f seconds", timeout)
      time.sleep(timeout)

    # Look for files
    files_found = 0
    while status['seen-files'] < filecount and \
          files_found < subrecipe['parameters'].get('burst-limit', 40) and \
          os.path.isfile(files[status['seen-files']]):
      filename = files[status['seen-files']]
      self.log.debug("Found %s", filename)

      files_found += 1
      status['seen-files'] += 1

      message_headers = { 'recipe': header['recipe'] }

      # Notify for first file
      if status['seen-files'] == 1:
        self.notify(current_recipe, subrecipe['output'].get('first'),
                    message_headers, filename, txn)

      # Notify for every file
      self.notify(current_recipe, subrecipe['output'].get('every'),
                  message_headers, filename, txn)

      # Notify for last file
      if status['seen-files'] == filecount:
        self.notify(current_recipe, subrecipe['output'].get('last'),
                    message_headers, filename, txn)

      # Notify for nth file
      self.notify(current_recipe,
                  subrecipe['output'].get(status['seen-files']),
                  message_headers, filename, txn)
      self.notify(current_recipe,
                  subrecipe['output'].get(str(status['seen-files'])),
                  message_headers, filename, txn)

      # Notify for selections
      for m, dest in selections.iteritems():
        if status['seen-files'] in (
            filecount,
            1 + round(status['seen-files'] * (m-1) // filecount) \
                * filecount // (m-1)):
          self.notify(current_recipe,
              subrecipe['output'].get(dest),
              message_headers, filename, txn)

    # Are we done?
    if status['seen-files'] == filecount:
      # Happy days
      self.log.info("%d files found. All done.", files_found)
      self._transport.transaction_commit(txn)
      return

    # If no files are found, set a minimum waiting time.
    # Otherwise note last time progress was made
    if files_found == 0:
      status['min-wait'] = time.time() + 1
      self.log.info("No files found this time")
    else:
      status['last-seen'] = time.time()
      self.log.info("%d files found this time", files_found)

    # Send results to myself for next round of processing
    self._transport.send('filewatcher',
        { 'filewatcher-status': status },
        headers={ 'recipe': header['recipe'],
                  'recipe-pointer': header['recipe-pointer'] },
        transaction=txn)
    self._transport.transaction_commit(txn)
