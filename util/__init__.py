from __future__ import absolute_import, division
import errno
import re
import os
import stat

def dls_tmp_folder():
  tmp_folder = '/dls/tmp/dlstbx'
  try:
    os.makedirs(tmp_folder)
  except OSError as exception:
    if exception.errno != errno.EEXIST:
      raise
  try:
    os.chmod(tmp_folder, \
      stat.S_IRUSR + stat.S_IWUSR + stat.S_IXUSR + \
      stat.S_IRGRP + stat.S_IWGRP + stat.S_IXGRP + \
      stat.S_IROTH + stat.S_IWOTH + stat.S_IXOTH)
  except OSError as exception:
    if exception.errno != errno.EPERM:
      raise
  return tmp_folder

_proc_getnumber = re.compile(':\s+([0-9]+)\s')
def get_process_uss(pid = None):
  '''Get the unique set size of a process in bytes.
     The unique set size is the amount of memory that would be freed if that
     process was terminated.
     Note that this will only work on linux.
  '''
  if not pid:
    pid = os.getpid() # Don't cache this. Multiprocessing would copy value.
  with open('/proc/%s/smaps' % str(pid), 'r') as fh:
    return 1024 * sum( int(_proc_getnumber.search(x).group(1))
                       for x in fh
                       if x.startswith('Private') )
try:
  get_process_uss()
except IOError as exception:
  if exception.errno == 2:
    # /proc not available on this platform
    get_process_uss = lambda pid=None: None
  else:
    raise
