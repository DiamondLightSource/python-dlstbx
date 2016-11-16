from __future__ import absolute_import, division
import errno
import os
import stat

def dls_tmp_folder():
  tmp_folder = '/dls/tmp/dlstbx'
  try:
    os.makedirs(tmp_folder)
  except OSError as exception:
    if exception.errno != errno.EEXIST:
      raise
  os.chmod(tmp_folder, \
    stat.S_IRUSR + stat.S_IWUSR + stat.S_IXUSR + \
    stat.S_IRGRP + stat.S_IWGRP + stat.S_IXGRP + \
    stat.S_IROTH + stat.S_IWOTH + stat.S_IXOTH)
  return tmp_folder
