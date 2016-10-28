from __future__ import absolute_import, division

def load_all_tests():
  '''Import all python files (except test_*) in directories. This is required
     for registration of system tests.
     :param paths: A path or list of paths containing files to import.
  '''
  import imp, pkgutil
  for _, name, _ in pkgutil.iter_modules(__path__):
    if not name.startswith('test_'):
      fid, pathname, desc = imp.find_module(name, __path__)
      imp.load_module(name, fid, pathname, desc)
      if fid:
        fid.close()

def get_all_tests():
  import dlstbx.system_test.common
  return dlstbx.system_test.common.CommonSystemTest.test_register
