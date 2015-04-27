def _list_all_modules():
  import os
  modules_path = os.path.dirname(os.path.abspath(__file__))
  files = {}
  for f in os.listdir(modules_path):
    if f.endswith('.py') and not f.startswith('__'):
      filepath = os.path.join(modules_path, f)
      data = f[:-3]
      if os.path.isfile(filepath):
        files[data] = filepath
  return files

_testlist = _list_all_modules()

def list_all_modules():
  return _testlist

_loadedmodules = {}

def load_module(name):
  if name in _testlist:
    global _loadedmodules

    filename = _testlist[name]
    import imp
    _loadedmodules[name] = imp.load_source('tests.%s' % name, filename)
    return _loadedmodules[name]
  raise Exception('Test module %s not found. (Do not use paths or filenames)' % name)

def load_module_cached(name):
  if name not in _loadedmodules:
    return load_module(name)
  return _loadedmodules[name]
