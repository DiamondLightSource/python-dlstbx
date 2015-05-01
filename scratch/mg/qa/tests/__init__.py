def _list_all_modules():
  import os
  modules_path = os.path.dirname(os.path.abspath(__file__))
  files = {}
  for f in os.listdir(modules_path):
    if f.endswith('.py') and not f.startswith('__'):
      filepath = os.path.join(modules_path, f)
      modulename = f[:-3]
      if os.path.isfile(filepath):
        files[modulename] = filepath
  return files

_modulelist = _list_all_modules()

def list_all_modules():
  return _modulelist

_loadedmodules = {}

def load_module(name):
  if name in _modulelist:
    global _loadedmodules

    filename = _modulelist[name]
    import imp
    _loadedmodules[name] = imp.load_source('tests.%s' % name, filename)
    return _loadedmodules[name]
  raise Exception('Test module %s not found. (Do not use paths or filenames)' % name)

def load_module_cached(name):
  if name in _loadedmodules:
    return _loadedmodules[name]

  return load_module(name)
