import imp
import os

_modulelist = None

def _load_all_modules():
  global _modulelist
  modules_path = os.path.dirname(os.path.abspath(__file__))
  _modulelist = {}
  for f in os.listdir(modules_path):
    if f.endswith('.py') and not f.startswith('__'):
      filepath = os.path.join(modules_path, f)
      modulename = f[:-3]
      if os.path.isfile(filepath):
        _modulelist[modulename] = filepath

def list_all_modules():
  if _modulelist is None:
    _load_all_modules()
  return _modulelist

def load_module(name):
  if _modulelist is None:
    _load_all_modules()
  if name in _modulelist:
    filename = _modulelist[name]
    return imp.load_source('tests.%s' % name, filename)
  raise Exception('Test module %s not found. (Do not use paths or filenames)' % name)
