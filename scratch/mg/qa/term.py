import sys

# only export the color() function (for 'import *' call)
__all__ = [ 'color' ]

_colors = {
  '': '\033[0m',
  'black': '\033[30m',
  'red': '\033[31m',
  'green': '\033[32m',
  'yellow': '\033[33m',
  'blue': '\033[34m',
  'magenta': '\033[35m',
  'cyan': '\033[36m',
  'white': '\033[37m',
  'grey': '\033[1;30m',
  'bright': '\033[1m',
  }

_colorsenabled = True
for handle in [sys.stdout]: # don't actually write to sys.stderr
  if not (hasattr(handle, "isatty") and handle.isatty()):
    _colorsenabled = False

def color(*args):
  if _colorsenabled:
    if not args:
      args = ['']
    for n in args:
      if n in _colors:
        sys.stdout.write(_colors[n]),
      else:
        print '%sunknown color %s%s' % (_colors['red'], n, _colors[''])
