import re

_rejects = [ '\._sweeps(\.|$)',
             '\._sweep_information(\.|$)',
             '\._scalr_integraters(\.|$)',
           ]

_compiled_rejects = map(re.compile, _rejects)

# only export the xia2dbfilter() function (for 'import *' call)
__all__ = [ 'xia2dbfilter' ]

def xia2dbfilter(json_path):
  return not any((re.search(regexp, json_path) for regexp in _compiled_rejects))
