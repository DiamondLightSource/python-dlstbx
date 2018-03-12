from __future__ import absolute_import, division, print_function

import os
import libtbx.load_env
from procrunner import run_process
import xml.etree.ElementTree

_h5dump = libtbx.env.under_base('bin/h5dump')

def get_external_references(filename):
  command_line = [ _h5dump, '-H', '-x', filename ]
  result = run_process(command_line, print_stdout=False, print_stderr=False)
  assert result['exitcode'] == 0, result

  xmlroot = xml.etree.ElementTree.fromstring(result['stdout'])
  links = filter(lambda tag: tag.tag == '{http://hdfgroup.org/HDF5/XML/schema/HDF5-File.xsd}ExternalLink', xmlroot.iter())
  files = filter(None, map(lambda tag: tag.attrib.get('TargetFilename'), links))
  return files

def find_all_references(startfile):
  startfile = os.path.abspath(startfile)
  known_files = set()
  unchecked_files = set([startfile])

  while unchecked_files:
    filename = unchecked_files.pop()
    filepath = os.path.dirname(filename)
    known_files.add(filename)
    for linked_file in get_external_references(filename):
      linked_file = os.path.abspath(os.path.join(filepath, linked_file))
      if linked_file in known_files:
        continue
      else:
        unchecked_files.add(linked_file)

  return sorted(known_files)
