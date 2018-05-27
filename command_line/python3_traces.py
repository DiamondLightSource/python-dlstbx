from __future__ import absolute_import, division, print_function

import glob
import re
import sys
import xml.dom.minidom

re_trace = re.compile('^.*\.py:[0-9]+:.*$', re.MULTILINE)

known_traces = {}

def parse_file(filename):
  with open(filename, 'r') as fh:
    x = xml.dom.minidom.parseString(fh.read())
  testcases = x.getElementsByTagName('testcase')
  count = { 'fail': 0, 'skip': 0, 'pass': 0 }
  for test in testcases:
    trace_candidate = test.getElementsByTagName('failure') or test.getElementsByTagName('error')
    if trace_candidate:
      trace_text = trace_candidate[0].firstChild.wholeText
      traces = re_trace.findall(trace_text)
      trace = traces[-1] or 'no trace'

      # Some semi-intelligent path mangling
      if 'cctbx_project/' in trace:
        trace = trace[trace.index('cctbx_project'):]
      while trace.startswith('../'):
        trace = trace[3:]
      if trace.startswith('cctbx/'):
        trace = 'cctbx_project/' + trace

      if trace in known_traces:
        known_traces[trace]['count'] += 1
      else:
        known_traces[trace] = { 'count': 1, 'full': trace_text }
      count['fail'] += 1
      continue
    if test.getElementsByTagName('skipped'):
      count['skip'] += 1
    else:
      count['pass'] += 1
  return count

def run(files=sys.argv[1:]):
  if not files:
    files = glob.glob('/dls/science/groups/scisoft/DIALS/RHEL7VM/workspace/dials_bootstrap_python3/tests/*.xml')
  for f in files:
    print(f)
    print(parse_file(f))

if __name__ == '__main__':
  run()
  for t in sorted(known_traces, key=lambda k: known_traces[k]['count']):
    print("\n%3dx %s" % (known_traces[t]['count'], t))
    line = known_traces[t]['full'].split('\n')
    line = list(filter(lambda l: l.startswith('E'), line))
    if line:
      print("      " + line[-1])
