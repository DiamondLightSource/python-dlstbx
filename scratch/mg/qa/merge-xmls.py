import os
import xml.etree.cElementTree as et

p = '/dls/mx-scratch/mgerstel/qa/logs'
merged = et.Element('testsuites')
for f in os.listdir(p):
  if f.endswith(".xml"):
    log = et.parse(os.path.join(p, f))
    suites = log.getroot()
    for n in suites.getchildren():
      merged.append(n)

et.ElementTree(merged).write('output.xml')
