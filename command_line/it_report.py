#!/usr/bin/env python
#
# LIBTBX_SET_DISPATCHER_NAME it.report

from __future__ import absolute_import, division
import datetime
from dials.util.procrunner import run_process
import dlstbx.profiling
from dlstbx.util.colorstreamhandler import ColorStreamHandler
import getpass
import json
import logging
import os
import re
from subprocess import call
import sys
import tempfile
import time

# Set this to false before changing the script
live = True

def select_issue():
  if hasattr(ColorStreamHandler, '_get_color'):
    def setbold():
      sys.stdout.write(ColorStreamHandler.BOLD)
    def setcolor(level):
      sys.stdout.write(getattr(ColorStreamHandler, '_get_color')(level))
    def resetcolor():
      sys.stdout.write(ColorStreamHandler.DEFAULT)
  else:
    setbold = lambda: None
    setcolor = lambda x: None
    resetcolor = lambda: None

  db = dlstbx.profiling.database()
  status = db.get_infrastructure_status()
  status = sorted(status, key=lambda s:-s['Level'])

  issues = {}
  counter = 0
  maxlen = len(str(len(status)))
  for group, colour in (('Error', logging.ERROR), \
                        ('Warning', logging.WARNING)):
    select = filter(lambda s: s['Group'] == group, status)
    if select:
      resetcolor()
      setcolor(colour)
      for s in select:
        resetcolor()
        age = (datetime.datetime.now() - s['Timestamp']).seconds
        if age < 30:
          age = "just now"
        elif age < 90:
          age = "%d sec ago" % age
        elif age < 90 * 60:
          age = "%d min ago" % round(age / 60)
        else:
          age = "%.1f hrs ago" % (age / 60 / 60)
        counter = counter + 1
        issues[counter] = s['Source']
        setbold()
        print "[ %%%ds ] " % maxlen % str(counter),
        setcolor(colour)
        print s['Source'],
        resetcolor()
        setcolor(colour)
        print " %s (%s)" % (s['Message'], age)
        indent = (len(s['Source']) + 2) * ' '
  resetcolor()

  if not issues:
    print "There are no issues to report. Happy days."
    sys.exit(0)

  print
  try:
    selected_issue = raw_input("Which issue would you like to report? ")
    selected_issue = int(selected_issue)
    issue = issues[selected_issue]
    issue = filter(lambda s: s['Source'] == issue, status)[0]
  except (SyntaxError, ValueError, KeyError):
    print "Until next time."
    sys.exit(1)
  return issue

def prepare_report(issue):
  issue['ext'] = json.loads(str(issue['ExtData'])) if issue.get('ExtData') else {}
#  from pprint import pprint
#  pprint(issue)
  if issue['Source'] in ('it.cluster', 'it.testcluster'):
    return report_cluster(issue)

  print "No prepared report available for this issue"


def report_cluster(issue):
  clustername = 'testcluster' if 'testcluster' in issue['Source'] else 'cluster'
  report = [ 'SUBJECT: ' + clustername + ' problems', '',  'Dear Tina,', '' ]
  report.append('Apologies if you are already aware of this, but it looks like there is something wrong with the ' + clustername + '.\n')
  firstitem = True

  if 'nodecrashed' in issue['ext']:
    crashed = len(issue['ext']['nodecrashed'])
    if crashed:
      if crashed == 1:
        crashreport = 'The cluster node %s appears to have crashed.' % list(issue['ext']['nodecrashed'])[0]
      elif crashed == 2:
        crashreport = 'Two cluster nodes, %s and %s, appear to have crashed.' % list(issue['ext']['nodecrashed'])
      elif crashed < 8:
        crashreport = 'The following %d cluster nodes appear to have crashed:\n' % crashed + "\n".join(sorted(list(issue['ext']['nodecrashed'])))
      else:
        crashreport = 'A lot of cluster nodes appear to have crashed, %d in total:\n' % crashed + "\n".join(sorted(list(issue['ext']['nodecrashed'])))
      report.append(crashreport)
      report.append('')
      firstitem = False

  if 'errqueue' in issue['ext']:
    erred = len(issue['ext']['errqueue'])
    if erred:
      if erred == 1:
        errreport = 'The queue %s is apparently in an error state.' % list(issue['ext']['errqueue'])[0]
      elif erred == 2:
        errreport = 'Two queues, %s and %s, are in an error state.' % list(issue['ext']['errqueue'])
      elif erred < 8:
        errreport = 'The following %d queues are in an error state:\n' % erred + "\n".join(sorted(list(issue['ext']['errqueue'])))
      else:
        errreport = 'There are a large number of queues showing an error state, %d in total:\n' % erred + "\n".join(sorted(list(issue['ext']['errqueue'])))
      if firstitem:
        firstitem = False
      else:
        errreport = 'Additionally, ' + errreport[:1].lower() + errreport[1:]
      report.append(errreport)
      report.append('')

  if 'longjobs' in issue['ext']:
    pass

  if issue['ext']['overall']['crashed'] == issue['ext']['overall']['total']:
    report.append('All nodes on the ' + clustername + ' have crashed. It looks like the ' + clustername + ' is down.')
    report.append('')
  elif issue['ext']['overall']['disabled'] == issue['ext']['overall']['total']:
    report.append('All nodes on the ' + clustername + ' are shown as disabled. It looks like the ' + clustername + ' is down.')
    report.append('')
  elif (issue['ext']['overall']['crashed'] + issue['ext']['overall']['disabled']) == issue['ext']['overall']['total']:
    report.append('All nodes on the ' + clustername + ' are shown as either crashed or disabled. It looks like the ' + clustername + ' is down.')
    report.append('')
  elif issue['ext']['overall']['crashed'] > ( issue['ext']['overall']['total'] * 0.5) :
    report.append('We are a bit concerned because the ' + clustername + ' is currently reduced to below half-capacity due to those problems.')
    report.append('')
  elif (issue['ext']['overall']['crashed'] + issue['ext']['overall']['disabled']) > (issue['ext']['overall']['total'] * 0.5) :
    report.append('We are a bit concerned because, taking the disabled nodes into account, the ' + clustername + ' is currently reduced to below half-capacity.')
    report.append('')

  report.append('Thanks.')
  report.append('')
  report.append(user)

  return "\n".join(report)

def pass_through_editor(initial_message):
  EDITOR = os.environ.get('EDITOR','vim') #that easy!
  instructions = '''
#
# All lines starting with '#' are removed.
#
# No mail will be sent if the mail contains only empty lines, or the
# string "ABORT". (So you can uncomment this line to abort)
#
# If a line starting with "SUBJECT:" is found, then this will become the mail subject
#
# Happy issue reporting.
#

'''
  with tempfile.NamedTemporaryFile(suffix=".tmp") as tf:
    tf.write(instructions)
    tf.write(initial_message)
    tf.flush()
    call([EDITOR, tf.name])

    # do the parsing with `tf` using regular File operations.
    # for instance:
    tf.seek(0)
    edited_message = tf.read()
  lines = edited_message.split("\n")
  lines = filter(lambda l: not l.lstrip().startswith('#'), lines)

  subject = filter(lambda l: 'SUBJECT:' in l, lines)
  if subject:
    lines = filter(lambda l: 'SUBJECT:' not in l, lines)
    subject = re.search('SUBJECT: *(.*)', subject[0]).group(1)
  else:
    subject = False

  while lines and lines[0].strip() == '':
    lines.pop(0)
  while lines and lines[-1].strip() == '':
    lines.pop()
  if not lines:
    return False, False
  lines = "\n".join(lines)
  if 'ABORT' in lines:
    return False, False
  return lines, subject

def send_mail(message, subject=None):
  if not live:
    print "Mail sending functionality disabled"
    sys.exit(1)

  command = ['/bin/mail']
  if mailaddr:
    command.extend(['-r', mailaddr])
  if subject:
    command.extend(['-s', subject])
  command.extend(['-c', 'scientificsoftware@diamond.ac.uk'])
  command.extend(['-c', 'scicomp@diamond.ac.uk'])

  command.append('itsupport@diamond.ac.uk')
  run_process(command, stdin=message)

username = getpass.getuser()
try:
  user, mailaddr = {
    'gw56':     ('Graeme', 'Graeme Winter <graeme.winter@diamond.ac.uk>'),
    'hko55533': ('Richard', 'Richard Gildea <richard.gildea@diamond.ac.uk>'),
    'wra62962': ('Markus', 'Markus Gerstel <markus.gerstel@diamond.ac.uk>'),
  }[username]
except KeyError:
  user = ''
  mailaddr = 'scisoft@diamond.ac.uk'

issue = select_issue()
report = prepare_report(issue)
if not report:
  sys.exit(0)

report, subject = pass_through_editor(report)
if report:
  print "==========================================="
  print subject
  print "------------------"
  print report
  print "==========================================="
  print "Sending in 7 seconds, last chance to abort."
  try:
    time.sleep(7)
  except KeyboardInterrupt:
    print "Aborted. No mail sent."
    sys.exit(0)
  send_mail(report, subject)
  print "Done"
else:
  print "Aborted. No mail sent."
