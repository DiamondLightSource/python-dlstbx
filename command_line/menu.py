#!/usr/bin/env python
#
# LIBTBX_SET_DISPATCHER_NAME dials.menu

from __future__ import absolute_import, division
import colorama
from datetime import datetime
import json
from HTMLParser import HTMLParser
import re
import urllib2

menu = urllib2.urlopen('http://www.tkmenus.com/elior/ral').read()
#with open('/dls/mx-scratch/mgerstel/menu', 'r') as fh:
#  menu = fh.read()

# Import JSON from HTML
RALjson = {}
class RALEncodedMenuParser(HTMLParser):
  def handle_starttag(self, tag, attrs):
    if tag == 'input':
      attrdict = dict(attrs)
      if attrdict.get('id') and attrdict.get('value'):
        RALjson[attrdict['id']] = attrdict['value']
RALEncodedMenuParser().feed(menu)

assert 'labelJson' in RALjson \
   and 'configJson' in RALjson \
   and 'resourceJson' in RALjson \
   and 'menuJson' in RALjson, 'RAL menu format has changed, sorry.'

RAL = {}
for key in ['label', 'config', 'resource', 'menu']:
  RAL[key] = json.loads(RALjson[key + 'Json'])

colorama.init()
from pprint import pprint
#pprint(RAL)

for day in RAL['menu']['ms']:
#  print day['sn']
  parsed_date = re.search('([0-9]{1,2})[a-z]{0,2} ([A-Za-z]+) ([0-9]{4})', day['sn'])
  if parsed_date:
    parsed_date = "%s %s %s 15:00" % parsed_date.groups()
    parsed_dt = datetime.strptime(parsed_date, '%d %B %Y %H:%M')
    if parsed_dt > datetime.now():
#     day['ss'] = None
      print colorama.Fore.WHITE + colorama.Style.BRIGHT + \
            "\n== %s ==" % day['sn'] + colorama.Style.RESET_ALL
      if day['rl']:
        for fclass in day['ss']:
          for item in fclass['rs']:
            if item['d']:
              print item['d']
            item['ns'] = None
#            pprint (item)
      else:
        print colorama.Fore.BLACK + \
              "     (menu not yet available)" + colorama.Style.RESET_ALL

#print RALjson
#print RALjson.keys()
print colorama.Style.RESET_ALL

