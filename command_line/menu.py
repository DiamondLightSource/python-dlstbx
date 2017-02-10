#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
#with open('/dls/mx-scratch/mgerstel/menu', 'w') as fh:
#  fh.write(menu)
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
#with open('/home/wra62962/ralmenu/everything.json', 'w') as fh:
#  fh.write(json.dumps(RAL, indent=2, sort_keys=True))

colorama.init()
from pprint import pprint
#pprint(RAL)

first_entry = True
weekday = ('?? (0)', '?? (1)', '?? (2)', 'Monday', 'Tuesday', 'Wednesday', 'Friday', '?? (7)')
prices = { 'R22 Favourites': '£3.10', 'Purple Pepper (Vegetarian)': '£3.30', 'Theatre': '£4.10', 'Hot Dessert': '£1.25'}
for day in RAL['menu']['ms']:
# print day['sn'], day['cw'], day['de'], day['wdi'], first_entry
  if day['cw'] and (day['de'] or not first_entry):
#     print day['sn']
#     pprint(day)
      print colorama.Fore.WHITE + colorama.Style.BRIGHT + \
            "\n== %s ==" % weekday[day['wdi']] + colorama.Style.RESET_ALL
      first_entry = False

      if day['rl']:
        daily_menu = {}
        for fclass in day['ss']:
          group = fclass.get('n')
          itemlist = []
          for item in fclass['rs']:
            itemname = item.get('n', '')
            itemdesc = ''
            if item['d']:
              itemdesc = colorama.Fore.BLACK + '(' + item['d'] + ')' + colorama.Style.RESET_ALL
            if itemname:
              itemlist.append((itemname, itemdesc))
          daily_menu[group] = itemlist
#         pprint(item)
#       pprint(daily_menu)

        longest_group = max(len(x) for x in daily_menu)
        first_group = True
        for group, items in daily_menu.iteritems():
          space = " " * (longest_group + 1)
          price = "%%%ds  " % longest_group % prices.get(group, '')
          group = "%%%ds:" % longest_group % group

          if first_group:
            first_group = False
          else:
            print
          for item in items:
            if item[0] and item[1]:
              print group, item[0]
              group, price = price, space
              print group, item[1]
              group, price = price, space
          minor_items = []
          for item in items:
            if not (item[0] and item[1]):
              minor_items.append(item[0] or item[1])
          if minor_items:
            minor_items = [ item + ',' for item in minor_items[:-1] ] + [minor_items[-1]]
            cursorpos = 1
            for item in minor_items:
              if cursorpos == 1:
                print group,
                group, price = price, space
                cursorpos += longest_group + 1
              print item,
              cursorpos += len(item) + 2
              if cursorpos > 120:
                print
                cursorpos = 1
            if cursorpos > 1:
              print
      else:
        print colorama.Fore.BLACK + \
              "     (menu not yet available)" + colorama.Style.RESET_ALL

#print RALjson
#print RALjson.keys()
print colorama.Style.RESET_ALL

