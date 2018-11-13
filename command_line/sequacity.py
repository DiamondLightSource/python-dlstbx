# Business logic layer

# Given a data collection ID - what should I do?
# https://jira.diamond.ac.uk/browse/SCI-7496

# dlstbx.sequacity 1956161
#   show available dictionary for DCID
#
# dlstbx.sequacity 1956161 -t processing-fast_dp
#   exit code 0 if this is a default recipe for this DCID, otherwise exit code 1

from __future__ import absolute_import, division, print_function

import sys
from optparse import SUPPRESS_HELP, OptionParser

import dlstbx.ispybtbx
import ispyb

def get_recipes_for_dcid(dcid):
  i_legacy = dlstbx.ispybtbx.ispybtbx()
  with ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg') as i:
    dc = i.get_data_collection(dcid)

    beamline = i_legacy.get_beamline_from_dcid(dcid)
    dc_class = i_legacy.classify_dc(i_legacy.get_dc_info(dcid))

    if dc_class['grid']:
      if beamline == 'i02-2':
        return ['archive-nexus', 'vmxi-spot-counts-per-image']
      else:
        return ['per-image-analysis-gridscan']

    if dc_class['screen']:
      return ['per-image-analysis-rotation', 'strategy-edna', 'strategy-mosflm']

    if not dc_class['rotation']:
      # possibly EM dataset
      return []

    recipes = [
        'per-image-analysis-rotation',
        'processing-autoproc',
        'processing-fast-dp',
        'processing-rlv',
        'processing-xia2-3dii',
        'processing-xia2-dials',
        'processing-xia2-dials-full',
    ]
    if beamline == 'i02-2':
      recipes = [
          'archive-nexus',
          'processing-fast-dp',
          'processing-xia2-dials',
          'processing-xia2-dials-full',
          'vmxi-per-image-analysis',
      ]

    related_dcs = i_legacy.get_dc_group(dcid)
    related = list(sorted(set(related_dcs)))
    related.remove(dcid)
    if related:
      recipes.append('processing-multi-xia2-dials')
      recipes.append('processing-multi-xia2-3dii')
  return recipes

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.sequacity [options] dcid")
  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-t", "--test", action="store", dest="test", default=None,
      help="check whether this recipe is in the list of recipes to run")

  (options, args) = parser.parse_args(sys.argv[1:])

  if not args or len(args) > 1:
    parser.error('You need to specify exactly one DCID')
  dcid = int(args[0])

  if options.test:
    if options.test in get_recipes_for_dcid(dcid):
      print("%s would be run for this data collection" % options.test)
      sys.exit(0)
    else:
      sys.exit("%s would not be run for this data collection" % options.test)

  print("Default recipes:", ", ".join(sorted(get_recipes_for_dcid(dcid))))
