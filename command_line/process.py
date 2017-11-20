#
# dlstbx.process
#   Set up datacollection processing by writing an entry into ISPyB
#

from __future__ import absolute_import, division, print_function

import ispyb
import ispyb.factory
import sys
import os
from optparse import SUPPRESS_HELP, OptionParser
import re

from pprint import pprint

# Example: dlstbx.process -n "zocalo development" -r xia2-3dii 1724968:1-100

def fail(text):
  print(text)
  sys.exit(1)

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.process -r RCP -n NAME [options] DCID:START-END [..]",
                        description="Creates an entry in the ISPyB processing table")

  available_recipes = filter(lambda r: r.startswith('ispyb-') and r.endswith('.json'), os.listdir('/dls_sw/apps/zocalo/live/recipes'))
  available_recipes = sorted(map(lambda r: r[6:-5], available_recipes))

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-r", "--recipe", dest="recipe", metavar="RCP",
      action="store", type="choice", default=None, choices=available_recipes,
      help="Name of the recipe to run. Recipe name must correspond to a filename " \
           "(plus ispyb- prefix and .json extension) in /dls_sw/apps/zocalo/live/recipes: %s" % ", ".join(available_recipes))
  parser.add_option("-n", "--name", dest="name",
      action="store", default='',
      help="Set the name of this processing job")
  parser.add_option("-c", "--comment", dest="comment",
      action="store", default='',
      help="Annotate the job with this note")
  parser.add_option("-p", "--parameter", dest="parameters",
      action="append", default=[], metavar="KEY=VALUE",
      help="Add a key=value pair to the processing parameters")
  parser.add_option("-d", "--dcid", dest="dcid",
      action="store", default='',
      help="Set the DCID to attach this processing job to (default: DCID of first sweep)")
  parser.add_option("--automatic", action="store_true", dest="automatic", default=False,
      help="Mark this processing job as having been triggered automatically")
  (options, args) = parser.parse_args(sys.argv[1:])

  if len(sys.argv) == 1:
    parser.print_help()
    sys.exit(0)

  if not options.recipe:
    parser.print_help()
    fail("\nYou must specify a recipe. Choose from: " + ", ".join(available_recipes))

  if not options.name:
    parser.print_help()
    fail("\nYou must specify a name for the processing job")

  parameters = []
  for param in options.parameters:
    m = re.match('^([^=]+)=(.*)$', param)
    if not m:
      fail("'{param}' is not a valid parameter definition. Must follow format KEY=VALUE".format(param=param))
    parameters.append(m.group(1,2))

  imagesweeps = []
  for sweep in args:
    m = re.match('^(\d+):(\d+)-(\d+)$', sweep)
    if not m:
      fail("'{sweep}' is not a valid sweep definition. Must follow format DCID:START-END".format(sweep=sweep))
    imagesweeps.append({'dcid': m.group(1), 'start': m.group(2), 'end': m.group(3)})
  if not imagesweeps:
    fail("No image sweeps given.\nMust specify at least one image sweep to process in the format DCID:START-END.")

  if options.dcid:
    dcid = options.dcid
  else:
    dcid = imagesweeps[0]['dcid']

  i = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
  i_mx = ispyb.factory.create_data_area(ispyb.factory.DataAreaType.MXPROCESSING, i)

  jp = i_mx.get_job_params()
  # _job_params = StrictOrderedDict([('id', None), ('datacollectionid', None), ('display_name', None), ('comments', None), ('recipe', None), ('automatic', None)])
  jp['automatic'] = options.automatic
  jp['comments'] = options.comment
  jp['datacollectionid'] = dcid
  jp['display_name'] = options.name
  jp['recipe'] = options.recipe
# pprint(jp)

  jobid = i_mx.upsert_job(jp.values())
  print("Created JobID={}".format(jobid))
  for key, value in parameters:
    jpp = i_mx.get_job_parameter_params()
    # _job_parameter_params = StrictOrderedDict([('id', None), ('job_id', None), ('parameter_key', None), ('parameter_value', None)])
    jpp['job_id'] = jobid
    jpp['parameter_key'] = key
    jpp['parameter_value'] = value
#   pprint(jpp)
    jppid = i_mx.upsert_job_parameter(jpp.values())
    print("Created JPP={}".format(jppid))

  for sweep in imagesweeps:
    jisp = i_mx.get_job_image_sweep_params()
    # _job_image_sweep_params = StrictOrderedDict([('id', None), ('job_id', None), ('datacollectionid', None), ('start_image', None), ('end_image', None)])
    jisp['job_id'] = jobid
    jisp['datacollectionid'] = sweep['dcid']
    jisp['start_image'] = sweep['start']
    jisp['end_image'] = sweep['end']
#   pprint(jisp)
    jispid = i_mx.upsert_job_image_sweep(jisp.values())
    print("Created JISP={}".format(jispid))

  print("All done. Processing job {} created".format(jobid))
  print()
  print("To trigger the processing job you now need to run:")
  print("  dlstbx.go -r {}".format(jobid))
