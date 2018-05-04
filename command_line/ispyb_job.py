#
# ispyb.job
#   Get/set information about processing jobs
#

# LIBTBX_SET_DISPATCHER_NAME ispyb.reprocessing
# LIBTBX_SET_DISPATCHER_NAME ispyb.job

from __future__ import absolute_import, division, print_function

import re
import os
import sys
from optparse import SUPPRESS_HELP, OptionGroup, OptionParser

import ispyb
import ispyb.factory
import ispyb.exception
import procrunner

# Create a new processing job:
#   ispyb.job --new --display "Dataprocessor 2000" --comment "The best program in the universe" \
#             --recipe dp2000 --add-param "space_group:P 21 21 21" --add-sweep 1234:1:600

# Display stored information:
#   ispyb.job 73
#   ispyb.job 73 -v  # show full record

# Create new processing program row:
#   ispyb.job 73 -c -p "program" -s "starting up..."

# Update stored information:
#   ispyb.job 73 -u 1234 -s "running..."
#   ispyb.job 73 -u 1234 -s "things are happening" --update-time "2017-08-25"
#   ispyb.job 73 -u 1234 -s "completed successfully" -r success
#   ispyb.job 73 -u 1234 -s "everything is broken" -r failure

def create_processing_job(i, options, i_legacy):
  sweeps = []
  for s in options.sweeps:
    match = re.match(r"^([0-9]+):([0-9]+):([0-9]+)$", s)
    if not match:
      sys.exit("Invalid sweep specification: " + s)
    values = tuple(map(int, match.groups()))
    if not all(map(lambda value: value > 0, values)) or values[2] < values[1]:
      sys.exit("Invalid sweep specification: " + s)
    sweeps.append(values)

  if options.dcid:
    match = re.match(r"^([0-9]+)$", options.dcid)
    if not match:
      sys.exit("Invalid data collection id: " + s)
    dcid = int(options.dcid)
  else:
    dcid = None

  if not sweeps:
    if not dcid:
      sys.exit("When creating a processing job you must specify at least one data collection sweep or a DCID")

    dc_info = i_legacy.get_datacollection_id(dcid)
    start = dc_info.get('startImageNumber')
    number = dc_info.get('numberOfImages')
    if not start or not number:
      sys.exit("Can not automatically infer data collection sweep for this DCID")
    end = start + number - 1
    sweeps = [ (dcid, start, end) ]
    print("Using images %d to %d for data collection sweep" % (start, end))

  parameters = []
  for p in options.parameters:
    if ':' not in p:
      sys.exit("Invalid parameter specification: " + p)
    parameters.append(p.split(':', 1))

  i_mx = ispyb.factory.create_data_area(ispyb.factory.DataAreaType.MXPROCESSING, i)

  jp = i_mx.get_job_params()
  # _job_params = StrictOrderedDict([('id', None), ('datacollectionid', None), ('display_name', None), ('comments', None), ('recipe', None), ('automatic', None)])
  jp['automatic'] = options.source == 'automatic'
  jp['comments'] = options.comment
  jp['datacollectionid'] = dcid or sweeps[0][0]
  jp['display_name'] = options.display
  jp['recipe'] = options.recipe
  print("Creating database entries...")

  jobid = i_mx.upsert_job(jp.values())
  print("  JobID={}".format(jobid))
  for key, value in parameters:
    jpp = i_mx.get_job_parameter_params()
    # _job_parameter_params = StrictOrderedDict([('id', None), ('job_id', None), ('parameter_key', None), ('parameter_value', None)])
    jpp['job_id'] = jobid
    jpp['parameter_key'] = key
    jpp['parameter_value'] = value
    jppid = i_mx.upsert_job_parameter(jpp.values())
    print("  JPP={}".format(jppid))

  for sweep in sweeps:
    jisp = i_mx.get_job_image_sweep_params()
    # _job_image_sweep_params = StrictOrderedDict([('id', None), ('job_id', None), ('datacollectionid', None), ('start_image', None), ('end_image', None)])
    jisp['job_id'] = jobid
    jisp['datacollectionid'] = sweep[0]
    jisp['start_image'] = sweep[1]
    jisp['end_image'] = sweep[2]
    jispid = i_mx.upsert_job_image_sweep(jisp.values())
    print("  JISP={}".format(jispid))

  print("All done. Processing job {} created".format(jobid))
  print()
  if options.trigger:
    result = procrunner.run(['dlstbx.go', '-p', str(jobid)])
    if result['exitcode'] or result['stderr']:
      sys.exit("Error triggering processing job")
    print("Successfully triggered processing job")
    print()

  else:
    print("To trigger the processing job you now need to run:")
    print("  dlstbx.go -p {}".format(jobid))
    print()

  return jobid

if __name__ == '__main__':
  parser = OptionParser(usage="ispyb.job [options] rpid",
                        description="Command line tool to manipulate ISPyB processing table entries.")

  available_recipes = filter(lambda r: r.startswith('ispyb-') and r.endswith('.json'), os.listdir('/dls_sw/apps/zocalo/live/recipes'))
  available_recipes = sorted(map(lambda r: r[6:-5], available_recipes))

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-v", "--verbose",
      action="store_true", dest="verbose", default=False,
      help="show full job record")

  group = OptionGroup(parser, "Processing job options",
      "These options can be used to create or modify "
      "a processing/reprocessing job.")
  group.add_option("--new", dest="new",
      action="store_true", default=False,
      help="create a new processing job. If --new is specified you must not specify another rpid")
  group.add_option("--dcid", dest="dcid",
      action="store", type="string", default=None,
      help="set the primary data collection ID for the processing job (default: DCID of first sweep)")
  group.add_option("--display", dest="display",
      action="store", type="string", default=None,
      help="set the display name of the processing job")
  group.add_option("--comment", dest="comment",
      action="store", type="string", default=None,
      help="set a comment string for the processing job")
  group.add_option("--recipe", dest="recipe",
      action="store", type="choice", default=None, choices=available_recipes,
      help="set a recipe for the processing job. Recipe name must correspond to a filename " \
           "(plus ispyb- prefix and .json extension) in /dls_sw/apps/zocalo/live/recipes: %s" % ", ".join(available_recipes))
  group.add_option("--source", dest="source",
      action="store", type="choice", default='user', choices=['user', 'automatic'],
      help="set whether the processing job was triggered by a 'user' (default) or by 'automatic' processing")
  group.add_option("--add-param", dest="parameters",
      action="append", type="string", default=[], metavar="KEY:VALUE",
      help="add a 'KEY:VALUE' pair string parameter to a processing job")
  group.add_option("--add-sweep", dest="sweeps",
      action="append", type="string", default=[], metavar="DCID:START:END",
      help="add an image range from a sweep of any data collection ID to the processing job. " \
           "Each job must have at least one sweep. " \
           "If no sweep is defined all images from the primary data collection ID are used")
  group.add_option("--trigger", dest="trigger",
      action="store_true", default=False,
      help="start the processing job immediately after creation")
  parser.add_option_group(group)

  group = OptionGroup(parser, "Processing program options",
      "These options can be used to create or update "
      "processing program entries belonging to a processing job.")
  group.add_option("-c", "--create", dest="create",
      action="store_true", default=False,
      help="create a new processing program entry for the rpid")
  group.add_option("-u", "--update", dest="update",
      action="store", type="int", default=None,
      help="update an existing processing program entry")
  parser.add_option_group(group)

  group = OptionGroup(parser, "Processing program attributes",
      "These options can be used when creating or updating "
      "processing program entries.")
  group.add_option("-p", "--program", dest="program",
      action="store", type="string", default=None,
      help="set a program name for processing entry")
  group.add_option("-l", "--cmdline", dest="cmdline",
      action="store", type="string", default=None,
      help="set full command line for processing entry")
  group.add_option("-e", "--environment", dest="environment",
      action="store", type="string", default=None,
      help="set an environment string for processing entry")
  group.add_option("-r", "--result", dest="result",
      action="store", type="choice", default=None, choices=['success', 'failure'],
      help="set a job result: success, failure")
  group.add_option("-s", "--status", dest="status",
      action="store", type="string", default=None,
      help="set program status information")
  group.add_option("--start-time", dest="starttime", metavar="TIMESTAMP",
      action="store", type="string", default=None,
      help="set the program start time (default: now)")
  group.add_option("--update-time", dest="updatetime", metavar="TIMESTAMP",
      action="store", type="string", default=None,
      help="date the updated information (default: now)")
  parser.add_option_group(group)
  (options, args) = parser.parse_args(sys.argv[1:])

  if not args and not options.new:
    if sys.argv[1:]:
      print("No job ID specified\n")
    parser.print_help()
    sys.exit(0)
  if len(args) > 1:
    sys.exit("Only one job ID can be specified")
  if options.new and args:
    sys.exit("Can not create a new job ID when a job ID is specified")
  if options.new and options.update:
    sys.exit("Can not update a program when creating a new job ID")

  driver = ispyb.legacy_get_driver(ispyb.legacy_Backend.DATABASE_MYSQL)
  i_legacy = driver(config_file='/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
  # because read access is only available with this login
  isp = driver(config_file='/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
  # because stored procedures are only available with that login
  i = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
  # because this is new

  exit_code = 0

  if options.new:
    rpid = create_processing_job(i, options, i_legacy)
  else:
    rpid = args[0]

  if options.create:
    try:
      isp.add_processing_program(
        reprocessing_id=rpid, programs=options.program,
        command_line=options.cmdline,
        environment=options.environment,
        start_time=options.starttime,
        update_time=options.updatetime,
        update_message=options.status,
        status=options.result,
      )
    except ispyb.exception.ISPyBWriteFailed:
      print("Error: Could not create processing program.\n")
      exit_code = 1

  elif options.update:
    try:
      isp.update_processing_status(
        options.update, status=options.result,
        start_time=options.updatetime,
        update_time=options.updatetime,
        update_message=options.status,
      )
    except ispyb.exception.ISPyBWriteFailed:
      print("Error: Could not update processing status.\n")
      exit_code = 1

  try:
    rp = i_legacy.get_reprocessing_id(rpid)
  except ispyb.exception.ISPyBNoResultException:
    print("Reprocessing ID %s not found" % rpid)
    sys.exit(1)
  print('''Reprocessing ID {processingJobId}:

       Name: {displayName}
     Recipe: {recipe}
   Comments: {comments}
 Primary DC: {dataCollectionId}
    Defined: {recordTimestamp}'''.format(**rp))

  if options.verbose:
    params = i_legacy.get_reprocessing_parameters(rpid)
    if params:
      maxlen = max(max(map(len, params)), 11)
      print("\n Parameters:")
      print('\n'.join("%%%ds: %%s" % maxlen % (key, params[key]) for key in sorted(params)))

    print("\n     Sweeps: ", end='')
    print(('\n' + ' ' * 13).join(map(
        lambda sweep:
          "DCID {dataCollectionId:7}  images{startImage:5} -{endImage:5}".format(**sweep),
        i_legacy.get_reprocessing_sweeps(rpid))))

  processing_programs = i_legacy.get_processing_instances_for_reprocessing_id(rpid)
  if processing_programs:
    print_format = "             {processingPrograms} (#{autoProcProgramId}, {readableStatus})"
    print_format = "\nProgram #{autoProcProgramId}: {processingPrograms}, {readableStatus}"

    if options.verbose:
      print_format += "\n    Command: {processingCommandLine}"
      print_format += "\nEnvironment: {processingEnvironment}"
      print_format += "\n    Defined: {recordTimeStamp}"
      print_format += "\n    Started: {processingStartTime}"
      print_format += "\nLast Update: {processingEndTime}"

    print_format += "\n  Last Info: {processingMessage}"

    for autoproc_instance in processing_programs:
      print(print_format.format(**autoproc_instance))
