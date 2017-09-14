#
# ispyb.reprocessing
#   Get/set information about reprocessing jobs
#

# LIBTBX_SET_DISPATCHER_NAME ispyb.reprocessing

from __future__ import division, absolute_import
from optparse import OptionGroup, OptionParser, SUPPRESS_HELP
import pprint
import sys
import ispyb
import dlstbx

# Display stored information:
#   ispyb.reprocessing 73
#   ispyb.reprocessing 73 -v  # show full record

# Create new processing program row:
#   ispyb.reprocessing 73 -c -p "program" -s "starting up..."

# Update stored information:
#   ispyb.reprocessing 73 -u 1234 -s "running..."
#   ispyb.reprocessing 73 -u 1234 -s "things are happening" --update-time "2017-08-25"
#   ispyb.reprocessing 73 -u 1234 -s "completed successfully" -r success
#   ispyb.reprocessing 73 -u 1234 -s "everything is broken" -r failure

if __name__ == '__main__':
  parser = OptionParser(usage="ispyb.reprocessing [options] rpid")

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-v", "--verbose",
      action="store_true", dest="verbose", default=False,
      help="show full reprocessing record")
  parser.add_option("-c", "--create", dest="create",
      action="store_true", default=False,
      help="create a new processing program entry for the rpid")
  parser.add_option("-u", "--update", dest="update",
      action="store", type="int", default=None,
      help="update an existing processing program entry")

  group = OptionGroup(parser, "Processing entry options",
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
      help="set a reprocessing result: success, failure.")
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

  if not args:
    print "No reprocessing ID specified\n"
    parser.print_help()
    sys.exit(0)
  if len(args) > 1:
    print "Only one reprocessing ID can be specified"
    sys.exit(1)

  dlstbx.ensure_ispyb_version(required="0.12")
  driver = ispyb.get_driver(ispyb.Backend.DATABASE_MYSQL)
  i = driver(config_file='/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
  # because read access is only available with this login
  isp = driver(config_file='/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
  # because stored procedures are only available with that login
  rpid = args[0]

  exit_code = 0

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
    except ispyb.exception.UpdateFailed:
      print "Error: Could not create processing program.\n"
      exit_code = 1

  elif options.update:
    try:
      isp.update_processing_status(
        options.update, status=options.result,
        start_time=options.updatetime,
        update_time=options.updatetime,
        update_message=options.status,
      )
    except ispyb.exception.UpdateFailed:
      print "Error: Could not update processing status.\n"
      exit_code = 1

  try:
    rp = i.get_reprocessing_id(rpid)
  except ispyb.exception.ISPyBNoResultException:
    print "Reprocessing ID %s not found" % rpid
    sys.exit(1)
  print '''Reprocessing ID {reprocessingId}:

       Name: {displayName}
     Recipe: {recipe}
   Comments: {comments}
 Primary DC: {dataCollectionId}
    Defined: {recordTimestamp}'''.format(**rp)

  if options.verbose:
    params = i.get_reprocessing_parameters(rpid)
    if params:
      maxlen = max(max(map(len, params)), 11)
      print "\n Parameters:"
      print '\n'.join("%%%ds: %%s" % maxlen % (key, params[key]) for key in sorted(params))

    print "\n     Sweeps:",
    print ('\n' + ' ' * 13).join(map(
        lambda sweep:
          "DCID {dataCollectionId:7}  images{startImage:5} -{endImage:5}".format(**sweep),
        i.get_reprocessing_sweeps(rpid)))

  processing_programs = i.get_processing_instances_for_reprocessing_id(rpid)
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
      print print_format.format(**autoproc_instance)
