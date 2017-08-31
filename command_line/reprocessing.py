#
# ispyb.reprocessing
#   Get/set information about reprocessing jobs
#

# LIBTBX_SET_DISPATCHER_NAME ispyb.reprocessing

from __future__ import division, absolute_import
from optparse import OptionParser, SUPPRESS_HELP
import pprint
import sys
import ispyb
import dlstbx

# Display stored information:
#   ispyb.reprocessing 73
#   ispyb.reprocessing 73 -v  # show full record
# Update stored information:
#   ispyb.reprocessing 73 --update "things are happening"
#   ispyb.reprocessing 73 --update "things are happening" --update-time "2017-08-25"
#   ispyb.reprocessing 73 --update "things are done" --status finished

if __name__ == '__main__':
  parser = OptionParser(usage="ispyb.reprocessing [options] rpid")

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-v", "--verbose",
      action="store_true", dest="verbose", default=False,
      help="show full reprocessing record")
  parser.add_option("-s", "--status", dest="status",
      action="store", type="choice", default=None, choices=['success', 'failure'],
      help="set a reprocessing result: success, failure.")
  parser.add_option("-u", "--update", dest="update",
      action="store", type="string", default=None,
      help="updates the reprocessing status information")
  parser.add_option("--update-time", dest="updatetime", metavar="TIMESTAMP",
      action="store", type="string", default=None,
      help="date the updated information (default: now)")
  (options, args) = parser.parse_args(sys.argv[1:])

  if not args:
    print "No reprocessing ID specified\n"
    parser.print_help()
    sys.exit(0)
  if len(args) > 1:
    print "Only one reprocessing ID can be specified"
    sys.exit(1)

  dlstbx.ensure_ispyb_version(required="0.11")
  driver = ispyb.get_driver(ispyb.Backend.DATABASE_MYSQL)
  i = driver(config_file='/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
  rpid = args[0]

  exit_code = 0

  if options.update or options.status:
    if not options.status:
      options.status = 'running'
    try:
      i.update_reprocessing_status(rpid, status=options.status,
                                   update_time=options.updatetime,
                                   start_time=options.updatetime,
                                   update_message=options.update)
    except ispyb.exception.UpdateFailed:
      print "Error: Could not update reprocessing ID.\n"
      exit_code = 1

  try:
    rp = i.get_reprocessing_id(rpid)
  except ispyb.exception.ISPyBNoResultException:
    print "Reprocessing ID %s not found" % rpid
    sys.exit(1)
  print '''Reprocessing ID {reprocessingId}:

       Name: {displayName}
   Comments: {comments}
     Status: {readableStatus}

       DCID: {dataCollectionId}
     Recipe: {recipe}

    Defined: {recordTimestamp}
    Started: {processingStartTime}
Last Update: {processingEndTime}
  with info: {processingMessage}'''.format(**rp)

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

