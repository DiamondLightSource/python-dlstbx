#
# dlstbx.last_data_collections_on [beamline]
#   Show information about the most recent data collections
#

from __future__ import absolute_import, division, print_function

import os
import re
import sys
from optparse import SUPPRESS_HELP, OptionParser

import ispyb
import ispyb.model.__future__
import procrunner

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.last_data_collections_on [beamline]",
                        description="Command line tool to view most recent data collections.")

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
  parser.add_option("-l", "--link",
      action="store_true", dest="link", default=False,
      help="show SynchWeb links for each data collection")
  parser.add_option("-n", "--collections",
      action="store", dest="limit", default=20, type="int", metavar="N",
      help="show the last N collections for each beamline")
  (options, args) = parser.parse_args(sys.argv[1:])

  if not args:
    parser.print_help()
    sys.exit(0)

  with ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg') as i:
    ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
    for n, beamline in enumerate(args):
      if n: print()
      with ispyb.model.__future__._db_cc() as cursor:
        cursor.run(
            'SELECT DataCollection.dataCollectionId,'
                  ' DataCollection.startTime,'
                  ' DataCollection.numberOfImages,'
                  ' CONCAT(GridInfo.steps_x, "x", GridInfo.steps_y) AS gridSize,'
                  ' CONCAT(TRIM(TRAILING "/" FROM DataCollection.imageDirectory), "/", DataCollection.fileTemplate) AS fileTemplate,'
                  ' CONCAT(Proposal.proposalCode, Proposal.proposalNumber, "-", BLSession.visit_number) as visit'
            ' FROM DataCollection'
            ' JOIN BLSession ON DataCollection.SESSIONID = BLSession.sessionID'
            ' JOIN Proposal ON BLSession.proposalId = Proposal.proposalId'
            ' LEFT JOIN GridInfo ON DataCollection.dataCollectionGroupId = GridInfo.dataCollectionGroupId'
            ' WHERE BLSession.beamLineName = %s AND Proposal.proposalCode != "nt"'
            ' ORDER BY DataCollection.startTime DESC'
            ' LIMIT %s;', beamline, options.limit)
        print(' Beamline {beamline:6} --DCID-- ---visit---'.format(beamline=beamline))
        for row in cursor.fetchall():
          if row['gridSize']:
            print('{startTime:%Y-%m-%d %H:%M} {dataCollectionId:8} {visit:<11} {numberOfImages:4} images, {gridSize:>5} grid   {fileTemplate}'.format(**row))
          else:
            print('{startTime:%Y-%m-%d %H:%M} {dataCollectionId:8} {visit:<11} {numberOfImages:4} images   {fileTemplate}'.format(**row))
          if options.link:
            print(' ' * 52 + 'https://ispyb.diamond.ac.uk/dc/visit/{visit}/id/{dataCollectionId}\n'.format(**row))
