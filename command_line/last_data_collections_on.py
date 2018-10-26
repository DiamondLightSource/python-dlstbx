#
# dlstbx.last_data_collections_on [beamline]
#   Show information about the most recent data collections
#

from __future__ import absolute_import, division, print_function

import re
import os
import sys
from optparse import SUPPRESS_HELP, OptionGroup, OptionParser

import ispyb
import ispyb.model.__future__
import procrunner

if __name__ == '__main__':
  parser = OptionParser(usage="dlstbx.last_data_collections_on [beamline]",
                        description="Command line tool to view most recent data collections.")

  parser.add_option("-?", action="help", help=SUPPRESS_HELP)
# parser.add_option("-v", "--verbose",
#     action="store_true", dest="verbose", default=False,
#     help="show full job record")
  (options, args) = parser.parse_args(sys.argv[1:])

  if not args:
    parser.print_help()
    sys.exit(0)

  with ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg') as i:
    ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
    for beamline in args:
      with ispyb.model.__future__._db_cc() as cursor:
        cursor.run(
            'SELECT DataCollection.dataCollectionId,'
                  ' DataCollection.startTime,'
                  ' DataCollection.numberOfImages,'
                  ' CONCAT(TRIM(TRAILING "/" FROM DataCollection.imageDirectory), "/", DataCollection.fileTemplate) AS fileTemplate,'
                  ' CONCAT(Proposal.proposalCode, Proposal.proposalNumber, "-", BLSession.visit_number) as visit'
            ' FROM DataCollection'
            ' JOIN BLSession ON DataCollection.SESSIONID = BLSession.sessionID'
            ' JOIN Proposal ON BLSession.proposalId = Proposal.proposalId'
            ' WHERE BLSession.beamLineName = %s AND DataCollection.axisRange != 0'
            ' ORDER BY DataCollection.startTime DESC'
            ' LIMIT 20;', beamline)
        print('\n Beamline {beamline:6} --DCID-- ---visit---'.format(beamline=beamline))
        for row in cursor.fetchall():
          print('{startTime:%Y-%m-%d %H:%M} {dataCollectionId:8} {visit:<11} {numberOfImages:4} images   {fileTemplate}'''.format(**row))
