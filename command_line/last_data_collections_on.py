#
# dlstbx.last_data_collections_on [beamline]
#   Show information about the most recent data collections
#

from __future__ import absolute_import, division, print_function

import sys
import time
from optparse import SUPPRESS_HELP, OptionParser

import ispyb
import ispyb.model.__future__

if __name__ == "__main__":
    parser = OptionParser(
        usage="dlstbx.last_data_collections_on [beamline]",
        description="Command line tool to view most recent data collections.",
    )

    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "-f",
        "--follow",
        dest="follow",
        default=False,
        action="store_true",
        help="Keep showing new data collections as they appear.",
    )
    parser.add_option(
        "-l",
        "--link",
        action="store_true",
        dest="link",
        default=False,
        help="show SynchWeb links for each data collection",
    )
    parser.add_option(
        "-n",
        "--collections",
        action="store",
        dest="limit",
        default=20,
        type="int",
        metavar="N",
        help="show the last N collections for each beamline",
    )
    (options, args) = parser.parse_args(sys.argv[1:])

    if not args:
        parser.print_help()
        sys.exit(0)
    with ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg") as i:
        ispyb.model.__future__.enable(
            "/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg"
        )
        last_results = {}
        while True:
            for n, beamline in enumerate(args):
                with ispyb.model.__future__._db_cc() as cursor:
                    cursor.run(
                        "SELECT DataCollection.dataCollectionId,"
                        " DataCollection.startTime,"
                        " DataCollection.numberOfImages,"
                        ' CONCAT(GridInfo.steps_x, "x", GridInfo.steps_y) AS gridSize,'
                        ' CONCAT(TRIM(TRAILING "/" FROM DataCollection.imageDirectory), "/", DataCollection.fileTemplate) AS fileTemplate,'
                        ' CONCAT(Proposal.proposalCode, Proposal.proposalNumber, "-", BLSession.visit_number) as visit'
                        " FROM DataCollection"
                        " JOIN BLSession ON DataCollection.SESSIONID = BLSession.sessionID"
                        " JOIN Proposal ON BLSession.proposalId = Proposal.proposalId"
                        " LEFT JOIN GridInfo ON DataCollection.dataCollectionGroupId = GridInfo.dataCollectionGroupId"
                        ' WHERE BLSession.beamLineName = %s AND Proposal.proposalCode != "nt"'
                        " ORDER BY DataCollection.startTime DESC"
                        " LIMIT %s;",
                        beamline,
                        options.limit,
                    )
                    rows = reversed(cursor.fetchall())
                    last_rows = last_results.setdefault(beamline, [])
                    new_rows = [r for r in rows if r not in last_rows]
                    last_rows.extend(new_rows)
                    if not new_rows:
                        continue
                    if n:
                        print()
                    print(
                        " Beamline {beamline:6} --DCID-- ---visit---".format(
                            beamline=beamline
                        )
                    )
                    for row in new_rows:
                        if row["gridSize"]:
                            print(
                                "{startTime:%Y-%m-%d %H:%M} {dataCollectionId:8} {visit:<11} {numberOfImages:4} images, {gridSize:>5} grid   {fileTemplate}".format(
                                    **row
                                )
                            )
                        else:
                            print(
                                "{startTime:%Y-%m-%d %H:%M} {dataCollectionId:8} {visit:<11} {numberOfImages:4} images   {fileTemplate}".format(
                                    **row
                                )
                            )
                        if options.link:
                            print(
                                " " * 52
                                + "https://ispyb.diamond.ac.uk/dc/visit/{visit}/id/{dataCollectionId}\n".format(
                                    **row
                                )
                            )
            if not options.follow:
                break
            time.sleep(1)
