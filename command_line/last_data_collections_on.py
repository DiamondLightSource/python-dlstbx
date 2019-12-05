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


def print_data_collections(rows, link=False):
    for row in reversed(rows):
        if row["gridSize"]:
            print(
                "{startTime:%Y-%m-%d %H:%M} {beamLineName:8} {dataCollectionId:8} {visit:<11} {numberOfImages:4} images, {gridSize:>5} grid   {fileTemplate}".format(
                    **row
                )
            )
        else:
            print(
                "{startTime:%Y-%m-%d %H:%M} {beamLineName:8} {dataCollectionId:8} {visit:<11} {numberOfImages:4} images   {fileTemplate}".format(
                    **row
                )
            )
        if link:
            print(
                " " * 52
                + "https://ispyb.diamond.ac.uk/dc/visit/{visit}/id/{dataCollectionId}".format(
                    **row
                )
            )


def get_last_data_collections_on(beamlines, cursor, limit=10, latest_dcid=None):
    query = (
        (
            "SELECT BLSession.beamLineName,"
            " DataCollection.dataCollectionId,"
            " DataCollection.startTime,"
            " DataCollection.numberOfImages,"
            ' CONCAT(GridInfo.steps_x, "x", GridInfo.steps_y) AS gridSize,'
            ' CONCAT(TRIM(TRAILING "/" FROM DataCollection.imageDirectory), "/", DataCollection.fileTemplate) AS fileTemplate,'
            ' CONCAT(Proposal.proposalCode, Proposal.proposalNumber, "-", BLSession.visit_number) as visit'
            " FROM DataCollection"
            " JOIN BLSession ON DataCollection.SESSIONID = BLSession.sessionID"
            " JOIN Proposal ON BLSession.proposalId = Proposal.proposalId"
            " LEFT JOIN GridInfo ON DataCollection.dataCollectionGroupId = GridInfo.dataCollectionGroupId"
            ' WHERE BLSession.beamLineName IN (%s) AND Proposal.proposalCode != "nt"'
        )
        % ", ".join('"%s"' % b for b in beamlines),
    )
    if latest_dcid:
        cursor.run(
            "%s AND DataCollection.dataCollectionId > %%s"
            " ORDER BY DataCollection.startTime DESC;" % query,
            latest_dcid,
        )
    else:
        cursor.run(
            "%s ORDER BY DataCollection.startTime DESC LIMIT %%s;" % query, limit
        )
    return cursor.fetchall()


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
        "-s",
        "--sleep",
        dest="sleep",
        default=60,
        type="float",
        help="Length of time (s) to sleep in conjunction with --follow mode.",
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
    t0 = time.time()
    with ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg") as i:
        ispyb.model.__future__.enable(
            "/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg"
        )
        latest_dcid = None
        print("------Date------ Beamline --DCID-- ---Visit---")
        # Terminate after 24 hours
        while time.time() - t0 < 60 * 60 * 24:
            with ispyb.model.__future__._db_cc() as cursor:
                rows = get_last_data_collections_on(
                    args, cursor, limit=options.limit, latest_dcid=latest_dcid
                )
                if not rows:
                    continue
                # Record the last observed dcid per beamline
                latest_dcid = rows[0]["dataCollectionId"]
                print_data_collections(rows, link=options.link)
            if not options.follow:
                break
            time.sleep(options.sleep)
