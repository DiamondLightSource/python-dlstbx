# Functions to simulate a data collection
#
# This
# * inserts new entries into the datacollection table using the DbserverClient.py script
# * copies images from the source data collection
# * runs the scripts RunAtStartOfDataCollection.sh and RunAtEndOfDataCollection.sh
#   at appropriate times.

from __future__ import absolute_import, division, print_function

import datetime
import errno
import glob
import logging
import os
import re
import procrunner
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

import dlstbx.dc_sim.definitions
import dlstbx.dc_sim.mydb

log = logging.getLogger("dlstbx.dc_sim")

# Constants
MX_SCRIPTS_BINDIR = "/dls_sw/apps/mx-scripts/bin"
DBSERVER_SRCDIR = "/dls_sw/apps/mx-scripts/ispyb-dbserver/src"
DBSERVER_HOST = "sci-serv3"
DBSERVER_PORT = "1994"


def f(_v):
    if _v is None:
        return float("nan")
    else:
        return float(_v)


def i(_v):
    if _v is None:
        return -1
    else:
        return int(_v)


def s(_v):
    if _v is None:
        return "null"
    else:
        return str(_v)


def call_dbserver(xml_input):
    try:
        f_in = tempfile.NamedTemporaryFile(suffix=".xml", dir="/tmp", delete=False)
        f_in_name = f_in.name
        f_in.write(xml_input)
        f_in.close()
        f_out = tempfile.NamedTemporaryFile(suffix=".xml", dir="/tmp", delete=False)
        f_out_name = f_out.name
        f_out.close()

        result = procrunner.run(
            [
                os.path.join(DBSERVER_SRCDIR, "DbserverClient.py"),
                "-h",
                DBSERVER_HOST,
                "-p",
                DBSERVER_PORT,
                "-i",
                f_in_name,
                "-d",
                "-o",
                f_out_name,
            ]
        )
        assert not result["exitcode"]

        with open(f_out_name, "r") as fh:
            return fh.read()
    finally:
        try:
            f_in.close()
        except Exception:
            pass
        try:
            f_out.close()
        except Exception:
            pass
        try:
            os.remove(f_in_name)
        except Exception:
            pass
        try:
            os.remove(f_out_name)
        except Exception:
            pass


def copy_via_temp_file(source, destination):
    dest_dir, dest_file = os.path.split(destination)
    temp_dest_file = ".tmp." + dest_file
    temp_destination = os.path.join(dest_dir, temp_dest_file)
    shutil.copyfile(source, temp_destination)
    os.rename(temp_destination, destination)


def populate_blsample_xml_template(_row):
    temp = blsample_xml % (
        s(_row["name"]),
        s(_row["code"]),
        s(_row["location"]),
        f(_row["holderlength"]),
        f(_row["looplength"]),
        s(_row["looptype"]),
        f(_row["wirewidth"]),
        s(_row["comments"]),
        s(_row["blsamplestatus"]),
        i(_row["isinsamplechanger"]),
        s(_row["lastknowncenteringposition"]),
    )

    # remove lines with null, nan and -1 values:
    temp = re.sub("\<.*\>null\</.*\>\n", "", temp)
    temp = re.sub("\<.*\>nan\</.*\>\n", "", temp)
    return re.sub("\<.*\>-1\</.*\>\n", "", temp)


def populate_dcg_xml_template(_row, _sessionid, _blsample_id):
    nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    blsample_id_elem = ""
    if _blsample_id != None:
        blsample_id_elem = "<blSampleId>%d</blSampleId>\n" % _blsample_id

    temp = dcg_temp_xml % (
        _sessionid,
        blsample_id_elem,
        s(_row["experimenttype"]),
        nowstr,
        s(_row["crystalclass"]),
        s(_row["detectormode"]),
    )

    temp_format = dcg_temp_xml_format.format(
        sessionid=_sessionid,
        blsample_xml=blsample_id_elem,
        comments="Simulated datacollection.",
        experimenttype=s(_row["experimenttype"]),
        starttime=nowstr,
        crystalclass=s(_row["crystalclass"]),
        detectormode=s(_row["detectormode"]),
    )
    assert temp == temp_format, "%s\n !=\n%s" % (temp, temp_format)

    # remove lines with null, nan and -1 values:
    temp = re.sub("\<.*\>null\</.*\>\n", "", temp)
    temp = re.sub("\<.*\>nan\</.*\>\n", "", temp)
    return re.sub("\<.*\>-1\</.*\>\n", "", temp)


def populate_grid_info_xml_template(_row, _dcgid):
    temp = grid_info_temp_xml % (
        _dcgid,
        f(_row["dx_mm"]),
        f(_row["dy_mm"]),
        i(_row["steps_x"]),
        i(_row["steps_y"]),
        f(_row["pixelspermicronx"]),
        f(_row["pixelspermicrony"]),
        f(_row["snapshot_offsetxpixel"]),
        f(_row["snapshot_offsetypixel"]),
        s(_row["orientation"]),
    )

    # remove lines with null, nan and -1 values:
    temp = re.sub("\<.*\>null\</.*\>\n", "", temp)
    temp = re.sub("\<.*\>nan\</.*\>\n", "", temp)
    return re.sub("\<.*\>-1\</.*\>\n", "", temp)


def populate_dc_xml_template(
    _row,
    _sessionid,
    _dcg_id,
    _no_images,
    _dir,
    _prefix,
    _run_number,
    _xtal_snapshot_path,
    _blsample_id,
    scenario_name=None,
):
    nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    suffix = _row["imagesuffix"]
    if suffix == "h5":
        file_template = "%s_%d_master.%s" % (_prefix, _run_number, suffix)
    else:
        file_template = "%s_%d_####.%s" % (_prefix, _run_number, suffix)
    if _blsample_id is None:
        blsample_id_elem = ""
    else:
        blsample_id_elem = "<blSampleId>%d</blSampleId>\n" % _blsample_id

    temp = dc_temp_xml % (
        _sessionid,
        _dcg_id,
        blsample_id_elem,
        _run_number,
        nowstr,
        s(_row["runstatus"]),
        f(_row["axisstart"]),
        f(_row["axisend"]),
        f(_row["axisrange"]),
        f(_row["overlap"]),
        _no_images,
        i(_row["startimagenumber"]),
        i(_row["numberofpasses"]),
        f(_row["exposuretime"]),
        _dir,
        _prefix,
        suffix,
        file_template,
        f(_row["wavelength"]),
        f(_row["resolution"]),
        f(_row["detectordistance"]),
        f(_row["xbeam"]),
        f(_row["ybeam"]),
        i(_row["printableforreport"]),
        f(_row["slitgapvertical"]),
        f(_row["slitgaphorizontal"]),
        f(_row["transmission"]),
        s(_row["synchrotronmode"]),
        _xtal_snapshot_path[0],
        _xtal_snapshot_path[1],
        _xtal_snapshot_path[2],
        _xtal_snapshot_path[3],
        s(_row["rotationaxis"]),
        f(_row["phistart"]),
        f(_row["chistart"]),
        f(_row["kappastart"]),
        f(_row["omegastart"]),
        f(_row["undulatorgap1"]),
        f(_row["beamsizeatsamplex"]),
        f(_row["beamsizeatsampley"]),
        f(_row["flux"]),
        i(_row["focalspotsizeatsamplex"]),
        i(_row["focalspotsizeatsampley"]),
    )
    temp = temp.format(
        comments="Simulated datacollection ({}).".format(scenario_name)
        if scenario_name
        else "Simulated datacollection."
    )

    # remove lines with null, nan and -1 values:
    temp = re.sub("\<.*\>null\</.*\>\n", "", temp)
    temp = re.sub("\<.*\>nan\</.*\>\n", "", temp)
    return re.sub("\<.*\>-1\</.*\>\n", "", temp)


blsample_xml = """<?xml version="1.0" ?>
<BLSample>
<name>%s</name>
<code>%s</code>
<location>%s</location>
<holderLength>%.2f</holderLength>
<loopLength>%.2f</loopLength>
<loopType>%s</loopType>
<wireWidth>%.2f</wireWidth>
<comments>%s</comments>
<blSampleStatus>%s</blSampleStatus>
<isInSampleChanger>%d</isInSampleChanger>
<lastKnownCenteringPosition>%s</lastKnownCenteringPosition>
</BLSample>"""

dcg_temp_xml = """<?xml version="1.0" ?>
<DataCollectionGroup>
<sessionId>%d</sessionId>
%s<comments>Simulated datacollection.</comments>
<experimentType>%s</experimentType>
<startTime>%s</startTime>
<crystalClass>%s</crystalClass>
<detectorMode>%s</detectorMode>
</DataCollectionGroup>"""
dcg_temp_xml_format = """<?xml version="1.0" ?>
<DataCollectionGroup>
<sessionId>{sessionid}</sessionId>
{blsample_xml}<comments>{comments}</comments>
<experimentType>{experimenttype}</experimentType>
<startTime>{starttime}</startTime>
<crystalClass>{crystalclass}</crystalClass>
<detectorMode>{detectormode}</detectorMode>
</DataCollectionGroup>"""

grid_info_temp_xml = """<?xml version="1.0" ?>
<GridInfo>
<dataCollectionGroupId>%d</dataCollectionGroupId>
<dx_mm>%.2f</dx_mm>
<dy_mm>%.2f</dy_mm>
<steps_x>%d</steps_x>
<steps_y>%d</steps_y>
<pixelsPerMicronX>%.4f</pixelsPerMicronX>
<pixelsPerMicronY>%.4f</pixelsPerMicronY>
<snapshot_offsetXPixel>%.4f</snapshot_offsetXPixel>
<snapshot_offsetYPixel>%.4f</snapshot_offsetYPixel>
<orientation>%s</orientation>
</GridInfo>
"""

dc_temp_xml = """<?xml version="1.0" ?>
<DataCollection>
<sessionId>%d</sessionId>
<dataCollectionGroupId>%d</dataCollectionGroupId>
%s<dataCollectionNumber>%d</dataCollectionNumber>
<startTime>%s</startTime>
<runStatus>%s</runStatus>
<axisStart>%.2f</axisStart>
<axisEnd>%.2f</axisEnd>
<axisRange>%.2f</axisRange>
<overlap>%.2f</overlap>
<numberOfImages>%d</numberOfImages>
<startImageNumber>%d</startImageNumber>
<numberOfPasses>%d</numberOfPasses>
<exposureTime>%.3f</exposureTime>
<imageDirectory>%s</imageDirectory>
<imagePrefix>%s</imagePrefix>
<imageSuffix>%s</imageSuffix>
<fileTemplate>%s</fileTemplate>
<wavelength>%.6f</wavelength>
<resolution>%.2f</resolution>
<detectorDistance>%.6f</detectorDistance>
<xBeam>%.6f</xBeam>
<yBeam>%.6f</yBeam>
<comments>{comments}</comments>
<printableForReport>%d</printableForReport>
<slitGapVertical>%.6f</slitGapVertical>
<slitGapHorizontal>%.6f</slitGapHorizontal>
<transmission>%.6f</transmission>
<synchrotronMode>%s</synchrotronMode>
<xtalSnapshotFullPath1>%s</xtalSnapshotFullPath1>
<xtalSnapshotFullPath2>%s</xtalSnapshotFullPath2>
<xtalSnapshotFullPath3>%s</xtalSnapshotFullPath3>
<xtalSnapshotFullPath4>%s</xtalSnapshotFullPath4>
<rotationAxis>%s</rotationAxis>
<phiStart>%.1f</phiStart>
<chiStart>%.1f</chiStart>
<kappaStart>%.1f</kappaStart>
<omegaStart>%.1f</omegaStart>
<undulatorGap1>%.6f</undulatorGap1>
<beamSizeAtSampleX>%.2f</beamSizeAtSampleX>
<beamSizeAtSampleY>%.2f</beamSizeAtSampleY>
<flux>%.6f</flux>
<focalSpotSizeAtSampleX>%d</focalSpotSizeAtSampleX>
<focalSpotSizeAtSampleY>%d</focalSpotSizeAtSampleY>
</DataCollection>"""

dc_endtime_temp_xml = """<?xml version="1.0" ?>
<DataCollection>
<dataCollectionId>%d</dataCollectionId>
<endTime>%s</endTime>
</DataCollection>"""

dcg_endtime_temp_xml = """<?xml version="1.0" ?>
<DataCollectionGroup>
<dataCollectionGroupId>%d</dataCollectionGroupId>
<endTime>%s</endTime>
</DataCollectionGroup>"""


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def retrieve_sessionid(_db, _visit):
    rows = _db.doQuery(
        "SELECT s.sessionid "
        "FROM BLSession s "
        "  INNER JOIN Proposal p ON p.proposalid = s.proposalid "
        "WHERE concat(p.proposalcode, p.proposalnumber, '-', s.visit_number)= '%s'"
        % _visit
    )
    if rows[0][0] is None:
        sys.exit("Could not find sessionid for visit %s" % _visit)
    return int(rows[0][0])


def retrieve_datacollection_group_values(_db, _src_dcgid):
    _db.cursor.execute(
        "SELECT comments, blsampleid, experimenttype, starttime, endtime, crystalclass, detectormode, actualsamplebarcode, "
        "actualsampleslotincontainer, actualcontainerbarcode, actualcontainerslotinsc, workflowid, xtalsnapshotfullpath "
        "FROM DataCollectionGroup "
        "WHERE datacollectiongroupid=%d" % _src_dcgid
    )

    desc = [d[0] for d in _db.cursor.description]
    result = [dict(zip(desc, line)) for line in _db.cursor]

    if len(result) == 0:
        sys.exit("Could not find datacollectiongroup %s" % _src_dcgid)
    return result[0]


def retrieve_grid_info_values(_db, _src_dcgid):
    _db.cursor.execute(
        "SELECT dx_mm, dy_mm, steps_x, steps_y, pixelspermicronx, pixelspermicrony, "
        "snapshot_offsetxpixel, snapshot_offsetypixel, orientation "
        "FROM GridInfo "
        "WHERE datacollectiongroupid=%d" % _src_dcgid
    )

    desc = [d[0] for d in _db.cursor.description]
    result = [dict(zip(desc, line)) for line in _db.cursor]

    if len(result) == 0:
        return None
    return result[0]


def retrieve_datacollection_values(_db, _sessionid, _dir, _prefix, _run_number):
    if _prefix is None:
        prefix_line = "AND imageprefix is NULL "
    else:
        prefix_line = "AND imageprefix='%s' " % _prefix

    _db.cursor.execute(
        "SELECT datacollectionid, datacollectiongroupid, blsampleid, startimagenumber, "
        "xtalsnapshotfullpath1, xtalsnapshotfullpath2, xtalsnapshotfullpath3, xtalsnapshotfullpath4, "
        "runstatus, axisstart, axisend, axisrange, overlap, numberofimages, startimagenumber, "
        "numberofpasses, exposuretime, imagesuffix, filetemplate, "
        "wavelength, resolution, detectordistance, xbeam, ybeam, comments, printableforreport, "
        "slitgapvertical, slitgaphorizontal, transmission, synchrotronmode, "
        "rotationaxis, phistart, chistart, kappastart, omegastart, undulatorgap1, "
        "beamsizeatsamplex, beamsizeatsampley, flux, focalspotsizeatsamplex, focalspotsizeatsampley "
        "FROM DataCollection "
        "WHERE sessionid=%d "
        "AND imagedirectory='%s' "
        "%s "
        "AND datacollectionnumber=%d "
        % (_sessionid, _dir + "/", prefix_line, _run_number)
    )

    desc = [d[0] for d in _db.cursor.description]
    result = [dict(zip(desc, line)) for line in _db.cursor]

    if result[0]["datacollectionid"] is None:
        sys.exit("Could not find the datacollectionid for visit %s" % _src_visit)
    if result[0]["startimagenumber"] is None:
        sys.exit("Could not find the startimagenumber for the row")
    return result[0]


def retrieve_blsample_values(_db, _src_blsampleid):
    _db.cursor.execute(
        "SELECT blsampleid, name, code, location, holderlength, looplength, looptype, wirewidth, comments, "
        "blsamplestatus, isinsamplechanger, lastknowncenteringposition "
        "FROM BLSample "
        "WHERE blsampleid=%d " % _src_blsampleid
    )

    desc = [d[0] for d in _db.cursor.description]
    result = [dict(zip(desc, line)) for line in _db.cursor]

    if result[0]["blsampleid"] is None:
        sys.exit("Could not find the blsampleid for visit %s" % _src_visit)

    return result[0]


def retrieve_no_images(_db, _dcid):
    no_images = None
    rows = _db.doQuery(
        "SELECT numberOfImages from DataCollection where datacollectionid=%d" % _dcid
    )
    if rows[0][0] is None:
        sys.exit("Could not find the number of images for datacollectionid %d" % _dcid)
    if int(rows[0][0]) is 0:
        sys.exit("Could not find the number of images for datacollectionid %d" % _dcid)
    return int(rows[0][0])


def retrieve_max_dcnumber(_db, _sessionid, _dest_dir, _dest_prefix):
    rows = _db.doQuery(
        "SELECT max(datacollectionnumber) "
        "FROM DataCollection "
        "WHERE sessionid=%d "
        "AND imagedirectory='%s' "
        "AND imageprefix='%s'" % (_sessionid, _dest_dir + "/", _dest_prefix)
    )
    return rows[0][0]


def simulate(
    _dest_visit,
    _beamline,
    _src_dir,
    _src_visit,
    _src_prefix,
    _src_run_number,
    _dest_prefix,
    _dest_visit_dir,
    _dest_dir,
    _sample_id,
    data_collection_group_id=None,
    scenario_name=None,
):
    _db = dlstbx.dc_sim.mydb.DB()

    log.debug("(SQL) Getting the source sessionid")
    src_sessionid = retrieve_sessionid(_db, _src_visit)

    log.debug("(SQL) Getting values from the source datacollection record")
    row = retrieve_datacollection_values(
        _db, src_sessionid, _src_dir, _src_prefix, _src_run_number
    )
    src_dcid = int(row["datacollectionid"])
    src_dcgid = int(row["datacollectiongroupid"])
    start_img_number = int(row["startimagenumber"])
    filetemplate = row["filetemplate"]
    src_xtal_snapshot_path = [
        row["xtalsnapshotfullpath1"],
        row["xtalsnapshotfullpath2"],
        row["xtalsnapshotfullpath3"],
        row["xtalsnapshotfullpath4"],
    ]

    log.debug("(SQL) Getting the number of images")
    no_images = retrieve_no_images(_db, src_dcid)
    log.debug("(ANS) Got %d" % no_images)

    # Get the sessionid for the dest_visit
    log.debug("(SQL) Getting the destination sessionid")
    sessionid = retrieve_sessionid(_db, _dest_visit)

    # Get the highest run number for the datacollections of this dest_visit with the particular img.dir and prefix
    log.debug(
        "(SQL) Getting the currently highest run number for this img. directory + prefix"
    )
    if filetemplate.endswith(".h5"):
        # Can't change the run number otherwise the link from the master.h5 to data_*.h5 will be incorrect
        run_number = _src_run_number
    else:
        run_number = retrieve_max_dcnumber(_db, sessionid, _dest_dir, _dest_prefix)
        if run_number is None:
            run_number = 1
        else:
            run_number = int(run_number) + 1

    log.debug("(SQL) Getting values from the source datacollectiongroup record")
    dcg_row = retrieve_datacollection_group_values(_db, src_dcgid)

    src_blsampleid = dcg_row["blsampleid"]

    log.debug(
        "(filesystem) Copy the xtal snapshot(s) (if any) from source to target directories"
    )
    dest_xtal_snapshot_path = ["", "", "", ""]
    for x in range(0, 4):
        if src_xtal_snapshot_path[x] is not None:
            if os.path.exists(src_xtal_snapshot_path[x]):
                png = re.sub("^.*/(.*)$", _dest_dir + r"/\1", src_xtal_snapshot_path[x])
                dest_xtal_snapshot_path[x] = re.sub(
                    "^" + _dest_visit_dir, _dest_visit_dir + "/jpegs", png
                )
                dir = os.path.dirname(dest_xtal_snapshot_path[x])
                log.debug("(filesystem) ... 'mkdir -p' %s" % dir)
                mkdir_p(dir)
                log.debug(
                    "(filesystem) ... copying %s to %s"
                    % (src_xtal_snapshot_path[x], dest_xtal_snapshot_path[x])
                )
                copy_via_temp_file(
                    src_xtal_snapshot_path[x], dest_xtal_snapshot_path[x]
                )

    # Get a blsampleId either from a copy of the blsample used by the src dc or use the blsampleId provided on the command-line
    blsample_id = None
    if src_blsampleid != None:
        if _sample_id is None:

            log.debug("(SQL) Getting values from the source blsample record")
            bls_row = retrieve_blsample_values(_db, int(src_blsampleid))

            blsample_xml = populate_blsample_xml_template(bls_row)
            print(blsample_xml)

            # Ingest the blsample data using the DbserverClient
            log.debug("(dbserver) Ingest the blsample XML")
            xml = call_dbserver(blsample_xml)

            m = re.search("<blSampleId>(\d+)</blSampleId>", xml)
            if m:
                blsample_id = int(m.groups()[0])
            else:
                sys.exit("No blsampleid found in output")

        else:
            blsample_id = _sample_id

    if data_collection_group_id is None:
        # Produce a DataCollectionGroup xml blob from the template
        dcg_xml = populate_dcg_xml_template(dcg_row, sessionid, blsample_id)

        # Ingest the DataCollectionGroup xml data using the DbserverClient
        log.debug("(dbserver) Ingest the datacollectiongroup XML")
        xml = call_dbserver(dcg_xml)

        datacollectiongroupid = None
        m = re.search("<dataCollectionGroupId>(\d+)</dataCollectionGroupId>", xml)
        if m:
            datacollectiongroupid = int(m.groups()[0])
        else:
            sys.exit("No datacollectiongroupid found in output")
    else:
        datacollectiongroupid = data_collection_group_id

    # Get the grid info values associated with the source dcg
    gi_row = retrieve_grid_info_values(_db, src_dcgid)

    # Prouce a GridInfo xml blob from the template if the source DataCollectionGroup has one:
    if gi_row is not None:
        gridinfo_xml = populate_grid_info_xml_template(gi_row, datacollectiongroupid)

        # Ingest the GridInfo.xml file data using the DbserverClient
        log.debug("(dbserver) Ingest the gridinfo XML")
        xml = call_dbserver(gridinfo_xml)
        gridinfoid = None
        m = re.search("<gridInfoId>(\d+)</gridInfoId>", xml)
        if m:
            gridinfoid = int(m.groups()[0])
        else:
            sys.exit("No gridinfoid found in output")

    # Produce a DataCollection xml blob from the template and use the new run number
    dc_xml = populate_dc_xml_template(
        row,
        sessionid,
        datacollectiongroupid,
        no_images,
        _dest_dir + "/",
        _dest_prefix,
        run_number,
        dest_xtal_snapshot_path,
        blsample_id,
        scenario_name=scenario_name,
    )

    # Ingest the DataCollection xml blob data using the DbserverClient
    log.debug("(dbserver) Ingest the datacollection XML")
    xml = call_dbserver(dc_xml)
    datacollectionid = None
    m = re.search("<dataCollectionId>(\d+)</dataCollectionId>", xml)
    if m:
        datacollectionid = int(m.groups()[0])
    else:
        sys.exit("No datacollectionid found in output")

    run_at_params = [
        "automaticProcessing_Yes",
        str(datacollectionid),
        _dest_visit_dir,
        filetemplate,
        _dest_dir + "/",
        _dest_prefix + "_" + str(run_number) + "_",
        os.path.splitext(filetemplate)[-1],
    ]

    command = ["%s/RunAtStartOfCollect-%s.sh" % (MX_SCRIPTS_BINDIR, _beamline)]
    command.extend(run_at_params)
    log.info("command: %s", " ".join(command))
    result = procrunner.run(command, timeout=180)
    log.info("runtime: %s", result["runtime"])
    if result["exitcode"] or result["timeout"]:
        log.info("timeout: %s", result["timeout"])
        log.debug(result["stdout"])
        log.debug(result["stderr"])
        log.error("RunAtStartOfCollect failed with exit code %d", result["exitcode"])

    if filetemplate.endswith(".cbf"):
        # Also copy images one by one from source to destination directory.
        for x in range(start_img_number, start_img_number + no_images):
            img_number = "%04d" % x
            src_prefix = ""
            if not _src_prefix is None:
                src_prefix = _src_prefix
            src_fname = "%s_%d_%s.cbf" % (src_prefix, _src_run_number, str(img_number))
            dest_fname = "%s_%d_%s.cbf" % (_dest_prefix, run_number, str(img_number))
            src = os.path.join(_src_dir, src_fname)
            target = os.path.join(_dest_dir, dest_fname)
            log.info("(filesystem) Copy file %s to %s" % (src, target))
            copy_via_temp_file(src, target)
    elif filetemplate.endswith(".h5"):
        files = []
        src_prefix = ""
        if not _src_prefix is None:
            src_prefix = _src_prefix
        for ext in ("_*.h5", ".nxs", "_meta.hdf5"):
            files.extend(
                glob.glob(
                    os.path.join(_src_dir, filetemplate.split("_master.h5")[0] + ext)
                )
            )
        for src in files:
            dest_fname = os.path.basename(src).replace(
                "%s_%d" % (src_prefix, _src_run_number),
                "%s_%d" % (_dest_prefix, run_number),
            )
            target = os.path.join(_dest_dir, dest_fname)
            log.info("(filesystem) Copy file %s to %s" % (src, target))
            copy_via_temp_file(src, target)
    else:
        raise RuntimeError("Unsupported file extension for %s" % filetemplate)

    # Populate a datacollection XML blob
    nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    dc_xml = dc_endtime_temp_xml % (datacollectionid, nowstr)
    print(dc_xml)
    log.debug(
        "(dbserver) Ingest the datacollection XML to update with the d.c. end time"
    )
    call_dbserver(dc_xml)

    # Populate a datacollectiongroup XML blob
    nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    dcg_xml = dcg_endtime_temp_xml % (datacollectiongroupid, nowstr)
    print(dcg_xml)

    # Ingest the DataCollectionGroup xml blob using the DbserverClient
    log.debug(
        "(dbserver) Ingest the datacollectiongroup XML to update with the d.c.g. end time"
    )
    call_dbserver(dcg_xml)

    command = ["%s/RunAtEndOfCollect-%s.sh" % (MX_SCRIPTS_BINDIR, _beamline)]
    command.extend(run_at_params)
    log.info("command: %s", " ".join(command))
    result = procrunner.run(command, timeout=180)
    log.info("runtime: %s", result["runtime"])
    if result["exitcode"] or result["timeout"]:
        log.info("timeout: %s", result["timeout"])
        log.debug(result["stdout"])
        log.debug(result["stderr"])
        log.error("RunAtEndOfCollect failed with exit code %d", result["exitcode"])

    return datacollectionid, datacollectiongroupid


def call_sim(test_name, beamline):
    scenario = dlstbx.dc_sim.definitions.tests.get(test_name)
    if not scenario:
        sys.exit("%s is not a valid test scenario" % test_name)

    src_dir = scenario["src_dir"]
    sample_id = scenario.get("use_sample_id")
    src_prefix = scenario["src_prefix"]

    # Calculate the destination directory
    now = datetime.datetime.now()
    if beamline == "i02-2":
        dest_visit = "nt18231-22"
        dest_visit_dir = "/dls/mx/data/nt18231/nt18231-22"
        dest_dir_fmt = (
            "{dest_visit_dir}/tmp/{now:%Y-%m-%d}/{now:%H}-{now:%M}-{now:%S}-{random}"
        )
        dest_dir = dest_dir_fmt.format(
            beamline=beamline,
            now=now,
            dest_visit_dir=dest_visit_dir,
            random=str(uuid.uuid4())[:8],
        )
    else:
        for cm_dir in os.listdir(
            "/dls/{beamline}/data/{now:%Y}".format(beamline=beamline, now=now)
        ):
            if cm_dir.startswith("nt18231"):
                dest_visit = cm_dir
                dest_dir_fmt = "/dls/{beamline}/data/{now:%Y}/{cm_dir}/tmp/{now:%Y-%m-%d}/{now:%H}-{now:%M}-{now:%S}-{random}"
                dest_dir = dest_dir_fmt.format(
                    beamline=beamline,
                    now=now,
                    cm_dir=cm_dir,
                    random=str(uuid.uuid4())[:8],
                )
                break
        else:
            log.error("Could not determine destination directory")
            sys.exit(1)

        # Set mandatory parameters
        dest_visit_dir = "/dls/{beamline}/data/{now:%Y}/{dest_visit}".format(
            beamline=beamline, now=now, dest_visit=dest_visit
        )

    # Extract necessary info from the source directory path
    m1 = re.search("(/dls/(\S+?)/data/\d+/)(\S+)", src_dir)
    if m1:
        subdir = m1.groups()[2]
        m2 = re.search("^(\S+?)/", subdir)
        if m2:
            src_visit = m2.groups()[0]
        elif subdir:
            src_visit = subdir
    else:
        m1 = re.search("(/dls/mx/data/)(\S+)", src_dir)
        if m1:
            subdir = m1.groups()[1]
            src_visit = subdir.split(os.sep)[1]

    if src_visit is None:
        sys.exit(
            "ERROR: The src_dir parameter does not appear to contain a valid visit directory."
        )

    start_script = "%s/RunAtStartOfCollect-%s.sh" % (MX_SCRIPTS_BINDIR, beamline)
    if not os.path.exists(start_script):
        log.error("The file %s was not found.", start_script)
        sys.exit(1)
    end_script = "%s/RunAtEndOfCollect-%s.sh" % (MX_SCRIPTS_BINDIR, beamline)
    if not os.path.exists(end_script):
        log.error("The file %s was not found.", end_script)
        sys.exit(1)

    # Create destination directory
    log.debug("Creating directory %s", dest_dir)
    mkdir_p(dest_dir)
    if os.path.isdir(dest_dir):
        log.info("Directory %s created successfully", dest_dir)
    else:
        log.error("Creating directory %s failed", dest_dir)

    # Call simulate
    dcid_list = []
    dcg_list = []
    for src_run_number in scenario["src_run_num"]:
        for src_prefix in scenario["src_prefix"]:
            dest_prefix = src_prefix
            if scenario.get("dcg") and len(dcg_list):
                dcg = dcg_list[0]
            else:
                dcg = None
            dcid, dcg = simulate(
                dest_visit,
                beamline,
                src_dir,
                src_visit,
                src_prefix,
                src_run_number,
                dest_prefix,
                dest_visit_dir,
                dest_dir,
                sample_id,
                data_collection_group_id=dcg,
                scenario_name=test_name,
            )
            dcid_list.append(dcid)
            dcg_list.append(dcg)
    return dcid_list
