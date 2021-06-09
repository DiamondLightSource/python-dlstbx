# Functions to simulate a data collection
#
# This
# * inserts new entries into the datacollection table using the DbserverClient.py script
# * copies images from the source data collection
# * runs the scripts RunAtStartOfDataCollection.sh and RunAtEndOfDataCollection.sh
#   at appropriate times.


import datetime
import glob
import logging
import os
import re
import procrunner
import shutil
import sys
import time
import uuid

import dlstbx.dc_sim.dbserverclient
import dlstbx.dc_sim.definitions
import dlstbx.dc_sim.mydb

log = logging.getLogger("dlstbx.dc_sim")

# Constants
MX_SCRIPTS_BINDIR = "/dls_sw/apps/mx-scripts/bin"


def copy_via_temp_file(source, destination):
    dest_dir, dest_file = os.path.split(destination)
    temp_dest_file = ".tmp." + dest_file
    temp_destination = os.path.join(dest_dir, temp_dest_file)
    shutil.copyfile(source, temp_destination)
    os.rename(temp_destination, destination)


def retrieve_sessionid(_db, _visit):
    rows = _db.doQuery(
        "SELECT s.sessionid "
        "FROM BLSession s "
        "  INNER JOIN Proposal p ON p.proposalid = s.proposalid "
        "WHERE concat(p.proposalcode, p.proposalnumber, '-', s.visit_number)= '%s'"
        % _visit
    )
    if rows[0][0] is None:
        sys.exit(f"Could not find sessionid for visit {_visit}")
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
        sys.exit(f"Could not find datacollectiongroup {_src_dcgid}")
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

    if not result[0].get("datacollectionid"):
        sys.exit(f"Could not find the datacollectionid for visit {_dir}")
    if not result[0].get("startimagenumber"):
        sys.exit("Could not find the startimagenumber for the row")
    return result[0]


def retrieve_blsample_values(_db, _src_blsampleid):
    _db.cursor.execute(
        "SELECT blsampleid, name, code, location, holderlength, looplength, looptype, wirewidth, comments, "
        "blsamplestatus, lastknowncenteringposition "
        "FROM BLSample "
        "WHERE blsampleid=%d " % _src_blsampleid
    )

    desc = [d[0] for d in _db.cursor.description]
    result = [dict(zip(desc, line)) for line in _db.cursor]

    if not result[0].get("blsampleid"):
        sys.exit(f"Could not find the blsampleid for {_src_blsampleid}")

    return result[0]


def retrieve_no_images(_db, _dcid):
    rows = _db.doQuery(
        "SELECT numberOfImages from DataCollection where datacollectionid=%d" % _dcid
    )
    if rows[0][0] is None:
        sys.exit(f"Could not find the number of images for datacollectionid {_dcid}")
    if int(rows[0][0]) == 0:
        sys.exit(f"Could not find the number of images for datacollectionid {_dcid}")
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
    dbsc = dlstbx.dc_sim.dbserverclient.DbserverClient()

    log.debug("Getting the source SessionID")
    src_sessionid = retrieve_sessionid(_db, _src_visit)
    log.debug("SessionID is %r", src_sessionid)

    row = retrieve_datacollection_values(
        _db, src_sessionid, _src_dir, _src_prefix, _src_run_number
    )
    if not row["rotationaxis"]:
        row["rotationaxis"] = None
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
    log.debug(
        "Source dataset from DCID %r, DCGID %r, file template %r",
        src_dcid,
        src_dcgid,
        filetemplate,
    )

    no_images = retrieve_no_images(_db, src_dcid)
    log.debug("Source dataset has %d images" % no_images)

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
                os.makedirs(dir, exist_ok=True)
                log.debug(
                    "(filesystem) ... copying %s to %s"
                    % (src_xtal_snapshot_path[x], dest_xtal_snapshot_path[x])
                )
                copy_via_temp_file(
                    src_xtal_snapshot_path[x], dest_xtal_snapshot_path[x]
                )

    log.info("539 src_blsampleid: %s", src_blsampleid)
    log.info("539 _sample_id: %s", _sample_id)
    # Get a blsampleId either from a copy of the blsample used by the src dc or use the blsampleId provided on the command-line
    blsample_id = None
    if src_blsampleid is not None:
        if _sample_id is None:

            log.debug("(SQL) Getting values from the source blsample record")
            bls_row = retrieve_blsample_values(_db, int(src_blsampleid))

            blsample_xml = dlstbx.dc_sim.dbserverclient.populate_blsample_xml_template(
                bls_row
            )
            print(blsample_xml)

            # Ingest the blsample data using the DbserverClient
            log.debug("(dbserver) Ingest the blsample XML")
            blsample_id = dbsc.storeBLSample(blsample_xml)
        else:
            blsample_id = _sample_id
    elif _sample_id is not None:
        blsample_id = _sample_id

    if data_collection_group_id is None:
        # Produce a DataCollectionGroup xml blob from the template
        dcg_xml = dlstbx.dc_sim.dbserverclient.populate_dcg_xml_template(
            dcg_row, sessionid, blsample_id
        )

        # Ingest the DataCollectionGroup xml data using the DbserverClient
        log.debug("(dbserver) Ingest the datacollectiongroup XML")
        datacollectiongroupid = dbsc.storeDataCollectionGroup(dcg_xml)
    else:
        datacollectiongroupid = data_collection_group_id

    # Get the grid info values associated with the source dcg
    gi_row = retrieve_grid_info_values(_db, src_dcgid)

    # Prouce a GridInfo xml blob from the template if the source DataCollectionGroup has one:
    if gi_row is not None:
        gridinfo_xml = dlstbx.dc_sim.dbserverclient.populate_grid_info_xml_template(
            gi_row, datacollectiongroupid
        )

        # Ingest the GridInfo.xml file data using the DbserverClient
        log.debug("(dbserver) Ingest the gridinfo XML")
        dbsc.storeGridInfo(gridinfo_xml)

    # Produce a DataCollection xml blob from the template and use the new run number
    dc_xml = dlstbx.dc_sim.dbserverclient.populate_dc_xml_template(
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
    datacollectionid = dbsc.storeDataCollection(dc_xml)

    run_at_params = [
        "automaticProcessing_Yes",
        str(datacollectionid),
        _dest_visit_dir,
        filetemplate,
        _dest_dir + "/",
        _dest_prefix + "_" + str(run_number) + "_",
        os.path.splitext(filetemplate)[-1],
    ]

    command = [f"{MX_SCRIPTS_BINDIR}/RunAtStartOfCollect-{_beamline}.sh"]
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
            if _src_prefix is not None:
                src_prefix = _src_prefix
            src_fname = "%s_%d_%s.cbf" % (src_prefix, _src_run_number, str(img_number))
            dest_fname = "%s_%d_%s.cbf" % (_dest_prefix, run_number, str(img_number))
            src = os.path.join(_src_dir, src_fname)
            target = os.path.join(_dest_dir, dest_fname)
            log.info(f"(filesystem) Copy file {src} to {target}")
            copy_via_temp_file(src, target)
    elif filetemplate.endswith(".h5"):
        files = []
        src_prefix = ""
        if _src_prefix is not None:
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
            log.info(f"(filesystem) Copy file {src} to {target}")
            copy_via_temp_file(src, target)
    else:
        raise RuntimeError("Unsupported file extension for %s" % filetemplate)

    # Populate a datacollection XML blob
    nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    dc_xml = dlstbx.dc_sim.dbserverclient.dc_endtime_temp_xml % (
        datacollectionid,
        nowstr,
    )
    print(dc_xml)
    log.debug(
        "(dbserver) Ingest the datacollection XML to update with the d.c. end time"
    )
    dbsc.updateDbObject(dc_xml)

    # Populate a datacollectiongroup XML blob
    nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    dcg_xml = dlstbx.dc_sim.dbserverclient.dcg_endtime_temp_xml % (
        datacollectiongroupid,
        nowstr,
    )
    print(dcg_xml)

    # Ingest the DataCollectionGroup xml blob using the DbserverClient
    log.debug(
        "(dbserver) Ingest the datacollectiongroup XML to update with the d.c.g. end time"
    )
    dbsc.updateDbObject(dcg_xml)

    command = [f"{MX_SCRIPTS_BINDIR}/RunAtEndOfCollect-{_beamline}.sh"]
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
    # These proposal numbers need to be updated every year
    if beamline.startswith("m"):
        proposal = "cm28212"
    else:
        proposal = "nt28218"
    if beamline.startswith("i02"):
        if beamline == "i02-2":
            dest_visit = f"{proposal}-1"
        elif beamline == "i02-1":
            dest_visit = f"{proposal}-2"
        dest_visit_dir = "/dls/mx/data/{proposal}/{visit}".format(
            proposal=proposal, visit=dest_visit
        )
    else:
        for cm_dir in os.listdir(f"/dls/{beamline}/data/{now:%Y}"):
            if cm_dir.startswith(proposal):
                dest_visit = cm_dir
                break
        else:
            log.error("Could not determine destination directory")
            sys.exit(1)

        # Set mandatory parameters
        dest_visit_dir = "/dls/{beamline}/data/{now:%Y}/{dest_visit}".format(
            beamline=beamline, now=now, dest_visit=dest_visit
        )

    dest_dir_fmt = (
        "{dest_visit_dir}/tmp/{now:%Y-%m-%d}/{now:%H}-{now:%M}-{now:%S}-{random}"
    )
    dest_dir = dest_dir_fmt.format(
        now=now, dest_visit_dir=dest_visit_dir, random=str(uuid.uuid4())[:8]
    )

    # Extract necessary info from the source directory path
    m1 = re.search(r"(/dls/(\S+?)/data/\d+/)(\S+)", src_dir)
    if m1:
        subdir = m1.groups()[2]
        m2 = re.search(r"^(\S+?)/", subdir)
        if m2:
            src_visit = m2.groups()[0]
        elif subdir:
            src_visit = subdir
    else:
        m1 = re.search(r"(/dls/mx/data/)(\S+)", src_dir)
        if m1:
            subdir = m1.groups()[1]
            src_visit = subdir.split(os.sep)[1]

    if src_visit is None:
        sys.exit(
            "ERROR: The src_dir parameter does not appear to contain a valid visit directory."
        )

    start_script = f"{MX_SCRIPTS_BINDIR}/RunAtStartOfCollect-{beamline}.sh"
    if not os.path.exists(start_script):
        log.error("The file %s was not found.", start_script)
        sys.exit(1)
    end_script = f"{MX_SCRIPTS_BINDIR}/RunAtEndOfCollect-{beamline}.sh"
    if not os.path.exists(end_script):
        log.error("The file %s was not found.", end_script)
        sys.exit(1)

    # Create destination directory
    log.debug("Creating directory %s", dest_dir)
    os.makedirs(dest_dir, exist_ok=True)
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
            if scenario.get("delay"):
                log.info("Sleeping for %s seconds" % scenario["delay"])
                time.sleep(scenario["delay"])
    return dcid_list
