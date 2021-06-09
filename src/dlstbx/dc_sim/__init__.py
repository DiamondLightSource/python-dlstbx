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
import shutil
import sys
import time
import uuid

import ispyb.sqlalchemy
import procrunner
import sqlalchemy

import dlstbx.dc_sim.dbserverclient
import dlstbx.dc_sim.definitions
import dlstbx.dc_sim.mydb as db

log = logging.getLogger("dlstbx.dc_sim")

# Constants
MX_SCRIPTS_BINDIR = "/dls_sw/apps/mx-scripts/bin"


def copy_via_temp_file(source, destination):
    dest_dir, dest_file = os.path.split(destination)
    temp_dest_file = ".tmp." + dest_file
    temp_destination = os.path.join(dest_dir, temp_dest_file)
    shutil.copyfile(source, temp_destination)
    os.rename(temp_destination, destination)


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


def retrieve_max_dcnumber(_db, _sessionid, _dest_dir, _dest_prefix):
    rows = _db.doQuery(
        "SELECT max(datacollectionnumber) "
        "FROM DataCollection "
        "WHERE sessionid=%d "
        "AND imagedirectory='%s' "
        "AND imageprefix='%s'" % (_sessionid, _dest_dir + "/", _dest_prefix)
    )
    return rows[0][0]


def _simulate(
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
    proc_params,
    data_collection_group_id,
    scenario_name,
):
    _db = db.DB()
    dbsc = dlstbx.dc_sim.dbserverclient.DbserverClient()
    url = ispyb.sqlalchemy.url()
    engine = sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    db_session = sqlalchemy.orm.sessionmaker(bind=engine)()

    log.debug("Getting the source SessionID")
    src_sessionid = db.retrieve_sessionid(db_session, _src_visit)
    log.debug("SessionID is {src_sessionid}")

    row = db.retrieve_datacollection(
        db_session, src_sessionid, _src_dir, _src_prefix, _src_run_number
    )
    src_dcid = row.dataCollectionId
    src_dcgid = row.dataCollectionGroupId
    start_img_number = row.startImageNumber
    filetemplate = row.fileTemplate
    src_xtal_snapshot_path = [
        row.xtalSnapshotFullPath1,
        row.xtalSnapshotFullPath2,
        row.xtalSnapshotFullPath3,
        row.xtalSnapshotFullPath4,
    ]
    log.debug(
        f"Source dataset from DCID {src_dcid}, DCGID {src_dcgid}, file template {filetemplate}"
    )

    no_images = row.numberOfImages
    if not no_images:
        sys.exit(f"Could not find the number of images for data collection")
    log.debug(f"Source dataset has {no_images} images")

    # Get the sessionid for the dest_visit
    log.debug("(SQL) Getting the destination sessionid")
    sessionid = db.retrieve_sessionid(db_session, _dest_visit)

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
    src_blsampleid = row.DataCollectionGroup.blSampleId

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
                path = os.path.dirname(dest_xtal_snapshot_path[x])
                log.debug("(filesystem) ... 'mkdir -p' %s" % path)
                os.makedirs(path, exist_ok=True)
                log.debug(
                    "(filesystem) ... copying %s to %s"
                    % (src_xtal_snapshot_path[x], dest_xtal_snapshot_path[x])
                )
                copy_via_temp_file(
                    src_xtal_snapshot_path[x], dest_xtal_snapshot_path[x]
                )

    log.info(f"539 src_blsampleid: {src_blsampleid}")
    log.info("539 _sample_id: %s", _sample_id)
    # Get a blsampleId either from a copy of the blsample used by the src dc or use the blsampleId provided on the command-line
    blsample_id = None
    if src_blsampleid is not None:
        if _sample_id is None:

            log.debug("(SQL) Getting values from the source blsample record")
            bls_row = retrieve_blsample_values(_db, src_blsampleid)

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
            row, sessionid, blsample_id
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
    row_as_dictionary = {
        name.lower(): getattr(x, name) for name in dir(row) if not name.startswith("_")
    }
    dc_xml = dlstbx.dc_sim.dbserverclient.populate_dc_xml_template(
        row_as_dictionary,
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

    return datacollectionid, datacollectiongroupid, None


def call_sim(test_name, beamline):
    scenario = dlstbx.dc_sim.definitions.tests.get(test_name)
    if not scenario:
        sys.exit(f"{test_name} is not a valid test scenario")

    src_dir = scenario["src_dir"]
    sample_id = scenario.get("use_sample_id")
    src_prefix = scenario["src_prefix"]
    proc_params = scenario.get("proc_params")

    # Calculate the destination directory
    now = datetime.datetime.now()
    # These proposal numbers need to be updated every year
    if beamline.startswith(("e", "m")):
        proposal = "cm28212"
    else:
        proposal = "nt28218"
    if beamline.startswith("i02"):
        if beamline == "i02-2":
            dest_visit = f"{proposal}-1"
        elif beamline == "i02-1":
            dest_visit = f"{proposal}-2"
        dest_visit_dir = f"/dls/mx/data/{proposal}/{dest_visit}"
    else:
        for cm_dir in os.listdir(f"/dls/{beamline}/data/{now:%Y}"):
            if cm_dir.startswith(proposal):
                dest_visit = cm_dir
                break
        else:
            log.error("Could not determine destination directory")
            sys.exit(1)

        # Set mandatory parameters
        dest_visit_dir = f"/dls/{beamline}/data/{now:%Y}/{dest_visit}"

    random_uuid = str(uuid.uuid4())[:8]
    dest_dir = (
        f"{dest_visit_dir}/tmp/{now:%Y-%m-%d}/{now:%H}-{now:%M}-{now:%S}-{random_uuid}"
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

    if scenario["type"] == "mx":
        start_script = f"{MX_SCRIPTS_BINDIR}/RunAtStartOfCollect-{beamline}.sh"
        if not os.path.exists(start_script):
            log.error(f"The file {start_script} was not found.")
            sys.exit(1)
        end_script = f"{MX_SCRIPTS_BINDIR}/RunAtEndOfCollect-{beamline}.sh"
        if not os.path.exists(end_script):
            log.error(f"The file {end_script} was not found.")
            sys.exit(1)

    # Create destination directory
    log.debug("Creating directory %s", dest_dir)
    os.makedirs(dest_dir, exist_ok=True)
    if os.path.isdir(dest_dir):
        log.info(f"Directory {dest_dir} created successfully")
    else:
        log.error(f"Creating directory {dest_dir} failed")

    # Call _simulate
    dcid_list = []
    dcg_list = []
    jobid_list = []
    for src_run_number in scenario["src_run_num"]:
        for src_prefix in scenario["src_prefix"]:
            dest_prefix = src_prefix
            if scenario.get("dcg") and len(dcg_list):
                dcg = dcg_list[0]
            else:
                dcg = None
            dcid, dcg, jobid = _simulate(
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
                proc_params,
                data_collection_group_id=dcg,
                scenario_name=test_name,
            )
            jobid_list.append(jobid)
            dcid_list.append(dcid)
            dcg_list.append(dcg)
            if scenario.get("delay"):
                log.info(f"Sleeping for {scenario['delay']} seconds")
                time.sleep(scenario["delay"])
    return dcid_list, jobid_list
