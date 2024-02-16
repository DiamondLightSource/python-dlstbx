# Functions to simulate a data collection
#
# This
# * inserts new entries into the datacollection table using the DbserverClient.py script
# * copies images from the source data collection
# * runs the scripts RunAtStartOfDataCollection.sh and RunAtEndOfDataCollection.sh
#   at appropriate times.

from __future__ import annotations

import collections
import errno
import glob
import logging
import os
import pathlib
import re
import shutil
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import ispyb.sqlalchemy
import procrunner
import sqlalchemy
from workflows.transport.stomp_transport import StompTransport

import dlstbx.dc_sim.dbserverclient
import dlstbx.dc_sim.definitions
import dlstbx.dc_sim.mydb as db

log = logging.getLogger("dlstbx.dc_sim")

# Constants
MX_SCRIPTS_BINDIR = "/dls_sw/apps/mx-scripts/bin"

SimulationResult = collections.namedtuple(
    "SimulationResult",
    [
        "DCIDs",
        "JobIDs",
        "URLs",
        "beamline",
        "scenario",
        "time_end",
        "time_start",
        "type",
    ],
)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


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
    db_session,
    data_collection_group_id,
    scenario_name,
    scenario_type,
):
    _db = db.DB()
    dbsc = dlstbx.dc_sim.dbserverclient.DbserverClient()
    ispyb.sqlalchemy.enable_debug_logging()

    log.debug("Getting the source SessionID")
    src_sessionid = db.retrieve_sessionid(db_session, _src_visit)
    log.debug(f"Source SessionID is {src_sessionid}")

    try:
        run_number = int(_src_run_number)
    except (TypeError, ValueError):
        run_number = None

    row = db.retrieve_datacollection(
        db_session, src_sessionid, _src_dir, _src_prefix, run_number
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

    if scenario_type == "em-spa":
        # start copying over data files
        log.info(
            f"Copying first 5 files from {_src_dir} to {pathlib.Path(_dest_dir) / 'raw'}"
        )

        data_dirs = [f for f in pathlib.Path(_src_dir).glob("**/*") if f.is_dir()]
        data_files = [f for f in pathlib.Path(_src_dir).glob("**/*") if f.is_file()]
        for dd in data_dirs:
            mkdir_p(pathlib.Path(_dest_dir) / "raw" / dd.relative_to(_src_dir))
        for df in data_files[:5]:
            copy_via_temp_file(
                df, pathlib.Path(_dest_dir) / "raw" / df.relative_to(_src_dir)
            )

        i = ispyb.open()

        if data_collection_group_id is None:
            # Get the sessionid for the dest_visit
            log.debug("(SQL) Getting the destination sessionid")
            sessionid = db.retrieve_sessionid(db_session, _dest_visit)

            dcgparams = i.mx_acquisition.get_data_collection_group_params()
            dcgparams["parentid"] = sessionid
            dcgparams["experimenttype"] = "EM"
            dcgparams["comments"] = "Created for simulated data collection"
            datacollectiongroupid = i.mx_acquisition.upsert_data_collection_group(
                list(dcgparams.values())
            )
            dcparams = i.mx_acquisition.get_data_collection_params()
            key_maps = {
                "runStatus": "run_status",
                "imageSuffix": "imgsuffix",
                "fileTemplate": "file_template",
                "comments": "comments",
            }
            for attr, key in key_maps.items():
                dcparams[key] = getattr(row, attr)
            dcparams["parentid"] = datacollectiongroupid
            dcparams["imgdir"] = str(pathlib.Path(_dest_dir) / "raw")
            dcparams["visitid"] = sessionid
            datacollectionid = i.mx_acquisition.upsert_data_collection(
                list(dcparams.values())
            )
        else:
            datacollectiongroupid = data_collection_group_id
            datacollectionid = src_dcid

        log.debug(
            "Source dataset from DCID %r, DCGID %r",
            src_dcid,
            src_dcgid,
        )

        # nowstr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        proc_job_values = i.mx_processing.get_job_params()
        proc_job_values["datacollectionid"] = datacollectionid
        proc_job_values["display_name"] = "RELION"
        proc_job_values["comments"] = "Submitted as part of simulated data collection"
        proc_job_values["recipe"] = "relion4"
        proc_job_values["automatic"] = 0
        procjobid = i.mx_processing.upsert_job(list(proc_job_values.values()))

        for k, v in proc_params.items():
            if k != "import_images":
                job_param_values = (None, procjobid, k, v)
            else:
                job_param_values = (
                    None,
                    procjobid,
                    k,
                    _dest_dir + "/raw/Frames/*.tiff",
                )
            i.mx_processing.upsert_job_parameter(job_param_values)

        default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"
        stomp = StompTransport()
        stomp.load_configuration_file(default_configuration)
        stomp.connect()

        dispatcher_message = {"parameters": {"ispyb_process": procjobid}}

        stomp.send("processing_recipe", dispatcher_message)

        num_data_file_blocks = len(data_files) // 5
        delay = 5 * 60
        for i in range(1, num_data_file_blocks):
            log.info(
                f"Waiting {delay} seconds (until {datetime.fromtimestamp(time.time()+delay):%H:%M:%S})"
            )
            time.sleep(delay)
            log.info(
                f"Copying further 5 files from {_src_dir} to {pathlib.Path(_dest_dir) / 'raw'}"
            )
            for df in data_files[i * 5 : (i + 1) * 5]:
                log.debug(df)
                copy_via_temp_file(
                    df, pathlib.Path(_dest_dir) / "raw" / df.relative_to(_src_dir)
                )
        log.info(
            f"Copying remaining files from {_src_dir} to {pathlib.Path(_dest_dir) / 'raw'}"
        )
        for df in data_files[num_data_file_blocks * 5 :]:
            log.debug(df)
            copy_via_temp_file(
                df, pathlib.Path(_dest_dir) / "raw" / df.relative_to(_src_dir)
            )

        return datacollectionid, datacollectiongroupid, procjobid

    if scenario_type == "mx":
        if start_img_number is None:
            sys.exit("Could not find the first image number for data collection")
        no_images = row.numberOfImages
        if not no_images:
            sys.exit("Could not find the number of images for data collection")
        log.debug(f"Source dataset has {no_images} images")

        # Get the sessionid for the dest_visit
        log.debug("(SQL) Getting the destination sessionid")
        sessionid = db.retrieve_sessionid(db_session, _dest_visit)

        # Get the highest run number for the datacollections of this dest_visit with the particular img.dir and prefix
        log.debug(
            "(SQL) Getting the currently highest run number for this img. directory + prefix"
        )
        # Can't change the run number for .h5 files otherwise the link from the master.h5 to data_*.h5 will be incorrect
        if not filetemplate.endswith(".h5"):
            run_number = retrieve_max_dcnumber(_db, sessionid, _dest_dir, _dest_prefix)
            try:
                run_number = int(run_number) + 1
            except (TypeError, ValueError):
                run_number = 1

        log.debug("(SQL) Getting values from the source datacollectiongroup record")
        src_blsampleid = row.DataCollectionGroup.blSampleId

        log.debug(
            "(filesystem) Copy the xtal snapshot(s) (if any) from source to target directories"
        )
        dest_xtal_snapshot_path = ["", "", "", ""]
        for x in range(0, 4):
            if src_xtal_snapshot_path[x] is not None:
                if os.path.exists(src_xtal_snapshot_path[x]):
                    png = re.sub(
                        "^.*/(.*)$", _dest_dir + r"/\1", src_xtal_snapshot_path[x]
                    )
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

                blsample_xml = (
                    dlstbx.dc_sim.dbserverclient.populate_blsample_xml_template(bls_row)
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
            name.lower(): getattr(row, name)
            for name in dir(row)
            if not name.startswith("_")
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
            f"{_dest_prefix}_{run_number}_",
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
            log.error(
                "RunAtStartOfCollect failed with exit code %d", result["exitcode"]
            )

        if filetemplate.endswith(".cbf"):
            # Also copy images one by one from source to destination directory.
            for x in range(start_img_number, start_img_number + no_images):
                num_digits = len(filetemplate.split(".")[0].split("_")[-1])
                _src_img_number = f"%0{num_digits}d" % x
                img_number = "%04d" % x
                src_prefix = ""
                if _src_prefix is not None:
                    src_prefix = _src_prefix
                src_fname = "%s_%s_%s.cbf" % (
                    src_prefix,
                    str(_src_run_number),
                    str(_src_img_number),
                )
                dest_fname = "%s_%d_%s.cbf" % (
                    _dest_prefix,
                    run_number,
                    str(img_number),
                )
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
                        os.path.join(
                            _src_dir, filetemplate.split("_master.h5")[0] + ext
                        )
                    )
                )
            for src in files:
                dest_fname = os.path.basename(src).replace(
                    "%s_%s" % (src_prefix, str(_src_run_number)),
                    "%s_%d" % (_dest_prefix, run_number),
                )
                target = os.path.join(_dest_dir, dest_fname)
                log.info(f"(filesystem) Copy file {src} to {target}")
                copy_via_temp_file(src, target)
        else:
            raise RuntimeError("Unsupported file extension for %s" % filetemplate)

        # Populate a datacollection XML blob
        nowstr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
        nowstr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

    raise ValueError("Unknown scenario type %s" % scenario_type)


def call_sim(
    test_name,
    beamline,
    src_dir=None,
    src_prefix=None,
    src_run_num=None,
    sample_id=None,
    dest_visit=None,
    dflt_proposals=None,
    src_dcid=None,
    src_allowed_visits=None,
    is_dcg=None,
    src_dcg=None,
):
    scenario = dlstbx.dc_sim.definitions.tests.get(test_name)
    if scenario is None:
        log.info(
            f"{test_name} is not a defined test scenario - attempting to use custom data"
        )
        scenario = {"type": "mx", "is_dcg": is_dcg}

    # Create database session
    url = ispyb.sqlalchemy.url()
    engine = sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    db_session = sqlalchemy.orm.sessionmaker(bind=engine)()

    # Check for values specified twice
    if scenario.get("src_dir") and (src_dcid or src_dcg):
        for item, inp_value in [
            ("src_dcid", src_dcid),
            ("src_dcg", src_dcg),
        ]:
            if inp_value:
                log.warning(
                    f"{item}: {inp_value} provided alongside non-custom scenario {test_name} - scenario data will be used"
                )
    else:
        for ref_key, inp_value in [
            ("src_dir", src_dir),
            ("src_prefix", src_prefix),
            ("src_run_num", src_run_num),
            ("sample_id", sample_id),
        ]:
            if scenario.get(ref_key) and inp_value:
                log.warning(
                    f"{ref_key} read from scenario but also specified in command line - using scenario value"
                )
            elif src_dcg and inp_value:
                log.warning(
                    f"{ref_key} Specified at command line but dcg: {src_dcg} also provided - using dcg data"
                )
            elif src_dcid and inp_value:
                log.warning(
                    f"{ref_key} Specified at command line but dcid: {src_dcid} also provided - using dcid data"
                )
        # Get parameters from datacollection group ID if supplied
        if src_dcg is not None:
            if src_dcid is not None:
                log.warning(
                    f"dcid: {src_dcid} supplied alongside dcg: {src_dcg} - using dcg only"
                )
            log.info(f"Getting source data from dcg: {src_dcg}")
            src_dir, src_prefix, src_run_num, sample_id = db.retrieve_dcs_from_dcg(
                db_session, src_dcg
            )
        # Get parameters from datacollection ID if supplied
        elif src_dcid is not None:
            log.info(f"Getting source data from dcid: {src_dcid}")
            # Lookup database entry for dcid
            row = db.retrieve_dc_from_dcid(db_session, src_dcid)
            # Set parameters from database entry
            src_dir = row.imageDirectory
            src_prefix = [row.imagePrefix]
            src_run_num = [row.dataCollectionNumber]
            sample_id = row.BLSAMPLEID
            log.info(
                f"Source file path = {src_dir}, prefix = {src_prefix[0]}, run number = {src_run_num[0]}, sample id = {sample_id}"
            )

    # Read in values from the scenario if present, otherwise use command line values
    try:
        src_dir = Path(scenario.get("src_dir", src_dir))
    except TypeError:
        raise ValueError("src_dir source data path not specified")
    if not (src_prefix := scenario.get("src_prefix", src_prefix)):
        log.warning("src_prefix not specified")
    if not (src_run_num := scenario.get("src_run_num", src_run_num)):
        log.warning("src_run_num not specified")
    try:
        sample_id = int(scenario.get("sample_id", sample_id))
    except TypeError:
        log.warning("sample_id value not specified")
        sample_id = None
    proc_params = scenario.get("proc_params")
    time_start = time.time()
    now = datetime.now()

    # Calculate the destination directory from specified visit number
    if dest_visit is not None:
        # Initial check to ensure that specified visit is either in-house or commissioning
        proposal = dest_visit.split("-")[0]
        if beamline.startswith("i02"):
            dest_visit_dir = Path("/dls/mx/data", proposal, dest_visit)
        else:
            dest_visit_dir = Path("/dls", beamline, "data", str(now.year), dest_visit)
        assert (
            dest_visit_dir.is_dir()
        ), f"Could not find {dest_visit_dir} directory for the specified visit and beamline."
    # Else, calculate the destination directory for default proposal numbers
    else:
        # Get default proposals if a visit is not specified
        if beamline.startswith(("e", "m")):
            proposal = dflt_proposals["em"]
        else:
            proposal = dflt_proposals["mx"]
        if beamline.startswith("i02"):
            if beamline == "i02-2":
                dest_visit = f"{proposal}-1"
            elif beamline == "i02-1":
                dest_visit = f"{proposal}-2"
            dest_visit_dir = Path("/dls/mx/data", proposal, dest_visit)
        elif scenario.get("visit_num"):
            dest_visit = f"{proposal}-{scenario['visit_num']}"
            dest_visit_dir = Path("/dls", beamline, "data", str(now.year), dest_visit)
        else:
            for cm_dir in Path("/dls", beamline, "data", str(now.year)).iterdir():
                if cm_dir.name.startswith(proposal):
                    dest_visit = cm_dir.name
                    break
            else:
                raise ValueError(
                    f"Could not determine destination directory for proposal {proposal}"
                )

            # Set mandatory parameters
            dest_visit_dir = Path("/dls", beamline, "data", str(now.year), dest_visit)

    random_uuid = str(uuid.uuid4())[:8]
    dest_dir = (
        dest_visit_dir
        / "tmp"
        / f"{now:%Y-%m-%d}"
        / f"{now:%H}-{now:%M}-{now:%S}-{random_uuid}"
    )

    if not src_dir.is_dir():
        sys.exit(
            "ERROR: The src_dir parameter does not appear to contain a valid directory."
        )

    # Extract necessary info from the source directory path
    m1 = re.search(r"(/dls/(\S+?)/data/\d+/)(\S+)", str(src_dir))
    if m1:
        subdir = m1.groups()[2]
        m2 = re.search(r"^(\S+?)/", subdir)
        if m2:
            src_visit = m2.groups()[0]
        elif subdir:
            src_visit = subdir
    else:
        m1 = re.search(r"(/dls/mx/data/)(\S+)", str(src_dir))
        if m1:
            subdir = m1.groups()[1]
            src_visit = subdir.split(os.sep)[1]

    if src_visit is None:
        sys.exit(
            "ERROR: The src_dir parameter does not appear to contain a valid visit directory."
        )

    # Compare to src_allowed_visits if src_dir not from scenario
    if "src_dir" not in scenario and not src_visit.startswith(
        tuple(src_allowed_visits)
    ):
        raise ValueError(f"Supplied src_dir from forbidden visit: {src_visit}")

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
    log.debug(f"Creating directory {dest_dir}")
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        log.info(f"Directory {dest_dir} created successfully.")
    except Exception:
        sys.exit(f"ERROR: Creating directory {dest_dir} failed.")

    # Call _simulate
    dcid_list = []
    dcg_list = []
    jobid_list = []
    for src_run_number in src_run_num:
        for src_prefix_item in src_prefix:
            dest_prefix = src_prefix_item
            if scenario.get("is_dcg") and len(dcg_list):
                dcg = dcg_list[0]
            else:
                dcg = None
            dcid, dcg, jobid = _simulate(
                dest_visit,
                beamline,
                str(src_dir),
                src_visit,
                src_prefix_item,
                src_run_number,
                dest_prefix,
                str(dest_visit_dir),
                str(dest_dir),
                sample_id,
                proc_params,
                db_session,
                data_collection_group_id=dcg,
                scenario_name=test_name,
                scenario_type=scenario["type"],
            )
            jobid_list.append(jobid)
            dcid_list.append(dcid)
            dcg_list.append(dcg)
            if scenario.get("delay"):
                log.info(f"Sleeping for {scenario['delay']} seconds")
                time.sleep(scenario["delay"])

    if not dcid_list:
        return None

    return SimulationResult(
        DCIDs=dcid_list,
        JobIDs=jobid_list,
        time_start=time_start,
        time_end=time.time(),
        type=scenario["type"],
        beamline=beamline,
        scenario=test_name,
        URLs=[
            f"https://ispyb.diamond.ac.uk/dc/visit/{dest_visit}/id/{dcid}"
            for dcid in dcid_list
        ],
    )
