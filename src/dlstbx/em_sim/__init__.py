# Functions to simulate a data collection
#
# This
# * inserts new entries into the processjob table
# * copies images from the source data collection
# * runs the scripts RunAtStartOfDataCollection.sh and RunAtEndOfDataCollection.sh
#   at appropriate times.


import datetime
import errno
import logging
import os
import re
import pathlib
import shutil
import sys
import time
import uuid
import ispyb
import ispyb.sqlalchemy
import sqlalchemy
import sqlalchemy.orm
from sqlalchemy.orm import Load
from ispyb.sqlalchemy import DataCollection, BLSession, Proposal
from workflows.transport.stomp_transport import StompTransport

import dlstbx.em_sim.definitions

log = logging.getLogger("dlstbx.em_sim")


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


def copy_via_temp_file(source, destination):
    dest_dir, dest_file = os.path.split(destination)
    temp_dest_file = ".tmp." + dest_file
    temp_destination = os.path.join(dest_dir, temp_dest_file)
    shutil.copyfile(source, temp_destination)
    os.rename(temp_destination, destination)


def clean_nan_null_minusone(s):
    return re.sub(r"\<[^<>]*\>(null|nan|-1)\</[^<>]*\>", "", s)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def retrieve_sessionid(_db, _visit):

    query = (
        _db.query(BLSession, Proposal)
        .options(
            Load(BLSession).load_only("sessionId", "visit_number", "proposalId"),
            Load(Proposal).load_only("proposalId", "proposalCode", "proposalNumber"),
        )
        .join(
            Proposal,
            Proposal.proposalId == BLSession.proposalId,
        )
        .filter(
            sqlalchemy.func.concat(
                Proposal.proposalCode,
                Proposal.proposalNumber,
                "-",
                BLSession.visit_number,
            )
            == _visit
        )
    )

    query_results = query.first()

    if query_results is None:
        sys.exit("Query to obtain sessionid failed for %s" % _visit)

    if query_results[0].sessionId is None:
        sys.exit("Could not find sessionid for visit %s" % _visit)
    return query_results[0].sessionId


def retrieve_datacollection_values(_db, _sessionid, _dir, _prefix, _run_number):

    records_to_collect = [
        "dataCollectionId",
        "dataCollectionGroupId",
        "runStatus",
        "imageSuffix",
        "fileTemplate",
        "comments",
        "printableForReport",
    ]

    query = (
        _db.query(DataCollection)
        .options(Load(DataCollection).load_only(*records_to_collect))
        .filter(DataCollection.SESSIONID == _sessionid)
        .filter(DataCollection.imageDirectory == _dir + "/")
        .filter(DataCollection.dataCollectionNumber == _run_number)
    )

    query.filter(DataCollection.imagePrefix == _prefix)

    return query.first()


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
    proc_params,
    data_collection_group_id,
    scenario_name=None,
):

    url = ispyb.sqlalchemy.url()
    engine = sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    db_session = sqlalchemy.orm.sessionmaker(bind=engine)

    log.debug("Getting the source SessionID")
    with db_session() as dbs:
        src_sessionid = retrieve_sessionid(dbs, _src_visit)
    log.debug("SessionID is %r", src_sessionid)

    with db_session() as dbs:
        row = retrieve_datacollection_values(
            dbs, src_sessionid, _src_dir, _src_prefix, _src_run_number
        )

    src_dcid = int(row.dataCollectionId)
    src_dcgid = int(row.dataCollectionGroupId)

    # create symlink to Movies data
    log.info(
        f"Created symlink between {_src_dir} and {pathlib.Path(_dest_dir) / 'raw'}"
    )
    os.symlink(_src_dir, pathlib.Path(_dest_dir) / "raw")

    data_files = list(pathlib.Path(_src_dir).glob("**/*"))
    for df in data_files[:5]:
        shutil.copyfile(df, pathlib.Path(_dest_dir) / "raw" / df.relative_to(_src_dir))

    i = ispyb.open()

    if data_collection_group_id is None:
        dcgparams = i.mx_acquisition.get_data_collection_group_params()
        dcgparams["parentid"] = src_sessionid
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
        dcparams["visitid"] = src_sessionid
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

    # nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    proc_job_values = i.mx_processing.get_job_params()
    proc_job_values["datacollectionid"] = datacollectionid
    proc_job_values["display_name"] = "RELION"
    proc_job_values["comments"] = "Submitted as part of simulated data collection"
    proc_job_values["recipe"] = "relion"
    proc_job_values["automatic"] = 0
    procjobid = i.mx_processing.upsert_job(list(proc_job_values.values()))

    for k, v in proc_params.items():
        if k != "import_images":
            job_param_values = (None, procjobid, k, v)
        else:
            job_param_values = (None, procjobid, k, _dest_dir + "/raw/Frames/*.tiff")
        i.mx_processing.upsert_job_parameter(job_param_values)

    default_configuration = "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"
    # StompTransport.load_configuration_file(default_configuration)
    stomp = StompTransport()
    stomp.load_configuration_file(default_configuration)
    stomp.connect()

    dispatcher_message = {"parameters": {"ispyb_process": procjobid}}

    stomp.send("processing_recipe", dispatcher_message)

    num_data_file_blocks = len(data_files) // 5
    for i in range(1, num_data_file_blocks):
        time.sleep(5 * 60)
        for df in data_files[i * 5 : (i + 1) * 5]:
            shutil.copyfile(
                df, pathlib.Path(_dest_dir) / "raw" / df.relative_to(_src_dir)
            )
    for df in data_files[num_data_file_blocks * 5 :]:
        shutil.copyfile(df, pathlib.Path(_dest_dir) / "raw" / df.relative_to(_src_dir))

    return datacollectionid, datacollectiongroupid, procjobid


def call_sim(test_name, beamline):
    scenario = dlstbx.em_sim.definitions.tests.get(test_name)
    if not scenario:
        sys.exit("%s is not a valid test scenario" % test_name)

    src_dir = scenario["src_dir"]
    sample_id = scenario.get("use_sample_id")
    src_prefix = scenario["src_prefix"]
    proc_params = scenario["proc_params"]

    # Calculate the destination directory
    now = datetime.datetime.now()
    # These proposal numbers need to be updated every year
    proposal = "cm28212"

    for cm_dir in os.listdir(f"/dls/{beamline}/data/{now:%Y}"):
        if cm_dir.startswith(proposal + "-" + str(scenario["src_dc_version"][0])):
            dest_visit = cm_dir
            break
    else:
        log.error("Could not determine destination directory")
        sys.exit(1)

    # Set mandatory parameters
    dest_visit_dir = f"/dls/{beamline}/data/{now:%Y}/{dest_visit}"

    dest_dir = f"{dest_visit_dir}/tmp/{now:%Y-%m-%d}/{now:%H}-{now:%M}-{now:%S}-{str(uuid.uuid4())[:8]}"

    print("destination directory:", dest_dir)

    # Extract necessary info from the source directory path
    m1 = re.search(r"(/dls/(\S+?)/data/\d+/)(\S+)", src_dir)
    if m1:
        subdir = m1.groups()[2]
        m2 = re.search(r"^(\S+?)/", subdir)
        if m2:
            src_visit = m2.groups()[0]
        elif subdir:
            src_visit = subdir

    if src_visit is None:
        sys.exit(
            "ERROR: The src_dir parameter does not appear to contain a valid visit directory."
        )

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
    pjid_list = []
    for src_run_number in scenario["src_run_num"]:
        for src_prefix in scenario["src_prefix"]:
            dest_prefix = src_prefix
            if scenario.get("dcg") and len(dcg_list):
                dcg = dcg_list[0]
            else:
                dcg = None
            dcid, dcg, pjid = simulate(
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
            pjid_list.append(pjid)
            dcid_list.append(dcid)
            dcg_list.append(dcg)
            if scenario.get("delay"):
                log.info("Sleeping for %s seconds" % scenario["delay"])
                time.sleep(scenario["delay"])
    return dcid_list, pjid_list
