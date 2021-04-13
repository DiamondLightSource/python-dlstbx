# Functions to simulate a data collection
#
# This
# * inserts new entries into the datacollection table using the DbserverClient.py script
# * copies images from the source data collection
# * runs the scripts RunAtStartOfDataCollection.sh and RunAtEndOfDataCollection.sh
#   at appropriate times.


import datetime
import errno
import logging
import os
import re
import procrunner
import shutil
import sys
import time
import uuid
import ispyb.sqlalchemy
import sqlalchemy.orm
from sqlalchemy.orm import Load
import sqlalchemy.func
from ispyb.sqlalchemy import DataCollection, BLSession, Proposal

import dlstbx.dc_sim.dbserverclient
import dlstbx.em_sim.definitions
import dlstbx.dc_sim.mydb

log = logging.getLogger("dlstbx.em_sim")

# Constants
EM_SCRIPTS_DIR = "/dls_sw/apps/EM/sim-scripts"
DBSERVER_HOST = "sci-serv3"
DBSERVER_PORT = "2611"


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


def populate_dcg_xml_template(_sessionid, _blsample_id):
    nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    blsample_id_elem = ""
    if _blsample_id is not None:
        blsample_id_elem = "<blSampleId>%d</blSampleId>\n" % _blsample_id

    temp = dcg_temp_xml_format.format(
        sessionid=_sessionid,
        blsample_xml=blsample_id_elem,
        comments="Simulated datacollection.",
        experimenttype=None,
        starttime=nowstr,
        crystalclass=None,
        detectormode=None,
    )

    # remove lines with null, nan and -1 values:
    temp = clean_nan_null_minusone(temp)
    return temp


def populate_dc_xml_template(
    _row,
    _sessionid,
    _dcg_id,
    _dir,
    _prefix,
    _run_number,
    _blsample_id,
    scenario_name=None,
):
    nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    suffix = _row["imagesuffix"]

    file_template = "Frames/*.tiff"

    blsample_id_elem = ""

    temp = dc_temp_xml % (
        _sessionid,
        _dcg_id,
        blsample_id_elem,
        _run_number,
        nowstr,
        s(_row["runstatus"]),
        _dir,
        _prefix,
        suffix,
        file_template,
        i(_row["printableforreport"]),
    )
    temp = temp.format(
        comments=f"Simulated datacollection ({scenario_name})."
        if scenario_name
        else "Simulated datacollection."
    )

    # remove lines with null, nan and -1 values:
    temp = clean_nan_null_minusone(temp)
    return temp


dcg_temp_xml_format = (
    '<?xml version="1.0" encoding="ISO-8859-1"?>'
    "<DataCollectionGroup>"
    "<sessionId>{sessionid}</sessionId>"
    "{blsample_xml}"
    "<experimentType>{experimenttype}</experimentType>"
    "<startTime>{starttime}</startTime>"
    "<crystalClass>{crystalclass}</crystalClass>"
    "<detectorMode>{detectormode}</detectorMode>"
    "<comments>{comments}</comments>"
    "</DataCollectionGroup>"
)

dc_temp_xml = (
    '<?xml version="1.0" encoding="ISO-8859-1"?>'
    "<DataCollection>"
    "<sessionId>%d</sessionId>"
    "<dataCollectionGroupId>%d</dataCollectionGroupId>"
    "%s<dataCollectionNumber>%d</dataCollectionNumber>"
    "<startTime>%s</startTime>"
    "<runStatus>%s</runStatus>"
    "<imageDirectory>%s</imageDirectory>"
    "<imagePrefix>%s</imagePrefix>"
    "<imageSuffix>%s</imageSuffix>"
    "<fileTemplate>%s</fileTemplate>"
    "<comments>{comments}</comments>"
    "<printableForReport>%d</printableForReport>"
    "</DataCollection>"
)

dc_endtime_temp_xml = (
    '<?xml version="1.0" encoding="ISO-8859-1"?>'
    "<DataCollection>"
    "<dataCollectionId>%d</dataCollectionId>"
    "<endTime>%s</endTime>"
    "</DataCollection>"
)

dcg_endtime_temp_xml = (
    '<?xml version="1.0" encoding="ISO-8859-1"?>'
    "<DataCollectionGroup>"
    "<dataCollectionGroupId>%d</dataCollectionGroupId>"
    "<endTime>%s</endTime>"
    "</DataCollectionGroup>"
)


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
            Propsal,
            Propsal.proposalId == BLSession.proposalId,
        )
        .filter(
            sqlalchemy.func.concat(
                Proposal.proposalCode, Proposal.proposalNumber, BLSession.visit_number
            )
            == _visit
        )
    )

    query_results = query.first()

    if query_results.sessionId is None:
        sys.exit("Could not find sessionid for visit %s" % _visit)
    return query_results.sessionId


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

    if _prefix is None:
        query.filter(DataCollection.imagePrefix == None)
    else:
        query.filter(DataCollection.imagePrefix == _prefix)

    query_results = query.all()
    required_lines = []
    for q in query_results:
        required_lines.append([q.getattr(r) for r in records_to_collect])

    desc = [d.lower() for d in records_to_collect]
    result = [dict(zip(desc, line)) for line in required_lines]

    if not result[0].get("datacollectionid"):
        sys.exit("Could not find the datacollectionid for visit %s" % _dir)
    if not result[0].get("startimagenumber"):
        sys.exit("Could not find the startimagenumber for the row")
    return result[0]


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

    url = ispyb.sqlalchemy.url("/dls_sw/dasc/mariadb/credentials/ispyb_scripts.cfg")
    engine = sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    db_session = sqlalchemy.orm.Session(bind=engine)

    _db = dlstbx.dc_sim.mydb.DB()
    dbsc = dlstbx.dc_sim.dbserverclient.DbserverClient(DBSERVER_HOST, DBSERVER_PORT)

    log.debug("Getting the source SessionID")
    src_sessionid = retrieve_sessionid(_db, _src_visit)
    log.debug("SessionID is %r", src_sessionid)

    row = retrieve_datacollection_values(
        db_session, src_sessionid, _src_dir, _src_prefix, _src_run_number
    )

    src_dcid = int(row["datacollectionid"])
    src_dcgid = int(row["datacollectiongroupid"])

    log.debug(
        "Source dataset from DCID %r, DCGID %r",
        src_dcid,
        src_dcgid,
    )

    # Get the sessionid for the dest_visit
    log.debug("(SQL) Getting the destination sessionid")
    sessionid = retrieve_sessionid(_db, _dest_visit)

    run_number = _src_run_number

    if data_collection_group_id is None:
        # Produce a DataCollectionGroup xml blob from the template
        dcg_xml = populate_dcg_xml_template(sessionid, None)

        # Ingest the DataCollectionGroup xml data using the DbserverClient
        log.debug("(dbserver) Ingest the datacollectiongroup XML")
        datacollectiongroupid = dbsc.storeDataCollectionGroup(dcg_xml)
    else:
        datacollectiongroupid = data_collection_group_id

    blsample_id = None

    # Produce a DataCollection xml blob from the template and use the new run number
    dc_xml = populate_dc_xml_template(
        row,
        sessionid,
        datacollectiongroupid,
        _dest_dir + "/",
        _dest_prefix,
        run_number,
        blsample_id,
        scenario_name=scenario_name,
    )

    # Ingest the DataCollection xml blob data using the DbserverClient
    log.debug("(dbserver) Ingest the datacollection XML")
    datacollectionid = dbsc.storeDataCollection(dc_xml)

    run_at_params = [str(datacollectionid)]

    command = [f"{EM_SCRIPTS_DIR}/RunAtStartOfCollect-{_beamline}.sh"]
    command.extend(run_at_params)
    log.info("command: %s", " ".join(command))
    result = procrunner.run(command, timeout=180)
    log.info("runtime: %s", result["runtime"])
    if result["exitcode"] or result["timeout"]:
        log.info("timeout: %s", result["timeout"])
        log.debug(result["stdout"])
        log.debug(result["stderr"])
        log.error("RunAtStartOfCollect failed with exit code %d", result["exitcode"])

    # Populate a datacollection XML blob
    nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    dc_xml = dc_endtime_temp_xml % (datacollectionid, nowstr)
    print(dc_xml)
    log.debug(
        "(dbserver) Ingest the datacollection XML to update with the d.c. end time"
    )
    dbsc.updateDbObject(dc_xml)

    # Populate a datacollectiongroup XML blob
    nowstr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    dcg_xml = dcg_endtime_temp_xml % (datacollectiongroupid, nowstr)
    print(dcg_xml)

    # Ingest the DataCollectionGroup xml blob using the DbserverClient
    log.debug(
        "(dbserver) Ingest the datacollectiongroup XML to update with the d.c.g. end time"
    )
    dbsc.updateDbObject(dcg_xml)

    command = [f"{EM_SCRIPTS_DIR}/RunAtEndOfCollect-{_beamline}.sh"]
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
    proposal = "cm28212-1"

    for cm_dir in os.listdir(f"/dls/{beamline}/data/{now:%Y}"):
        if cm_dir.startswith(proposal):
            dest_visit = cm_dir
            break
    else:
        log.error("Could not determine destination directory")
        sys.exit(1)

    # Set mandatory parameters
    dest_visit_dir = f"/dls/{beamline}/data/{now:%Y}/{dest_visit}"

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

    if src_visit is None:
        sys.exit(
            "ERROR: The src_dir parameter does not appear to contain a valid visit directory."
        )

    start_script = f"{EM_SCRIPTS_DIR}/RunAtStartOfCollect-{beamline}.sh"
    if not os.path.exists(start_script):
        log.error("The file %s was not found.", start_script)
        sys.exit(1)
    end_script = f"{EM_SCRIPTS_DIR}/RunAtEndOfCollect-{beamline}.sh"
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
            if scenario.get("delay"):
                log.info("Sleeping for %s seconds" % scenario["delay"])
                time.sleep(scenario["delay"])
    return dcid_list
