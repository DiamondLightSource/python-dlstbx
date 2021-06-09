# Functions to simulate an EM data collection and Relion processing
#
# This
# * inserts new entries into the processjob table
# * copies images from the source data collection
# * sends the message that SynchWeb would send to launch processing to Zocalo


import errno
import logging
import os
import pathlib
import shutil
import time

import ispyb
import ispyb.sqlalchemy
import sqlalchemy
import sqlalchemy.orm
from workflows.transport.stomp_transport import StompTransport

import dlstbx.dc_sim.mydb

log = logging.getLogger("dlstbx.em_sim")


def copy_via_temp_file(source, destination):
    dest_dir, dest_file = os.path.split(destination)
    temp_dest_file = ".tmp." + dest_file
    temp_destination = os.path.join(dest_dir, temp_dest_file)
    shutil.copyfile(source, temp_destination)
    os.rename(temp_destination, destination)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


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
        src_sessionid = dlstbx.dc_sim.mydb.retrieve_sessionid(dbs, _src_visit)
        log.debug("SessionID is %r", src_sessionid)

        row = dlstbx.dc_sim.mydb.retrieve_datacollection(
            dbs, src_sessionid, _src_dir, _src_prefix, _src_run_number
        )

    src_dcid = int(row.dataCollectionId)
    src_dcgid = int(row.dataCollectionGroupId)

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
    stomp = StompTransport()
    stomp.load_configuration_file(default_configuration)
    stomp.connect()

    dispatcher_message = {"parameters": {"ispyb_process": procjobid}}

    stomp.send("processing_recipe", dispatcher_message)

    num_data_file_blocks = len(data_files) // 5
    for i in range(1, num_data_file_blocks):
        log.info(
            f"Waiting and then copying another 5 files from {_src_dir} to {pathlib.Path(_dest_dir) / 'raw'}"
        )
        time.sleep(5 * 60)
        for df in data_files[i * 5 : (i + 1) * 5]:
            copy_via_temp_file(
                df, pathlib.Path(_dest_dir) / "raw" / df.relative_to(_src_dir)
            )
    log.info(
        f"Copying remaining files from {_src_dir} to {pathlib.Path(_dest_dir) / 'raw'}"
    )
    for df in data_files[num_data_file_blocks * 5 :]:
        copy_via_temp_file(
            df, pathlib.Path(_dest_dir) / "raw" / df.relative_to(_src_dir)
        )

    return datacollectionid, datacollectiongroupid, procjobid
