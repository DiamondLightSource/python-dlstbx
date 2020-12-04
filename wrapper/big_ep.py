import os
import logging
import zocalo

from datetime import datetime
import py
import json
from argparse import Namespace
import getpass
from pprint import pformat
from dlstbx.util.big_ep_helpers import (
    write_settings_file,
    read_data,
    read_mtz_datasets,
    get_heavy_atom_job,
    copy_results,
)


logger = logging.getLogger("dlstbx.wrap.big_ep")


def get_bigep_parameters(big_ep_params, working_directory, logger):

    dcid = big_ep_params["dcid"]
    msg_default = {}
    if big_ep_params.get("ispyb_parameters"):
        abs_hklin_path = os.path.abspath(big_ep_params["ispyb_parameters"]["data"])
    else:
        abs_hklin_path = os.path.abspath(big_ep_params["data"])
    msg_default.update({"hklin": abs_hklin_path})

    try:
        msg_default["sequence"] = big_ep_params["protein_info"]["sequence"]
    except Exception:
        logger.debug(f"Cannot read protein sequence information for dcid {dcid}")
        msg_default["sequence"] = None

    try:
        msg_default["atom"] = big_ep_params["diffraction_plan_info"][
            "anomalousscatterer"
        ]
    except Exception:
        logger.debug(f"Anomalous scatterer info for dcid {dcid} not available")

    with open(big_ep_params["fast_ep_data"]) as fp:
        fast_ep_data = json.load(fp)
        msg_default["spacegroup"] = fast_ep_data["_spacegroup"][0]
        msg_default["nsites"] = fast_ep_data["nsite_real"]

    try:
        resol = big_ep_params["resol"]
        if resol.get("high"):
            msg_default.update({"resol_high": resol["high"]})
        if resol.get("low"):
            msg_default.update({"resol_low": resol["low"]})
    except KeyError:
        logger.debug("Resolution cut-off values not set")

    datasets = []
    if not big_ep_params.get("energy_scan_info"):
        datasets = [{"name": "peak"}]
    else:
        edge_data = big_ep_params["energy_scan_info"]
        if "fp" in edge_data and "fpp" in edge_data:
            tmp_dict = {
                "name": edge_data["edge_position"],
                "fp": edge_data["fp"],
                "fpp": edge_data["fpp"],
            }
        else:
            tmp_dict = {"name": edge_data["edge_position"]}
        datasets.append(tmp_dict)
    msg_default["datasets"] = datasets

    return msg_default


def bootstrap_pipeline(params, working_directory, results_directory, logger):
    """Read command-line options and setup data processing parameters"""

    big_ep_params = get_bigep_parameters(params, working_directory, logger)

    big_ep_params.update(
        {
            "_root_wd": working_directory,
            "_wd": working_directory,
            "_results_wd": results_directory,
            "user": getpass.getuser(),
            "sharp_module": params["sharp_module"],
            "phenix_module": params["phenix_module"],
            "ccp4_module": params["ccp4_module"],
            "crank2_bin": params["crank2_bin"],
        }
    )

    msg = Namespace(**big_ep_params)
    return msg


def setup_big_ep_jobs(msg):
    """Create a directory to run big_ep"""

    msg._root_wd = os.path.join(msg._root_wd, f"big_ep_{msg.datetime_stamp}")

    msg._wd = msg._root_wd
    if not os.path.exists(msg._wd):
        os.makedirs(msg._wd)

    msg.synchweb_ticks = os.path.join(
        msg._results_wd, f"big_ep_{msg.datetime_stamp}.log"
    )
    msg._results_wd = os.path.join(msg._results_wd, f"big_ep_{msg.datetime_stamp}")
    return msg


class BigEPWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        if params.get("ispyb_parameters"):
            self.recwrap.environment.update(params["ispyb_parameters"])

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])
        ispyb_working_directory = py.path.local(params["ispyb_working_directory"])
        ispyb_results_directory = py.path.local(params["ispyb_results_directory"])

        # Create working directory with symbolic link
        dt = datetime.now()
        dt_stamp = dt.strftime("%Y%m%d_%H%M%S")
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            big_ep_path = ispyb_working_directory.join("..", "big_ep")
            big_ep_path.ensure(dir=True)
            try:
                symlink_path = big_ep_path.join(dt_stamp)
                symlink_path.mksymlinkto(ispyb_working_directory.join("big_ep"))
            except py.error.EEXIST:
                logger.warning("Symlink %s already exists", symlink_path.strpath)

        # Create big_ep directory to update status in Synchweb
        if "devel" not in params:
            ispyb_results_directory.ensure(dir=True)
            big_ep_path = ispyb_results_directory.join("..", "big_ep")
            big_ep_path.ensure(dir=True)
            if params.get("create_symlink"):
                symlink_path = big_ep_path.join(dt_stamp)
                try:
                    symlink_path.mksymlinkto(ispyb_results_directory.join("big_ep"))
                except py.error.EEXIST:
                    logger.warning("Symlink %s already exists", symlink_path.strpath)
        try:
            msg = bootstrap_pipeline(
                params, working_directory.strpath, results_directory.strpath, logger
            )
            msg.datetime_stamp = dt_stamp
            msg.cluster_project = self.recwrap.recipe_step["parameters"][
                "cluster_project"
            ]
        except Exception:
            logger.exception("Error reading big_ep parameters")
            return False
        try:
            setup_big_ep_jobs(msg)
        except Exception:
            logger.exception("Error configuring big_ep jobs")
            return False
        try:
            read_data(msg)
        except Exception:
            logger.exception("Error reading big_ep input .mtz file")
            return False
        try:
            get_heavy_atom_job(msg)
        except Exception:
            logger.exception("Error reading big_ep parameters")
            return False
        try:
            read_mtz_datasets(msg, logger)
        except Exception:
            logger.exception("Error reading big_ep parameters")
            return False
        try:
            write_settings_file(msg)
        except Exception:
            logger.exception("Error reading big_ep parameters")
            return False

        logger.info("Sending message to downstream channel")
        self.recwrap.send_to("downstream", vars(msg))

        if params.get("results_directory"):
            copy_results(msg._wd, msg._results_wd, logger)
            js_settings = os.path.join(msg._results_wd, "big_ep_settings.json")
            self.record_result_individual_file(
                {
                    "file_path": os.path.dirname(js_settings),
                    "file_name": os.path.basename(js_settings),
                    "file_type": "result",
                }
            )
        else:
            logger.debug("Result directory not specified")

        email_message = pformat(
            {
                "payload": vars(msg),
                "ispyb_reprocessing_parameters": params["ispyb_parameters"],
                "diffraction_plan_info": params["diffraction_plan_info"],
                "protein_info": params["protein_info"],
                "energy_scan_info": params["energy_scan_info"],
            }
        )
        self.recwrap.send_to("email", email_message)

        return True
