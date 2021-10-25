import json
import logging
import os
import shutil
from argparse import Namespace
from pathlib import Path
from pprint import pformat

import ispyb
import zocalo
from iotbx import mtz

from dlstbx.util.big_ep_helpers import (
    get_heavy_atom_job,
    read_data,
    read_mtz_datasets,
    spacegroup_short,
    write_sequence_file,
    write_settings_file,
)
from dlstbx.util.symlink import create_parent_symlink

logger = logging.getLogger("dlstbx.wrap.big_ep_setup")


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
            "anomalousScatterer"
        ]
    except Exception:
        logger.debug(f"Anomalous scatterer info for dcid {dcid} not available")

    if big_ep_params.get("fast_ep_data"):
        with open(big_ep_params["fast_ep_data"]) as fp:
            fast_ep_data = json.load(fp)
            msg_default["spacegroup"] = fast_ep_data["_spacegroup"][0]
            msg_default["nsites"] = fast_ep_data["nsite_real"]
    try:
        msg_default["spacegroup"] = big_ep_params["spacegroup"]
    except KeyError:
        mtz_obj = mtz.object(msg_default["hklin"])
        msg_default["spacegroup"] = (
            mtz_obj.space_group().type().lookup_symbol().replace(" ", "")
        )
    try:
        msg_default["nsites"] = big_ep_params["nsites"]
    except KeyError:
        logger.debug("Number of heavy atom sites was not specified")

    try:
        assert big_ep_params["resolution"]
        msg_default["resolution"] = float(big_ep_params["resolution"])
    except Exception:
        logger.debug(f"Resolution info for dcid {dcid} not available")
    try:
        resol = big_ep_params["resolution_limits"]
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
    msg_default["dataset_names"] = "|".join([ds["name"] for ds in datasets])

    msg_default["compound"] = "Protein"

    return msg_default


def bootstrap_pipeline(params, working_directory, logger):
    """Read command-line options and setup data processing parameters"""

    big_ep_params = get_bigep_parameters(params, working_directory, logger)

    # big_ep_params.update(
    #    {
    #        "sharp_module": params["sharp_module"],
    #        "phenix_module": params["phenix_module"],
    #        "ccp4_module": params["ccp4_module"],
    #        "crank2_bin": params["crank2_bin"],
    #    }
    # )

    msg = Namespace(**big_ep_params)
    return msg


def setup_autosharp_jobs(msg, working_directory):
    """Setup input directory to run autoSHARP."""

    # msg._wd = os.path.join(msg._wd, "autoSHARP")
    # msg._results_wd = os.path.join(msg._results_wd, "autoSHARP")
    # os.symlink(working_directory.join("autoSHARP"), msg._wd)
    # os.symlink(results_directory.join("autoSHARP"), msg._results_wd)

    write_sequence_file(working_directory, msg)

    try:
        msg.enableArpWarp = msg.resolution < 2.5
    except Exception:
        msg.enableArpWarp = False

    if hasattr(msg, "spacegroup"):
        msg.spacegroup = spacegroup_short(msg.spacegroup, logger)

    shutil.copyfile(msg.hklin, working_directory / os.path.basename(msg.hklin))

    return msg


def setup_autosol_jobs(msg, working_directory):
    """Setup working directory for running Phenix AutoSol pipeline"""

    # msg._wd = os.path.join(msg._wd, "AutoSol")
    # msg._results_wd = os.path.join(msg._results_wd, "AutoSol")
    # os.symlink(working_directory, msg._wd)
    # if not os.path.exists(msg._wd):
    #    os.makedirs(msg._wd)

    write_sequence_file(working_directory, msg)

    msg.autosol_hklin = os.path.basename(msg.hklin)

    shutil.copyfile(msg.hklin, str(working_directory / msg.autosol_hklin))


def setup_crank2_jobs(working_directory, msg):
    """Setup directory to run Crank2 pipeline"""

    # Setup pointless job
    msg.input_hkl = str(working_directory / os.path.basename(msg.hklin))
    shutil.copyfile(msg.hklin, msg.input_hkl)

    (_, filext) = os.path.split(msg.hklin)
    (filename, ext) = os.path.splitext(filext)

    msg.input_hkl = os.path.basename(msg.hklin)
    msg.hklin = "".join([filename, msg.spacegroup, ext])

    try:
        msg.enableArpWarp = msg.resolution < 2.5
    except Exception:
        msg.enableArpWarp = False

    write_sequence_file(working_directory, msg)


def write_singularity_script(working_directory, image_name):
    singularity_script = working_directory / "run_singularity.sh"
    commands = [
        "#!/bin/bash",
        f"/usr/bin/singularity exec --home ${{PWD}} --bind ${{PWD}}/TMP:/opt/xia2/tmp {image_name} $@",
    ]
    with open(singularity_script, "w") as fp:
        fp.write("\n".join(commands))


def record_big_ep_settings_in_ispyb(rpid, msg):
    big_ep_settings = {
        "atom": msg.atom,
        "dataset": msg.dataset_names,
        "spacegroup": msg.spacegroup,
        "nsites": msg.nsites,
        "compound": msg.compound,
        "sequence": msg.sequence,
    }
    ispyb_conn = ispyb.open()
    for key, value in big_ep_settings.items():
        jpp = ispyb_conn.mx_processing.get_job_parameter_params()
        jpp["job_id"] = rpid
        jpp["parameter_key"] = key
        jpp["parameter_value"] = value
        ispyb_conn.mx_processing.upsert_job_parameter(list(jpp.values()))
    logger.info(f"big_ep settings jobid {rpid}: {pformat(big_ep_settings)}")


class BigEPSetupWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        self.recwrap.environment.update(params["ispyb_parameters"])

        working_directory = Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)
        tmp_path = working_directory / "TMP"
        tmp_path.mkdir(parents=True, exist_ok=True)

        # Create working directory with symbolic link
        pipeline = self.recwrap.environment.get("pipeline")
        if params.get("create_symlink"):
            upstream = params["create_symlink"].replace("/", "-")
            create_parent_symlink(
                str(working_directory), f"{pipeline}-{upstream}", levels=1
            )
            os.symlink(params["shelxc_path"], str(working_directory / "shelxc"))
            os.symlink(params["fast_ep_path"], str(working_directory / "fast_ep"))

        try:
            msg = bootstrap_pipeline(params, str(working_directory), logger)
            msg.cluster_project = self.recwrap.recipe_step["parameters"][
                "cluster_project"
            ]
        except Exception:
            logger.exception("Error reading big_ep parameters")
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
            write_settings_file(working_directory, msg)
        except Exception:
            logger.exception("Error reading big_ep parameters")
        try:
            record_big_ep_settings_in_ispyb(params["rpid"], msg)
        except Exception:
            logger.exception("Error recording big_ep settings into ISPyB")

        try:
            if pipeline == "Crank2":
                setup_crank2_jobs(working_directory, msg)
            elif pipeline == "autoSHARP":
                setup_autosharp_jobs(msg, working_directory)
            elif pipeline == "AutoBuild":
                setup_autosol_jobs(msg, working_directory)
        except Exception:
            logger.exception(f"Error configuring {pipeline} jobs")
            return False

        singularity_image = params.get("singularity_image")
        if singularity_image:
            try:
                # shutil.copy(singularity_image, str(working_directory))
                # image_name = Path(singularity_image).name
                write_singularity_script(working_directory, singularity_image)
            except Exception:
                logger.exception("Error writing singularity script")
                return False

        logger.info("Sending message to downstream channel")
        logger.info(f"Message: {msg}")
        # self.recwrap.send_to("downstream", vars(msg))
        self.recwrap.environment["msg"] = vars(msg)

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
