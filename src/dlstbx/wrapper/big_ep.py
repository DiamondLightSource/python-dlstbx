from __future__ import annotations

import json
import os
import shutil
from argparse import Namespace
from pathlib import Path
from pprint import pformat

import ispyb
import procrunner
from iotbx import mtz
from jinja2.environment import Environment
from jinja2.exceptions import UndefinedError
from jinja2.loaders import PackageLoader

import dlstbx.util.big_ep as bpu
import dlstbx.util.fast_ep as fpu
from dlstbx.util.big_ep_helpers import (
    get_autobuild_model_files,
    get_autosharp_model_files,
    get_crank2_model_files,
    get_heavy_atom_job,
    ispyb_write_model_json,
    read_data,
    read_mtz_datasets,
    send_results_to_ispyb,
    spacegroup_short,
    write_coot_script,
    write_sequence_file,
    write_settings_file,
)
from dlstbx.util.iris import write_singularity_script
from dlstbx.util.symlink import create_parent_symlink
from dlstbx.wrapper import Wrapper
from dlstbx.wrapper.helpers import copy_results


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
    msg = Namespace(**big_ep_params)
    return msg


def setup_autosharp_jobs(msg, working_directory, logger):
    """Setup input directory to run autoSHARP."""
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


def record_big_ep_settings_in_ispyb(rpid, msg, logger):
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


class BigEPWrapper(Wrapper):

    _logger_name = "zocalo.wrap.big_ep"

    def setup(self, working_directory, params):

        # Create working directory with symbolic link
        pipeline = self.recwrap.environment.get("pipeline")
        if params.get("create_symlink"):
            upstream = params["create_symlink"].replace("/", "-")
            create_parent_symlink(working_directory, f"{pipeline}-{upstream}", levels=1)
            os.symlink(params["shelxc_path"], str(working_directory / "shelxc"))
            os.symlink(params["fast_ep_path"], str(working_directory / "fast_ep"))

        try:
            msg = bootstrap_pipeline(params, str(working_directory), self.log)
            msg.cluster_project = self.recwrap.recipe_step["wrapper"][
                "task_information"
            ]
        except Exception:
            self.log.exception("Error reading big_ep parameters")
            return False
        try:
            read_data(msg)
        except Exception:
            self.log.exception("Error reading big_ep input .mtz file")
            return False
        try:
            get_heavy_atom_job(msg)
        except Exception:
            self.log.exception("Error reading big_ep parameters")
            return False
        try:
            read_mtz_datasets(msg, self.log)
        except Exception:
            self.log.exception("Error reading big_ep parameters")
            return False
        try:
            record_big_ep_settings_in_ispyb(params["rpid"], msg, self.log)
        except Exception:
            self.log.exception("Error recording big_ep settings into ISPyB")

        try:
            if pipeline == "Crank2":
                setup_crank2_jobs(working_directory, msg)
            elif pipeline == "autoSHARP":
                setup_autosharp_jobs(msg, working_directory, self.log)
            elif pipeline == "AutoBuild":
                setup_autosol_jobs(msg, working_directory)
        except Exception:
            self.log.exception(f"Error configuring {pipeline} jobs")
            return False

        singularity_image = params.get("singularity_image")
        if singularity_image:
            try:
                tmp_path = working_directory / "TMP"
                tmp_path.mkdir(parents=True, exist_ok=True)
                # shutil.copy(singularity_image, str(working_directory))
                # image_name = Path(singularity_image).name
                write_singularity_script(
                    working_directory, singularity_image, tmp_path.name
                )
                self.recwrap.environment.update(
                    {"singularity_image": singularity_image}
                )
            except Exception:
                self.log.exception("Error writing singularity script")
                return False

        self.log.info("Sending message to downstream channel")
        self.log.info(f"Message: {msg}")
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

    def run_big_ep(self, working_directory, params):

        # Collect parameters from payload and check them
        self.msg = Namespace(**self.recwrap.environment["msg"])

        pipeline = self.recwrap.environment.get("pipeline")

        output_directory = working_directory / pipeline
        output_directory.mkdir(parents=True, exist_ok=True)

        input_mtz = Path(params["ispyb_parameters"]["data"]).name
        shutil.move(working_directory / input_mtz, output_directory)
        self.msg.wd = str(output_directory)

        tmpl_env = Environment(loader=PackageLoader("dlstbx.util", "big_ep_templates"))
        pipeline_template = tmpl_env.get_template(f"{pipeline}.sh")
        pipeline_script = output_directory / f"run_{pipeline}.sh"

        self.msg.singularity_image = params.get("singularity_image")

        try:
            write_sequence_file(output_directory, self.msg)
        except Exception:
            self.log.exception("Error writing sequence file")
        try:
            write_settings_file(output_directory, self.msg)
        except Exception:
            self.log.exception("Error reading big_ep parameters")

        self.log.info(f"Message object: {pformat(self.msg)}")
        self.log.info(f"Parameters: {params}")
        with open(pipeline_script, "w") as fp:
            try:
                pipeline_input = pipeline_template.render(self.msg.__dict__)
            except UndefinedError:
                self.log.exception(f"Error rendering {pipeline} script template")
                return False
            fp.write(pipeline_input)

        result = procrunner.run(
            ["sh", pipeline_script],
            timeout=params.get("timeout"),
            working_directory=output_directory,
        )
        self.log.info("command: %s", " ".join(result["command"]))
        self.log.info("runtime: %s", result["runtime"])

        # Just log exit state of the program and try to read any
        # intermediate models in case of failure/timeout
        success = not result["exitcode"] and not result["timeout"]
        if success:
            self.log.info(
                f"{pipeline} successful, took %.1f seconds", result["runtime"]
            )
        else:
            self.log.info(
                f"{pipeline} failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            self.log.debug(result["stdout"])
            self.log.debug(result["stderr"])

        # HTCondor resolves symlinks while transferring data and doesn't support symlinks to directories
        if self.msg.singularity_image:
            for tmp_file in output_directory.rglob("*"):
                if (
                    tmp_file.is_symlink() and tmp_file.is_dir()
                ) or tmp_file.suffix == ".h5":
                    tmp_file.unlink(True)

        return True

    def report(self, working_directory, params):
        results_directory = Path(params["results_directory"])
        results_directory.mkdir(parents=True, exist_ok=True)

        tmpl_env = Environment(loader=PackageLoader("dlstbx.util", "big_ep_templates"))

        dcid = params["dcid"]
        fast_ep_path = params["fast_ep_path"]
        email_list = params["email_list"]

        xia2_log_files = [
            os.path.join(row["filePath"], row["fileName"])
            for row in self.recwrap.environment["ispyb_program_attachments"]
        ]

        if "data" not in params:
            params["data"] = params.get(
                "ispyb_parameters", self.recwrap.environment
            ).get("data", "N/A")
        try:
            pipeline = params["pipeline"]
        except KeyError:
            pipeline = params.get("ispyb_parameters", self.recwrap.environment).get(
                "pipeline", "N/A"
            )

        tmpl_data = {
            "pipeline": pipeline,
            "big_ep_path": params["big_ep_path"],
            "dcid": dcid,
            "visit": params["visit"],
            "proposal": self.recwrap.environment["proposal_title"],
            "mtz": params["data"],
            "image_template": params["image_template"],
            "image_directory": params["image_directory"],
            "xia2_logs": xia2_log_files,
            "html_images": {},
        }

        tmpl_data.update({"settings": self.recwrap.environment["msg"]})

        if pipeline == "autoSHARP":
            working_directory = working_directory / "autoSHARP"
            mdl_dict = get_autosharp_model_files(working_directory, self.log)
        elif pipeline == "AutoBuild":
            mdl_dict = get_autobuild_model_files(working_directory, self.log)
        elif pipeline == "Crank2":
            mdl_dict = get_crank2_model_files(working_directory, self.log)
        else:
            self.log.error(f"Big_EP was run with an unknown {pipeline = }.")
            return False
        if mdl_dict is None:
            self.log.info(f"Cannot process {pipeline} results.")
            return False

        ispyb_write_model_json(str(working_directory), mdl_dict, self.log)
        write_coot_script(str(working_directory), mdl_dict)

        if "devel" not in params:
            skip_copy = [".launch", ".recipewrap"]
            if params.get("results_directory"):
                copy_results(
                    str(working_directory),
                    str(results_directory),
                    skip_copy,
                    self.log,
                )
                if params.get("create_symlink"):
                    upstream = params["create_symlink"].replace("/", "-")
                    create_parent_symlink(results_directory, f"{pipeline}-{upstream}")
                send_results_to_ispyb(
                    params.get("results_directory"),
                    params.get("log_files"),
                    self.record_result_individual_file,
                )
            else:
                self.log.debug("Result directory not specified")

        self.log.debug("Generating model density images")
        try:
            bpu.generate_model_snapshots(str(working_directory), tmpl_env, tmpl_data)
        except Exception:
            self.log.debug(
                "Exception raised while generating model snapshots", exc_info=True
            )

        self.log.debug("Generating plots for fast_ep summary")
        try:
            axis, data, best_vals = fpu.parse_fastep_table(fast_ep_path)
            fpu.fastep_radar_plot(tmpl_data, axis, data, best_vals)
            fpu.fastep_sites_plot(tmpl_data, axis, data["No. found"], *best_vals[1:])
        except Exception:
            self.log.debug(
                "Exception raised while composing fast_ep report", exc_info=True
            )

        self.log.debug("Reading PIA results from ISPyB")
        try:
            import ispyb
            import ispyb.model.__future__

            ispyb.model.__future__.enable(
                "/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg"
            )

            with ispyb.open(
                "/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg"
            ) as ispyb_conn:
                dc = ispyb_conn.get_data_collection(dcid)
                image_quality = list(dc.image_quality)
                image_number = [iqi.image_number for iqi in image_quality]
                resolution = [iqi.resolution_method_2 for iqi in image_quality]
                spot_count = [iqi.spot_count for iqi in image_quality]
                bragg_candidates = [iqi.bragg_candidates for iqi in image_quality]

            bpu.get_pia_plot(
                tmpl_data, image_number, resolution, spot_count, bragg_candidates
            )
        except Exception:
            self.log.debug("Exception raised while composing PIA report", exc_info=True)

        self.log.debug("Reading crystal snapshots")
        try:
            bpu.get_image_files(tmpl_data)
        except Exception:
            self.log.debug(
                "Exception raised while reading crystal snapshots", exc_info=True
            )

        self.log.debug("Reading data metrics from xia2 logs")
        try:
            bpu.read_xia2_processing(tmpl_data)
        except Exception:
            self.log.debug(
                "Exception raised while composing xia2 summary", exc_info=True
            )

        self.log.debug("Generating HTML summary")
        html_template = tmpl_env.get_template("bigep_summary.html")
        with open(working_directory / "bigep_report.html", "w") as fp:
            try:
                summary_html = html_template.render(tmpl_data)
            except UndefinedError:
                self.log.exception("Error rendering big_ep summary report")
                return False
            fp.write(summary_html)
            bpu.send_html_email_message(summary_html, pipeline, email_list, tmpl_data)

        # Create results directory and symlink if they don't already exist
        self.log.info(f"Copying big_ep report to {str(results_directory)}")
        keep_ext = {".html": "log", ".png": "log"}
        for filename in working_directory.iterdir():
            filetype = keep_ext.get(filename.suffix)
            if filetype is None:
                continue
            destination = results_directory / filename.basename
            shutil.copy(filename, destination)
            if filename.suffix == ".png":
                self.record_result_individual_file(
                    {
                        "file_path": str(destination.parent),
                        "file_name": destination.name,
                        "file_type": filetype,
                        "importance_rank": 2,
                    }
                )
        return True

    def run(self):

        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]
        self.recwrap.environment.update(params["ispyb_parameters"])

        # Create working directory with symbolic link
        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)

        stage = params.get("stage")
        assert stage in {None, "setup", "run", "report"}
        success = True

        if stage in {None, "setup"}:
            success = self.setup(working_directory, params)

        if stage in {None, "run"} and success:
            success = self.run_big_ep(working_directory, params)

        if stage in {None, "report"} and success:
            success = self.report(working_directory, params)

        return success
