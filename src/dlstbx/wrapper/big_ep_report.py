import logging
import os

import py
import zocalo.wrapper
from jinja2.environment import Environment
from jinja2.exceptions import UndefinedError
from jinja2.loaders import PackageLoader

import dlstbx.util.big_ep as bpu
import dlstbx.util.fast_ep as fpu
from dlstbx.util.big_ep_helpers import (
    copy_results,
    get_autobuild_model_files,
    get_autosharp_model_files,
    get_crank2_model_files,
    ispyb_write_model_json,
    send_results_to_ispyb,
    write_coot_script,
)
from dlstbx.util.symlink import create_parent_symlink

logger = logging.getLogger("zocalo.wrap.big_ep_report")


class BigEPReportWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        working_directory.ensure(dir=True)

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
            mdl_dict = get_autosharp_model_files(working_directory, logger)
        elif pipeline == "AutoBuild":
            mdl_dict = get_autobuild_model_files(working_directory, logger)
        elif pipeline == "Crank2":
            mdl_dict = get_crank2_model_files(working_directory, logger)
        if mdl_dict is None:
            logger.warning(f"Cannot process {pipeline} results")
            return True

        ispyb_write_model_json(str(working_directory), mdl_dict, logger)
        write_coot_script(str(working_directory), mdl_dict)

        if "devel" not in params:
            skip_copy = [".launch", ".recipewrap"]
            if params.get("results_directory"):
                copy_results(
                    working_directory.strpath,
                    results_directory.strpath,
                    skip_copy,
                    logger,
                )
                if params.get("create_symlink"):
                    upstream = params["create_symlink"].replace("/", "-")
                    create_parent_symlink(
                        results_directory.strpath, f"{pipeline}-{upstream}"
                    )
                send_results_to_ispyb(
                    params.get("results_directory"),
                    params.get("log_files"),
                    self.record_result_individual_file,
                )
            else:
                logger.debug("Result directory not specified")

        logger.debug("Generating model density images")
        try:
            bpu.generate_model_snapshots(working_directory.strpath, tmpl_env, tmpl_data)
        except Exception:
            logger.debug(
                "Exception raised while generating model snapshots", exc_info=True
            )

        logger.debug("Generating plots for fast_ep summary")
        try:
            axis, data, best_vals = fpu.parse_fastep_table(fast_ep_path)
            fpu.fastep_radar_plot(tmpl_data, axis, data, best_vals)
            fpu.fastep_sites_plot(tmpl_data, axis, data["No. found"], *best_vals[1:])
        except Exception:
            logger.debug(
                "Exception raised while composing fast_ep report", exc_info=True
            )

        logger.debug("Reading PIA results from ISPyB")
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
            logger.debug("Exception raised while composing PIA report", exc_info=True)

        logger.debug("Reading crystal snapshots")
        try:
            bpu.get_image_files(tmpl_data)
        except Exception:
            logger.debug(
                "Exception raised while reading crystal snapshots", exc_info=True
            )

        logger.debug("Reading data metrics from xia2 logs")
        try:
            bpu.read_xia2_processing(tmpl_data)
        except Exception:
            logger.debug("Exception raised while composing xia2 summary", exc_info=True)

        logger.debug("Generating HTML summary")
        html_template = tmpl_env.get_template("bigep_summary.html")
        with open(working_directory.join("bigep_report.html").strpath, "w") as fp:
            try:
                summary_html = html_template.render(tmpl_data)
            except UndefinedError:
                logger.exception("Error rendering big_ep summary report")
                return False
            fp.write(summary_html)
            bpu.send_html_email_message(summary_html, pipeline, email_list, tmpl_data)

        results_directory.ensure(dir=True)
        logger.info("Copying big_ep report to %s", results_directory.strpath)
        keep_ext = {".html": "log", ".png": "log"}
        allfiles = []
        for filename in working_directory.listdir():
            filetype = keep_ext.get(filename.ext)
            if filetype is None:
                continue
            destination = results_directory.join(filename.basename)
            filename.copy(destination)
            allfiles.append(destination.strpath)
            if filename.ext == ".png":
                self.record_result_individual_file(
                    {
                        "file_path": destination.dirname,
                        "file_name": destination.basename,
                        "file_type": filetype,
                        "importance_rank": 2,
                    }
                )
        return True
