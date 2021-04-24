from jinja2.environment import Environment
from jinja2.loaders import PackageLoader
from jinja2.exceptions import UndefinedError

import logging
import os
import dlstbx.util.fast_ep as fpu
import dlstbx.util.big_ep as bpu
import py
import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.big_ep_report")


class BigEPReportWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        working_directory.ensure(dir=True)

        tmpl_env = Environment(
            loader=PackageLoader("dlstbx.util.big_ep", "big_ep_templates")
        )

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

        tmpl_data = {
            "_root_wd": working_directory.strpath,
            "pipeline": params["pipeline"],
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

        tmpl_data.update(
            {"settings": params.get("ispyb_parameters", self.recwrap.environment)}
        )

        logger.debug("Generating model density images")
        try:
            bpu.generate_model_snapshots(tmpl_env, tmpl_data)
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
            bpu.send_html_email_message(
                summary_html, params["pipeline"], email_list, tmpl_data
            )

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
