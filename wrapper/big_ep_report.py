from __future__ import absolute_import, division, print_function

from jinja2.environment import Environment
from jinja2.loaders import PackageLoader

import logging
import os
import dlstbx.util.fast_ep
import dlstbx.util.big_ep
import py
import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.big_ep_report")


class BigEPReportWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = py.path.local(params["working_directory"])

        working_directory.ensure(dir=True)

        tmpl_env = Environment(
            loader=PackageLoader("dlstbx.util.big_ep", "big_ep_templates")
        )

        dcid = params["dcid"]
        fast_ep_path = params["fast_ep_path"]
        email_list = params["email_list"]

        from dlstbx.ispybtbx import ispybtbx

        ispyb_conn = ispybtbx()

        xia2_log_files = [
            os.path.join(row["filePath"], row["fileName"])
            for row in self.recwrap.environment["ispyb_program_attachments"]
        ]

        tmpl_data = {
            "_root_wd": working_directory.strpath,
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

        logger.debug("Generating model density images")
        try:
            dlstbx.util.big_ep.generate_model_snapshots(tmpl_env, tmpl_data)
        except Exception:
            logger.debug(
                "Exception raised while generating model snapshots", exc_info=True
            )
        logger.debug("Generating plots for fast_ep summary")
        try:
            axis, data, best_vals = dlstbx.util.fast_ep.parse_fastep_table(fast_ep_path)
            dlstbx.util.fast_ep.fastep_radar_plot(tmpl_data, axis, data, best_vals)
            dlstbx.util.fast_ep.fastep_sites_plot(
                tmpl_data, axis, data["No. found"], *best_vals[1:]
            )
        except Exception:
            logger.debug(
                "Exception raised while composing fast_ep report", exc_info=True
            )

        logger.debug("Reading big_ep setting file")
        try:
            dlstbx.util.big_ep.read_settings_file(tmpl_data)
        except Exception:
            logger.debug(
                "Exception raised while reading big_ep settings file", exc_info=True
            )

        logger.debug("Reading PIA results from ISPyB")
        try:
            pia_results = ispyb_conn.get_pia_results(
                [dcid],
                [
                    "imagenumber AS nim",
                    "method2res AS res",
                    "spottotal AS spots",
                    "goodbraggcandidates AS good",
                ],
            )[1]
            dlstbx.util.big_ep.get_pia_plot(tmpl_data, pia_results)
        except Exception:
            logger.debug("Exception raised while composing PIA report", exc_info=True)

        logger.debug("Reading crystal snapshots")
        try:
            dlstbx.util.big_ep.get_image_files(tmpl_data)
        except Exception:
            logger.debug(
                "Exception raised while reading crystal snapshots", exc_info=True
            )

        logger.debug("Reading data metrics from xia2 logs")
        try:
            dlstbx.util.big_ep.read_xia2_processing(tmpl_data)
        except Exception:
            logger.debug("Exception raised while composing xia2 summary", exc_info=True)

        logger.debug("Generating HTML summary")
        html_template = tmpl_env.get_template("bigep_summary.html")
        with open(
            working_directory.join("bigep_summary_email.html").strpath, "w"
        ) as fp:
            summary_html = html_template.render(tmpl_data)
            fp.write(summary_html)
            dlstbx.util.big_ep.send_html_email_message(
                summary_html, email_list, tmpl_data
            )
        return True
