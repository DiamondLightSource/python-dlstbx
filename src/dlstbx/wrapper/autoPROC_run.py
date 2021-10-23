import logging
import os
from pathlib import Path

import procrunner
import requests
import zocalo.wrapper
from dxtbx.model.experiment_list import ExperimentListFactory
from dxtbx.serialize import xds

from dlstbx.util.merging_statistics import get_merging_statistics

logger = logging.getLogger("dlstbx.wrap.autoPROC_run")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
    "FONTCONFIG_PATH": "",
    "FONTCONFIG_FILE": "",
}


class autoPROCRunWrapper(zocalo.wrapper.BaseWrapper):
    def construct_commandline(self, working_directory, params):
        """Construct autoPROC command line.
        Takes job parameter dictionary, returns array."""

        image_template = params["autoproc"]["image_template"]
        image_directory = params["autoproc"].get("image_directory", os.getcwd())
        image_first = params["autoproc"]["image_first"]
        image_last = params["autoproc"]["image_last"]
        image_pattern = params["image_pattern"]
        project = params["autoproc"].get("project")
        crystal = params["autoproc"].get("crystal")
        nproc = params["autoproc"].get("nproc")

        beamline = params["beamline"]

        # prefix = image_template.split("#")[0]
        # crystal = prefix.replace("_", "").replace(" ", "").replace("-", "")

        command = [
            "process",
            f"-nthreads={nproc}",
            "-M",
            "HighResCutOnCChalf",
            'autoPROC_CreateSummaryImageHrefLink="no"',
            'autoPROC_Summary2Base64_Run="yes"',
            'StopIfSubdirExists="no"',
            "-d",
            str(working_directory),
        ]
        if project:
            command.append(f"pname={project}")
        if crystal:
            command.append(f"xname={crystal}")

        # If any keywords defined in the following macros are also defined after
        # the macro on the command line, then the value on the command line "wins"
        if beamline == "i23":
            self._macro = "DiamondI23"
        elif beamline == "i04":
            self._macro = "DiamondI04"
        else:
            self._macro = None

        if self._macro is not None:
            command.extend(["-M", self._macro])

        command.extend(
            [
                "-Id",
                ",".join(
                    (
                        crystal,
                        image_directory,
                        image_template,
                        image_first,
                        image_last,
                    )
                ),
            ]
        )
        if image_template.endswith(".h5"):
            # command.extend(
            #    [
            #        "-h5",
            #        os.path.join(image_directory, image_template),
            #        "DistributeBackgroundImagesForHdf5=no",
            #    ]
            # )
            plugin_name = "durin-plugin.so"
            hdf5_lib = ""
            for d in os.environ["PATH"].split(os.pathsep):
                if os.path.exists(os.path.join(d, plugin_name)):
                    hdf5_lib = "autoPROC_XdsKeyword_LIB=%s" % os.path.join(
                        d, plugin_name
                    )
            if not hdf5_lib:
                logger.warning("Couldn't find plugin %s in PATH" % plugin_name)
            if hdf5_lib:
                command.append(hdf5_lib)
            untrusted_rectangles = self.get_untrusted_rectangles(
                os.path.join(image_directory, image_template)
            )
            if untrusted_rectangles:
                command.append(
                    'autoPROC_XdsKeyword_UNTRUSTED_RECTANGLE="%s"'
                    % " | ".join(untrusted_rectangles)
                )

        else:
            first_image_path = os.path.join(
                image_directory, image_pattern % int(image_first)
            )
            untrusted_rectangles = self.get_untrusted_rectangles(first_image_path)
            if untrusted_rectangles:
                command.append(
                    'autoPROC_XdsKeyword_UNTRUSTED_RECTANGLE="%s"'
                    % " | ".join(untrusted_rectangles)
                )
            if beamline == "i24":
                # i24 can run in tray mode (horizontal gonio) or pin mode
                # (vertical gonio)
                with open(first_image_path, "rb") as f:
                    for line in f.readlines():
                        if b"Oscillation_axis" in line and b"+SLOW" in line:
                            command.append(
                                'autoPROC_XdsKeyword_ROTATION_AXIS="0.000000 -1.000000  0.000000"'
                            )
                            break
                        elif b"Oscillation_axis" in line and b"+FAST" in line:
                            command.append(
                                'autoPROC_XdsKeyword_ROTATION_AXIS="-1.000000  0.000000  0.000000"'
                            )
                            break

        if params.get("ispyb_parameters"):
            if params["ispyb_parameters"].get("d_min"):
                # Can we set just d_min alone?
                # -R <reslow> <reshigh>
                pass
            if params["ispyb_parameters"].get("spacegroup"):
                command.append("symm=%s" % params["ispyb_parameters"]["spacegroup"])
            if params["ispyb_parameters"].get("unit_cell"):
                command.append(
                    "cell=%s"
                    % params["ispyb_parameters"]["unit_cell"].replace(",", " ")
                )

        return command

    def get_untrusted_rectangles(self, first_image):
        rectangles = []

        if self._macro is not None:
            # Parse any existing untrusted rectangles out of the macro
            with open(
                os.path.expandvars(
                    "$autoPROC_home/autoPROC/macros/%s.macro" % self._macro
                )
            ) as f:
                for line in f.readlines():
                    line = line.strip()
                    if line.strip().startswith(
                        "autoPROC_XdsKeyword_UNTRUSTED_RECTANGLE="
                    ):
                        rectangles.append(line.split("=")[-1].strip('"'))

        # Now add any untrusted rectangles defined in the dxtbx model
        expts = ExperimentListFactory.from_filenames([str(first_image)])
        to_xds = xds.to_xds(expts[0].imageset)
        for panel, (x0, _, y0, _) in zip(to_xds.get_detector(), to_xds.panel_limits):
            for f0, s0, f1, s1 in panel.get_mask():
                rectangles.append(
                    "%d %d %d %d" % (f0 + x0 - 1, f1 + x0, s0 + y0 - 1, s1 + y0)
                )

        return rectangles

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = (
            Path(params.get("working_directory", os.getcwd())) / "autoPROC"
        )
        working_directory.mkdir(parents=True, exist_ok=True)

        if "s3_urls" in self.recwrap.payload:
            try:
                s3_urls = self.recwrap.payload["s3_urls"]
                for filename, s3_url in s3_urls.items():
                    file_data = requests.get(s3_url)
                    with open(filename, "wb") as fp:
                        fp.write(file_data.content)
            except (KeyError, TypeError):
                logger.error("Cannot read input files from S3 store.")
                return False

        command = self.construct_commandline(working_directory, params)

        # disable control sequence parameters from autoPROC output
        # https://www.globalphasing.com/autoproc/wiki/index.cgi?RunningAutoProcAtSynchrotrons#settings
        logger.info("command: %s", " ".join(command))
        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            environment_override={"autoPROC_HIGHLIGHT": "no", **clean_environment},
            working_directory=str(working_directory),
        )

        success = not result["exitcode"] and not result["timeout"]
        if success:
            logger.info("autoPROC successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "autoPROC failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"])
            logger.debug(result["stderr"])

        autoproc_log = working_directory / "autoPROC.log"
        autoproc_log.write_bytes(result["stdout"])

        # cd $jobdir
        # tar -xzvf summary.tar.gz

        # Visit=`basename ${3}`
        ## put history into the log files
        # echo "Attempting to add history to mtz files"
        # find $jobdir -name '*.mtz' -exec /dls_sw/apps/mx-scripts/misc/AddHistoryToMTZ.sh $Beamline $Visit {} $2 autoPROC \;

        if success:
            json_file = working_directory / "iotbx-merging-stats.json"
            scaled_unmerged_mtz = working_directory / "aimless_unmerged.mtz"
            if scaled_unmerged_mtz.is_file():
                json_file.write_text(
                    get_merging_statistics(str(scaled_unmerged_mtz)).as_json()
                )

        # move summary_inlined.html to summary.html
        inlined_html = working_directory / "summary_inlined.html"
        if inlined_html.is_file():
            inlined_html.rename(working_directory / "summary.html")

        return success
