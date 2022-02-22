from __future__ import annotations

import logging
import os
from pathlib import Path

import procrunner
import zocalo.wrapper

from dlstbx.util.iris import get_objects_from_s3
from dlstbx.util.merging_statistics import get_merging_statistics
from dlstbx.wrap.autoPROC import construct_commandline

logger = logging.getLogger("zocalo.wrap.autoPROC_run")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
    "FONTCONFIG_PATH": "",
    "FONTCONFIG_FILE": "",
}


class autoPROCRunWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)

        if "s3_urls" in self.recwrap.environment:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            try:
                get_objects_from_s3(
                    working_directory, self.recwrap.environment.get("s3_urls"), logger
                )
            except Exception:
                logger.exception(
                    "Exception raised while downloading files from S3 object store"
                )
                return False

        procrunner_directory = working_directory / params["create_symlink"]
        procrunner_directory.mkdir(parents=True, exist_ok=True)

        command = construct_commandline(
            params,
            working_directory=working_directory,
            image_directory=procrunner_directory,
        )

        # disable control sequence parameters from autoPROC output
        # https://www.globalphasing.com/autoproc/wiki/index.cgi?RunningAutoProcAtSynchrotrons#settings
        logger.info("command: %s", " ".join(command))
        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            environment_override={"autoPROC_HIGHLIGHT": "no", **clean_environment},
            working_directory=str(procrunner_directory),
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

        autoproc_log = procrunner_directory / "autoPROC.log"
        autoproc_log.write_bytes(result["stdout"])

        # HTCondor resolves symlinks while transferring data and doesn't support symlinks to directories
        if "s3_urls" in self.recwrap.environment:
            for tmp_file in procrunner_directory.rglob("*"):
                if (
                    tmp_file.is_symlink() and tmp_file.is_dir()
                ) or tmp_file.suffix == ".h5":
                    tmp_file.unlink(True)

        # cd $jobdir
        # tar -xzvf summary.tar.gz

        # Visit=`basename ${3}`
        ## put history into the log files
        # echo "Attempting to add history to mtz files"
        # find $jobdir -name '*.mtz' -exec /dls_sw/apps/mx-scripts/misc/AddHistoryToMTZ.sh $Beamline $Visit {} $2 autoPROC \;

        if success:
            json_file = procrunner_directory / "iotbx-merging-stats.json"
            scaled_unmerged_mtz = procrunner_directory / "aimless_unmerged.mtz"
            if scaled_unmerged_mtz.is_file():
                json_file.write_text(
                    get_merging_statistics(str(scaled_unmerged_mtz)).as_json()
                )

        # move summary_inlined.html to summary.html
        inlined_html = procrunner_directory / "summary_inlined.html"
        if inlined_html.is_file():
            inlined_html.rename(procrunner_directory / "summary.html")

        return success
