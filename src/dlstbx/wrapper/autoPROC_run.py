from __future__ import annotations

import logging
import os
import shutil
import time
from pathlib import Path

import procrunner

from dlstbx.util.iris import get_objects_from_s3
from dlstbx.util.merging_statistics import get_merging_statistics
from dlstbx.wrapper import Wrapper
from dlstbx.wrapper.autoPROC import construct_commandline

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
    "FONTCONFIG_PATH": "",
    "FONTCONFIG_FILE": "",
}


class autoPROCRunWrapper(Wrapper):

    _logger_name = "zocalo.wrap.autoPROC_run"

    name = "autoPROC"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)
        procrunner_directory = working_directory / params["create_symlink"]
        procrunner_directory.mkdir(parents=True, exist_ok=True)
        image_directory = None

        if "s3_urls" in self.recwrap.environment:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.setLevel(logging.DEBUG)
            try:
                get_objects_from_s3(
                    working_directory, self.recwrap.environment.get("s3_urls"), self.log
                )
            except Exception:
                self.log.exception(
                    "Exception raised while downloading files from S3 object store"
                )
                return False
            # We only want to override the image_directory when running in The Cloud,
            # as only then will the images have been copied locally. Otherwise use the
            # original image_directory.
            image_directory = working_directory

        command = construct_commandline(
            params,
            self.log,
            working_directory=procrunner_directory,
            image_directory=image_directory,
        )

        # disable control sequence parameters from autoPROC output
        # https://www.globalphasing.com/autoproc/wiki/index.cgi?RunningAutoProcAtSynchrotrons#settings
        self.log.info("command: %s", " ".join(command))
        start_time = time.perf_counter()
        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            environment_override={"autoPROC_HIGHLIGHT": "no", **clean_environment},
            working_directory=str(procrunner_directory),
        )
        runtime = time.perf_counter() - start_time
        self.log.info(f"xia2 took {runtime} seconds")
        self._runtime_hist.observe(runtime)

        success = not result["exitcode"] and not result["timeout"]
        if success:
            self.log.info("autoPROC successful, took %.1f seconds", result["runtime"])
        else:
            self.log.info(
                "autoPROC failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            self.log.debug(result["stdout"])
            self.log.debug(result["stderr"])

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
            shutil.copy2(inlined_html, procrunner_directory / "summary.html")

        if success:
            self._success_counter.inc()
        else:
            self._failure_counter.inc()

        return success
