from __future__ import annotations

import logging
import pathlib

import dlstbx.util.symlink
from dlstbx.util.big_ep_helpers import copy_results
from dlstbx.wrapper import Wrapper

logger = logging.getLogger("dlstbx.wrap.mrbump_report")


class MrBUMPReportWrapper(Wrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = pathlib.Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                str(working_directory), params["create_symlink"]
            )

        if params.get("results_directory"):
            results_directory = pathlib.Path(params["results_directory"])
            logger.info(f"Copying MrBUMP results to {results_directory}")
            skip_copy = [".launch", ".recipewrap"]
            copy_results(
                str(working_directory),
                str(results_directory),
                skip_copy,
                logger,
            )

        # Create results directory and symlink if they don't already exist
        results_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                str(results_directory), params["create_symlink"]
            )

        hklout = pathlib.Path(params["mrbump"]["hklout"])
        xyzout = pathlib.Path(params["mrbump"]["xyzout"])
        success = hklout.is_file() and xyzout.is_file()
        keep_ext = {".log": "log", ".mtz": "result", ".pdb": "result"}
        for filename in results_directory.iterdir():
            filetype = keep_ext.get(filename.suffix)
            if filetype is None:
                continue
            if filetype:
                self.record_result_individual_file(
                    {
                        "file_path": str(filename.parent),
                        "file_name": filename.name,
                        "file_type": filetype,
                        "importance_rank": 1,
                    }
                )

        return success
