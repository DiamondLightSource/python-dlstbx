from __future__ import annotations

import procrunner
import py

import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper


class Screen19Wrapper(Wrapper):
    _logger_name = "dlstbx.wrap.screen19"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        # create working directory
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            # Create symbolic link above working directory
            dlstbx.util.symlink.create_parent_symlink(
                working_directory.strpath, params["create_symlink"]
            )

        # construct screen19 command line
        command = ["screen19", params["screen-selection"]]

        # run screen19
        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            working_directory=working_directory.strpath,
            environment_override={"PYTHONIOENCODING": "UTF-8"},
        )
        self.log.info("command: %s", " ".join(result["command"]))
        self.log.info("timeout: %s", result["timeout"])
        self.log.info("time_start: %s", result["time_start"])
        self.log.info("time_end: %s", result["time_end"])
        self.log.info("runtime: %s", result["runtime"])
        self.log.info("exitcode: %s", result["exitcode"])
        self.log.debug(result["stdout"])
        self.log.debug(result["stderr"])
        success = result["exitcode"] == 0

        # copy output files to result directory
        results_directory.ensure(dir=True)

        defaultfiles = ["screen19.log"]
        if working_directory.join("wilson_plot.png").check():
            defaultfiles.append("wilson_plot.png")
        if working_directory.join("integrated.refl").check():
            defaultfiles.append("integrated.refl")
            defaultfiles.append("integrated.expt")
        elif working_directory.join("indexed.refl").check():
            defaultfiles.append("indexed.refl")
            defaultfiles.append("indexed.expt")
        elif working_directory.join("strong.refl").check():
            defaultfiles.append("strong.refl")
            defaultfiles.append("imported.expt")
            if working_directory.join("stronger.refl").check():
                defaultfiles.append("stronger.refl")
        if working_directory.join("dials-report.html").check():
            defaultfiles.append("dials-report.html")

        foundfiles = []
        for filename in params.get("keep_files", defaultfiles):
            if working_directory.join(filename).check():
                dst = results_directory.join(filename)
                self.log.debug(f"Copying {filename} to {dst.strpath}")
                working_directory.join(filename).copy(dst)
                foundfiles.append(dst.strpath)
                self.record_result_individual_file(
                    {
                        "file_path": dst.dirname,
                        "file_name": dst.basename,
                        "file_type": (
                            "log"
                            if filename.endswith(".log") or filename.endswith(".html")
                            else "result"
                        ),
                    }
                )
            else:
                self.log.warning("Expected output file %s missing", filename)
                success = False

        if foundfiles:
            self.log.info("Notifying for found files: %s", str(foundfiles))
            self.record_result_all_files({"filelist": foundfiles})

        if params.get("create_symlink"):
            # Create symbolic link above results directory
            dlstbx.util.symlink.create_parent_symlink(
                results_directory.strpath, params["create_symlink"]
            )

        self.log.info("Done.")

        return success
