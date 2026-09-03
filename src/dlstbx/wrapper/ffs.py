from __future__ import annotations

import json
import shutil
import subprocess
import time
from fnmatch import fnmatch
from pathlib import Path

import dlstbx.util.symlink
import dlstbx.wrapper

# Fields the pipeline reports, paired with the ISPyB attachment type and
# the rank that orders them in SynchWeb. Absent entries are the stages
# that did not run.
REPORTED_RESULTS = (
    ("integrated_reflections", "Result", 1),
    ("indexed_experiments", "Result", 2),
    ("strong_reflections", "Result", 2),
)


class PipelineWrapper(dlstbx.wrapper.Wrapper):
    """
    Run one fast-feedback-service pipeline and publish what it wrote.

    Subclasses name the console script and the summary it writes. The
    stages, and the arguments the binaries take, stay behind that
    console script.
    """

    # Console script the FFS module puts on PATH, which doubles as the
    # key in job_parameters holding this pipeline's own parameters and
    # as the name the recipe passes to dlstbx.wrap
    program: str
    summary_filename: str

    def build_commandline(self, params: dict) -> list[str]:
        """
        Assemble the pipeline command line from the recipe.

        Every parameter is named as the pipeline's own request model
        names it, so the flag is the field with its underscores turned
        into hyphens. The FFS test suite locks that mapping, which
        keeps a translation table out of here.

        Args:
            params: The job_parameters block of the recipe step
        """
        command = [
            self.program,
            "--working-directory",
            str(params["working_directory"]),
        ]
        if params.get("dcid") is not None:
            command += ["--dcid", str(params["dcid"])]

        for key, value in (params.get(self.program) or {}).items():
            if value is None or value == "":
                continue
            command += [f"--{key.replace('_', '-')}", str(value)]

        return command

    def prepare_directory(self, path: Path, symlink_name: str | None) -> Path:
        """Create a published directory and link to it from the visit."""
        path.mkdir(parents=True, exist_ok=True)
        if symlink_name:
            dlstbx.util.symlink.create_parent_symlink(path, symlink_name)
        return path

    def read_summary(self, working_directory: Path) -> dict | None:
        """
        Read the summary the pipeline wrote.

        Returns:
            dict | None: The summary, or None when no readable one
                exists
        """
        summary = working_directory / self.summary_filename
        try:
            return json.loads(summary.read_text())
        except (OSError, json.JSONDecodeError):
            self.log.error(
                "No readable summary at %s; the pipeline did not get far "
                "enough to write one",
                summary,
            )
            return None

    def publish(self, params: dict, summary: dict, working_directory: Path) -> None:
        """
        Copy the results where the recipe asks, and attach them.

        Attachment fields follow the ISPyB AutoProcProgramAttachment
        columns. A result the pipeline did not reach is absent from the
        summary; a file its stage failed to write is skipped rather
        than attached as a broken path.
        """
        produced = [(working_directory / self.summary_filename, "Log", 2)]
        produced += [
            (Path(summary[field]), file_type, rank)
            for field, file_type, rank in REPORTED_RESULTS
            if summary.get(field)
        ]

        symlink_name = params.get("create_symlink")
        if symlink_name:
            self.prepare_directory(working_directory, symlink_name)

        results_directory = params.get("results_directory")
        if results_directory:
            results_directory = self.prepare_directory(
                Path(results_directory), symlink_name
            )

        final = params.get("pipeline-final") or {}
        final_directory = final.get("path")
        final_patterns = final.get("patterns") or []
        if final_directory:
            final_directory = self.prepare_directory(
                Path(final_directory), symlink_name
            )

        attached = []
        for path, file_type, rank in produced:
            if not path.is_file():
                self.log.warning("Not attaching %s, which was not written", path)
                continue

            if results_directory:
                path = Path(shutil.copy(path, results_directory / path.name))
            if final_directory and any(
                fnmatch(path.name, pattern) for pattern in final_patterns
            ):
                shutil.copy(path, final_directory / path.name)

            self.record_result_individual_file(
                {
                    "file_path": str(path.parent),
                    "file_name": path.name,
                    "file_type": file_type,
                    "importance_rank": rank,
                }
            )
            attached.append(str(path))

        if attached:
            self.record_result_all_files({"filelist": attached})

    def run(self) -> bool:
        """
        Run the pipeline named by this wrapper.

        Returns:
            bool: True when every stage succeeded. A missing parameter
                or an unreadable summary returns False, so the recipe
                reports a processing failure instead of a crashed job.
        """
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        try:
            command = self.build_commandline(params)
        except KeyError as e:
            self.log.error("Recipe is missing job parameter %s", e)
            return False

        working_directory = Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)

        self.log.info("command: %s", " ".join(command))
        try:
            start_time = time.perf_counter()
            result = subprocess.run(command, timeout=params.get("timeout"))
            runtime = time.perf_counter() - start_time
            self.log.info("%s took %.1f seconds", self.program, runtime)
            self._runtime_hist.observe(runtime)
        except subprocess.TimeoutExpired as e:
            self.log.warning("%s timed out after %s s", self.program, e.timeout)
            self._timeout_counter.inc()
            self._failure_counter.inc()
            return False

        summary = self.read_summary(working_directory)
        if summary is not None:
            self.publish(params, summary, working_directory)
            if not summary["success"]:
                failed = [s["stage"] for s in summary["stages"] if s["exit_code"]]
                self.log.error("Pipeline failed at: %s", ", ".join(failed))

        # The pipeline's own exit code is the outcome; a crash leaves
        # no summary at all.
        succeeded = not result.returncode and summary is not None
        if succeeded:
            self._success_counter.inc()
        else:
            self._failure_counter.inc()
        return succeeded


class IndexIntegrateWrapper(PipelineWrapper):
    """Index and integrate a dataset the spotfinder service has processed."""

    _logger_name = "dlstbx.wrap.ffs_index_integrate"
    name = "ffs_index_integrate"

    program = "ffs_index_integrate"
    summary_filename = "ffs_index_integrate.json"


class SpotfindIndexIntegrateWrapper(PipelineWrapper):
    """Spotfind, index and integrate a dataset from its raw images."""

    _logger_name = "dlstbx.wrap.ffs_spotfind_index_integrate"
    name = "ffs_spotfind_index_integrate"

    program = "ffs_spotfind_index_integrate"
    summary_filename = "ffs_spotfind_index_integrate.json"
