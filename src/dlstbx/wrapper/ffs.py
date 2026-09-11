from __future__ import annotations

import json
import math
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
    ("integrated_reflections", "result", 1),
    ("indexed_experiments", "result", 2),
    ("strong_reflections", "result", 2),
)

# Files the merge chain produces, paired with the ISPyB attachment type
# and the rank that orders them in SynchWeb.
MERGE_RESULTS = (
    ("merged.mtz", "result", 1),
    ("scaled_unmerged.mtz", "result", 2),
    ("scaled.expt", "result", 2),
    ("dials.merge.html", "graph", 2),
    ("dials.scale.html", "graph", 2),
    ("dials.symmetry.html", "graph", 3),
    ("dials.symmetry.log", "log", 3),
    ("dials.scale.log", "log", 3),
    ("dials.merge.log", "log", 3),
)

# ISPyB AutoProcScaling column paired with the key holding it in the
# statistics dials.merge writes.
SCALING_FIELDS = {
    "n_tot_obs": "n_obs",
    "n_tot_unique_obs": "n_uniq",
    "multiplicity": "multiplicity",
    "completeness": "completeness",
    "mean_i_sig_i": "i_over_sigma_mean",
    "r_merge": "r_merge",
    "r_meas_all_iplusi_minus": "r_meas",
    "r_pim_all_iplusi_minus": "r_pim",
    "cc_half": "cc_one_half",
    "cc_anom": "cc_anom",
    "anom_completeness": "anom_completeness",
}

MERGE_JSON = "dials.merge.json"

UNMERGED_MTZ = "scaled_unmerged.mtz"


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

    def construct_commandline(self, params: dict) -> list[str]:
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
        produced = [(working_directory / self.summary_filename, "log", 2)]
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
            command = self.construct_commandline(params)
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


def resolution_limits(*d_star_sq: float | None) -> tuple[float | None, float | None]:
    """
    Convert d* squared bounds to low and high resolution limits.

    The bounds dials.merge reports are named min and max but are not
    ordered by value, so they are sorted here.

    Args:
        d_star_sq: The bounds, in 1/A squared

    Returns:
        tuple: Low and high resolution limits in Angstrom, or a pair of
            None when no usable bound was given
    """
    resolutions = sorted(1.0 / math.sqrt(v) for v in d_star_sq if v)
    if not resolutions:
        return None, None
    return resolutions[-1], resolutions[0]


class MergeWrapper(dlstbx.wrapper.Wrapper):
    """
    Symmetry determination, scaling and merging of an integrated sweep.

    The GPU pipeline stops at integration, so the DIALS command line
    tools finish the job on a CPU node. Statistics reach ISPyB the way
    xia2 sends them, as a write_autoproc followed by an insert_scaling,
    which yields the AutoProcScalingId that downstream triggers key off.
    """

    _logger_name = "dlstbx.wrap.ffs_merge"
    name = "ffs_merge"

    def construct_commandlines(self, params: dict) -> list[tuple[str, list[str]]]:
        """
        Build the three DIALS stages, in the order they run.

        Args:
            params: The job_parameters block of the recipe step

        Returns:
            list: Stage name paired with the argv to run for it
        """
        experiment = params.get("experiment") or "indexed.expt"
        reflections = params.get("reflections") or "integrated.refl"
        return [
            ("symmetry", ["dials.symmetry", experiment, reflections]),
            (
                "scale",
                [
                    "dials.scale",
                    "symmetrized.expt",
                    "symmetrized.refl",
                    f"output.unmerged_mtz={UNMERGED_MTZ}",
                    "output.json=dials.scale.json",
                ],
            ),
            (
                "merge",
                ["dials.merge", "scaled.expt", "scaled.refl", f"json={MERGE_JSON}"],
            ),
        ]

    def read_statistics(self, working_directory: Path) -> dict | None:
        """
        Read the merging statistics dials.merge records.

        The file is keyed by wavelength with a single entry for a
        rotation sweep, so the first value is taken.

        Returns:
            dict | None: The merging statistics, or None when the file
                is missing or holds nothing usable
        """
        path = working_directory / MERGE_JSON
        try:
            report = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            self.log.error("No readable merging statistics at %s", path)
            return None

        for entry in report.values():
            stats = entry.get("merging_stats")
            if stats:
                return stats
        self.log.error("No merging statistics present in %s", path)
        return None

    def shell(self, stats: dict, index: int | None = None) -> dict:
        """
        Assemble one ISPyB scaling shell from the merging statistics.

        Args:
            stats: The merging statistics dials.merge recorded
            index: Resolution bin to read, or None for the overall
                figures. Bins run from low resolution upwards.

        Returns:
            dict: The shell, in the shape insert_scaling expects
        """
        if index is None:
            source = stats.get("overall", {})
            get = source.get
        else:

            def get(key, default=None):
                values = stats.get(key) or []
                return values[index] if len(values) > abs(index) else default

        shell = {column: get(key) for column, key in SCALING_FIELDS.items()}
        low, high = resolution_limits(get("d_star_sq_min"), get("d_star_sq_max"))
        shell["res_lim_low"] = low
        shell["res_lim_high"] = high
        shell["anom"] = get("anom_completeness") is not None
        return shell

    def crystal_parameters(self, working_directory: Path) -> dict:
        """
        Read the space group and cell that scaling settled on.

        A missing or unreadable file leaves the fields absent rather
        than failing the job, since the scaling statistics are still
        worth recording without them.
        """
        from dxtbx.model.experiment_list import ExperimentListFactory

        path = working_directory / "scaled.expt"
        try:
            experiments = ExperimentListFactory.from_json_file(
                str(path), check_format=False
            )
            crystal = experiments[0].crystal
        except (OSError, IndexError, ValueError, RuntimeError):
            self.log.warning("Could not read a crystal model from %s", path)
            return {}

        a, b, c, alpha, beta, gamma = crystal.get_unit_cell().parameters()
        return {
            "spacegroup": str(crystal.get_space_group().info()),
            "refinedcell_a": a,
            "refinedcell_b": b,
            "refinedcell_c": c,
            "refinedcell_alpha": alpha,
            "refinedcell_beta": beta,
            "refinedcell_gamma": gamma,
        }

    def send_results_to_ispyb(self, stats: dict, working_directory: Path) -> None:
        """Register the AutoProc record and hang the scaling off it."""
        register_autoproc = {
            "ispyb_command": "write_autoproc",
            "autoproc_id": None,
            "store_result": "ispyb_autoproc_id",
            "program_id": "$ispyb_autoprocprogram_id",
        }
        register_autoproc.update(self.crystal_parameters(working_directory))

        insert_scaling = {
            "ispyb_command": "insert_scaling",
            "autoproc_id": "$ispyb_autoproc_id",
            "store_result": "ispyb_autoprocscaling_id",
            "overall": self.shell(stats),
            "innerShell": self.shell(stats, 0),
            "outerShell": self.shell(stats, -1),
        }

        self.recwrap.send_to(
            "ispyb", {"ispyb_command_list": [register_autoproc, insert_scaling]}
        )
        self.log.info("Sent scaling statistics to ISPyB")

    def publish(self, working_directory: Path) -> None:
        """Attach whatever the chain produced."""
        attached = []
        for name, file_type, rank in MERGE_RESULTS:
            path = working_directory / name
            if not path.is_file():
                self.log.warning("Not attaching %s, which was not written", path)
                continue
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
        Run symmetry, scaling and merging, then report what came out.

        Returns:
            bool: True when every stage succeeded and the statistics
                were readable
        """
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        try:
            working_directory = Path(params["working_directory"])
        except KeyError as e:
            self.log.error("Recipe is missing job parameter %s", e)
            return False

        start_time = time.perf_counter()
        for stage, command in self.construct_commandlines(params):
            self.log.info("command: %s", " ".join(command))
            try:
                result = subprocess.run(
                    command, cwd=working_directory, timeout=params.get("timeout")
                )
            except subprocess.TimeoutExpired as e:
                self.log.warning("%s timed out after %s s", stage, e.timeout)
                self._timeout_counter.inc()
                self._failure_counter.inc()
                return False
            if result.returncode:
                self.log.error("%s failed with exit code %s", stage, result.returncode)
                self.publish(working_directory)
                self._failure_counter.inc()
                return False

        self._runtime_hist.observe(time.perf_counter() - start_time)
        self.publish(working_directory)

        stats = self.read_statistics(working_directory)
        if stats is None:
            self._failure_counter.inc()
            return False
        self.send_results_to_ispyb(stats, working_directory)

        self._success_counter.inc()
        return True
