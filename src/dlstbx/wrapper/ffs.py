from __future__ import annotations

import json
import math
import re
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
    ("ffs.log", "log", 1),
    ("merged.mtz", "result", 1),
    ("scaled_unmerged.mtz", "result", 2),
    ("scaled.expt", "result", 2),
    ("dials.merge.html", "graph", 2),
    ("dials.scale.html", "graph", 2),
    ("dials.estimate_resolution.html", "graph", 2),
    ("dials.symmetry.html", "graph", 3),
    ("dials.symmetry.log", "log", 3),
    ("dials.scale.log", "log", 3),
    ("dials.estimate_resolution.log", "log", 3),
    ("dials.merge.log", "log", 3),
    ("ffs_merge.json", "log", 2),
)

# ISPyB AutoProcScaling column paired with its key in the merging_stats
# block of dials.merge.json.
SCALING_FIELDS = {
    "n_tot_obs": "n_obs",
    "n_tot_unique_obs": "n_uniq",
    "multiplicity": "multiplicity",
    "completeness": "completeness",
    "mean_i_sig_i": "i_over_sigma_mean",
    "r_meas_all_iplusi_minus": "r_meas",
    "r_pim_all_iplusi_minus": "r_pim",
    "cc_half": "cc_one_half",
}

# ISPyB AutoProcScaling column paired with its key in the
# merging_stats_anom block of dials.merge.json.
ANOMALOUS_SCALING_FIELDS = {
    "r_merge": "r_merge",
    "r_meas_within_iplusi_minus": "r_meas",
    "r_pim_within_iplusi_minus": "r_pim",
    "anom_completeness": "anom_completeness",
    "anom_multiplicity": "multiplicity",
    "cc_anom": "cc_anom",
}

MERGE_JSON = "dials.merge.json"

UNMERGED_MTZ = "scaled_unmerged.mtz"

ESTIMATE_LOG = "dials.estimate_resolution.log"

MERGE_SUMMARY = "ffs_merge.json"

# The line dials.estimate_resolution logs for the CC½ criterion. The
# colon straight after the name keeps the cc_half_significance_level
# line from matching.
CC_HALF_LIMIT = re.compile(r"^Resolution cc_half:\s*([\d.]+)\s*$", re.MULTILINE)

# Unit cell axis and angle names, in the order dxtbx reports them.
CELL_AXES = ("a", "b", "c", "alpha", "beta", "gamma")

REPORT = "ffs.log"

REPORT_RULE = "-" * 80

# Rows of the report's statistics table: label, key in the ISPyB shell,
# and how to print the value. Anomalous completeness arrives as a
# fraction, unlike completeness, which arrives as a percentage.
REPORT_ROWS = (
    ("Low resolution", "res_lim_low", "{:.2f}".format),
    ("High resolution", "res_lim_high", "{:.2f}".format),
    ("Rmerge", "r_merge", "{:.3f}".format),
    ("Rmeas", "r_meas_all_iplusi_minus", "{:.3f}".format),
    ("Rpim", "r_pim_all_iplusi_minus", "{:.3f}".format),
    ("I/sigma", "mean_i_sig_i", "{:.2f}".format),
    ("Completeness", "completeness", "{:.1f}".format),
    ("Multiplicity", "multiplicity", "{:.1f}".format),
    ("CC 1/2", "cc_half", "{:.3f}".format),
    ("Anom. Completeness", "anom_completeness", lambda v: f"{100 * v:.1f}"),
    ("Anom. Multiplicity", "anom_multiplicity", "{:.1f}".format),
    ("Anom. Correlation", "cc_anom", "{:.3f}".format),
    ("Nrefl", "n_tot_obs", "{:d}".format),
    ("Nunique", "n_tot_unique_obs", "{:d}".format),
)


def autoproc_parameters(model: dict) -> dict:
    """
    Map an experiment model onto the fields write_autoproc expects.

    An empty model yields an empty dict, so an unreadable experiment
    sends no AutoProc fields at all.
    """
    if not model:
        return {}
    params = {f"refinedcell_{name}": v for name, v in zip(CELL_AXES, model["cell"])}
    params["spacegroup"] = model["spacegroup"]
    return params


def integration_parameters(model: dict) -> dict:
    """
    Map an experiment model onto the fields upsert_integration expects.

    Fields absent from the model are left out rather than sent as
    None, so a partial read does not blank columns already holding a
    value.
    """
    if not model:
        return {}
    params = {f"cell_{name}": v for name, v in zip(CELL_AXES, model["cell"])}
    if "image_range" in model:
        params["start_image_no"], params["end_image_no"] = model["image_range"]
    if "detector_distance" in model:
        params["refined_detector_dist"] = model["detector_distance"]
    if "beam_centre" in model:
        # Temporarily swapped x and y until SynchWeb changes made historic database data swapped
        params["refined_ybeam"], params["refined_xbeam"] = model["beam_centre"]
    return params


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


def estimated_resolution(log: str) -> float | None:
    """
    Read the CC½ resolution limit from a dials.estimate_resolution log.

    Args:
        log: Text of the log

    Returns:
        float | None: The limit in Angstrom, or None when the fit failed
            and no limit was logged
    """
    match = CC_HALF_LIMIT.search(log)
    return float(match.group(1)) if match else None


def user_resolution_limit(params: dict) -> float | None:
    """
    Read the resolution limit a reprocessing request asked for.

    The recipe fills dmin from the request, so it arrives as a string
    and is None or empty on an automatic run.

    Args:
        params: The job_parameters block of the recipe step

    Returns:
        float | None: The limit in Angstrom, or None when none was given

    Raises:
        ValueError: The value is not a number
    """
    value = params.get("dmin")
    if value is None or value == "":
        return None
    return float(value)


def format_report(
    stages: list[dict],
    model: dict,
    shells: list[dict] | None,
    resolution_limit: float | None,
    resolution_limit_source: str | None,
) -> str:
    """
    Lay out the short processing report SynchWeb shows first.

    A row of the statistics table is left out when no shell has a
    value for it, as the anomalous rows are when no anomalous merging
    ran.

    Args:
        stages:           Every stage that ran, GPU pipeline first, each
                          with its name, exit code and duration
        model:            Space group, cell and image range, or empty
        shells:           Overall, inner and outer ISPyB shells, or None
                          when merging did not finish
        resolution_limit: High resolution cut in Angstrom, or None
        resolution_limit_source: "estimate" or "request" for a cut

    Returns:
        str: The report text
    """
    lines = ["Fast Feedback Service GPU processing"]

    # What was processed and the model it settled on
    if "image_range" in model:
        first, last = model["image_range"]
        lines.append(f"Processing images: {first} -> {last}")
    if "spacegroup" in model:
        lines.append(f"Space group: {model['spacegroup']}")
    if "cell" in model:
        lines.append("Unit cell: " + " ".join(f"{v:6.2f}" for v in model["cell"]))

    # The cut merging used, then the statistics in the overall, inner
    # and outer shells
    if shells:
        if resolution_limit is None:
            lines.append("Resolution cut: none")
        else:
            how = "requested" if resolution_limit_source == "request" else "CC 1/2 fit"
            lines.append(f"Resolution cut: {resolution_limit:.2f} ({how})")
        lines += [REPORT_RULE, f"{'':>20} {'Overall':>7} {'Inner':>7} {'Outer':>7}"]
        for label, key, show in REPORT_ROWS:
            values = [shell.get(key) for shell in shells]
            if all(v is None for v in values):
                continue
            cells = ("-" if v is None else show(v) for v in values)
            lines.append(f"{label:>20} " + " ".join(f"{c:>7}" for c in cells))

    # Wall time of each stage, marking the one that stopped the job
    lines += [REPORT_RULE, "Timings:"]
    for stage in stages:
        line = f"{stage['stage']:>20} {stage['duration']:7.1f} s"
        if stage["exit_code"] is None:
            line += "  timed out"
        elif stage["exit_code"]:
            line += f"  failed with exit code {stage['exit_code']}"
        lines.append(line)
    total = sum(stage["duration"] for stage in stages)
    lines += [f"{'Total':>20} {total:7.1f} s", REPORT_RULE]
    return "\n".join(lines) + "\n"


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

    The integrator does not cut by resolution, so the merge step does.
    A limit given on the request is used as it is; without one, the
    merge cuts at the CC½ limit dials.estimate_resolution fits. Scaling
    runs before the limit is known, so the unmerged MTZ still covers
    the full resolution range while the merged MTZ and the statistics
    are cut.
    """

    _logger_name = "dlstbx.wrap.ffs_merge"
    name = "ffs_merge"

    def construct_commandlines(
        self, params: dict, estimate: bool = True
    ) -> list[tuple[str, list[str]]]:
        """
        Build the DIALS stages that run before merging, in order.

        Args:
            params:   The job_parameters block of the recipe step
            estimate: Whether to fit a resolution limit after scaling

        Returns:
            list: Stage name paired with the argv to run for it
        """
        experiment = params.get("experiment") or "indexed.expt"
        reflections = params.get("reflections") or "integrated.refl"
        stages = [
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
        ]
        if estimate:
            stages.append(
                (
                    "estimate_resolution",
                    ["dials.estimate_resolution", "scaled.expt", "scaled.refl"],
                )
            )
        return stages

    def merge_commandline(self, d_min: float | None) -> list[str]:
        """
        Build the merging stage.

        Args:
            d_min: High resolution cut in Angstrom, or None to merge every
                scaled reflection

        Returns:
            list: The argv to run
        """
        command = ["dials.merge", "scaled.expt", "scaled.refl", f"json={MERGE_JSON}"]
        if d_min is not None:
            command.append(f"d_min={d_min}")
        return command

    def read_resolution_limit(self, working_directory: Path) -> float | None:
        """
        Read the limit dials.estimate_resolution fitted.

        A missing log or a failed fit degrades to an uncut merge rather
        than failing the job.

        Returns:
            float | None: The limit in Angstrom, or None to merge uncut
        """
        path = working_directory / ESTIMATE_LOG
        try:
            d_min = estimated_resolution(path.read_text())
        except OSError:
            self.log.warning("No readable %s, so merging without a cut", path)
            return None
        if d_min is None:
            self.log.warning("No CC½ limit in %s, so merging without a cut", path)
        else:
            self.log.info("Merging with the CC½ resolution limit of %.2f A", d_min)
        return d_min

    def record_stage(
        self,
        working_directory: Path,
        stage: str,
        command: list[str],
        exit_code: int | None,
        duration: float,
    ) -> None:
        """
        Add a finished stage to the summary and rewrite it.

        The summary is rewritten after every stage, so a job that stops
        part way still records how far it got.

        Args:
            working_directory: Where the summary is written
            stage:             Name of the stage
            command:           The argv it ran
            exit_code:         Its exit code, or None when it timed out
            duration:          Wall time it took, in seconds
        """
        self.stages.append(
            {
                "stage": stage,
                "command": command,
                "exit_code": exit_code,
                "duration": duration,
            }
        )
        merged = bool(self.stages) and self.stages[-1]["stage"] == "merge"
        summary = {
            "working_directory": str(working_directory),
            "stages": self.stages,
            "resolution_limit": self.resolution_limit,
            "resolution_limit_source": self.resolution_limit_source,
            "success": merged and all(s["exit_code"] == 0 for s in self.stages),
        }
        path = working_directory / MERGE_SUMMARY
        try:
            path.write_text(json.dumps(summary, indent=2))
        except OSError as e:
            self.log.warning("Could not write the merge summary to %s: %s", path, e)
        self.write_report(working_directory)

    def read_pipeline_stages(self, working_directory: Path) -> list[dict]:
        """
        Read the stages the GPU pipeline recorded in its summary.

        Either pipeline may have run before the merge, so whichever
        summary is present is read.

        Returns:
            list: The pipeline's stages, or empty when no summary is
                readable
        """
        for name in (
            SpotfindIndexIntegrateWrapper.summary_filename,
            IndexIntegrateWrapper.summary_filename,
        ):
            try:
                summary = json.loads((working_directory / name).read_text())
            except (OSError, json.JSONDecodeError):
                continue
            return summary.get("stages") or []
        self.log.warning("No pipeline summary in %s to report", working_directory)
        return []

    def write_report(
        self,
        working_directory: Path,
        entry: dict | None = None,
        model: dict | None = None,
    ) -> None:
        """
        Write the short processing report.

        It is rewritten after every stage, as the summary is, and once
        more with the statistics when merging has finished.

        Args:
            working_directory: Where the report is written
            entry:             The merging statistics, once there are any
            model:             The model scaling settled on, once read
        """
        shells = None
        if entry is not None:
            shells = [self.shell(entry), self.shell(entry, 0), self.shell(entry, -1)]
        report = format_report(
            self.pipeline_stages + self.stages,
            model or {},
            shells,
            self.resolution_limit,
            self.resolution_limit_source,
        )
        path = working_directory / REPORT
        try:
            path.write_text(report)
        except OSError as e:
            self.log.warning("Could not write the report to %s: %s", path, e)

    def run_stage(
        self,
        stage: str,
        command: list[str],
        working_directory: Path,
        timeout: float | None,
    ) -> bool:
        """
        Run one DIALS stage, recording a failure against the job.

        Returns:
            bool: True when the stage exited cleanly
        """
        self.log.info("command: %s", " ".join(command))
        start_time = time.perf_counter()
        try:
            result = subprocess.run(command, cwd=working_directory, timeout=timeout)
        except subprocess.TimeoutExpired as e:
            duration = time.perf_counter() - start_time
            self.record_stage(working_directory, stage, command, None, duration)
            self.log.warning("%s timed out after %s s", stage, e.timeout)
            self._timeout_counter.inc()
            self._failure_counter.inc()
            return False
        duration = time.perf_counter() - start_time
        self.record_stage(
            working_directory, stage, command, result.returncode, duration
        )
        if result.returncode:
            self.log.error("%s failed with exit code %s", stage, result.returncode)
            self.publish(working_directory)
            self._failure_counter.inc()
            return False
        return True

    def read_statistics(self, working_directory: Path) -> dict | None:
        """
        Read the merging statistics dials.merge records.

        The file is keyed by wavelength with a single entry for a
        rotation sweep, so the first entry holding statistics is taken.

        Returns:
            dict | None: The wavelength entry, holding merging_stats and,
                when anomalous merging ran, merging_stats_anom. None when
                the file is missing or holds nothing usable
        """
        path = working_directory / MERGE_JSON
        try:
            report = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            self.log.error("No readable merging statistics at %s", path)
            return None

        for entry in report.values():
            if entry.get("merging_stats"):
                return entry
        self.log.error("No merging statistics present in %s", path)
        return None

    def shell(self, entry: dict, index: int | None = None) -> dict:
        """
        Assemble one ISPyB scaling shell from the merging statistics.

        The anomalous columns come from the merging_stats_anom block, so
        they are None when dials.merge did not write one.

        Args:
            entry: The wavelength entry read_statistics returns
            index: Resolution bin to read, or None for the overall
                figures. Bins run from low resolution upwards.

        Returns:
            dict: The shell, in the shape insert_scaling expects
        """

        def column(stats: dict, key: str):
            if index is None:
                return stats.get("overall", {}).get(key)
            values = stats.get(key) or []
            return values[index] if -len(values) <= index < len(values) else None

        stats = entry["merging_stats"]
        anom = entry.get("merging_stats_anom") or {}
        shell = {col: column(stats, key) for col, key in SCALING_FIELDS.items()}
        shell.update(
            {col: column(anom, key) for col, key in ANOMALOUS_SCALING_FIELDS.items()}
        )
        low, high = resolution_limits(
            column(stats, "d_star_sq_min"), column(stats, "d_star_sq_max")
        )
        shell["res_lim_low"] = low
        shell["res_lim_high"] = high
        return shell

    def experiment_parameters(self, working_directory: Path) -> dict:
        """
        Read the model that scaling settled on.

        A missing or unreadable file leaves the fields absent rather
        than failing the job, since the scaling statistics are still
        worth recording without them. Geometry is absent for a
        stills experiment, which carries no scan.

        Returns:
            dict: Space group, cell, image range, detector distance and
                beam centre, or an empty dict when unreadable
        """
        from dxtbx.model.experiment_list import ExperimentListFactory

        path = working_directory / "scaled.expt"
        try:
            experiments = ExperimentListFactory.from_json_file(
                str(path), check_format=False
            )
            experiment = experiments[0]
        except (OSError, IndexError, ValueError, RuntimeError):
            self.log.warning("Could not read an experiment model from %s", path)
            return {}

        crystal = experiment.crystal
        model = {
            "spacegroup": str(crystal.get_space_group().info()),
            "cell": crystal.get_unit_cell().parameters(),
        }
        if experiment.scan:
            model["image_range"] = experiment.scan.get_image_range()
        if experiment.detector and experiment.beam:
            panel = experiment.detector[0]
            model["detector_distance"] = panel.get_distance()
            model["beam_centre"] = panel.get_beam_centre(experiment.beam.get_s0())
        return model

    def send_results_to_ispyb(self, entry: dict, model: dict) -> None:
        """
        Register the AutoProc record and attach the scaling to it.

        The commands travel as one list and in this order, because each
        refers to an identifier the one before it stored.

        Args:
            entry: The merging statistics
            model: The model scaling settled on, or empty when unreadable
        """

        register_autoproc = {
            "ispyb_command": "write_autoproc",
            "autoproc_id": None,
            "store_result": "ispyb_autoproc_id",
            "program_id": "$ispyb_autoprocprogram_id",
        }
        register_autoproc.update(autoproc_parameters(model))

        insert_scaling = {
            "ispyb_command": "insert_scaling",
            "autoproc_id": "$ispyb_autoproc_id",
            "store_result": "ispyb_autoprocscaling_id",
            "overall": self.shell(entry),
            "innerShell": self.shell(entry, 0),
            "outerShell": self.shell(entry, -1),
        }

        # The recipe opens an integration record before processing
        # starts, leaving it empty and unattached until scaling exists.
        link_integration = {
            "ispyb_command": "upsert_integration",
            "integration_id": "$ispyb_integration_id",
            "program_id": "$ispyb_autoprocprogram_id",
            "scaling_id": "$ispyb_autoprocscaling_id",
        }
        link_integration.update(integration_parameters(model))

        self.recwrap.send_to(
            "ispyb",
            {
                "ispyb_command_list": [
                    register_autoproc,
                    insert_scaling,
                    link_integration,
                ]
            },
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
        Run symmetry, scaling, resolution estimation and merging, then
        report what came out.

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

        # A limit on the request replaces the estimate, so none is fitted
        try:
            d_min = user_resolution_limit(params)
        except ValueError:
            self.log.error("Resolution limit %r is not a number", params["dmin"])
            return False

        self.stages: list[dict] = []
        self.resolution_limit: float | None = None
        self.resolution_limit_source: str | None = None
        self.pipeline_stages = self.read_pipeline_stages(working_directory)

        start_time = time.perf_counter()
        timeout = params.get("timeout")

        # Merging needs the limit the earlier stages fit, so it runs last
        stages = self.construct_commandlines(params, estimate=d_min is None)
        for stage, command in stages:
            if not self.run_stage(stage, command, working_directory, timeout):
                return False

        if d_min is None:
            d_min = self.read_resolution_limit(working_directory)
            source = None if d_min is None else "estimate"
        else:
            self.log.info("Merging with the requested resolution limit of %s A", d_min)
            source = "request"
        self.resolution_limit = d_min
        self.resolution_limit_source = source
        merge = self.merge_commandline(d_min)
        if not self.run_stage("merge", merge, working_directory, timeout):
            return False

        self._runtime_hist.observe(time.perf_counter() - start_time)

        # The report needs the statistics, so it is finished before
        # anything is attached
        entry = self.read_statistics(working_directory)
        if entry is not None:
            model = self.experiment_parameters(working_directory)
            self.write_report(working_directory, entry, model)
        self.publish(working_directory)
        if entry is None:
            self._failure_counter.inc()
            return False
        self.send_results_to_ispyb(entry, model)

        self._success_counter.inc()
        return True
