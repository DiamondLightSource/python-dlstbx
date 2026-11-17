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

# Unit cell axis and angle names, in the order dxtbx reports them.
CELL_AXES = ("a", "b", "c", "alpha", "beta", "gamma")


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
        params["refined_xbeam"], params["refined_ybeam"] = model["beam_centre"]
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

    def send_results_to_ispyb(self, entry: dict, working_directory: Path) -> None:
        """
        Register the AutoProc record and attach the scaling to it.

        The commands travel as one list and in this order, because each
        refers to an identifier the one before it stored.
        """
        model = self.experiment_parameters(working_directory)

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

        entry = self.read_statistics(working_directory)
        if entry is None:
            self._failure_counter.inc()
            return False
        self.send_results_to_ispyb(entry, working_directory)

        self._success_counter.inc()
        return True
