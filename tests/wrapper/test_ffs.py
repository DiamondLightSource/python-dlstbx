from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from dlstbx.wrapper.ffs import (
    IndexIntegrateWrapper,
    MergeWrapper,
    SpotfindIndexIntegrateWrapper,
    autoproc_parameters,
    estimated_resolution,
    format_report,
    integration_parameters,
)

# dials.estimate_resolution.log from DIALS 3.31.0 on the i03 insulin
# test collection, cm44137-5 TestInsulin/ins_1/ins_1_1_master.h5
SCALE_LOG_TEXT = """\
The reflection table variances have been adjusted to account for the
uncertainty in the scaling model
Applying auto resolution cut at 2.34

Total time taken: 15.1313s 
"""

# The same log when the CC½ fit fails, in the form
# dials.util.resolution_analysis.Resolutionizer.resolution_auto logs it
FAILED_FIT_LOG_TEXT = """\
The reflection table variances have been adjusted to account for the
uncertainty in the scaling model

Total time taken: 15.1313s 
"""


def summary(working_directory: Path, name: str, success: bool = True, **paths) -> Path:
    """Write the summary the pipeline would have left behind."""
    for produced in paths.values():
        Path(produced).parent.mkdir(parents=True, exist_ok=True)
        Path(produced).touch()
    path = working_directory / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "dcid": 12345,
                "working_directory": str(working_directory),
                "success": success,
                "stages": [
                    {
                        "stage": "indexer",
                        "command": [],
                        "exit_code": 0 if success else 3,
                        "duration": 1.0,
                    }
                ],
                **{k: str(v) for k, v in paths.items()},
            }
        )
    )
    return path


@pytest.fixture
def job_parameters(tmp_path):
    """The job_parameters block a recipe step would carry."""
    visit = tmp_path / "visit"

    def make(program: str, **publishing):
        return {
            program: {
                "reflection": str(tmp_path / "results_ffs.h5"),
                "experiment": str(tmp_path / "imported.expt"),
                "max_cell": 100.0,
                "dmin": None,
            },
            "dcid": 12345,
            "working_directory": str(visit / "tmp" / "12345" / "ffs"),
            **publishing,
        }

    return make


def test_the_recipe_parameters_become_flags(job_parameters):
    wrapper = IndexIntegrateWrapper()
    params = job_parameters("ffs_index_integrate")

    command = wrapper.construct_commandline(params)

    assert command[0] == "ffs_index_integrate", "the console script leads the argv"
    assert "--max-cell" in command, "underscores in a field become hyphens in a flag"
    assert "100.0" == command[command.index("--max-cell") + 1], (
        "the value must follow the flag it belongs to"
    )
    assert "--dcid" in command, "the DCID comes from the common block, not the pipeline"
    assert "--dmin" not in command, (
        "an unset parameter must be dropped so the pipeline default applies"
    )


def test_a_missing_working_directory_fails_rather_than_raising(job_parameters, mocker):
    wrapper = IndexIntegrateWrapper()
    params = job_parameters("ffs_index_integrate")
    del params["working_directory"]
    wrapper.recwrap = mocker.Mock(recipe_step={"job_parameters": params})

    assert wrapper.run() is False, (
        "a bad recipe must land as a processing failure, not a crashed job"
    )


def test_a_successful_run_publishes_and_attaches(tmp_path, job_parameters, mocker):
    params = job_parameters(
        "ffs_index_integrate",
        results_directory=str(tmp_path / "visit" / "processed" / "12345" / "ffs"),
        create_symlink="ffs",
    )
    working = Path(params["working_directory"])

    def fake_run(command, **kwargs):
        summary(
            working,
            "ffs_index_integrate.json",
            integrated_reflections=working / "integrated.refl",
            indexed_experiments=working / "indexed.expt",
        )
        return subprocess.CompletedProcess(command, 0)

    mocker.patch("subprocess.run", side_effect=fake_run)
    wrapper = IndexIntegrateWrapper()
    wrapper.recwrap = mocker.Mock(recipe_step={"job_parameters": params})
    wrapper.record_result_individual_file = mocker.Mock()

    assert wrapper.run() is True, "a zero exit code with a summary is a success"

    attached = {
        call.args[0]["file_name"]
        for call in wrapper.record_result_individual_file.call_args_list
    }
    assert "integrated.refl" in attached, "the integrated reflections must be attached"
    assert "ffs_index_integrate.json" in attached, "the summary must be attached"
    assert (Path(params["results_directory"]) / "integrated.refl").is_file(), (
        "results must be copied out of the working directory"
    )


def test_a_pipeline_that_stopped_early_reports_failure(
    tmp_path, job_parameters, mocker
):
    params = job_parameters("ffs_index_integrate")
    working = Path(params["working_directory"])

    def fake_run(command, **kwargs):
        summary(working, "ffs_index_integrate.json", success=False)
        return subprocess.CompletedProcess(command, 1)

    mocker.patch("subprocess.run", side_effect=fake_run)
    wrapper = IndexIntegrateWrapper()
    wrapper.recwrap = mocker.Mock(recipe_step={"job_parameters": params})
    wrapper.record_result_individual_file = mocker.Mock()

    assert wrapper.run() is False, "a non-zero exit code is a processing failure"
    assert wrapper.record_result_individual_file.called, (
        "the summary must still be attached, since it records where it stopped"
    )


def test_a_crash_without_a_summary_is_a_failure(job_parameters, mocker):
    """A pipeline killed by the scheduler leaves nothing to read."""
    params = job_parameters("ffs_index_integrate")
    mocker.patch(
        "subprocess.run",
        return_value=subprocess.CompletedProcess([], 0),
    )
    wrapper = IndexIntegrateWrapper()
    wrapper.recwrap = mocker.Mock(recipe_step={"job_parameters": params})

    assert wrapper.run() is False, (
        "a zero exit code with no summary cannot be called a success"
    )


def test_a_timeout_is_a_failure_rather_than_an_exception(job_parameters, mocker):
    params = job_parameters("ffs_index_integrate")
    mocker.patch("subprocess.run", side_effect=subprocess.TimeoutExpired([], 60))
    wrapper = IndexIntegrateWrapper()
    wrapper.recwrap = mocker.Mock(recipe_step={"job_parameters": params})

    assert wrapper.run() is False, "a timeout must be reported, not raised"


def test_the_two_wrappers_drive_different_programs():
    assert IndexIntegrateWrapper.program != SpotfindIndexIntegrateWrapper.program, (
        "one recipe must be able to carry parameters for both"
    )
    assert IndexIntegrateWrapper.summary_filename != (
        SpotfindIndexIntegrateWrapper.summary_filename
    ), "both pipelines can share a working directory, so summaries must not collide"


# A scaled.expt read of a rotation sweep, as experiment_parameters returns it.
FULL_MODEL = {
    "spacegroup": "P 4 2 2",
    "cell": (123.534, 123.534, 87.240, 90.0, 90.0, 90.0),
    "image_range": (1, 3600),
    "detector_distance": 283.705,
    "beam_centre": (152.154, 165.049),
}


def test_the_cell_is_named_differently_for_each_ispyb_command():
    autoproc = autoproc_parameters(FULL_MODEL)
    integration = integration_parameters(FULL_MODEL)

    assert autoproc["refinedcell_a"] == 123.534, (
        "write_autoproc takes the cell under refinedcell_ names"
    )
    assert integration["cell_a"] == 123.534, (
        "upsert_integration takes the same cell under cell_ names"
    )
    assert autoproc["spacegroup"] == "P 4 2 2", (
        "the space group only belongs on the AutoProc record"
    )
    assert "spacegroup" not in integration, (
        "AutoProcIntegration has no space group column"
    )


def test_the_integration_record_carries_the_refined_geometry():
    integration = integration_parameters(FULL_MODEL)

    assert integration["start_image_no"] == 1, "scan start reaches ISPyB"
    assert integration["end_image_no"] == 3600, "scan end reaches ISPyB"
    assert integration["refined_detector_dist"] == 283.705, (
        "the detector distance scaling settled on reaches ISPyB"
    )
    # Swapped to match the other wrappers until SynchWeb reads the true order (#406)
    assert (integration["refined_xbeam"], integration["refined_ybeam"]) == (
        165.049,
        152.154,
    ), "the beam centre reaches ISPyB swapped, as SynchWeb currently expects"


def test_geometry_absent_from_the_model_is_omitted_rather_than_blanked():
    stills = {"spacegroup": "P 1", "cell": (10.0, 20.0, 30.0, 90.0, 90.0, 90.0)}

    integration = integration_parameters(stills)

    assert integration["cell_c"] == 30.0, "the cell is still reported"
    for absent in (
        "start_image_no",
        "end_image_no",
        "refined_detector_dist",
        "refined_xbeam",
        "refined_ybeam",
    ):
        assert absent not in integration, (
            f"{absent} must be left out rather than sent as None, "
            "which would blank a column that already holds a value"
        )


@pytest.mark.parametrize("mapper", [autoproc_parameters, integration_parameters])
def test_an_unreadable_experiment_maps_to_nothing(mapper):
    assert mapper({}) == {}, "an unreadable scaled.expt must not fabricate ISPyB fields"


# The limit is read from the CC½ line alone; the significance-level line
# that follows it must not be mistaken for it, and a failed fit or an
# empty log must give no limit rather than raise.
@pytest.mark.parametrize(
    "log, expected",
    [
        (SCALE_LOG_TEXT, 2.34),
        (FAILED_FIT_LOG_TEXT, None),
        ("", None),
    ],
)
def test_the_cc_half_limit_is_read_from_the_estimate_log(log, expected):
    assert estimated_resolution(log) == expected, (
        "only the 'Applying auto resolution cut' line may set the resolution limit"
    )


# A limit given on the request replaces the estimate: no fit runs, and
# scaling and merging cut where the request asked. The recipe fills the
# value in as a string.
@pytest.mark.parametrize("dmin", ["2.0", 2.0])
def test_a_requested_limit_replaces_the_estimate(tmp_path, mocker, dmin):
    params = {"working_directory": str(tmp_path), "dmin": dmin}
    commands = []

    def fake_run(command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0)

    mocker.patch("subprocess.run", side_effect=fake_run)
    wrapper = MergeWrapper()
    wrapper.recwrap = mocker.Mock(recipe_step={"job_parameters": params})
    wrapper.publish = mocker.Mock()
    wrapper.read_statistics = mocker.Mock(return_value={"merging_stats": {}})
    wrapper.send_results_to_ispyb = mocker.Mock()

    assert wrapper.run() is True, "a requested limit must merge successfully"
    scales = [c for c in commands if c[0] == "dials.scale"]
    assert len(scales) == 1 and "d_min=2.0" in scales[0], (
        "a requested limit must be applied by a single scaling"
    )


# A limit that is not a number is a bad request, so it must fail the job
# before any DIALS stage runs rather than merge at a limit nobody asked for.
def test_an_unreadable_requested_limit_fails_before_any_stage(tmp_path, mocker):
    params = {"working_directory": str(tmp_path), "dmin": "high"}
    run = mocker.patch("subprocess.run")
    wrapper = MergeWrapper()
    wrapper.recwrap = mocker.Mock(recipe_step={"job_parameters": params})

    assert wrapper.run() is False, "an unreadable limit must fail the job"
    assert not run.called, "no DIALS stage may run on a bad request"


# The report SynchWeb shows first: a card with the release, the model,
# the cut and where it came from, then the shell statistics with the
# rows no shell has left out (here the anomalous ones, as after a merge
# without anomalous statistics), and every stage as a bar scaled to the
# longest, grouped by the tool that ran it.
def test_the_report_lays_out_the_card_statistics_and_timings():
    def shell(low, high, isig, comp, cc, n, u):
        return {
            "res_lim_low": low,
            "res_lim_high": high,
            "r_meas_all_iplusi_minus": 0.083,
            "r_pim_all_iplusi_minus": 0.013,
            "mean_i_sig_i": isig,
            "completeness": comp,
            "multiplicity": 40.1,
            "cc_half": cc,
            "anom_completeness": None,
            "n_tot_obs": n,
            "n_tot_unique_obs": u,
        }

    # 20 s fills the 40-cell bar, so 5.3 s is 10.6 cells and 1.5 s is 3
    pipeline = [
        {"stage": "spotfinder", "exit_code": 0, "duration": 20.0},
        {"stage": "integrator", "exit_code": 0, "duration": 5.3},
    ]
    merge = [{"stage": "merge", "exit_code": 0, "duration": 1.5}]
    model = {
        "image_range": (1, 3600),
        "spacegroup": "I 2 3",
        "cell": (77.8, 77.8, 77.8, 90.0, 90.0, 90.0),
    }
    shells = [
        shell(54.8, 2.34, 30.1, 99.9, 0.999, 140000, 3490),
        shell(54.8, 6.35, 90.2, 99.5, 1.0, 6100, 175),
        shell(2.38, 2.34, 3.1, 100.0, 0.81, 7300, 174),
    ]

    report = format_report(pipeline, merge, model, shells, 2.34, "estimate", "2026.11")

    def card(row: str) -> str:
        return f"│  {row:<60}  │\n"

    assert report == (
        f"╭─ Fast Feedback Service GPU {'─' * 26} 2026.11 ─╮\n"
        + card("I 2 3   77.80 77.80 77.80 90.00 90.00 90.00")
        + card("Images 1 -> 3600")
        + card("2.34 Å (CC½ fit)   CC½ 0.999   99.9 % complete")
        + card("ffs 25.3 s   DIALS 1.5 s")
        + f"╰{'─' * 64}╯\n"
        "\n"
        "                     Overall   Inner   Outer\n"
        "      Low resolution   54.80   54.80    2.38\n"
        "     High resolution    2.34    6.35    2.34\n"
        "               Rmeas   0.083   0.083   0.083\n"
        "                Rpim   0.013   0.013   0.013\n"
        "             I/sigma   30.10   90.20    3.10\n"
        "        Completeness    99.9    99.5   100.0\n"
        "        Multiplicity    40.1    40.1    40.1\n"
        "                 CC½   0.999   1.000   0.810\n"
        "               Nrefl  140000    6100    7300\n"
        "             Nunique    3490     175     174\n"
        "\n"
        f"  ffs   spotfinder             20.0 s  {'█' * 40}\n"
        f"        integrator              5.3 s  {'█' * 10}▋\n"
        "  DIALS merge                   1.5 s  ███\n"
        "        total                  26.8 s\n"
    ), "the report must lay out the card, the shells and the stage bars"
