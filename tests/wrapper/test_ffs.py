from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from dlstbx.wrapper.ffs import IndexIntegrateWrapper, SpotfindIndexIntegrateWrapper


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
