"""Unit tests for the cluster service submit_to_slurm function."""

from __future__ import annotations

import logging
from unittest import mock

import pytest
import requests
from zocalo.util import slurm

from dlstbx.services.cluster import JobSubmissionParameters, submit_to_slurm


@pytest.fixture
def mock_slurm_api():
    with mock.patch(
        "dlstbx.services.cluster.slurm.SlurmRestApi.from_zocalo_configuration"
    ) as mock_api_class:
        api = mock.Mock()
        api.user_name = "gda2"
        api.submit_job = mock.Mock()
        mock_api_class.return_value = api
        yield api


@pytest.fixture
def mock_successful_submission_response():
    response = mock.Mock()
    response.job_id = 12345
    response.errors = None
    return response


@pytest.fixture
def mock_error_response():
    error1 = slurm.models.OpenapiError(error_number=1, error="Invalid request")
    error2 = slurm.models.OpenapiError(error_number=2, error="Permission denied")

    response = slurm.models.OpenapiJobSubmitResponse(
        errors=slurm.models.OpenapiErrors(root=[error1, error2])
    )
    return response


@pytest.fixture
def logger():
    return logging.getLogger("test_logger")


@pytest.fixture
def mock_config():
    config = mock.Mock()
    config.active_environments = ["live"]
    return config


@pytest.fixture
def temp_working_dir(tmp_path):
    return tmp_path / "work"


def test_submit_to_slurm_success(
    mock_slurm_api,
    mock_successful_submission_response,
    logger,
    mock_config,
    temp_working_dir,
):
    mock_slurm_api.submit_job.return_value = mock_successful_submission_response

    params = JobSubmissionParameters(commands="echo 'Hello'", partition="cs04r")

    job_id = submit_to_slurm(
        params,
        temp_working_dir,
        logger,
        mock_config,
        scheduler="slurm",
        recipewrapper="/tmp/recipe.json",
    )

    assert job_id == 12345
    assert mock_slurm_api.submit_job.call_count == 1

    job_submission = mock_slurm_api.submit_job.call_args[0][0]
    assert isinstance(job_submission, slurm.models.JobSubmitReq)
    assert job_submission.job.partition == "cs04r"
    assert job_submission.script.startswith("#!/bin/bash")


def test_submit_to_slurm_response_errors(
    mock_slurm_api,
    mock_error_response,
    logger,
    mock_config,
    temp_working_dir,
):
    mock_slurm_api.submit_job.return_value = mock_error_response

    params = JobSubmissionParameters(commands="echo 'Hello'", partition="cs04r")

    job_id = submit_to_slurm(
        params,
        temp_working_dir,
        logger,
        mock_config,
        scheduler="slurm",
        recipewrapper="/tmp/recipe.json",
    )

    assert job_id is None


def test_submit_to_slurm_http_error(
    mock_slurm_api,
    logger,
    mock_config,
    temp_working_dir,
):
    error_response = mock.Mock()
    error_response.text = "Internal Server Error"
    http_error = requests.HTTPError("500 Server Error")
    http_error.response = error_response
    mock_slurm_api.submit_job.side_effect = http_error

    params = JobSubmissionParameters(commands="echo 'Hello'", partition="cs04r")

    job_id = submit_to_slurm(
        params,
        temp_working_dir,
        logger,
        mock_config,
        scheduler="slurm",
        recipewrapper="/tmp/recipe.json",
    )

    assert job_id is None


@pytest.fixture
def cluster_service(mock_config):
    from dlstbx.services.cluster import DLSCluster

    service = DLSCluster(environment={"config": mock_config})
    service._transport = mock.Mock()
    service.initializing()
    service.schedulers = {"slurm": mock.Mock(return_value=12345)}
    return service


def make_rw(parameters, transport):
    from workflows.recipe.wrapper import RecipeWrapper

    return RecipeWrapper(
        message={
            "recipe": {
                "1": {"service": "cluster", "queue": "x", "parameters": parameters},
                "start": [(1, [])],
            },
            "recipe-pointer": 1,
            "environment": {"ID": "abc"},
            "payload": None,
        },
        transport=transport,
    )


HEADER = {"message-id": 1, "subscription": 1}


def valid_parameters(tmp_path, **kwargs):
    return {
        "cluster": {"commands": ["echo $RECIPEWRAP", "echo $$ $ID"]},
        "workingdir": str(tmp_path / "work"),
        "recipewrapper": str(tmp_path / "wrap.json"),
        **kwargs,
    }


def test_run_submit_job(cluster_service, tmp_path):
    transport = cluster_service._transport
    rw = make_rw(valid_parameters(tmp_path), transport)
    cluster_service.run_submit_job(rw, HEADER, None)

    submit = cluster_service.schedulers["slurm"]
    submit.assert_called_once()
    params, working_directory = submit.call_args.args[:2]
    # Only the service's own placeholders are replaced in the commands
    assert params.commands == f"echo {tmp_path / 'wrap.json'}\necho $$ $ID"
    assert working_directory == tmp_path / "work"
    assert working_directory.is_dir()
    assert (tmp_path / "wrap.json").is_file()
    transport.ack.assert_called_once()
    transport.nack.assert_not_called()


@pytest.mark.parametrize(
    "change",
    [
        {"cluster": None},
        {"cluster": {"partition": "no commands"}},
        {"workingdir": "relative/path"},
        {"workingdir": None},
        {"recipewrapper": None},
        {"cluster_submission_parameters": {"queue": "old"}},
    ],
)
def test_run_submit_job_rejects_invalid_parameters(cluster_service, tmp_path, change):
    transport = cluster_service._transport
    parameters = {
        k: v
        for k, v in {**valid_parameters(tmp_path), **change}.items()
        if v is not None
    }
    rw = make_rw(parameters, transport)
    cluster_service.run_submit_job(rw, HEADER, None)

    cluster_service.schedulers["slurm"].assert_not_called()
    transport.nack.assert_called_once_with(HEADER, requeue=False)
    # Rejected before any job files are written
    assert not (tmp_path / "wrap.json").exists()


def test_run_submit_job_unknown_scheduler_is_requeued(cluster_service, tmp_path):
    transport = cluster_service._transport
    parameters = valid_parameters(tmp_path)
    parameters["cluster"]["scheduler"] = "unknown"
    rw = make_rw(parameters, transport)
    cluster_service.run_submit_job(rw, HEADER, None)

    cluster_service.schedulers["slurm"].assert_not_called()
    transport.nack.assert_called_once_with(HEADER)
