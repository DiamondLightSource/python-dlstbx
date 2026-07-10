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
