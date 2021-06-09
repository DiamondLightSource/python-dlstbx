from unittest import mock
import pytest
import time

import dlstbx.dc_sim.check
import dlstbx.dc_sim.definitions

all_programs = ["relion"]


def test_check_relion_outcomes_pass_checks():
    frame_numbers = (
        list(range(21, 32)) + list(range(35, 38)) + [39, 40] + list(range(42, 50))
    )

    def db_motion_corr(i):
        motion_corr = mock.Mock()
        motion_corr.micrographFullPath = (
            f"MotionCorr/job002/Movies/Frames/20170629_000{i}_frameImage.mrc"
        )
        motion_corr.totalMotion = 15
        motion_corr.averageMotionPerFrame = 16
        return motion_corr

    db_ctf = mock.Mock()
    db_ctf.astigmatism = 247
    db_ctf.astigmatismAngle = 83
    db_ctf.estimatedResolution = 5
    db_ctf.estimatedDefocus = 10800
    db_ctf.ccValue = 0.15

    dc_results = {
        "motion_correction": [db_motion_corr(frame) for frame in frame_numbers],
        "ctf": [db_ctf for _ in frame_numbers],
    }

    expected_outcome = dlstbx.dc_sim.definitions.tests.get("relion", {}).get("results")

    check_result = dlstbx.dc_sim.check.check_relion_outcomes(
        dc_results, expected_outcome, 1
    )
    assert check_result["relion"]["success"]


def test_check_relion_outcomes_fail_checks():
    frame_numbers = (
        list(range(21, 32)) + list(range(35, 38)) + [39, 40] + list(range(42, 50))
    )

    def db_motion_corr_f(i):
        motion_corr = mock.Mock()
        motion_corr.micrographFullPath = (
            f"MotionCorr/job002/Movies/Frames/20170629_000{i}_frameImage.mrc"
        )
        motion_corr.totalMotion = 15
        if i == 30:
            motion_corr.averageMotionPerFrame = -16
        else:
            motion_corr.averageMotionPerFrame = 16
        return motion_corr

    db_ctf_f = mock.Mock()
    db_ctf_f.astigmatism = 247
    db_ctf_f.astigmatismAngle = 83
    db_ctf_f.estimatedResolution = 5
    db_ctf_f.estimatedDefocus = 10800
    db_ctf_f.ccValue = 0.15

    dc_results_f = {
        "motion_correction": [db_motion_corr_f(frame) for frame in frame_numbers],
        "ctf": [db_ctf_f for _ in frame_numbers],
    }

    expected_outcome_f = dlstbx.dc_sim.definitions.tests.get("relion", {}).get(
        "results"
    )

    check_result = dlstbx.dc_sim.check.check_relion_outcomes(
        dc_results_f, expected_outcome_f, 1
    )
    assert not check_result["relion"]["success"]
    assert check_result["relion"]["reason"] == [
        f"averageMotionPerFrame: -16 outside range {pytest.approx(16, 0.75)}, program: relion, JobID:1"
    ]


@mock.patch("dlstbx.dc_sim.check._retrieve_motioncorr")
@mock.patch("dlstbx.dc_sim.check._retrieve_ctf")
@mock.patch("ispyb.sqlalchemy.url")
@mock.patch("sqlalchemy.create_engine")
@mock.patch("sqlalchemy.orm.Session")
def test_check_test_outcome_success(
    mock_sess, mock_eng, mock_url, mock_ctf, mock_mcorr
):

    frame_numbers = (
        list(range(21, 32)) + list(range(35, 38)) + [39, 40] + list(range(42, 50))
    )

    mock_url.return_value = ""

    def db_motion_corr(i):
        motion_corr = mock.Mock()
        motion_corr.micrographFullPath = (
            f"MotionCorr/job002/Movies/Frames/20170629_000{i}_frameImage.mrc"
        )
        motion_corr.totalMotion = 15
        motion_corr.averageMotionPerFrame = 16
        return motion_corr

    db_ctf = mock.Mock()
    db_ctf.astigmatism = 247
    db_ctf.astigmatismAngle = 83
    db_ctf.estimatedResolution = 5
    db_ctf.estimatedDefocus = 10800
    db_ctf.ccValue = 0.15

    mock_mcorr.return_value = [db_motion_corr(_) for _ in frame_numbers], 1
    mock_ctf.return_value = [db_ctf for _ in frame_numbers]

    test = {
        "beamline": "m12",
        "scenario": "relion",
        "DCIDs": [1],
        "JobIDs": [1],
        "time_start": time.time() - 900,
        "time_end": time.time() - 1,
    }
    test = dlstbx.dc_sim.check.check_test_outcome(test, mock.Mock())
    assert test["success"]


@mock.patch("dlstbx.dc_sim.check._retrieve_motioncorr")
@mock.patch("dlstbx.dc_sim.check._retrieve_ctf")
@mock.patch("ispyb.sqlalchemy.url")
@mock.patch("sqlalchemy.create_engine")
@mock.patch("sqlalchemy.orm.Session")
def test_check_test_outcome_failure(
    mock_sess, mock_eng, mock_url, mock_ctf, mock_mcorr
):

    frame_numbers = (
        list(range(21, 32)) + list(range(35, 38)) + [39, 40] + list(range(42, 50))
    )

    mock_url.return_value = ""

    def db_motion_corr(i):
        motion_corr = mock.Mock()
        motion_corr.micrographFullPath = (
            f"MotionCorr/job002/Movies/Frames/20170629_000{i}_frameImage.mrc"
        )
        motion_corr.totalMotion = 15
        if i == 30:
            motion_corr.averageMotionPerFrame = 0
        else:
            motion_corr.averageMotionPerFrame = 16
        return motion_corr

    db_ctf = mock.Mock()
    db_ctf.astigmatism = 247
    db_ctf.astigmatismAngle = 83
    db_ctf.estimatedResolution = 5
    db_ctf.estimatedDefocus = 10800
    db_ctf.ccValue = 0.15

    mock_mcorr.return_value = [db_motion_corr(_) for _ in frame_numbers], 1
    mock_ctf.return_value = [db_ctf for _ in frame_numbers]

    test = {
        "beamline": "m12",
        "scenario": "relion",
        "DCIDs": [1],
        "JobIDs": [1],
        "time_start": time.time() - 900,
        "time_end": time.time() - 1,
    }
    test_checked = dlstbx.dc_sim.check.check_test_outcome(test, mock.Mock())
    assert not test_checked["success"]
    assert (
        test_checked["reason"]
        == f"averageMotionPerFrame: 0 outside range {pytest.approx(16, 0.75)}, program: relion, JobID:1"
    )
