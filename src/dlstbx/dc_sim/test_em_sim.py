import time
from dataclasses import dataclass
from unittest import mock

import pytest

import dlstbx.dc_sim.check
import dlstbx.dc_sim.definitions


@dataclass(frozen=True)
class MotioncorrectionResult:
    micrographFullPath: str
    totalMotion: float = 15
    averageMotionPerFrame: float = 0.5


@dataclass(frozen=True)
class CTFResult:
    MotionCorrection: MotioncorrectionResult
    astigmatism: float = 247
    astigmatismAngle: float = 83
    estimatedResolution = 5
    estimatedDefocus = 10800
    ccValue = 0.15


def test_check_relion_outcomes_pass_checks():
    frame_numbers = dlstbx.dc_sim.definitions.tests["relion"]["frames"]

    dc_results = {
        "motion_correction": [
            MotioncorrectionResult(
                micrographFullPath=f"MotionCorr/job002/Movies/Frames/20170629_000{frame}_frameImage.mrc"
            )
            for frame in frame_numbers
        ],
        "ctf": [
            CTFResult(
                MotionCorrection=MotioncorrectionResult(
                    micrographFullPath=f"MotionCorr/job002/Movies/Frames/20170629_000{frame}_frameImage.mrc"
                ),
                astigmatism=(0 if frame == 23 else 247),
                astigmatismAngle=(-35 if frame == 23 else 83),
            )
            for frame in frame_numbers
        ],
    }

    expected_outcome = dlstbx.dc_sim.definitions.tests.get("relion", {}).get("results")

    check_result = dlstbx.dc_sim.check.check_relion_outcomes(
        dc_results, expected_outcome, 1
    )
    assert check_result["relion"]["success"]


def test_check_relion_outcomes_fail_checks():
    frame_numbers = dlstbx.dc_sim.definitions.tests["relion"]["frames"]

    def db_motion_corr_f(frame):
        if frame == 30:
            return MotioncorrectionResult(
                micrographFullPath=f"MotionCorr/job002/Movies/Frames/20170629_000{frame}_frameImage.mrc",
                averageMotionPerFrame=-16,
            )
        else:
            return MotioncorrectionResult(
                micrographFullPath=f"MotionCorr/job002/Movies/Frames/20170629_000{frame}_frameImage.mrc"
            )

    dc_results_f = {
        "motion_correction": [db_motion_corr_f(frame) for frame in frame_numbers],
        "ctf": [
            CTFResult(
                MotionCorrection=db_motion_corr_f(frame),
                astigmatism=(0 if frame == 23 else 247),
                astigmatismAngle=(-35 if frame == 23 else 83),
            )
            for frame in frame_numbers
        ],
    }

    expected_outcome_f = dlstbx.dc_sim.definitions.tests.get("relion", {}).get(
        "results"
    )

    check_result = dlstbx.dc_sim.check.check_relion_outcomes(
        dc_results_f, expected_outcome_f, 1
    )
    assert not check_result["relion"]["success"]
    assert check_result["relion"]["reason"] == [
        f"motion correction for MotionCorr/job002/Movies/Frames/20170629_00030_frameImage.mrc averageMotionPerFrame: -16 outside range {pytest.approx(0.5, 1)} in JobID:1"
    ]


@mock.patch("dlstbx.dc_sim.check._retrieve_motioncorr")
@mock.patch("dlstbx.dc_sim.check._retrieve_ctf")
@mock.patch("sqlalchemy.create_engine")
@mock.patch("sqlalchemy.orm.Session")
def test_check_test_outcome_success(mock_sess, mock_eng, mock_ctf, mock_mcorr):
    frame_numbers = dlstbx.dc_sim.definitions.tests["relion"]["frames"]

    def db_motion_corr(i):
        motion_corr = mock.Mock()
        motion_corr.micrographFullPath = (
            f"MotionCorr/job002/Movies/Frames/20170629_000{i}_frameImage.mrc"
        )
        motion_corr.totalMotion = 15
        motion_corr.averageMotionPerFrame = 0.5
        return motion_corr

    mock_mcorr.return_value = [db_motion_corr(_) for _ in frame_numbers], 1
    mock_ctf.return_value = [
        CTFResult(
            MotionCorrection=db_motion_corr(frame),
            astigmatism=(0 if frame == 23 else 247),
            astigmatismAngle=(-35 if frame == 23 else 83),
        )
        for frame in frame_numbers
    ]

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
@mock.patch("sqlalchemy.create_engine")
@mock.patch("sqlalchemy.orm.Session")
def test_check_test_outcome_failure(mock_sess, mock_eng, mock_ctf, mock_mcorr):
    frame_numbers = dlstbx.dc_sim.definitions.tests["relion"]["frames"]

    def db_motion_corr(i):
        motion_corr = mock.Mock()
        motion_corr.micrographFullPath = (
            f"MotionCorr/job002/Movies/Frames/20170629_000{i}_frameImage.mrc"
        )
        motion_corr.totalMotion = 15
        if i == 30:
            motion_corr.averageMotionPerFrame = -0.1
        else:
            motion_corr.averageMotionPerFrame = 0.5
        return motion_corr

    mock_mcorr.return_value = [db_motion_corr(frame) for frame in frame_numbers], 1
    mock_ctf.return_value = [
        CTFResult(
            MotionCorrection=db_motion_corr(frame),
            astigmatism=(0 if frame == 23 else 247),
            astigmatismAngle=(-35 if frame == 23 else 83),
        )
        for frame in frame_numbers
    ]

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
    assert test_checked["reason"].endswith(
        f"averageMotionPerFrame: -0.1 outside range {pytest.approx(0.5, 1)} in JobID:1"
    )
