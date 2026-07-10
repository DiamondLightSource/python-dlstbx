from __future__ import annotations

import json
from unittest import mock

import numpy as np
import pytest

import dlstbx.util.xray_centering
import dlstbx.util.xray_centering_3d


def test_gridscan3d():
    # Data from:
    #   dials.import /dls/i03/data/2020/cm26458-4/gw/20201007/lyso_-45_1_master.h5 output.experiments=lyso_m45_1.expt
    #   dials.find_spots lyso_m45_1.expt output.reflections=lyso_m45_1.refl
    #   dials.import /dls/i03/data/2020/cm26458-4/gw/20201007/lyso_45_1_master.h5 output.experiments=lyso_p45_1.expt
    #   dials.find_spots lyso_p45_1.expt output.reflections=lyso_p45_1.refl
    #   dlstbx.gridscan3d lyso_m45_1.{expt,refl} lyso_p45_1.{expt,refl} -vvv plot=True

    # fmt: off
    data = np.array(
        [
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 28, 19, 0, 0, 0, 0, 0, 0, 10, 58, 87, 89, 52, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 12, 32, 32, 22, 0, 0, 0, 2, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 6, 5, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 37, 51, 40, 20, 0, 0, 0, 0, 11, 50, 72, 82, 41, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 8, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ]
    )
    # fmt: on
    sample_id = 12345

    steps = (14, 9)
    data = tuple(
        dlstbx.util.xray_centering.reshape_grid(
            d,
            steps,
            snaked=True,
            orientation=dlstbx.util.xray_centering.Orientation.HORIZONTAL,
        )
        for d in data
    )

    results = dlstbx.util.xray_centering_3d.gridscan3d(
        data, sample_id=sample_id, plot=False
    )
    assert len(results) == 1
    result_d = results[0].model_dump()
    # check that the results are JSON-serializable
    json.dumps(result_d)
    assert result_d == {
        "centre_of_mass": pytest.approx(
            (4.858891830104681, 4.500240644928408, 4.197689808687282)
        ),
        "max_voxel": (5, 4, 4),
        "max_count": 7298,
        "n_voxels": 9,
        "total_count": 33244,
        "bounding_box": ((3, 3, 3), (7, 6, 5)),
        "sample_id": 12345,
    }


def test_gridscan3d_with_absolute_threshold():
    # Data from:
    #   dials.import /dls/i03/data/2020/cm26458-4/gw/20201007/lyso_-45_1_master.h5 output.experiments=lyso_m45_1.expt
    #   dials.find_spots lyso_m45_1.expt output.reflections=lyso_m45_1.refl
    #   dials.import /dls/i03/data/2020/cm26458-4/gw/20201007/lyso_45_1_master.h5 output.experiments=lyso_p45_1.expt
    #   dials.find_spots lyso_p45_1.expt output.reflections=lyso_p45_1.refl
    #   dlstbx.gridscan3d lyso_m45_1.{expt,refl} lyso_p45_1.{expt,refl} -vvv plot=True

    # fmt: off
    data = np.array(
        [
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 28, 19, 0, 0, 0, 0, 0, 0, 10, 58, 87, 89, 52, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 12, 32, 32, 22, 0, 0, 0, 2, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 6, 5, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 37, 51, 40, 20, 0, 0, 0, 0, 11, 50, 72, 82, 41, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 8, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ]
    )
    # fmt: on
    sample_id = 12345

    steps = (14, 9)
    data = tuple(
        dlstbx.util.xray_centering.reshape_grid(
            d,
            steps,
            snaked=True,
            orientation=dlstbx.util.xray_centering.Orientation.HORIZONTAL,
        )
        for d in data
    )

    results = dlstbx.util.xray_centering_3d.gridscan3d(
        data, threshold=0.05, threshold_absolute=5, sample_id=sample_id, plot=False
    )
    assert len(results) == 1
    result_d = results[0].model_dump()
    # check that the results are JSON-serializable
    json.dumps(result_d)
    assert result_d == {
        "centre_of_mass": pytest.approx((4.74100, 4.56832, 4.13685)),
        "max_voxel": (5, 4, 4),
        "max_count": 7298.0,
        "n_voxels": 20,
        "total_count": 44128.0,
        "bounding_box": ((2, 3, 2), (7, 6, 6)),
        "sample_id": 12345,
    }


def test_gridscan3d_for_multipin_sample():
    # Create simple dataset comprising two 12x1 grids with two real peaks and one noise peak.
    ydata = np.array([0, 3, 3, 0, 0, 20, 10, 0, 0, 1, 0, 1])
    zdata = np.array([0, 3, 3, 0, 0, 10, 10, 0, 0, 1, 1, 1])
    data = np.array([ydata, zdata])
    steps = (12, 1)

    data = tuple(
        dlstbx.util.xray_centering.reshape_grid(
            d,
            steps,
            snaked=True,
            orientation=dlstbx.util.xray_centering.Orientation.HORIZONTAL,
        )
        for d in data
    )

    sample_id = 12345
    multipin_sample_ids = {1: 12345, 2: 12346, 3: 12347}
    well_limits = [(0.0, 4.0), (4.0, 7.0), (8.0, 11.0)]

    # Fit peaks with multipin threshold parameters and check that correct results are obtained.
    results = dlstbx.util.xray_centering_3d.gridscan3d(
        data,
        threshold=0.25,
        threshold_absolute=3,
        sample_id=sample_id,
        plot=False,
        multipin_sample_ids=multipin_sample_ids,
        well_limits=well_limits,
    )

    assert len(results) == 2
    expected_results = [
        {
            "centre_of_mass": mock.ANY,
            "max_voxel": (5, 0, 0),
            "max_count": 200.0,
            "n_voxels": 2,
            "total_count": 300.0,
            "sample_id": 12346,
            "bounding_box": ((5, 0, 0), (7, 1, 1)),
        },
        {
            "centre_of_mass": mock.ANY,
            "max_voxel": (1, 0, 0),
            "max_count": 9.0,
            "n_voxels": 2,
            "total_count": 18.0,
            "sample_id": 12345,
            "bounding_box": ((1, 0, 0), (3, 1, 1)),
        },
    ]

    for result_num, result in enumerate(results):
        result_d = result.model_dump()
        # check that the results are JSON-serializable
        json.dumps(result_d)
        assert result_d == expected_results[result_num]
