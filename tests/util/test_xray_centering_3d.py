from __future__ import annotations

import json

import dlstbx.util.xray_centering
import dlstbx.util.xray_centering_3d
import numpy as np
import pytest


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

    results = dlstbx.util.xray_centering_3d.gridscan3d(data, plot=False)
    assert len(results) == 1
    result_d = results[0].dict()
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
    }
