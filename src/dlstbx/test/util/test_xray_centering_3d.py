from __future__ import annotations

import numpy as np

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

    max_idx = dlstbx.util.xray_centering_3d.gridscan3d(
        data,
        steps=(14, 9),
        snaked=True,
        orientation=dlstbx.util.xray_centering.Orientation.HORIZONTAL,
    )
    assert max_idx == (4, 5, 5)
