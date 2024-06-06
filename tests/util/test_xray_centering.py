from __future__ import annotations

import dlstbx.util.xray_centering
import numpy as np
import pytest


def test_xray_centering():
    data = np.array(
        [
            [0, 73, 187, 119, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0, 144, 449, 539, 418, 141, 2],
            [0, 100, 402, 592, 538, 394, 221, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 151, 352, 440, 506, 515, 229, 13, 0],
            [0, 2, 27, 229, 415, 481, 387, 389, 90, 1, 0, 0, 0, 0],
            [0, 0, 0, 0, 107, 441, 436, 385, 289, 96, 74, 18, 0, 0],
            [0, 0, 0, 25, 26, 41, 179, 376, 382, 295, 28, 0, 0, 0],
            [0, 0, 14, 115, 217, 159, 52, 29, 44, 9, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 11, 57, 24, 22, 16, 32, 41, 11, 0],
            [26, 48, 27, 24, 16, 8, 2, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 28, 48],
        ]
    ).flatten()
    results, stdout = dlstbx.util.xray_centering.gridscan2d(
        data,
        steps=(14, 11),
        box_size_px=(1.25, 1.25),
        snapshot_offset=(396.2, 241.2),
        snaked=True,
        orientation=dlstbx.util.xray_centering.Orientation.HORIZONTAL,
    )
    assert "There are 592 reflections in image #32." in stdout
    assert "[  .   . 402 592 538 394   .   .   .   .   .   .   .   .]" in stdout
    assert results.dict() == {
        "centre_of_mass": (5.45, 3.8),
        "max_voxel": (3, 2),
        "max_count": 592.0,
        "n_voxels": 20,
        "total_count": 8837.0,
        "steps": (14, 11),
        "box_size_px": (1.25, 1.25),
        "snapshot_offset": (396.2, 241.2),
        "centre_x": 403.0125,
        "centre_y": 245.95,
        "centre_x_box": 5.45,
        "centre_y_box": 3.8,
        "status": "ok",
        "message": "ok",
        "best_image": 32,
        "reflections_in_best_image": 592,
        "best_region": [
            (1, 2),
            (1, 3),
            (1, 4),
            (2, 2),
            (2, 3),
            (2, 4),
            (2, 5),
            (3, 3),
            (3, 4),
            (3, 5),
            (3, 6),
            (4, 4),
            (4, 5),
            (4, 6),
            (4, 7),
            (5, 6),
            (5, 7),
            (5, 8),
            (6, 7),
            (6, 8),
        ],
    }

    # verify that the results can be serialized to json
    assert results.json()


def test_xray_centering_second_example():
    # fmt: off
    data = np.array([
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  0,  1,  0,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  2,  2,  2,  3,  3,  2,  2,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  1,  3,  5,  9,  4, 11,  7,  7,  2,  4,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  8,  5,  9,  6,  7,  7,  9,  6,  3,  1,  2,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  1,  1,  3,  4,  6,  6,  7, 10,  9,  8,  9,  6,  3,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  3,  8,  9,  9,  7,  9, 10, 11, 10,  6,  2,  2,  1,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  2,  3,  5,  5, 11, 10,  9, 11,  8,  8,  7,  9,  5,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  3, 12, 12,  7,  9,  7,  8, 14,  8,  6,  3,  2,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  2,  7,  9, 12,  9,  8,  6, 10,  8,  9,  4,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  2,  4,  8,  4,  6,  7, 11,  5, 10,  4,  2,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  1,  3,  6,  8,  7,  7,  8,  6,  7,  5,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  4,  6,  8,  6,  6,  4,  3,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  5,  4,  7,  7,  3,  2,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  3,  2,  2,  2,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  2,  1,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0],
       [ 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0]
    ]).flatten()
    # fmt: on

    results, stdout = dlstbx.util.xray_centering.gridscan2d(
        data,
        steps=(36, 23),
        box_size_px=(17.6678445229682, 17.6678445229682),
        snapshot_offset=(339.919, 182.59),
        snaked=True,
        orientation=dlstbx.util.xray_centering.Orientation.HORIZONTAL,
    )

    assert "There are 14 reflections in image #351." in stdout
    assert (
        "[ .  .  .  .  .  .  .  .  7  9 12  9  8  . 10  8  9  .  .  .  .  .  .  ."
        in stdout
    )
    # fmt: off
    best_region = sorted([(4, 11), (4, 13), (4, 14), (4, 15), (5, 10), (5, 11), (5, 12), (5, 14), (5, 16), (6, 11), (6, 12), (6, 13), (6, 14), (6, 15), (7, 8), (7, 9), (7, 10), (7, 11), (7, 12), (7, 13), (7, 14), (7, 15), (8, 9), (8, 10), (8, 11), (8, 12), (8, 13), (8, 14), (8, 15), (8, 16), (9, 8), (9, 9), (9, 10), (9, 11), (9, 12), (9, 13), (9, 14), (9, 15), (10, 8), (10, 9), (10, 10), (10, 11), (10, 12), (10, 14), (10, 15), (10, 16), (11, 9), (11, 11), (11, 12), (11, 15), (12, 11), (12, 12), (12, 13), (12, 14), (12, 16), (13, 14), (14, 15), (14, 16)])
    # fmt: on
    assert results.dict() == {
        "centre_of_mass": (12.879310344827585, 8.913793103448276),
        "max_voxel": (9, 9),
        "max_count": 14.0,
        "n_voxels": 58,
        "total_count": 507.0,
        "steps": (36, 23),
        "box_size_px": (17.6678445229682, 17.6678445229682),
        "snapshot_offset": (339.919, 182.59),
        "centre_x": 567.4686527354697,
        "centre_y": 340.0775106616303,
        "centre_x_box": 12.879310344827585,
        "centre_y_box": 8.913793103448276,
        "status": "ok",
        "message": "ok",
        "best_image": 351,
        "reflections_in_best_image": 14,
        "best_region": best_region,
    }


def test_vertical_1d():
    # fmt: off
    data = np.array([0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 9, 20, 21, 21, 11, 3, 13, 35, 40, 45, 49, 53, 59, 75, 76, 78, 80, 75, 78, 75, 79, 83, 86, 90, 94, 107, 114, 107, 99, 91, 86, 77, 73, 63, 52, 37, 22, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    # fmt: on
    results, stdout = dlstbx.util.xray_centering.gridscan2d(
        data,
        steps=(1, 80),
        box_size_px=(24.096385542168676, 6.024096385542169),
        snapshot_offset=(446.952, 123.036),
        snaked=True,
        orientation=dlstbx.util.xray_centering.Orientation.VERTICAL,
    )
    assert "There are 114 reflections in image #52." in stdout
    assert results.dict() == {
        "centre_of_mass": (0.5, 48.0),
        "max_voxel": (0, 51),
        "max_count": 114.0,
        "n_voxels": 22,
        "total_count": 1845.0,
        "steps": (1, 80),
        "box_size_px": (24.096385542168676, 6.024096385542169),
        "snapshot_offset": (446.952, 123.036),
        "centre_x": 459.00019277108436,
        "centre_y": 412.1926265060241,
        "centre_x_box": 0.5,
        "centre_y_box": 48.0,
        "status": "ok",
        "message": "ok",
        "best_image": 52,
        "reflections_in_best_image": 114,
        "best_region": [
            (37, 0),
            (38, 0),
            (39, 0),
            (40, 0),
            (41, 0),
            (42, 0),
            (43, 0),
            (44, 0),
            (45, 0),
            (46, 0),
            (47, 0),
            (48, 0),
            (49, 0),
            (50, 0),
            (51, 0),
            (52, 0),
            (53, 0),
            (54, 0),
            (55, 0),
            (56, 0),
            (57, 0),
            (58, 0),
        ],
    }


def test_vertical_2d():
    # fmt: off
    data = np.array([0, 0, 6, 54, 38, 0, 5, 41, 44, 5, 0, 0, 0, 0, 0, 3, 4, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    # fmt: on
    results, stdout = dlstbx.util.xray_centering.gridscan2d(
        data,
        steps=(5, 6),
        box_size_px=(45.45454545454545, 45.45454545454545),
        snapshot_offset=(339.273, 236.727),
        snaked=True,
        orientation=dlstbx.util.xray_centering.Orientation.VERTICAL,
    )
    assert "There are 54 reflections in image #4." in stdout
    assert results.dict() == {
        "centre_of_mass": (1.0, 4.0),
        "max_voxel": (0, 3),
        "max_count": 54.0,
        "n_voxels": 4,
        "total_count": 177.0,
        "steps": (5, 6),
        "box_size_px": (45.45454545454545, 45.45454545454545),
        "snapshot_offset": (339.273, 236.727),
        "centre_x": 384.72754545454546,
        "centre_y": 418.54518181818185,
        "centre_x_box": 1.0,
        "centre_y_box": 4.0,
        "status": "ok",
        "message": "ok",
        "best_image": 4,
        "reflections_in_best_image": 54,
        "best_region": [(3, 0), (3, 1), (4, 0), (4, 1)],
    }


def test_blank_scan():
    data = np.zeros((5, 6))
    results, stdout = dlstbx.util.xray_centering.gridscan2d(
        data,
        steps=(5, 6),
        box_size_px=(45.45, 45.45),
        snapshot_offset=(339.273, 236.727),
        snaked=True,
        orientation=dlstbx.util.xray_centering.Orientation.VERTICAL,
    )
    assert isinstance(stdout, str)
    assert results.dict() == {
        "centre_of_mass": None,
        "max_voxel": None,
        "max_count": None,
        "n_voxels": None,
        "total_count": None,
        "steps": (5, 6),
        "box_size_px": (45.45, 45.45),
        "snapshot_offset": (339.273, 236.727),
        "centre_x": None,
        "centre_y": None,
        "centre_x_box": None,
        "centre_y_box": None,
        "status": "fail",
        "message": "No good images found",
        "best_image": None,
        "reflections_in_best_image": None,
        "best_region": None,
    }


A = np.full((6, 6), 6, int)
B = np.full((6, 4), 5, int)
C = np.full((4, 4), 4, int)
D = np.full((4, 6), 3, int)


@pytest.mark.parametrize(
    ("data", "reflections_in_best_image"),
    (
        (np.ones(100, int), 1),
        (np.full(100, 5, int), 5),
        (np.block([[A, B], [C, D]]).flatten(), 6),
    ),
)
def test_single_connected_region(data, reflections_in_best_image):
    """
    Ensure that an X-ray centring grid can consist entirely of strong diffraction.

    Usually a grid scan will consist of some strongly diffracting images and some
    weakly diffracting images.  The X-ray centring utility differentiates between
    strong and weak and then finds connected regions of strong diffraction.  Usually
    there will be one or more connected regions, and some images that are weakly
    diffracting and hence disconnected.  Sometimes though, every image may be strongly
    diffracting.  In such cases the entire grid is a single connected region.  This
    is a valid (if trivial) case for X-ray centring, so we should accept it.

    Test that X-ray centring works on a data set in which every image meets the
    criterion for strong diffraction.  The default criterion is that an image
    contains a number of reflections equal to or greater than half the number of
    reflections in the strongest-diffracting image.
    """
    result, _ = dlstbx.util.xray_centering.gridscan2d(
        data=data,
        steps=(10, 10),
        box_size_px=(1, 1),
        snapshot_offset=(0, 0),
        snaked=False,
        orientation=dlstbx.util.xray_centering.Orientation.HORIZONTAL,
    )
    assert result.status == "ok"
    assert result.message == "ok"
    assert result.best_image == 1
    assert result.reflections_in_best_image == reflections_in_best_image
    np.testing.assert_array_equal(
        result.best_region, np.transpose(np.unravel_index(np.arange(100), (10, 10)))
    )
    assert result.centre_x == result.centre_x_box == 5
    assert result.centre_y == result.centre_y_box == 5
