from __future__ import annotations

import json
from unittest import mock

import numpy as np
import pytest
import scipy.ndimage

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
        "ambiguous": False,
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
        "ambiguous": False,
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
            "ambiguous": False,
        },
        {
            "centre_of_mass": mock.ANY,
            "max_voxel": (1, 0, 0),
            "max_count": 9.0,
            "n_voxels": 2,
            "total_count": 18.0,
            "sample_id": 12345,
            "bounding_box": ((1, 0, 0), (3, 1, 1)),
            "ambiguous": False,
        },
    ]

    for result_num, result in enumerate(results):
        result_d = result.model_dump()
        # check that the results are JSON-serializable
        json.dumps(result_d)
        assert result_d == expected_results[result_num]


def test_gridscan3d_invert_total_count_and_ordering():
    # Pre-inverted (1/x) data along x for the two orthogonal scans.
    # Region A (x=0): original 0.5 in both scans -> inverted 2.0, product 4.0
    # Region B (x=2,3): original 1.0 in both scans -> inverted 1.0, product 1.0
    d0 = np.array([[2.0], [0.0], [1.0], [1.0], [0.0]])
    d1 = np.array([[2.0], [0.0], [1.0], [1.0], [0.0]])
    results = dlstbx.util.xray_centering_3d.gridscan3d(
        data=(d0, d1), threshold=0.25, invert=True
    )
    assert len(results) == 2
    # Ranked by the inverted signal: region A (4.0) before region B (1.0 + 1.0)
    assert results[0].max_voxel == (0, 0, 0)
    assert results[0].max_count == pytest.approx(4.0)  # still inverted
    # total_count in original units: sum of the per-voxel products of originals
    assert results[0].total_count == pytest.approx(0.5 * 0.5)
    assert results[1].total_count == pytest.approx(1.0 * 1.0 + 1.0 * 1.0)
    assert results[1].n_voxels == 2


def _sum_projections(f):
    """Line-integral projections of a 3D phantom f[x, y, z] along z and y."""
    return f.sum(axis=2), f.sum(axis=1)


def _max_projections(f):
    """Max projections of a 3D phantom f[x, y, z] along z and y."""
    return f.max(axis=2), f.max(axis=1)


def _true_com(f):
    return tuple(
        c + 0.5 for c in scipy.ndimage.center_of_mass(f, labels=(f > 0), index=1)
    )


def test_combine_projections():
    a = np.array([[1.0, 3.0], [0.0, 2.0]])
    b = np.array([[2.0, 2.0, 0.0], [4.0, 0.0, 0.0]])
    combine = dlstbx.util.xray_centering_3d.combine_projections

    product = combine(a, b, "product")
    assert product.shape == (2, 2, 3)
    np.testing.assert_allclose(product, a[:, :, None] * b[:, None, :])
    np.testing.assert_allclose(
        combine(a, b, "geometric_mean"), np.sqrt(a[:, :, None] * b[:, None, :])
    )
    np.testing.assert_allclose(
        combine(a, b, "min"), np.minimum(a[:, :, None], b[:, None, :])
    )
    # S(x) = (sum_y A + sum_z B) / 2 = [4, 3]
    np.testing.assert_allclose(
        combine(a, b, "normalised"), product / np.array([4.0, 3.0])[:, None, None]
    )
    # An empty x column must not produce NaN/inf
    a[1] = 0
    b[1] = 0
    assert np.all(np.isfinite(combine(a, b, "normalised")))

    with pytest.raises(ValueError):
        combine(a, b, "not_a_method")
    with pytest.raises(ValueError):
        combine(a, b[:1], "product")


def test_combine_normalised_reproduces_projections():
    rng = np.random.default_rng(42)
    # A separable phantom is exactly recovered from two projections
    f = (
        rng.random(6)[:, None, None]
        * rng.random(5)[None, :, None]
        * rng.random(4)[None, None, :]
    )
    a, b = _sum_projections(f)
    r = dlstbx.util.xray_centering_3d.combine_projections(a, b, "normalised")
    np.testing.assert_allclose(r, f)
    np.testing.assert_allclose(r.sum(axis=2), a)
    np.testing.assert_allclose(r.sum(axis=1), b)


def test_gridscan3d_normalised_removes_x_bias():
    # Single crystal with an asymmetric profile along x (rotation axis)
    w = np.array([0, 1, 2, 3, 6, 8, 0, 0], dtype=float)
    g = np.array([0, 1, 3, 1, 0, 0], dtype=float)
    h = np.array([0, 0, 2, 4, 2, 0, 0], dtype=float)
    f = w[:, None, None] * g[None, :, None] * h[None, None, :]
    truth = _true_com(f)

    results = {
        method: dlstbx.util.xray_centering_3d.gridscan3d(
            _sum_projections(f), threshold=1e-9, combine=method
        )
        for method in ("product", "normalised")
    }
    for method, res in results.items():
        assert len(res) == 1, method
        # y and z are unaffected by the choice of combination
        assert res[0].centre_of_mass[1:] == pytest.approx(truth[1:])

    normalised = results["normalised"][0]
    assert normalised.centre_of_mass == pytest.approx(truth)
    assert normalised.total_count == pytest.approx(f.sum())
    assert normalised.max_count == pytest.approx(f.max())

    # The plain product squares the x profile, pulling the centre to brighter x
    assert results["product"][0].centre_of_mass[0] > truth[0] + 0.1


def test_gridscan3d_geometric_mean_matches_product_peak():
    w = np.array([0, 1, 2, 3, 6, 8, 0, 0], dtype=float)
    g = np.array([0, 1, 3, 1, 0, 0], dtype=float)
    h = np.array([0, 0, 2, 4, 2, 0, 0], dtype=float)
    f = w[:, None, None] * g[None, :, None] * h[None, None, :]
    data = _sum_projections(f)

    (product,) = dlstbx.util.xray_centering_3d.gridscan3d(data, threshold=0.25)
    # threshold 0.5 on the geometric mean selects the same voxels as 0.25 on the product
    (geomean,) = dlstbx.util.xray_centering_3d.gridscan3d(
        data, threshold=0.5, combine="geometric_mean"
    )
    assert geomean.max_voxel == product.max_voxel
    assert geomean.n_voxels == product.n_voxels
    assert geomean.bounding_box == product.bounding_box
    assert geomean.max_count == pytest.approx(np.sqrt(product.max_count))


@pytest.mark.parametrize("combine", ["product", "normalised", "geometric_mean", "min"])
def test_gridscan3d_ambiguous_ghosts(combine):
    # Two crystals sharing the same x range at (y=1, z=5) and (y=6, z=1)
    f = np.zeros((6, 9, 8))
    f[2:4, 1:3, 5:7] = 10
    f[2:4, 6:8, 1:3] = 10
    results = dlstbx.util.xray_centering_3d.gridscan3d(
        _sum_projections(f), threshold=0.25, combine=combine
    )
    # Two real peaks plus two ghosts at (y=1, z=1) and (y=6, z=5)
    assert len(results) == 4
    assert all(r.ambiguous for r in results)
    peaks = {r.max_voxel[1:] for r in results}
    assert peaks == {(1, 5), (6, 1), (1, 1), (6, 5)}


def test_gridscan3d_not_ambiguous():
    # Two crystals at different x: no ghosts
    f = np.zeros((10, 9, 8))
    f[1:3, 1:3, 5:7] = 10
    f[6:8, 6:8, 1:3] = 5
    results = dlstbx.util.xray_centering_3d.gridscan3d(
        _sum_projections(f),
        threshold=0.25,
        multipin_sample_ids={1: 1, 2: 2},
        well_limits=[(0.0, 5.0), (5.0, 10.0)],
        combine="normalised",
    )
    assert len(results) == 2
    assert not any(r.ambiguous for r in results)

    # Two crystals at the same x and y but different z: both peaks are real
    f = np.zeros((6, 9, 8))
    f[2:4, 3:5, 0:2] = 10
    f[2:4, 3:5, 5:7] = 10
    results = dlstbx.util.xray_centering_3d.gridscan3d(
        _sum_projections(f), threshold=0.25, combine="normalised"
    )
    assert len(results) == 2
    assert not any(r.ambiguous for r in results)


def test_gridscan3d_min_for_max_like_metric():
    # Quality metric (higher is better) observed as a max along each beam path
    f = np.zeros((10, 6, 7))
    f[1:4, 1:3, 4:6] = 3.0
    f[6:9, 3:5, 1:3] = 7.0
    results = dlstbx.util.xray_centering_3d.gridscan3d(
        _max_projections(f),
        threshold=0.25,
        multipin_sample_ids={1: 1, 2: 2},
        well_limits=[(0.0, 5.0), (5.0, 10.0)],
        combine="min",
    )
    assert len(results) == 2
    best, other = results
    # min of back-projections recovers the true support and native values
    assert best.max_count == pytest.approx(7.0)
    assert best.n_voxels == np.count_nonzero(f == 7.0)
    assert best.bounding_box == ((6, 3, 1), (9, 5, 3))
    assert best.total_count == pytest.approx(f[f == 7.0].sum())
    assert other.max_count == pytest.approx(3.0)
    assert other.bounding_box == ((1, 1, 4), (4, 3, 6))


def test_gridscan3d_min_with_invert():
    # "Lower is better" metric (d_min, Angstrom) observed as a min along each path
    d = np.full((5, 4, 4), np.inf)
    d[1:3, 1:3, 1:3] = 2.0
    d[2, 1, 1] = 1.5
    d_a, d_b = d.min(axis=2), d.min(axis=1)
    # Caller inverts, as the service does
    inverted = tuple(np.where(np.isfinite(x), 1.0 / x, 0.0) for x in (d_a, d_b))
    (result,) = dlstbx.util.xray_centering_3d.gridscan3d(
        inverted, threshold=0.5, invert=True, combine="min"
    )
    assert result.max_voxel == (2, 1, 1)
    # max_count is in inverted units: 1 / max(d_A, d_B) = 1 / 1.5
    assert 1.0 / result.max_count == pytest.approx(1.5)
    # total_count in original units: sum over voxels of max(d_A, d_B)
    assert result.total_count == pytest.approx(1.5 + 7 * 2.0)


def test_gridscan3d_does_not_modify_input():
    a = np.array([[0, 1, 5], [2, 8, 3]])
    b = np.array([[4, 1], [9, 2]])
    data = (a.copy(), b.copy())
    dlstbx.util.xray_centering_3d.gridscan3d(data, threshold_absolute=3)
    np.testing.assert_array_equal(data[0], a)
    np.testing.assert_array_equal(data[1], b)
