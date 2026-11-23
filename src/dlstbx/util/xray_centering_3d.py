from __future__ import annotations

import enum
import logging
import math
import operator
from typing import Tuple

import numpy as np
import scipy.ndimage

from dlstbx.util.xray_centering import GridScanResultBase, tag_sample_id

logger = logging.getLogger(__name__)


Coordinate = tuple[int, int, int]


class CombineMethod(str, enum.Enum):
    """
    How the two orthogonal 2D projections A[x, y] and B[x, z] are combined into a
    3D volume R[x, y, z].

    PRODUCT: R = A * B. The original behaviour. Values are in squared units and,
        for additive metrics, the profile along x is effectively squared, biasing
        the x position towards the brightest columns.
    NORMALISED: R = A * B / S(x), where S(x) = (sum_y A + sum_z B) / 2. The
        maximum-entropy reconstruction for additive (line-integral) metrics such
        as spot counts or total intensity: R reproduces both projections and is in
        the original units.
    GEOMETRIC_MEAN: R = sqrt(A * B). Same peak and region support as PRODUCT but in
        the original units; the relative threshold applies per projection.
    MIN: R = min(A, B). The largest volume consistent with both projections for
        max-like quality metrics, where the value seen along a beam path is set by
        the best crystal in it (e.g. resolution). Use with `invert` for "lower is
        better" metrics, so that the result is max(d_A, d_B) in original units.
    """

    PRODUCT = "product"
    NORMALISED = "normalised"
    GEOMETRIC_MEAN = "geometric_mean"
    MIN = "min"


def combine_projections(
    a: np.ndarray, b: np.ndarray, method: CombineMethod | str = CombineMethod.PRODUCT
) -> np.ndarray:
    """
    Combine two 2D projections A[x, y] and B[x, z], sharing the x axis, into a 3D
    volume R[x, y, z]. See CombineMethod for the available methods.
    """
    method = CombineMethod(method)
    if a.shape[0] != b.shape[0]:
        raise ValueError(
            f"Projections must share the x axis: shapes {a.shape} and {b.shape}"
        )
    a3 = a[:, :, np.newaxis]
    b3 = b[:, np.newaxis, :]
    if method is CombineMethod.PRODUCT:
        return a3 * b3
    if method is CombineMethod.MIN:
        return np.minimum(a3, b3)
    if method is CombineMethod.GEOMETRIC_MEAN:
        return np.sqrt(a3 * b3, dtype=float)
    # NORMALISED
    s = 0.5 * (a.sum(axis=1) + b.sum(axis=1)).astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        inv_s = np.where(s > 0, 1.0 / s, 0.0)
    return a3 * b3 * inv_s[:, np.newaxis, np.newaxis]


def _n_runs(mask: np.ndarray) -> int:
    """Number of separate runs of True values in a 1D boolean array."""
    return scipy.ndimage.label(mask)[1]


def is_ambiguous(kept_mask: np.ndarray, x_slice: slice) -> bool:
    """
    Flag results whose (y, z) position cannot be uniquely determined from two
    orthogonal projections.

    If, within the x range of a result, the identified regions occupy two or more
    separate intervals along y *and* two or more separate intervals along z, then
    the pairing of y with z positions is ambiguous: e.g. two crystals at
    (y1, z1) and (y2, z2) are indistinguishable from crystals at (y1, z2) and
    (y2, z1), and the reconstruction contains "ghost" peaks at the latter.
    """
    slab = kept_mask[x_slice]
    return _n_runs(slab.any(axis=(0, 2))) >= 2 and _n_runs(slab.any(axis=(0, 1))) >= 2


class GridScan3DResult(GridScanResultBase):
    """
    Represents a single gridscan result, corresponding to a diffracting centre.

    Coordinates expressed are in terms of grid boxes, with the centre of the box
    lying on half-integer coordinates.

    Attributes:
         centre_of_mass: The position of the centre of mass of the crystal, for a
         crystal of size (1, 1, 1) this will be on half-integer coordinates
         max_voxel: Position of the maximum voxel, on integer coordinates!!!
         max_count: max count achieved in a single voxel for the crystal
         n_voxels: Number of voxels in the diffracting centre
         total_count: Total of above-threshold spot counts in the labelled voxels
         bounding_box: The rectangular prism that bounds the crystal, expressed
            as the volume of whole boxes as a half-open range i.e such that
            p1 = (x1, y1, z1) <= p < p2 = (x2, y2, z2) and
            p2 - p1 gives the dimensions in whole voxels.
    """

    centre_of_mass: Tuple[float, ...]
    max_voxel: Tuple[int, ...]
    max_count: float
    n_voxels: int
    total_count: float
    bounding_box: Tuple[Coordinate, Coordinate]
    ambiguous: bool = False


def gridscan3d(
    data: tuple[np.ndarray, ...],
    threshold: float = 0.25,
    threshold_absolute: float = 0,
    plot: bool = False,
    sample_id: int | None = None,
    multipin_sample_ids: dict[int, int] = {},
    well_limits: list[tuple[float, float]] = [],
    invert: bool = False,
    combine: CombineMethod | str = CombineMethod.PRODUCT,
) -> list[GridScan3DResult]:
    """
    3D gridscan analysis from 2 x 2D perpendicular gridscans.

    Fitting routine first filters out any pixels from either gridscan with intensity
    less than threshold_absolute, then constructs a pseudo-3D grid by taking the outer
    product (or another combination, see `combine`) of the 2D scans. For multi-sample pins, any separate continuous regions of
    signal are found first, whereas for single sample pins the entire 3D grid is treated
    as a single region for the following steps. For each region, a relative threshold of
    the max voxel intensity of the region is applied, with the aim to separate/disconnect
    peaks of intensity within the region. Separate continuous sub-regions are then found
    after applying the threshold, which are then stored as grid_scan results. The purpose
    of this second step is to try and locate the individual centres of crystals that are
    close together in the gridscan, which would be merged into a single region without
    applying the relative threshold. For multi-sample pins, the centre of mass of the found
    sub-regions are tagged with the corresponding sample_id by comparing the x-coordinate
    to the well limits provided.

    Assumption: X is along the rotation axis, Y, Z are perpendicular

    - spot find on images at -45°, +45° -> 2 x 2D array of spot finding metrics
      (e.g. total signal or whatever)
    - for each position along X, grab slice of array along Y from scan 1 and Z from scan
      2 (arbitrary naming) and form the matrix outer product of these, add back to a
      slice of a 3D array at that X position
    - find the maximum of this 3D array - yay you have found the best position to shoot
      at in the X, Y, Z directions -> remap to proper positions

    Args:
        data: A tuple of spot counts from 2 orthogonal 2D gridscans
        threshold: Mask out values less than this fraction of the maximum data value
            in an identified region of the reconstructed 3d grid.
        threshold_absolute: filter out identified regions of continuous signal where the
            max value is less than this absolute value.
        plot: Show interactive debug plots of the grid scan analysis (default=False)
        sample_id: The sample id attributed to the grid_scan. This will usually be the
            sample in sublocation 1 in the case of multi-sample pins.
        multipin_sample_ids: Sample_ids for all samples on a multi-sample pin.
            A dictionary of sample ids with the corresponding sub-locations in the
            pin as keys.
        well_limits: The lower and upper limits of the x-coordinate for each well in
            the multi-sample pin in units of grid scan boxes.
        invert: Set if the input data contain the reciprocal (1/x) of a "lower is
            better" metric. total_count is then returned in the original metric
            units (sum of 1/value), whereas max_count remains in inverted units and
            must be converted back by the caller. Results are still ranked by the
            summed (inverted) signal.
        combine: How to combine the two 2D projections into a 3D volume (see
            CombineMethod). Note that `threshold` is relative to the combined
            values, so the default of 0.25 for "product" corresponds to roughly
            0.5 for "geometric_mean" or "min".

    Returns:
        list[GridScan3DResult]
    """
    assert len(data) == 2
    assert data[0].ndim == 2
    assert data[1].ndim == 2

    # Apply absolute threshold to screen out noise (on copies, leaving the caller's
    # arrays untouched).
    data = tuple(np.where(d < threshold_absolute, 0, d) for d in data)

    reconstructed_3d = combine_projections(data[0], data[1], combine)
    logger.debug(data[0].shape)
    logger.debug(data[1].shape)
    logger.debug(reconstructed_3d.shape)

    # Count corner-corner contacts as a continuous region
    structure = np.ones((3, 3, 3))
    # For multi-sample pins first find separate continuous regions in the 3d grid
    if multipin_sample_ids:
        # Find all continuous regions
        labels, n_regions = scipy.ndimage.label(reconstructed_3d, structure=structure)
        logger.info(f"Found {n_regions} distinct regions")

    else:
        # For single sample pins, treat the entire grid as a single region
        labels = np.ones_like(reconstructed_3d, dtype=int)
        n_regions = 1

    # (ranking score, result) pairs; the score is always in the (possibly inverted)
    # space of the input data, so the best region sorts first regardless of `invert`.
    scored_results: list[tuple[float, GridScan3DResult]] = []
    # Loop over each continuous region, apply a relative threshold then find sub-regions
    region_sub_labels = []
    for label in range(1, n_regions + 1):
        labelled_data = (labels == label) * reconstructed_3d
        # Apply relative threshold to filter out edge effects and to separate out multiple centres in a single region.
        thresholded = (labelled_data >= threshold * labelled_data.max()) * labelled_data
        sub_labels, n_sub_regions = scipy.ndimage.label(
            thresholded, structure=structure
        )
        logger.debug(f"For label {label}, {n_sub_regions} sub regions found")
        region_sub_labels.append((thresholded, sub_labels, n_sub_regions))

    # Voxels retained in any sub-region of any region, used to detect y/z ambiguity
    kept_mask = np.zeros(reconstructed_3d.shape, dtype=bool)
    for _, sub_labels, _ in region_sub_labels:
        kept_mask |= sub_labels > 0

    for thresholded, sub_labels, n_sub_regions in region_sub_labels:
        object_slices = scipy.ndimage.find_objects(sub_labels)

        for sub_label in range(1, n_sub_regions + 1):
            com = tuple(
                c + 0.5
                for c in scipy.ndimage.center_of_mass(
                    thresholded, labels=sub_labels, index=sub_label
                )
            )
            max_voxel = tuple(
                int(i)
                for i in scipy.ndimage.maximum_position(
                    thresholded, labels=sub_labels, index=sub_label
                )
            )
            val = thresholded[max_voxel]
            max_count = (
                int(val)
                if np.issubdtype(reconstructed_3d.dtype, np.integer)
                else float(val)
            )
            n_voxels = np.count_nonzero(sub_labels == sub_label)
            tot = scipy.ndimage.sum_labels(
                thresholded, labels=sub_labels, index=sub_label
            )
            if invert:
                # Data were inverted by the caller (1/x); report the total in the
                # original metric units by summing the per-voxel reciprocals.
                total_count = float(np.sum(1.0 / thresholded[sub_labels == sub_label]))
            else:
                total_count = (
                    int(tot)
                    if np.issubdtype(reconstructed_3d.dtype, np.integer)
                    else float(tot)
                )
            x, y, z = object_slices[sub_label - 1]
            ambiguous = is_ambiguous(kept_mask, x)
            bounding_box = ((x.start, y.start, z.start), (x.stop, y.stop, z.stop))
            tagged_sample_id = tag_sample_id(
                sample_id, multipin_sample_ids, well_limits, com[0]
            )

            result = GridScan3DResult(
                centre_of_mass=com,
                max_voxel=max_voxel,
                max_count=max_count,
                n_voxels=n_voxels,
                total_count=total_count,
                bounding_box=bounding_box,
                sample_id=tagged_sample_id,
                ambiguous=ambiguous,
            )
            scored_results.append((float(tot), result))

    scored_results.sort(key=operator.itemgetter(0), reverse=True)
    results = [r for _, r in scored_results]

    if plot:
        plot_data = data
        plot_recon = reconstructed_3d
        if invert:
            plot_data = tuple(
                np.where((d > 0) & np.isfinite(d), 1.0 / d, 0.0) for d in data
            )
            plot_recon = np.where(
                (reconstructed_3d > 0) & np.isfinite(reconstructed_3d),
                1.0 / reconstructed_3d,
                0.0,
            )
        plot_gridscan3d_results(plot_data, plot_recon, results)

    return results


def plot_gridscan3d_results(
    data: tuple[np.ndarray, ...],
    reconstructed_data: np.ndarray,
    results: list[GridScan3DResult],
):
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    from matplotlib.ticker import MaxNLocator

    fig, axes = plt.subplots(nrows=1, ncols=2)
    vmax = max(counts.max() for counts in data)
    labels = ("xy", "xz", "yz")
    for i, (ax, d) in enumerate(zip(axes, data)):
        ax.imshow(d.T, vmin=0, vmax=vmax)
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        ax.set_xlabel(labels[i][0])
        ax.set_ylabel(labels[i][1])
    for result in results:
        (x1, y1, z1), (x2, y2, z2) = result.bounding_box
        axes[0].scatter(
            result.max_voxel[0],
            result.max_voxel[1],
            marker="x",
            c="red",
        )
        axes[1].scatter(
            result.max_voxel[0],
            result.max_voxel[2],
            marker="x",
            c="red",
        )
        axes[0].scatter(
            result.centre_of_mass[0] - 0.5,
            result.centre_of_mass[1] - 0.5,
            marker="x",
            c="orange",
        )
        axes[1].scatter(
            result.centre_of_mass[0] - 0.5,
            result.centre_of_mass[2] - 0.5,
            marker="x",
            c="orange",
        )
        axes[0].add_patch(
            Rectangle(
                (x1 - 0.5, y1 - 0.5),
                (x2 - x1),
                (y2 - y1),
                edgecolor="pink",
                facecolor="blue",
                fill=False,
                lw=1,
            )
        )
        axes[1].add_patch(
            Rectangle(
                (x1 - 0.5, z1 - 0.5),
                (x2 - x1),
                (z2 - z1),
                edgecolor="pink",
                facecolor="blue",
                fill=False,
                lw=1,
            )
        )
    plt.tight_layout()
    plt.show()

    fig, axes = plt.subplots(nrows=1, ncols=3)
    vmax = reconstructed_data.max()
    for (
        i,
        ax,
    ) in enumerate(axes):
        ax.imshow(reconstructed_data.T.sum(axis=i))
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        ax.set_xlabel(labels[i][0])
        ax.set_ylabel(labels[i][1])
    for result in results:
        (x1, y1, z1), (x2, y2, z2) = result.bounding_box
        axes[0].scatter(
            result.max_voxel[0],
            result.max_voxel[1],
            marker="x",
            c="red",
        )
        axes[1].scatter(
            result.max_voxel[0],
            result.max_voxel[2],
            marker="x",
            c="red",
        )
        axes[2].scatter(
            result.max_voxel[1],
            result.max_voxel[2],
            marker="x",
            c="red",
        )
        axes[0].scatter(
            result.centre_of_mass[0] - 0.5,
            result.centre_of_mass[1] - 0.5,
            marker="x",
            c="orange",
        )
        axes[1].scatter(
            result.centre_of_mass[0] - 0.5,
            result.centre_of_mass[2] - 0.5,
            marker="x",
            c="orange",
        )
        axes[2].scatter(
            result.centre_of_mass[1] - 0.5,
            result.centre_of_mass[2] - 0.5,
            marker="x",
            c="orange",
        )
        axes[0].add_patch(
            Rectangle(
                (x1 - 0.5, y1 - 0.5),
                (x2 - x1),
                (y2 - y1),
                edgecolor="pink",
                facecolor="blue",
                fill=False,
                lw=1,
            )
        )
        axes[1].add_patch(
            Rectangle(
                (x1 - 0.5, z1 - 0.5),
                (x2 - x1),
                (z2 - z1),
                edgecolor="pink",
                facecolor="blue",
                fill=False,
                lw=1,
            )
        )
        axes[2].add_patch(
            Rectangle(
                (y1 - 0.5, z1 - 0.5),
                (y2 - y1),
                (z2 - z1),
                edgecolor="pink",
                facecolor="blue",
                fill=False,
                lw=1,
            )
        )
    plt.tight_layout()
    plt.show()

    nx = reconstructed_data.shape[0]
    vmax = reconstructed_data.max()
    fig, axes = plt.subplots(nrows=1, ncols=nx)
    for i in range(nx):
        axes[i].imshow(reconstructed_data[i, :, :].T, vmin=0, vmax=vmax)
        axes[i].yaxis.set_major_locator(MaxNLocator(integer=True))
        for result in results:
            (x1, y1, z1), (x2, y2, z2) = result.bounding_box
            if i == result.max_voxel[0]:
                axes[i].scatter(
                    result.max_voxel[1], result.max_voxel[2], marker="x", c="red"
                )
            if i == math.floor(result.centre_of_mass[0]):
                axes[i].scatter(
                    result.centre_of_mass[1] - 0.5,
                    result.centre_of_mass[2] - 0.5,
                    marker="x",
                    c="orange",
                )
            if i >= x1 and i < x2:
                axes[i].add_patch(
                    Rectangle(
                        (y1 - 0.5, z1 - 0.5),
                        (y2 - y1),
                        (z2 - z1),
                        edgecolor="pink",
                        facecolor="blue",
                        fill=False,
                        lw=1,
                    )
                )
    plt.tight_layout()
    plt.show()
