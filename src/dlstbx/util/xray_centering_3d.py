from __future__ import annotations

import logging
import math
import operator
from typing import Tuple

import numpy as np
import scipy.ndimage

from dlstbx.util.xray_centering import GridScanResultBase, tag_sample_id

logger = logging.getLogger(__name__)


Coordinate = tuple[int, int, int]


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


def gridscan3d(
    data: tuple[np.ndarray, ...],
    threshold: float = 0.05,
    threshold_absolute: float = 5,
    plot: bool = False,
    sample_id: int | None = None,
    multipin_sample_ids: dict[int, int] = {},
    well_limits: list[tuple[float, float]] = [],
) -> list[GridScan3DResult]:
    """
    3D gridscan analysis from 2 x 2D perpendicular gridscans.

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
        threshold: mask out values less than this fraction of the maximum data value
                in the reconstructed 3d grid
        threshold_absolute: mask out values less than this absolute value in the
                original grids
        plot: Show interactive debug plots of the grid scan analysis (default=False)
        sample_id: The sample id attributed to the grid_scan. This will usually be the
            sample in sublocation 1 in the case of multi-sample pins.
        multipin_sample_ids: Sample_ids for all samples on a multi-sample pin.
                A dictionary of sample ids with the corresponding sub-locations in the
                pin as keys.
        well_limits: The lower and upper limits of the x-coordinate for each well in
            the multi-sample pin in units of grid scan boxes.

    Returns:
        list[GridScan3DResult]
    """
    assert len(data) == 2
    assert data[0].ndim == 2
    assert data[1].ndim == 2

    # mask out grid scans to reduce impact of noisy pixels / edge effects
    data[0][data[0] < threshold_absolute] = 0
    data[1][data[1] < threshold_absolute] = 0

    reconstructed_3d = data[0][:, :, np.newaxis] * data[1][:, np.newaxis, :]
    logger.debug(data[0].shape)
    logger.debug(data[1].shape)
    logger.debug(reconstructed_3d.shape)
    max_idx = tuple(
        int(r[0]) for r in np.where(reconstructed_3d == reconstructed_3d.max())
    )
    max_count = int(reconstructed_3d[max_idx])

    thresholded = (reconstructed_3d >= threshold * max_count) * reconstructed_3d
    # Count corner-corner contacts as a contiguous region
    structure = np.ones((3, 3, 3))
    labels, n_regions = scipy.ndimage.label(thresholded, structure=structure)
    logger.info(f"Found {n_regions} distinct regions")

    object_slices = scipy.ndimage.find_objects(labels)

    results: list[GridScan3DResult] = []
    for index in range(1, n_regions + 1):
        com = tuple(
            c + 0.5
            for c in scipy.ndimage.center_of_mass(
                thresholded, labels=labels, index=index
            )
        )
        max_voxel = tuple(
            int(i)
            for i in scipy.ndimage.maximum_position(
                thresholded, labels=labels, index=index
            )
        )
        max_count = int(thresholded[max_voxel])
        n_voxels = np.count_nonzero(labels == index)
        total_count = int(
            scipy.ndimage.sum_labels(thresholded, labels=labels, index=index)
        )
        x, y, z = object_slices[index - 1]
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
        )
        results.append(result)

    if plot:
        plot_gridscan3d_results(data, reconstructed_3d, results)

    return sorted(results, key=operator.attrgetter("total_count"), reverse=True)


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
