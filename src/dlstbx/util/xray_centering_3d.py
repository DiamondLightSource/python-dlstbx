from __future__ import annotations

import dataclasses
import logging
import math
from typing import Tuple

import numpy as np
import scipy.ndimage

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class GridScan3DResult:
    max_voxel: Tuple[int, ...]
    centre_of_mass: Tuple[float, ...]


def gridscan3d(
    data: tuple[np.ndarray, np.ndarray],
    plot: bool = False,
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
        experiment_lists: 2 experiment lists for perpendicular 2D gridscans
        reflection_tables: 2 reflection tables corresponding to the spotfinding results
                           for the 2 perpendicular gridscans
        plot: Show interactive debug plots of the grid scan analysis (default=False)

    Returns:
        list[GridScan3DResult]
    """
    assert len(data) == 2
    assert data[0].ndim == 2
    assert data[1].ndim == 2

    grid3d = data[0][np.newaxis, :, :] * data[1][:, np.newaxis, :]
    logger.debug(data[0].shape)
    logger.debug(data[1].shape)
    logger.debug(grid3d.shape)
    max_idx = tuple(int(r[0]) for r in np.where(grid3d == grid3d.max()))
    max_count = int(grid3d[max_idx])
    threshold = (grid3d >= 0.5 * max_count) * grid3d
    com = tuple(c + 0.5 for c in scipy.ndimage.center_of_mass(threshold))
    logger.info(f"Max voxel: {max_idx} with count {max_count}\nCentre of mass: {com}")

    if plot:
        import matplotlib.pyplot as plt
        from matplotlib.ticker import MaxNLocator

        fig, axes = plt.subplots(nrows=1, ncols=2)
        vmax = max(counts.max() for counts in data)
        for ax, d in zip(axes, data):
            ax.imshow(d, vmin=0, vmax=vmax)
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        plt.show()

        fig, axes = plt.subplots(nrows=1, ncols=3)
        vmax = max(counts.max() for counts in data)
        for (
            i,
            ax,
        ) in enumerate(axes):
            ax.imshow(grid3d.sum(axis=i))
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        plt.show()

        nx = grid3d.shape[2]
        vmax = grid3d[max_idx]
        fig, axes = plt.subplots(nrows=1, ncols=nx)
        for i in range(nx):
            logger.debug(grid3d[:, :, i].shape)
            axes[i].imshow(grid3d[:, :, i], vmin=0, vmax=vmax)
            axes[i].yaxis.set_major_locator(MaxNLocator(integer=True))
            if i == max_idx[2]:
                axes[i].scatter(max_idx[1], max_idx[0], marker="x", c="red")
            if i == math.floor(com[2]):
                axes[i].scatter(com[1], com[0], marker="x", c="grey")
        plt.show()

    return [GridScan3DResult(max_voxel=max_idx, centre_of_mass=com)]
