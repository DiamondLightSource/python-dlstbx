from typing import Tuple

import numpy as np

from dlstbx.util.xray_centering import Orientation


def gridscan3d(
    data: np.ndarray,
    steps: Tuple[int, int],
    snaked: bool,
    orientation: Orientation,
    plot: bool = False,
):
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
        max_idx: the index of the maximum point of the resulting 3D array
    """

    assert len(data.shape) == 2
    assert data.shape[0] == 2
    if orientation == Orientation.VERTICAL:
        data = data.reshape([2] + list(steps))
        data = data.transpose(axes=(0, 2, 1))
    else:
        data = data.reshape([2] + list(reversed(steps)))

    if snaked and orientation == Orientation.HORIZONTAL:
        # Reverse the direction of every second row
        data[:, 1::2, :] = data[:, 1::2, ::-1]
    elif snaked and orientation == Orientation.VERTICAL:
        # Reverse the direction of every second column
        data[:, :, 1::2] = data[:, ::-1, 1::2]

    nx, ny = data.shape[1:]
    grid3d = np.zeros((nx, ny, ny))
    for i in range(nx):
        grid3d[i, :, :] = np.outer(data[0][i, :], data[1][i, :])

    max_idx = tuple(r[0] for r in np.where(grid3d == grid3d.max()))

    if plot:
        import matplotlib.pyplot as plt
        from matplotlib.ticker import MaxNLocator

        fig, axes = plt.subplots(nrows=1, ncols=2)
        vmax = max(counts.max() for counts in data)
        for ax, d in zip(axes, data):
            ax.imshow(d, vmin=0, vmax=vmax)
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        plt.show()

        vmax = grid3d[max_idx]
        fig, axes = plt.subplots(nrows=1, ncols=nx)
        for i in range(nx):
            axes[i].imshow(grid3d[i, :, :], vmin=0, vmax=vmax)
            axes[i].yaxis.set_major_locator(MaxNLocator(integer=True))
            if i == max_idx[0]:
                axes[i].scatter(max_idx[2], max_idx[1], marker="x", c="red")
        plt.show()

    return max_idx
