from __future__ import annotations

import enum

import numpy as np
import pydantic
import scipy.ndimage


class Orientation(enum.Enum):
    HORIZONTAL = "horizontal"
    VERTICAL = "vertical"


class GridScanResultBase(pydantic.BaseModel):
    centre_of_mass: tuple[float, ...] | None = None
    max_voxel: tuple[int, ...] | None = None
    max_count: float | None = None
    n_voxels: int | None = None
    total_count: float | None = None


# Only the following items currently are used by GDA
# (see /dls_sw/i03/software/gda/mx-config/scripts/XrayCentring.py):
#   centre_x
#   centre_y
#   centre_x_box
#   centre_y_box
#   status
# @dataclasses.dataclass
class GridScan2DResult(GridScanResultBase):
    steps: tuple[int, int]
    box_size_px: tuple[float, float]
    snapshot_offset: tuple[float, float]
    centre_x: float | None = None
    centre_y: float | None = None
    centre_x_box: float | None = None
    centre_y_box: float | None = None
    status: str = "fail"
    message: str = "fail"
    best_image: int | None = None
    reflections_in_best_image: int | None = None
    best_region: list[tuple[int, int]] | None = None


def reshape_grid(
    data: np.ndarray, steps: tuple[int, int], snaked: bool, orientation: Orientation
) -> np.ndarray:
    if orientation == Orientation.VERTICAL:
        data = data.reshape(steps)
    else:
        data = data.reshape(*reversed(steps)).T

    if snaked and orientation == Orientation.HORIZONTAL:
        # Reverse the direction of every second column
        data[:, 1::2] = data[::-1, 1::2]
    elif snaked and orientation == Orientation.VERTICAL:
        # Reverse the direction of every second row
        data[1::2, :] = data[1::2, ::-1]
    return data


def main(
    data: np.ndarray,
    steps: tuple[int, int],
    box_size_px: tuple[float, float],
    snapshot_offset: tuple[float, float],
    snaked: bool,
    orientation: Orientation,
) -> tuple[GridScan2DResult, str]:
    output = [
        f"steps_x/y: {steps}",
        f"box_size_px: {box_size_px}",
        f"snapshot_offset: {snapshot_offset}",
    ]
    idx = np.argmax(data)
    maximum_spots = int(data[np.unravel_index(idx, data.shape)])
    best_image = int(idx + 1)
    if maximum_spots == 0:
        return GridScan2DResult(
            steps=steps,
            box_size_px=box_size_px,
            snapshot_offset=snapshot_offset,
            status="fail",
            message="No good images found",
        ), "\n".join(output)

    data = reshape_grid(data, steps, snaked, orientation).T

    output.append(f"There are {maximum_spots} reflections in image #{best_image}.")

    threshold = (data >= 0.5 * maximum_spots) * data
    # Count corner-corner contacts as a contiguous region
    structure = np.ones((3, 3))
    labels, _ = scipy.ndimage.label(threshold, structure=structure)
    unique, counts = np.unique(labels, return_counts=True)
    # When finding the biggest labelled region, ignore the unlabelled regions
    # (label == 0), if there are any (i.e. if unique[0] == 0).
    best = unique[np.argmax(counts)] if unique[0] else unique[np.argmax(counts[1:]) + 1]
    com = scipy.ndimage.center_of_mass(labels == best)
    max_pixel = scipy.ndimage.maximum_position(threshold, labels == best)
    n_voxels = np.count_nonzero(labels == best)
    total_count = int(scipy.ndimage.sum_labels(threshold, labels=labels, index=best))
    output.append(f"grid:\n{threshold}".replace(" 0", " ."))
    best_region = list(zip(*(w.tolist() for w in np.where(labels == best))))

    centre_x_box, centre_y_box = reversed([c + 0.5 for c in com])
    centre_x = snapshot_offset[0] + centre_x_box * box_size_px[0]
    centre_y = snapshot_offset[1] + centre_y_box * box_size_px[1]
    output.append(f"centre_x,centre_y={centre_x},{centre_y}")

    return GridScan2DResult(
        centre_of_mass=(centre_x, centre_y),
        max_count=maximum_spots,
        max_voxel=max_pixel,
        n_voxels=n_voxels,
        total_count=total_count,
        # legacy GDA json file fields
        steps=steps,
        box_size_px=box_size_px,
        snapshot_offset=snapshot_offset,
        status="ok",
        message="ok",
        centre_x=centre_x,
        centre_y=centre_y,
        centre_x_box=centre_x_box,
        centre_y_box=centre_y_box,
        best_region=best_region,
        best_image=best_image,
        reflections_in_best_image=maximum_spots,
    ), "\n".join(output)
