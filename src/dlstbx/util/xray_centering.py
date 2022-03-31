from __future__ import annotations

import dataclasses
import enum
from typing import List, Optional, Tuple

import numpy as np
import scipy.ndimage


class Orientation(enum.Enum):
    HORIZONTAL = "horizontal"
    VERTICAL = "vertical"


# Only the following items currently are used by GDA
# (see /dls_sw/i03/software/gda/mx-config/scripts/XrayCentring.py):
#   centre_x
#   centre_y
#   centre_x_box
#   centre_y_box
#   status
@dataclasses.dataclass
class Result:
    steps: Tuple[int, int]
    box_size_px: Tuple[float, float]
    snapshot_offset: Tuple[float, float]
    centre_x: Optional[int] = None
    centre_y: Optional[int] = None
    centre_x_box: Optional[int] = None
    centre_y_box: Optional[int] = None
    status: str = "fail"
    message: str = "fail"
    best_image: Optional[int] = None
    reflections_in_best_image: Optional[int] = None
    best_region: Optional[List[Tuple[int, int]]] = None


def main(
    data: np.ndarray,
    steps: Tuple[int, int],
    box_size_px: Tuple[float, float],
    snapshot_offset: Tuple[float, float],
    snaked: bool,
    orientation: Orientation,
) -> Tuple[Result, str]:
    result = Result(
        steps,
        box_size_px=box_size_px,
        snapshot_offset=snapshot_offset,
    )

    output = [
        f"steps_x/y: {steps}",
        f"box_size_px: {box_size_px}",
        f"snapshot_offset: {snapshot_offset}",
    ]

    if orientation == Orientation.VERTICAL:
        data = data.reshape(steps).T
    else:
        data = data.reshape(*reversed(steps))

    idx = np.argmax(data)
    maximum_spots = int(data[np.unravel_index(idx, data.shape)])
    best_image = int(idx + 1)
    if maximum_spots == 0:
        result.message = "No good images found"
        return result, "\n".join(output)

    if snaked and orientation == Orientation.HORIZONTAL:
        # Reverse the direction of every second row
        data[1::2, :] = data[1::2, ::-1]
    elif snaked and orientation == Orientation.VERTICAL:
        # Reverse the direction of every second column
        data[:, 1::2] = data[::-1, 1::2]

    result.best_image = best_image
    result.reflections_in_best_image = maximum_spots
    output.append(f"There are {maximum_spots} reflections in image #{best_image}.")

    threshold = (data >= 0.5 * maximum_spots) * data
    # Count corner-corner contacts as a contiguous region
    structure = np.ones((3, 3))
    labels, n_regions = scipy.ndimage.label(threshold, structure=structure)
    unique, counts = np.unique(labels, return_counts=True)
    # When finding the biggest labelled region, ignore the unlabelled regions
    # (label == 0), if there are any (i.e. if unique[0] == 0).
    best = unique[np.argmax(counts)] if unique[0] else unique[np.argmax(counts[1:]) + 1]
    com = scipy.ndimage.center_of_mass(labels == best)
    output.append(f"grid:\n{threshold}".replace(" 0", " ."))
    result.best_region = list(zip(*(w.tolist() for w in np.where(labels == best))))

    centre_x_box, centre_y_box = reversed([c + 0.5 for c in com])
    centre_x = snapshot_offset[0] + centre_x_box * box_size_px[0]
    centre_y = snapshot_offset[1] + centre_y_box * box_size_px[1]
    output.append(f"centre_x,centre_y={centre_x},{centre_y}")
    result.centre_x = centre_x
    result.centre_y = centre_y
    result.centre_x_box = centre_x_box
    result.centre_y_box = centre_y_box
    result.status = "ok"
    result.message = "ok"
    return result, "\n".join(output)
