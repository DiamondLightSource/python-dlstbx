"""
3D gridscan analysis from 2 x 2D perpendicular gridscans.
"""
# isort: skip_file

import enum
import logging
import numpy as np
import sys
from typing import List
from typing import Union

# We need to parse command-line arguments to PHIL scopes.
import libtbx.phil

from dxtbx.model import ExperimentList
import dials.util
import dials.util.log
from dials.algorithms.spot_finding import per_image_analysis
from dials.array_family import flex
from dials.util.options import OptionParser
from dials.util.version import dials_version

import h5py  # This must be after the first dxtbx import

from dlstbx.util import xray_centering
from dlstbx.util import xray_centering_3d


logger = logging.getLogger("dials.gridscan3d")

# Define the master PHIL scope for this program.
phil_scope = libtbx.phil.parse(
    """
    plot = False
      .type = bool
    metric = *n_spots_total n_spots_no_ice n_spots_4A total_intensity estimated_d_min
      .type = choice
    """
)


class metrics(enum.Enum):
    """Supported metrics for 3D gridscan analysis."""

    N_SPOTS_TOTAL = "n_spots_total"
    N_SPOTS_NO_ICE = "n_spots_no_ice"
    N_SPOTS_4A = "n_spots_4A"
    TOTAL_INTENSITY = "total_intensity"
    D_MIN = "estimated_d_min"


def gridscan3d(
    experiment_lists: List[ExperimentList],
    reflection_tables: List[flex.reflection_table],
    metric: Union[str, metrics] = "n_spots_total",
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

    assert len(experiment_lists) == len(reflection_tables) == 2

    data = []
    for experiments, reflections in zip(experiment_lists, reflection_tables):
        reflections.centroid_px_to_mm(experiments)
        reflections.map_centroids_to_reciprocal_space(experiments)
        master_h5 = experiments.imagesets()[0].get_path(0)
        logger.debug(master_h5)
        with h5py.File(master_h5, "r") as handle:
            x, y, z, omega = (
                handle.get(f"/entry/sample/transformations/{item}")[()]
                for item in ("sam_x", "sam_y", "sam_z", "omega")
            )

        unique_x = np.array(sorted(set(x)))
        unique_y = np.array(sorted(set(y)))

        logger.debug(f"x: {sorted(unique_x)}")
        logger.debug(f"y: {sorted(unique_x)}")
        logger.debug(f"z: {sorted(set(z))}")
        logger.debug(f"omega: {sorted(set(omega))}")

        n = len(experiments)
        nx = len(unique_x)
        ny = len(unique_y)
        data.append(np.zeros(n))
        for i in range(n):
            refl = reflections.select(reflections["id"] == i)
            stats = per_image_analysis.stats_for_reflection_table(refl)._asdict()
            logger.debug(stats)
            data[-1][i] = stats[metric]

    max_idx = xray_centering_3d.gridscan3d(
        data=np.array(data),
        steps=(nx, ny),
        snaked=True,
        orientation=xray_centering.Orientation.HORIZONTAL,
        plot=plot,
    )
    return max_idx


@dials.util.show_mail_on_error()
def run(args: List[str] = None, phil: libtbx.phil.scope = phil_scope) -> None:
    """
    Check command-line input and call other functions to do the legwork.

    Args:
        args: The arguments supplied by the user (default: sys.argv[1:])
        phil: The PHIL scope definition (default: phil_scope, the master PHIL scope
        for this program).
    """
    usage = "dlstbx.gridscan3d [options] m45_imported.expt m45_strong.refl p45_imported.expt p45_strong.refl"

    parser = OptionParser(
        usage=usage,
        phil=phil,
        read_reflections=True,
        read_experiments=True,
        check_format=False,
        epilog=__doc__,
    )

    params, options = parser.parse_args(args=args, show_diff_phil=False)

    # Configure the logging.
    dials.util.log.config(options.verbose)

    # Log the dials version
    logger.info(dials_version())

    # Log the difference between the PHIL scope definition and the active PHIL scope,
    # which will include the parsed user inputs.
    diff_phil = parser.diff_phil.as_str()
    if diff_phil:
        logger.info(f"The following parameters have been modified:\n{diff_phil}")

    if len(params.input.reflections) != 2:
        sys.exit(
            f"Exactly two reflection files required ({len(params.input.reflections)} provided)"
        )
    if len(params.input.experiments) != 2:
        sys.exit(
            f"Exactly two experiment files required ({len(params.input.experiment)} provided)"
        )

    reflection_tables = [refl.data for refl in params.input.reflections]
    experiment_lists = [expt.data for expt in params.input.experiments]

    max_idx = gridscan3d(
        experiment_lists, reflection_tables, metric=params.metric, plot=params.plot
    )
    logger.info(f"max_idx: {max_idx}")


if __name__ == "__main__":
    run()
