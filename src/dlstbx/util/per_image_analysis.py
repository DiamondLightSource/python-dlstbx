from __future__ import annotations

import copy
import enum
import logging
import pathlib
import time
from typing import Optional

import pydantic
from cctbx import uctbx
from dials.algorithms.spot_finding import per_image_analysis
from dials.array_family import flex
from dials.command_line.find_spots import phil_scope as find_spots_phil_scope
from dials.util import phil
from dxtbx.model.experiment_list import ExperimentList, ExperimentListFactory

logger = logging.getLogger(__name__)


class ThresholdAlgorithm(enum.Enum):
    DISPERSION = "dispersion"


class PerImageAnalysisParameters(pydantic.BaseModel):
    d_min: Optional[pydantic.PositiveFloat] = None
    d_max: Optional[pydantic.PositiveFloat] = 40
    threshold_algorithm: ThresholdAlgorithm = ThresholdAlgorithm.DISPERSION
    disable_parallax_correction: bool = True
    scan_range: Optional[tuple[int, int]] = None
    filter_ice: bool = True
    ice_rings_width: pydantic.NonNegativeFloat = 0.004

    @pydantic.validator("scan_range", pre=True)
    def str_to_tuple(cls, v):
        if isinstance(v, str):
            return tuple(int(i) for i in v.split(","))
        else:
            return tuple(v)


class PerImageAnalysisResults(pydantic.BaseModel):
    n_spots_4A: pydantic.NonNegativeInt
    n_spots_no_ice: pydantic.NonNegativeInt
    n_spots_total: pydantic.NonNegativeInt
    total_intensity: pydantic.NonNegativeFloat
    d_min_distl_method_1: Optional[float] = None
    d_min_distl_method_2: Optional[float] = None
    estimated_d_min: Optional[float] = None
    noisiness_method_1: Optional[float] = None
    noisiness_method_2: Optional[float] = None


def do_per_image_analysis(
    filename: pathlib.Path, params: PerImageAnalysisParameters
) -> tuple[ExperimentList, flex.reflection_table, PerImageAnalysisResults]:
    if filename.suffix in {".h5", ".nxs"} and params.scan_range:
        experiments = ExperimentListFactory.from_filenames(
            [filename], load_models=False
        )
        if params.scan_range:
            start, end = params.scan_range
        if end > start:
            for _ in range(end - start):
                experiments.append(copy.deepcopy(experiments[0]))
        for i, expt in enumerate(experiments):
            expt.load_models(index=start - 1 + i)
    else:
        experiments = ExperimentListFactory.from_filenames([filename])

    phil_params = find_spots_phil_scope.fetch(source=phil.parse("")).extract()
    phil_params.spotfinder.scan_range = (params.scan_range,)
    phil_params.spotfinder.threshold.algorithm = params.threshold_algorithm.value
    phil_params.spotfinder.filter.disable_parallax_correction = (
        params.disable_parallax_correction
    )

    t0 = time.perf_counter()
    reflections = flex.reflection_table.from_observations(experiments, phil_params)

    if params.d_min or params.d_max:
        reflections = _filter_by_resolution(
            experiments, reflections, d_min=params.d_min, d_max=params.d_max
        )

    t1 = time.perf_counter()
    logger.info("Spotfinding took %.2f seconds", t1 - t0)

    reflections.centroid_px_to_mm(experiments)
    reflections.map_centroids_to_reciprocal_space(experiments)
    stats = per_image_analysis.stats_for_reflection_table(
        reflections,
        filter_ice=params.filter_ice,
        ice_rings_width=params.ice_rings_width,
    )._asdict()
    t2 = time.perf_counter()
    logger.info("Resolution analysis took %.2f seconds", t2 - t1)
    print(stats)
    return experiments, reflections, PerImageAnalysisResults(**stats)


def _filter_by_resolution(experiments, reflections, d_min=None, d_max=None):
    reflections.centroid_px_to_mm(experiments)
    reflections.map_centroids_to_reciprocal_space(experiments)
    d_star_sq = flex.pow2(reflections["rlp"].norms())
    reflections["d"] = uctbx.d_star_sq_as_d(d_star_sq)
    # Filter based on resolution
    if d_min is not None:
        selection = reflections["d"] >= d_min
        reflections = reflections.select(selection)
        # logger.debug(f"Selected {len(reflections)} reflections with d >= {d_min:f}")

    # Filter based on resolution
    if d_max is not None:
        selection = reflections["d"] <= d_max
        reflections = reflections.select(selection)
        # logger.debug(f"Selected {len(reflections)} reflections with d <= {d_max:f}")
    return reflections
