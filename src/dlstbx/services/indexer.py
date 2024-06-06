from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pydantic
import workflows.recipe
from cctbx import sgtbx, uctbx
from dials.algorithms.indexing import indexer
from dials.array_family import flex
from dials.command_line.index import phil_scope as index_phil_scope
from dials.util import phil
from dxtbx.model.experiment_list import (
    Experiment,
    ExperimentList,
    ExperimentListFactory,
)
from workflows.services.common_service import CommonService

from dlstbx.services.per_image_analysis import msgpack_mangle_for_receiving
from dlstbx.util import ChainMapWithReplacement

UnitCell = tuple[float, float, float, float, float, float]


class IndexingPayload(pydantic.BaseModel):
    class Config:
        arbitrary_types_allowed = True

    experiments: ExperimentList
    reflections: flex.reflection_table
    unit_cell: Optional[uctbx.unit_cell] = None
    space_group: Optional[sgtbx.space_group_info] = None
    max_lattices: pydantic.PositiveInt = 1
    reference_geometry: Optional[Path] = None

    @pydantic.validator("unit_cell", pre=True)
    def check_unit_cell(cls, v):
        if not v:
            return None
        orig_v = v
        if isinstance(v, str):
            v = v.replace(",", " ").split()
        v = [float(v) for v in v]
        try:
            v = uctbx.unit_cell(v)
        except Exception:
            raise ValueError(f"Invalid unit_cell {orig_v}")
        return v

    @pydantic.validator("space_group", pre=True)
    def check_space_group(cls, v):
        if not v:
            return None
        try:
            v = sgtbx.space_group_info(v)
        except Exception:
            raise ValueError(f"Invalid space group {v}")
        return v


class IndexedLatticeResult(pydantic.BaseModel):
    unit_cell: tuple[float, float, float, float, float, float]
    space_group: str
    n_indexed: pydantic.NonNegativeInt


class IndexingResult(pydantic.BaseModel):
    lattices: list[IndexedLatticeResult]
    n_unindexed: pydantic.NonNegativeInt


class DLSIndexer(CommonService):
    """A service that analyses individual images."""

    # Human readable service name
    _service_name = "DLS Indexer"

    _logger_name = "dlstbx.services.indexer"

    def initializing(self):
        logging.getLogger("dials").setLevel(logging.WARNING)
        workflows.recipe.wrap_subscribe(
            self._transport,
            "index",
            self.index,
            acknowledgement=True,
            log_extender=self.extend_log,
            mangle_for_receiving=msgpack_mangle_for_receiving,
        )

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def index(self, rw: workflows.recipe.RecipeWrapper, header: dict, message: dict):
        parameters = ChainMapWithReplacement(
            message if isinstance(message, dict) else {},
            rw.recipe_step["parameters"],
            substitutions=rw.environment,
        )
        try:
            payload = IndexingPayload(**parameters)
        except pydantic.ValidationError as e:
            self.log.error(e, exc_info=True)
            rw.transport.ack(header)
            return

        if (n_refl := payload.reflections.size()) < 10:
            self.log.debug(
                f"Skipping indexing for reflection list with {n_refl} reflections"
            )
            indexing_result = IndexingResult(
                lattices=[],
                n_unindexed=payload.reflections.size(),
            )
        else:
            try:
                if payload.reference_geometry:
                    self.log.debug(
                        f"Loading reference geometry from {payload.reference_geometry}"
                    )
                    reference_expts = ExperimentListFactory.from_serialized_format(
                        payload.reference_geometry,
                        check_format=False,
                    )
                    reference_detector = reference_expts.detectors()[0]
                    reference_goniometer = (
                        reference_expts.goniometers()[0]
                        if len(reference_expts.goniometers())
                        else None
                    )

                    # copy across geometry to input experiment
                    imageset = payload.experiments[0].imageset
                    imageset.set_detector(reference_detector)
                    if reference_goniometer and imageset.get_goniometer():
                        imageset.set_goniometer()
                    payload.experiments = ExperimentList(
                        [
                            Experiment(
                                imageset=imageset,
                                beam=imageset.get_beam(),
                                detector=imageset.get_detector(),
                                goniometer=imageset.get_goniometer(),
                                scan=imageset.get_scan(),
                                crystal=None,
                            )
                        ]
                    )

                phil_params = index_phil_scope.fetch(source=phil.parse("")).extract()
                phil_params.indexing.known_symmetry.space_group = payload.space_group
                phil_params.indexing.known_symmetry.unit_cell = payload.unit_cell
                phil_params.indexing.multiple_lattice_search.max_lattices = (
                    payload.max_lattices
                )
                phil_params.indexing.method = "fft1d"
                phil_params.refinement.parameterisation.detector.fix = "all"
                idxr = indexer.Indexer.from_parameters(
                    payload.reflections,
                    payload.experiments,
                    params=phil_params,
                )
                idxr.index()
                indexed_expts = idxr.refined_experiments
                indexed_refl = idxr.refined_reflections
                indexed_refl.extend(idxr.unindexed_reflections)

                indexing_result = IndexingResult(
                    lattices=[
                        IndexedLatticeResult(
                            unit_cell=expt.crystal.get_unit_cell().parameters(),
                            space_group=str(expt.crystal.get_space_group().info()),
                            n_indexed=(indexed_refl["id"] == i_expt).count(True),
                        )
                        for i_expt, expt in enumerate(indexed_expts)
                    ],
                    n_unindexed=idxr.unindexed_reflections.size(),
                )
                self.log.info(indexing_result.json(indent=2))

            except Exception as e:
                self.log.debug(f"Indexing failed with message: {e}")
                indexing_result = IndexingResult(
                    lattices=[],
                    n_unindexed=payload.reflections.size(),
                )

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        result = indexing_result.dict()
        # Pass through all file* fields
        for key in (x for x in message if x.startswith("file")):
            result[key] = message[key]
        self.log.info(f"{message=}")
        self.log.info(f"{result=}")
        # Send results onwards
        rw.set_default_channel("result")
        rw.send_to("result", result, transaction=txn)
        rw.transport.transaction_commit(txn)
