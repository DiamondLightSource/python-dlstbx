from __future__ import annotations

import logging

import pydantic
import workflows.recipe
from dials.algorithms.indexing import indexer
from dials.array_family import flex
from dials.command_line.index import phil_scope as index_phil_scope
from dials.util import phil
from dxtbx.model import ExperimentList
from workflows.services.common_service import CommonService

from dlstbx.services.per_image_analysis import msgpack_mangle_for_receiving


class IndexingPayload(pydantic.BaseModel):
    class Config:
        arbitrary_types_allowed = True

    experiments: ExperimentList
    reflections: flex.reflection_table


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
    def index(
        self, rw: workflows.recipe.RecipeWrapper, header: dict, message: IndexingPayload
    ):
        results = {}
        if (n_refl := message.reflections.size()) < 10:
            self.log.debug(
                f"Skipping indexing for reflection list with {n_refl} reflections"
            )
        else:
            try:
                phil_params = index_phil_scope.fetch(source=phil.parse("")).extract()
                idxr = indexer.Indexer.from_parameters(
                    message.reflections,
                    message.experiments,
                    params=phil_params,
                )
                idxr.index()
                indexed_expts = idxr.refined_experiments
                indexed_refl = idxr.refined_reflections
                indexed_refl.extend(idxr.unindexed_reflections)

                results = {
                    "unit_cells": [
                        expt.crystal.get_unit_cell().parameters()
                        for expt in indexed_expts
                    ],
                    "n_indexed": [
                        (indexed_refl["id"] == i_expt).count(True)
                        for i_expt in range(len(indexed_expts))
                    ],
                }

                # A context manager would be really useful here
                logging.getLogger("dials").setLevel(logging.INFO)
                idxr.show_experiments(indexed_expts, indexed_refl)
                logging.getLogger("dials").setLevel(logging.WARNING)
            except Exception as e:
                self.log.debug(f"Indexing failed with message: {e}")

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        # Send results onwards
        rw.set_default_channel("result")
        rw.send_to("result", results, transaction=txn)
        rw.transport.transaction_commit(txn)
