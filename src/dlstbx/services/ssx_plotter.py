from __future__ import annotations

import logging
import pathlib
import threading

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pydantic
import workflows.recipe
from workflows.services.common_service import CommonService

from dlstbx.util import ChainMapWithReplacement

from .aggregator import PerImageAnalysisPayload

log = logging.getLogger(__name__)

matplotlib.use("Agg")


class Payload(pydantic.BaseModel):
    dcid: pydantic.NonNegativeInt
    pia_results: list[PerImageAnalysisPayload]
    plot_file: pathlib.Path
    spot_count_cutoff: pydantic.NonNegativeInt = 16


class SSXPlotter(CommonService):
    """A service that generates SSX analysis plots"""

    # Human readable service name
    _service_name = "SSX plotter"

    _logger_name = "dlstbx.services.ssx_plotter"

    def initializing(self):
        self._pia_data: dict[int, list[PerImageAnalysisPayload]] = {}
        self._dcid_to_subscription_id = {}
        self._lock = threading.Lock()

        workflows.recipe.wrap_subscribe(
            self._transport,
            "ssx.plot_pia",
            self.plot_pia,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def plot_pia(
        self,
        rw: workflows.recipe.RecipeWrapper,
        header: dict,
        message: dict,
    ):
        parameters = ChainMapWithReplacement(
            message if isinstance(message, dict) else {},
            rw.recipe_step["parameters"],
            substitutions=rw.environment,
        )
        payload = Payload(**parameters)

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        n_spots_total = {
            result.file_number: result.n_spots_total for result in payload.pia_results
        }

        plt.figure()
        ax = plt.subplot()

        plot_hits(n_spots_total, ax=ax)

        filename = payload.plot_file
        filename.parent.mkdir(parents=True, exist_ok=True)
        thumbnail = filename.parent / f"{filename.stem}t{filename.suffix}"
        plt.savefig(filename)
        self.log.info(f"Saved plot to {filename}")
        plt.savefig(thumbnail)
        self.log.info(f"Saved thumbnail plot to {thumbnail}")

        rw.transport.transaction_commit(txn)
        return


def plot_hits(n_spots_total: dict[int, int], spot_count_cutoff: int = 16, ax=plt.Axes):
    if ax is None:
        ax = plt.gca()

    image_numbers = np.array(list(n_spots_total.keys()))
    strong = np.array(list(n_spots_total.values()))

    # sort by image number
    ind = image_numbers.argsort()
    image_numbers = image_numbers[ind]
    strong = strong[ind]

    ax.scatter(image_numbers, strong)
    if spot_count_cutoff:
        hit_rate = 100 * np.count_nonzero(strong > spot_count_cutoff) / len(strong)
        ax.text(
            0.05,
            0.95,
            f"Estimate hits rate: {hit_rate:.1f} %",
            transform=ax.transAxes,
            va="top",
        )
    ax.set_xlabel("Image Number")
    ax.set_ylabel("Spot Counts")
    return ax
