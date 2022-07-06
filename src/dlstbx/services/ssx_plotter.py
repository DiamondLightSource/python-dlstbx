from __future__ import annotations

import json
import logging
import pathlib
import threading
from typing import Optional

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pydantic
import workflows.recipe
from workflows.services.common_service import CommonService

from dlstbx.util import ChainMapWithReplacement

log = logging.getLogger(__name__)

matplotlib.use("Agg")


class Payload(pydantic.BaseModel):
    dcid: pydantic.NonNegativeInt
    plot_file: pathlib.Path
    every: pydantic.NonNegativeInt = 1
    files_expected: Optional[pydantic.NonNegativeInt] = pydantic.Field(
        alias="files-expected"
    )
    images_expected: Optional[pydantic.NonNegativeInt] = pydantic.Field(
        alias="images-expected"
    )
    spot_count_cutoff: pydantic.NonNegativeInt = 16


class PerImageAnalysisPayload(pydantic.BaseModel):
    file_number: pydantic.PositiveInt = pydantic.Field(alias="file-number")
    n_spots_total: pydantic.NonNegativeInt
    file_seen_at: pydantic.NonNegativeFloat = pydantic.Field(alias="file-seen-at")


class SSXPlotter(CommonService):
    """A service that analyses individual images."""

    # Human readable service name
    _service_name = "SSX plotter"

    _logger_name = "dlstbx.services.ssx_plotter"

    def initializing(self):
        self._data: dict[int, list[PerImageAnalysisPayload]] = {}
        self._dcid_to_subscription_id = {}
        self._lock = threading.Lock()

        workflows.recipe.wrap_subscribe(
            self._transport,
            "plot_ssx",
            self.plot,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def plot(
        self,
        rw: workflows.recipe.RecipeWrapper,
        header: dict,
        message: dict,
    ):
        self.log.info(f"received message {message}")
        parameters = ChainMapWithReplacement(
            message if isinstance(message, dict) else {},
            rw.recipe_step["parameters"],
            substitutions=rw.environment,
        )
        payload = Payload(**parameters)
        self.log.debug(f"{payload=}")

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        expected_total = payload.files_expected or payload.images_expected
        every = payload.every
        expected_result_count = expected_total // every
        message_delay = 1  # second

        def receive_result(
            rw: workflows.recipe.RecipeWrapper,
            header: dict,
            message: dict,
        ):
            dcid = int(rw.recipe_step["parameters"]["dcid"])

            with self._lock:
                if dcid == payload.dcid:
                    if dcid not in self._data:
                        # We've already unsubscribed from this subscription
                        return
                    self.log.debug(f"Received message {message}")
                    pia_payload = PerImageAnalysisPayload(**message)
                    self.log.debug(f"Received payload {pia_payload}")
                    self._data[dcid].append(pia_payload)

            try:
                self.log.debug(f"ACK'ing {header=}")
                rw.transport.ack(header)
            except Exception as e:
                self.log.debug(f"Failed to ACK {header=} with {e}")

        with self._lock:

            self.log.debug(f"{payload=}")
            self.log.debug(f"{self._data=}")
            self.log.debug(f"{self._dcid_to_subscription_id=}")
            if payload.dcid not in self._data:
                self._data[payload.dcid] = []

                self.log.debug(f"Subscribing to results stream for dcid={payload.dcid}")

                stream_subscription_id = workflows.recipe.wrap_subscribe(
                    self._transport,
                    "ssx.stream",
                    receive_result,
                    acknowledgement=True,
                    log_extender=self.extend_log,
                    arguments={"x-stream-offset": "first"},
                )
                self._dcid_to_subscription_id[payload.dcid] = stream_subscription_id
                # Send results to myself for next round of processing
                rw.checkpoint(
                    json.loads(payload.json(by_alias=True)),
                    delay=message_delay,
                    transaction=txn,
                )
                rw.transport.transaction_commit(txn)
                return

            elif len(results := self._data[payload.dcid]) < expected_result_count:
                self.log.info(f"Found {len(results)} / {expected_result_count} results")
                # Send results to myself for next round of processing
                rw.checkpoint(
                    json.loads(payload.json(by_alias=True)),
                    delay=message_delay,
                    transaction=txn,
                )
                rw.transport.transaction_commit(txn)
                return

            else:
                # Found all results \o/

                n_spots_total = {
                    result.file_number: result.n_spots_total for result in results
                }

                plt.figure()
                ax = plt.subplot()

                plot_hits(n_spots_total, ax=ax)

                filename = payload.plot_file
                filename.parent.mkdir(parents=True, exist_ok=True)
                thumbnail = filename.parent / f"{filename.stem}t{filename.suffix}"
                plt.savefig(filename)
                self.log.debug(f"Saved plot to {filename}")
                plt.savefig(thumbnail)
                self.log.debug(f"Saved thumbnail plot to {thumbnail}")

                rw.transport.transaction_commit(txn)

                # Clean up after ourselves by removing the stream subscription
                subscription_id = self._dcid_to_subscription_id.pop(payload.dcid)
                if subscription_id is not None:
                    self.log.debug(f"Unsubscribing {subscription_id=}")
                    rw.transport.unsubscribe(
                        subscription_id, drop_callback_reference=False
                    )
                # And deleting any data associated with this dcid
                del self._data[payload.dcid]
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
