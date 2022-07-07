from __future__ import annotations

import json
import logging
import threading
import time
from typing import Optional

import pydantic
import workflows.recipe
from workflows.services.common_service import CommonService

from dlstbx.util import ChainMapWithReplacement

log = logging.getLogger(__name__)


class Status(pydantic.BaseModel):
    start_time: Optional[pydantic.PositiveFloat] = None


class Payload(pydantic.BaseModel):
    dcid: pydantic.NonNegativeInt
    every: pydantic.NonNegativeInt = 1
    files_expected: Optional[pydantic.NonNegativeInt] = pydantic.Field(
        alias="files-expected"
    )
    images_expected: Optional[pydantic.NonNegativeInt] = pydantic.Field(
        alias="images-expected"
    )
    timeout: pydantic.PositiveFloat = 3600
    status: Optional[Status]


class PerImageAnalysisPayload(pydantic.BaseModel):
    file_number: pydantic.PositiveInt = pydantic.Field(alias="file-number")
    n_spots_total: pydantic.NonNegativeInt
    file_seen_at: pydantic.NonNegativeFloat = pydantic.Field(alias="file-seen-at")


class Aggregator(CommonService):
    """A service that aggregates results in a scatter-gather manner"""

    # Human readable service name
    _service_name = "Aggregator"

    _logger_name = "dlstbx.services.aggregator"

    def initializing(self):
        self._pia_data: dict[int, list[PerImageAnalysisPayload]] = {}
        self._dcid_to_subscription_id = {}
        self._lock = threading.Lock()

        workflows.recipe.wrap_subscribe(
            self._transport,
            "aggregate.pia",
            self.aggregate_pia,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def aggregate_pia(
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

        if payload.status is None:
            payload.status = Status(start_time=time.time())

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
                    if dcid not in self._pia_data:
                        # We've already unsubscribed from this subscription
                        return
                    pia_payload = PerImageAnalysisPayload(**message)
                    self._pia_data[dcid].append(pia_payload)

            try:
                rw.transport.ack(header)
            except Exception as e:
                self.log.debug(f"Failed to ACK {header=} with {e}")

        with self._lock:

            if payload.dcid not in self._pia_data:
                self.log.info(f"Subscribing to PIA results for dcid={payload.dcid}")

                self._pia_data[payload.dcid] = []

                stream_subscription_id = workflows.recipe.wrap_subscribe(
                    self._transport,
                    "pia.stream",
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

            results: list[PerImageAnalysisPayload] = self._pia_data[payload.dcid]
            self.log.info(f"Found {len(results)} / {expected_result_count} results")

            if len(results) < expected_result_count:
                if (payload.status.start_time + payload.timeout) < time.time():
                    # Not found all messages, so checkpoint message with a delay
                    rw.checkpoint(
                        json.loads(payload.json(by_alias=True)),
                        delay=message_delay,
                        transaction=txn,
                    )
                else:
                    # Give up waiting
                    self.log.info(
                        f"Timed out waiting for PIA results for dcid={payload.dcid}"
                    )
            else:
                # Found all results \o/
                unique_results = {r.file_number: r for r in results}
                sorted_results = [r for _, r in sorted(unique_results.items())]
                results_as_dicts = [r.dict(by_alias=True) for r in sorted_results]
                rw.send({"pia_results": results_as_dicts})

            rw.transport.transaction_commit(txn)
            return
