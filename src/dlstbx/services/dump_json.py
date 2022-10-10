from __future__ import annotations

import itertools
import json
import threading
from pathlib import Path

import pydantic
import workflows.recipe
from workflows.services.common_service import CommonService

from dlstbx.services.indexer import IndexedLatticeResult
from dlstbx.util import ChainMapWithReplacement


def lookup_command(command, refclass):
    return getattr(refclass, "do_" + command, None)


class PerImageAnalysisPayload(pydantic.BaseModel):
    output_filename: Path
    file_number: pydantic.PositiveInt = pydantic.Field(alias="file-number")
    n_spots_total: pydantic.NonNegativeInt

    class Config:
        allow_population_by_field_name = True


class IndexingPayload(pydantic.BaseModel):
    output_filename: Path
    file_number: pydantic.NonNegativeInt = pydantic.Field(alias="file-number")
    lattices: list[IndexedLatticeResult]
    n_unindexed: pydantic.NonNegativeInt


class JSON(CommonService):
    """A service that takes RabbitMQ messages and moves them to ActiveMQ."""

    # Human readable service name
    _service_name = "JSON"

    # Logger name
    _logger_name = "dlstbx.services.json"

    def initializing(self):

        self._register_idle(1, self.process_messages)
        workflows.recipe.wrap_subscribe(
            self._transport,
            "json",
            self.receive_msg,
            acknowledgement=True,
            exclusive=True,
            log_extender=self.extend_log,
            prefetch_count=100,
        )
        self._lock = threading.Lock()
        self._data = {}

    def receive_msg(
        self, rw: workflows.recipe.RecipeWrapper, header: dict, message: dict
    ):
        command = rw.recipe_step["parameters"].get("command")
        if not command:
            self.log.error("Received message is not a valid json command")
            rw.transport.nack(header)
            return
        command_function = lookup_command(command, self)
        if not command_function:
            self.log.error("Received unknown json command (%s)", command)
            rw.transport.nack(header)
            return

        with self._lock:
            self._data.setdefault(command, [])
            parameters = ChainMapWithReplacement(
                message if isinstance(message, dict) else {},
                rw.recipe_step["parameters"],
                substitutions=rw.environment,
            )
            self._data[command].append((header, parameters))
            if len(self._data) == 100:
                self.process_messages()

    def process_messages(self):
        with self._lock:
            for command, data in self._data.items():
                self.log.debug("Running json call %s", command)
                command_function = lookup_command(command, self)
                for output_filename, grouped_data in itertools.groupby(
                    data, key=lambda d: d[1]["output_filename"]
                ):
                    grouped_data = list(grouped_data)
                    header, _ = grouped_data[0]
                    txn = self.transport.transaction_begin(
                        subscription_id=header["subscription"]
                    )
                    try:
                        command_function(
                            output_filename=output_filename,
                            results=[params for _, params in grouped_data],
                        )
                    except Exception as e:
                        self.log.error(
                            f"Uncaught exception {e!r} in json function {command!r}",
                            exc_info=True,
                        )
                        for header, _ in grouped_data:
                            self.transport.nack(header)
                        return
                    else:
                        for header, _ in grouped_data:
                            self.transport.ack(header, transaction=txn)
                    self.transport.transaction_commit(txn)

            self._data = {}

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def do_store_per_image_analysis_result(
        self,
        *,
        output_filename: Path,
        results: list[PerImageAnalysisPayload],
        **kwargs,
    ):
        self.log.debug(f"{results=}")
        output_filename.parent.mkdir(exist_ok=True, parents=True)
        with output_filename.open(mode="a") as fh:
            for result in results:
                fh.write(
                    json.dumps(result.dict(include={"file_number", "n_spots_total"}))
                    + "\n"
                )

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def do_store_indexing_result(
        self,
        *,
        output_filename: Path,
        results: list[IndexingPayload],
        **kwargs,
    ):
        self.log.debug(f"{results=}")
        output_filename.parent.mkdir(exist_ok=True, parents=True)
        with output_filename.open(mode="a") as fh:
            for result in results:
                fh.write(
                    json.dumps(
                        result.dict(include={"file_number", "lattices", "n_unindexed"})
                    )
                    + "\n"
                )
