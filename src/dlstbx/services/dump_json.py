from __future__ import annotations

import json
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


class IndexingResult(pydantic.BaseModel):
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

        workflows.recipe.wrap_subscribe(
            self._transport,
            "json",
            self.receive_msg,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

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

        self.log.debug("Running json call %s", command)
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.set_default_channel("output")

        parameters = ChainMapWithReplacement(
            message if isinstance(message, dict) else {},
            rw.recipe_step["parameters"],
            substitutions=rw.environment,
        )
        self.log.info(f"{parameters=}")

        try:
            result = command_function(
                rw=rw,
                message=message,
                parameters=parameters,
                transaction=txn,
                header=header,
            )
        except Exception as e:
            self.log.error(
                f"Uncaught exception {e!r} in json function {command!r}, "
                "quarantining message and shutting down instance.",
                exc_info=True,
            )
            self.log.debug(f"{parameters=}")
            rw.transport.transaction_abort(txn)
            rw.transport.nack(header)
            self._request_termination()
            return

        if result and result.get("success"):
            rw.send({"result": result.get("return_value")}, transaction=txn)
            rw.transport.ack(header, transaction=txn)
        else:
            rw.transport.transaction_abort(txn)
            rw.transport.nack(header)
            return
        rw.transport.transaction_commit(txn)

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def do_store_per_image_analysis_result(
        self,
        *,
        parameters: PerImageAnalysisPayload,
        **kwargs,
    ):
        self.log.debug(f"{parameters=}")
        parameters.output_filename.parent.mkdir(exist_ok=True, parents=True)
        with parameters.output_filename.open(mode="a") as fh:
            fh.write(
                json.dumps(parameters.dict(include={"file_number", "n_spots_total"}))
                + "\n"
            )
        return {"success": True}

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def do_store_indexing_result(
        self,
        *,
        parameters: IndexingResult,
        **kwargs,
    ):
        self.log.debug(f"{parameters=}")
        parameters.output_filename.parent.mkdir(exist_ok=True, parents=True)
        with parameters.output_filename.open(mode="a") as fh:
            fh.write(
                json.dumps(
                    parameters.dict(include={"file_number", "lattices", "n_unindexed"})
                )
                + "\n"
            )
        return {"success": True}
