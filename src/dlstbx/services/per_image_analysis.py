from __future__ import annotations

import io
import logging
import pathlib
import time
from typing import Optional

import dials.util.ext
import msgpack
import pydantic
import workflows.recipe
from dials.array_family import flex
from dxtbx.model.experiment_list import ExperimentList, ExperimentListFactory
from workflows.services.common_service import CommonService

import dlstbx.util.sanity
from dlstbx.util import ChainMapWithReplacement
from dlstbx.util.per_image_analysis import (
    PerImageAnalysisParameters,
    do_per_image_analysis,
)


class PerImageAnalysisPayload(pydantic.BaseModel):
    file: pathlib.Path
    parameters: Optional[PerImageAnalysisParameters] = None


def msgpack_mangle_for_sending(message):
    return msgpack.packb(message, default=msgpack_serializer)


def msgpack_mangle_for_receiving(message):
    return msgpack.unpackb(
        message,
        object_hook=decode_experiment_list,
        ext_hook=msgpack_ext_hook,
        strict_map_key=False,
    )


def msgpack_serializer(obj):
    if isinstance(obj, flex.reflection_table):
        buf = io.BytesIO()
        obj.as_msgpack_to_file(dials.util.ext.streambuf(python_file_obj=buf))
        return msgpack.ExtType(1, buf.getvalue())
    elif isinstance(obj, ExperimentList):
        return obj.to_dict()
    raise TypeError("Unknown type: %r" % (obj,))


def msgpack_ext_hook(code, data):
    if code == 1:
        return flex.reflection_table.from_msgpack(data)
    return msgpack.ExtType(code, data)


def decode_experiment_list(obj):
    if (oid := obj.get("__id__")) and oid == "ExperimentList":
        return ExperimentListFactory.from_dict(obj, check_format=False)
    return obj


class DLSPerImageAnalysis(CommonService):
    """A service that analyses individual images."""

    # Human readable service name
    _service_name = "DLS Per-Image-Analysis"

    _logger_name = "dlstbx.services.per_image_analysis"

    def initializing(self):
        logging.getLogger("dials").setLevel(logging.WARNING)
        logging.getLogger("dials.util.masking").setLevel(logging.DEBUG)

        self.log.debug("Checking node health before starting service")
        missing_fs = ",".join(dlstbx.util.sanity.get_missing_file_systems())
        if missing_fs:
            self.log.critical(
                "Rejecting service initialisation: node missing access to file system(s) %s",
                missing_fs,
            )
            self._request_termination()
            return
        self.log.info("Node self-check passed")

        # The main per_image_analysis queue.
        # For every received message a single frame will be analysed.
        workflows.recipe.wrap_subscribe(
            self._transport,
            self._environment.get("queue") or "per_image_analysis",
            self.per_image_analysis,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    # @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def per_image_analysis(
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
        payload = PerImageAnalysisPayload(**parameters)
        """Run PIA on one image.

        Recipe parameters:
        { "parameters": { "d_max": 40, ... } }
        These will all be passed through as parameters to dials.find_spots.
        If no parameters are defined the shown default will be used.

        Minimum message payload:
        { "file": full file path }
        All fields named file* are passed on to the output.

        Recommended message payload format:
        { "file": full file path,
          "file-number": int(image number, starting at 1),
          "file-pattern-index": int(variable part of filename), if applicable,
          "parameters": dict(values will be .updated() into the recipe parameters) }

        Output streams:
        "result": This is the default output.

        Output message format:
        { "file*": copied over from input message,

            other fields are determined by the DIALS work() function,
            currently these are:
          "d_min_distl_method_1": ...,
          "d_min_distl_method_2": ...,
          "estimated_d_min": ...,
          "n_spots_4A": ...,
          "n_spots_no_ice": ...,
          "n_spots_total": ...,
          "noisiness_method_1": ...,
          "noisiness_method_2": ...,
          "total_intensity": ... }
        """

        self.log.debug("Starting PIA on %s", payload.file)
        params = payload.parameters or PerImageAnalysisParameters()

        # Do the per-image-analysis
        start = time.time()
        try:
            expts, reflections, pia_results = do_per_image_analysis(
                payload.file, params
            )
        except Exception as e:
            # if isinstance(e, RuntimeError) and str(e).startswith(
            #     "Server does not have read access to file"
            # ):
            #     missing_fs = ",".join(dlstbx.util.sanity.get_missing_file_systems())
            #     self.log.critical(
            #         "Terminating service after filesystem access error with %r.\nNode missing access to file systems: %s",
            #         e,
            #         missing_fs,
            #         exc_info=True,
            #     )
            #     self._request_termination()
            #     return
            self.log.error(
                "PIA on %s with parameters %s failed with %r",
                payload.file,
                params,
                e,
                exc_info=True,
            )
            txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
            rw.transport.nack(header, transaction=txn)
            rw.transport.transaction_commit(txn)
            return
        runtime = time.time() - start

        results = pia_results.dict()
        # Pass through all file* fields
        for key in (x for x in message if x.startswith("file")):
            results[key] = message[key]

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        if (
            (output := rw.recipe_step.get("output", {}))
            and isinstance(output, dict)
            and (recipe_pointer := output.get("reflections"))
        ):
            reflections_queue = rw.recipe[recipe_pointer].get("queue")
            # We don't need the shoeboxes for indexing and they just take up space
            del reflections["shoebox"]

            payload_out = {
                "reflections": reflections,
                "experiments": expts,
            }
            # Pass through all file* fields
            for key in (x for x in message if x.startswith("file")):
                payload_out[key] = message[key]

            self.log.info(f"Sending reflections to {reflections_queue}")
            rw.send_to(
                "reflections",
                payload_out,
                mangle_for_sending=msgpack_mangle_for_sending,
                transaction=txn,
            )

        # Send results onwards
        rw.set_default_channel("result")
        rw.send_to("result", results, transaction=txn)
        rw.transport.transaction_commit(txn)
        self.log.info(
            "PIA completed on %s with parameters %s, %d spots found in %.2f seconds",
            payload.file,
            params,
            results["n_spots_total"],
            runtime,
            extra={
                "pia-time": runtime,
            },
        )
