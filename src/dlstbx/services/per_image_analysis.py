import logging
import time

import bitshuffle  # noqa, F401; cf. https://github.com/DiamondLightSource/python-dlstbx/issues/5
import workflows.recipe
from dials.command_line.find_spots_server import work
from workflows.services.common_service import CommonService

import dlstbx.util.sanity


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
            "per_image_analysis",
            self.per_image_analysis,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def per_image_analysis(self, rw, header, message):
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

        # Set up PIA parameters
        filename = message["file"]
        parameters = rw.recipe_step.get("parameters", {})
        if not parameters:
            parameters = {"d_max": 40}
        if isinstance(message.get("parameters"), dict):
            parameters.update(message["parameters"])
        parameters = [f"{k}={v}" for k, v in parameters.items()]

        self.log.debug("Starting PIA on %s", filename)

        # Do the per-image-analysis
        start = time.time()
        try:
            results = work(filename, cl=parameters)
        except Exception as e:
            if isinstance(e, RuntimeError) and str(e).startswith(
                "Server does not have read access to file"
            ):
                missing_fs = ",".join(dlstbx.util.sanity.get_missing_file_systems())
                self.log.critical(
                    "Terminating service after filesystem access error with %r.\nNode missing access to file systems: %s",
                    e,
                    missing_fs,
                    exc_info=True,
                )
                self._request_termination()
                return
            self.log.error(
                "PIA on %s with parameters %s failed with %r",
                filename,
                parameters,
                e,
                exc_info=True,
            )
            rw.transport.nack(header)
            return
        runtime = time.time() - start

        # Pass through all file* fields
        for key in (x for x in message if x.startswith("file")):
            results[key] = message[key]

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        # Send results onwards
        rw.set_default_channel("result")
        rw.send_to("result", results, transaction=txn)
        rw.transport.transaction_commit(txn)
        self.log.info(
            "PIA completed on %s with parameters %s, %d spots found in %.2f seconds",
            filename,
            parameters,
            results["n_spots_total"],
            runtime,
            extra={
                "pia-time": runtime,
            },
        )
