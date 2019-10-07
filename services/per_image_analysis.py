from __future__ import absolute_import, division, print_function

import logging
import time

import dlstbx.util.sanity
import workflows.recipe
from dials.command_line.find_spots_server import work
from dlstbx.services.filewatcher import is_file_selected
from workflows.services.common_service import CommonService


class DLSPerImageAnalysis(CommonService):
    """A service that analyses individual images."""

    # Human readable service name
    _service_name = "DLS Per-Image-Analysis"

    _logger_name = "dlstbx.services.per_image_analysis"

    def initializing(self):
        logging.getLogger("dials").setLevel(logging.WARNING)

        # Check node health before starting service
        missing_fs = ",".join(dlstbx.util.sanity.get_missing_file_systems())
        if missing_fs:
            self.log.critical(
                "Rejecting service initialisation: node missing access to file system(s) %s",
                missing_fs,
            )
            self._request_termination()
            return

        # The main per_image_analysis queue.
        # For every received message a single frame will be analysed.
        workflows.recipe.wrap_subscribe(
            self._transport,
            "per_image_analysis",
            self.per_image_analysis,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

        # A queue for a helper function that can generate valid PIA messages for
        # EIGER/HDF5 data collections. It needs to know the location of the
        # master file, the image range and how many images should be picked.
        # Messages are generated without opening or validating the file.
        workflows.recipe.wrap_subscribe(
            self._transport,
            "per_image_analysis.hdf5_select",
            self.hdf5_select,
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

        # Extract the filename
        filename = str(message["file"])  # https://github.com/cctbx/dxtbx/issues/12

        # Set up PIA parameters
        parameters = rw.recipe_step.get("parameters", {})
        if not parameters:
            parameters = {"d_max": 40}
        if isinstance(message.get("parameters"), dict):
            parameters.update(message["parameters"])
        parameters = ["{k}={v}".format(k=k, v=v) for k, v in parameters.items()]

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
        for key in filter(lambda x: x.startswith("file"), message):
            results[key] = message[key]

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin()
        rw.transport.ack(header, transaction=txn)

        # Send results onwards
        rw.set_default_channel("result")
        rw.send_to("result", results, transaction=txn)
        rw.transport.transaction_commit(txn)
        self.log.info(
            "PIA completed on %s with parameters %s, %d spots found in %.1f seconds",
            filename,
            parameters,
            results["n_spots_total"],
            runtime,
        )

    def hdf5_select(self, rw, header, message):
        """Generate PIA messages for an HDF5/EIGER file.

        Recipe parameters:
        { "parameters": { "file": location of master file,
                          "image-start": int(first image number),
                          "image-end": int(last image number),
        }               }

        Message payload:
        is ignored

        Output streams:
        "every": message is generated for every image number n in
                 start <= n <= end. This is the default output.
        "select-n": message is generated for up to n approximately equidistant
                    images within the range.

        Output message format:
        { "file": copied over from recipe parameter,
          "file-number": int(image number n),
          "parameters": {"scan_range": "n,n" (image number)}
        }
        """

        # Validate input
        parameters = rw.recipe_step.get("parameters", {})
        filename = parameters.get("file")
        start = parameters.get("image-start")
        end = parameters.get("image-end")
        if (
            None in (filename, start, end)
            or not str(start).isdigit()
            or not str(end).isdigit()
            or int(start) > int(end)
        ):
            self.log.warning(
                "Can not generate PIA messages for invalid range specification %r:%r:%r",
                filename,
                start,
                end,
            )
            rw.transport.nack(header)
            return
        start, end = int(start), int(end)
        count = end - start + 1
        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin()
        rw.transport.ack(header, transaction=txn)

        # Identify selections to notify for
        selections = [
            k
            for k in rw.recipe_step["output"]
            if isinstance(k, basestring) and k.startswith("select-")
        ]
        selections = {int(k[7:]): k for k in selections}

        # Generate messages
        for n in range(start, end + 1):
            message = {
                "file": filename,
                "file-number": n,
                "parameters": {"scan_range": "{0},{0}".format(n)},
            }

            # Notify for every file
            rw.send_to("every", message, transaction=txn)

            # Notify for selections
            for m, dest in selections.iteritems():
                if is_file_selected(n, m, count):
                    rw.send_to(dest, message, transaction=txn)

        # Finish up
        rw.transport.transaction_commit(txn)
        self.log.info("PIA messages generated for %s:%d:%d", filename, start, end)
