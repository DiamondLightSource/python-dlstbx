from __future__ import annotations

import os.path
from typing import Any

import pydantic
from workflows.recipe.typed import typed_handler
from workflows.services.common_service import CommonService

import dlstbx.util.hdf5


class NexusParserParameters(pydantic.BaseModel):
    file: str | None = None
    run_status: str | None = None
    expected_images: int | None = None

    @property
    def error_expected(self) -> bool:
        """Whether errors are expected given the data collection run status"""
        return (
            self.run_status is not None
            and self.run_status != "DataCollection Successful"
        )


class DLSNexusParser(CommonService):
    """A service that answers questions about Nexus files."""

    # Human readable service name
    _service_name = "DLS NexusParser"

    # Logger name
    _logger_name = "dlstbx.services.nexusparser"

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Nexusparser starting")
        self.subscribe(
            "nexusparser.find_related_files",
            self.find_related_files,
        )

    @typed_handler
    def find_related_files(
        self, rw, header, message: Any, parameters: NexusParserParameters
    ):
        """Take a single file and recursively find all referenced files."""

        # Find file to start with
        root_file = parameters.file
        if not root_file and isinstance(message, dict):
            root_file = message.get("file")

        # Sanity check received message
        if not root_file:
            self.log.error("No file specified")
            rw.transport.nack(header)
            return

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        if not os.path.exists(root_file):
            if parameters.error_expected:
                self.log.info(
                    "Ignoring missing file %s due to run status '%s'",
                    root_file,
                    parameters.run_status,
                )
            else:
                self.log.error("File %s not found", root_file)
                rw.send_to("error", "File %s not found" % root_file, transaction=txn)
            self._transport.transaction_commit(txn)
            return

        # Get list of all referenced files
        self.log.debug("Finding files related to %s", root_file)
        try:
            related = dlstbx.util.hdf5.find_all_references(root_file)
        except (ValueError, KeyError):
            self.log.error(
                f"Could not find files related to {root_file}", exc_info=True
            )
            rw.transport.transaction_abort(txn)
            rw.transport.nack(header)
            return
        self.log.info("Found %d files related to %s", len(related), root_file)

        expected_images = parameters.expected_images
        if expected_images is not None:
            # Check all images are there
            seen_images = sum(v for v in related.values() if v is not None)
            if seen_images < expected_images:
                errors = [
                    "Dataset contains %d images instead of expected %d images"
                    % (seen_images, expected_images)
                ]
                for k in sorted(related):
                    if related[k] is None:
                        errors.append("File %s is invalid or missing" % k)
                    else:
                        errors.append("File %s contains %d images" % (k, related[k]))
                if parameters.error_expected:
                    self.log.info(
                        "Ignoring the following errors due to run status '%s'\n%s",
                        parameters.run_status,
                        "\n".join(errors),
                    )
                else:
                    self.log.warning("\n".join(errors))
                    rw.send_to("error", "\n".join(errors), transaction=txn)

        # Notify listeners
        for filename in related:
            rw.send_to("every", {"file": filename}, transaction=txn)

        rw.set_default_channel("all")
        rw.send_to("all", {"filelist": sorted(related)}, transaction=txn)

        self._transport.transaction_commit(txn)
        self.log.debug("Done.")
