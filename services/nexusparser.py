from __future__ import absolute_import, division, print_function

import os.path

import dlstbx.util.hdf5
import workflows.recipe
from workflows.services.common_service import CommonService


class DLSNexusParser(CommonService):
    """A service that answers questions about Nexus files."""

    # Human readable service name
    _service_name = "DLS NexusParser"

    # Logger name
    _logger_name = "dlstbx.services.nexusparser"

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Nexusparser starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "nexusparser.find_related_files",
            self.find_related_files,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def find_related_files(self, rw, header, message):
        """Take a single file and recursively find all referenced files."""

        # Find file to start with
        root_file = rw.recipe_step.get("parameters", {}).get("file")
        if not root_file:
            root_file = message.get("file")

        # Sanity check received message
        if not root_file:
            self.log.error("No file specified")
            rw.transport.nack(header)
            return

        if not os.path.exists(root_file):
            self.log.error("File %s not found", root_file)
            rw.transport.nack(header)
            return

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin()
        rw.transport.ack(header, transaction=txn)

        # Get list of all referenced files
        self.log.debug("Finding files related to %s", root_file)
        try:
            related = dlstbx.util.hdf5.find_all_references(root_file)
        except ValueError:
            self.log.error("Could not find files related to %s", root_file)
            rw.transport.nack(header)
            return
        self.log.info("Found %d files related to %s", len(related), root_file)

        # Notify listeners
        for filename in related:
            rw.send_to("every", {"file": filename}, transaction=txn)

        rw.set_default_channel("all")
        rw.send_to("all", {"filelist": list(related)}, transaction=txn)

        self._transport.transaction_commit(txn)
        self.log.debug("Done.")
