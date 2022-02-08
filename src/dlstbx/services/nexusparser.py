from __future__ import annotations

import os.path

import workflows.recipe
from workflows.services.common_service import CommonService

import dlstbx.util.hdf5


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

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        error_expected = (
            "run_status" in rw.recipe_step.get("parameters", {})
            and rw.recipe_step["parameters"]["run_status"]
            != "DataCollection Successful"
        )

        if not os.path.exists(root_file):
            if error_expected:
                self.log.info(
                    "Ignoring missing file %s due to run status '%s'",
                    root_file,
                    rw.recipe_step["parameters"]["run_status"],
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
        except ValueError:
            self.log.error("Could not find files related to %s", root_file)
            rw.transport.nack(header)
            return
        self.log.info("Found %d files related to %s", len(related), root_file)

        expected_images = rw.recipe_step.get("parameters", {}).get("expected_images")
        if expected_images is not None:
            # Check all images are there
            try:
                expected_images = int(expected_images)
            except ValueError:
                self.log.error(
                    "Invalid number of expected images (%r)", expected_images
                )
                rw.transport.nack(header)
                return
            seen_images = sum(v for v in related.values() if v is not None)
            if seen_images != expected_images:
                errors = [
                    "Dataset contains %d images instead of expected %d images"
                    % (seen_images, expected_images)
                ]
                for k in sorted(related):
                    if related[k] is None:
                        errors.append("File %s is invalid or missing" % k)
                    else:
                        errors.append("File %s contains %d images" % (k, related[k]))
                if (
                    "run_status" in rw.recipe_step.get("parameters", {})
                    and rw.recipe_step["parameters"]["run_status"]
                    != "DataCollection Successful"
                ):
                    self.log.info(
                        "Ignoring the following errors due to run status '%s'\n%s",
                        rw.recipe_step["parameters"]["run_status"],
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
