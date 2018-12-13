from __future__ import absolute_import, division, print_function

import os

import PIL.Image
import procrunner
import workflows.recipe
from workflows.services.common_service import CommonService


class DLSImages(CommonService):
    """A service that generates images and thumbnails."""

    # Human readable service name
    _service_name = "DLS Images"

    # Logger name
    _logger_name = "dlstbx.services.images"

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Image service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "images",
            self.image_call,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def image_call(self, rw, header, message):
        """Call dispatcher."""
        command = rw.recipe_step.get("parameters", {}).get("image_command")
        if command == "thumbnail":
            return self.thumbnail(rw, header, message)
        self.log.error("Unknown command: %r", command)
        rw.transport.nack(header)

    def thumbnail(self, rw, header, message):
        """Take a single file and create a smaller version of the same file."""
        filename = rw.recipe_step.get("parameters", {}).get("file")
        if isinstance(message, dict) and message.get("file"):
            filename = message["file"]
        if not os.path.exists(filename):
            self.log.error("File %s not found", filename)
            rw.transport.nack(header)
            return
        sizex = rw.recipe_step.get("parameters", {}).get("size-x", 400)
        if isinstance(message, dict) and message.get("size-x"):
            sizex = message["size-x"]
        sizey = rw.recipe_step.get("parameters", {}).get("size-y", 192)
        if isinstance(message, dict) and message.get("size-y"):
            sizey = message["size-y"]
        output = rw.recipe_step.get("parameters", {}).get("output")
        if isinstance(message, dict) and message.get("output"):
            output = message["output"]
        if not output:
            # If not set add a 't' in front of the last '.' in the filename
            output = (
                filename[: filename.rindex(".")]
                + "t"
                + filename[filename.rindex(".") :]
            )

        with PIL.Image.open(filename) as fh:
            fh.thumbnail((sizex, sizey))
            fh.save(output)

        self.log.info("Created thumbnail %s -> %s", filename, output)
        rw.transport.ack(header)
