from __future__ import absolute_import, division, print_function

import errno
import os
import re

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
        func = "do_" + str(command)
        if hasattr(self, func):
            return getattr(self, func)(rw, header, message)
        self.log.error("Unknown command: %r", command)
        rw.transport.nack(header)

    def do_diffraction(self, rw, header, message):
        """Take a diffraction data file and transform it into JPEGs."""
        filename = rw.recipe_step.get("parameters", {}).get("file")
        if isinstance(message, dict) and message.get("file"):
            filename = message["file"]

        imageset_index = 1
        if not filename:
            # 'file' is a filename
            # 'input' is a xia2-type string, may need to remove :x:x suffix
            filename = rw.recipe_step.get("parameters", {}).get("input")
            if isinstance(message, dict) and message.get("input"):
                filename = message["input"]
            if ":" in filename:
                filename, imageset_index = filename.split(":")[0:2]

        if not filename or filename == "None":
            self.log.debug(
                "Skipping diffraction JPG generation: filename not specified"
            )
            rw.transport.ack(header)
            return
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
            # split off extension
            output = filename[: filename.rindex(".")]
            # deduct image filename
            output = re.sub(
                r"(/[a-z]{2}[0-9]{4,}-[0-9]+/)", r"\g<0>jpegs/", output, count=1
            )
            output = output + ".jpeg"
            # create directory for image if necessary
            try:
                os.makedirs(os.path.dirname(output))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
        output_small = output[: output.rindex(".")] + ".thumb.jpeg"

        result = procrunner.run(
            [
                "dials.export_bitmaps",
                filename,
                "imageset_index=%s" % imageset_index,
                "output.format=jpeg",
                "quality=95",
                "binning=4",
                "brightness=60",
                'output.file="%s"' % output,
            ]
        )
        if result["exitcode"]:
            self.log.error(
                "Export of {f} failed with exitcode {r[exitcode]}:\n{r[stderr]}".format(
                    f=filename, r=result
                )
            )
            rw.transport.nack(header)
            return
        if not os.path.exists(output):
            self.log.error("Output file %s not found", output)
            rw.transport.nack(header)
            return
        with PIL.Image.open(output) as fh:
            fh.thumbnail((sizex, sizey))
            fh.save(output_small)

        self.log.info(
            "Created thumbnail %s -> %s -> %s", filename, output, output_small
        )
        rw.transport.ack(header)

    def do_thumbnail(self, rw, header, message):
        """Take a single file and create a smaller version of the same file."""
        filename = rw.recipe_step.get("parameters", {}).get("file")
        if isinstance(message, dict) and message.get("file"):
            filename = message["file"]
        if not filename or filename == "None":
            self.log.debug("Skipping thumbnail generation: filename not specified")
            rw.transport.ack(header)
            return
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
