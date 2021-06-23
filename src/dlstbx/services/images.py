import errno
import logging
import os
import re
from typing import Any, Callable, NamedTuple

import PIL.Image
import pkg_resources
import procrunner
import workflows.recipe
from workflows.services.common_service import CommonService

logger = logging.getLogger("dlstbx.services.images")


class FunctionParameter(NamedTuple):
    rw: workflows.recipe.wrapper.RecipeWrapper
    header: dict[str, Any]
    message: dict[str, Any]


class DLSImages(CommonService):
    """A service that generates images and thumbnails."""

    # Human readable service name
    _service_name = "DLS Images"

    # Logger name
    _logger_name = "dlstbx.services.images"

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Image service starting")
        self.image_functions: dict[str, Callable] = {
            e.name: e.load()
            for e in pkg_resources.iter_entry_points("images.zocalo.service.plugins")
        }
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
        if self.image_functions.get(command):
            return self.image_functions.get(command)(
                FunctionParameter(rw, header, message)
            )
        self.log.error("Unknown command: %r", command)
        rw.transport.nack(header)


def do_diffraction(params):
    """Take a diffraction data file and transform it into JPEGs."""
    filename = params.rw.recipe_step.get("parameters", {}).get("file")
    if isinstance(params.message, dict) and params.message.get("file"):
        filename = params.message["file"]

    imageset_index = 1
    if not filename:
        # 'file' is a filename
        # 'input' is a xia2-type string, may need to remove :x:x suffix
        filename = params.rw.recipe_step.get("parameters", {}).get("input")
        if isinstance(params.message, dict) and params.message.get("input"):
            filename = params.message["input"]
        if ":" in filename:
            filename, imageset_index = filename.split(":")[0:2]

    if not filename or filename == "None":
        logger.debug("Skipping diffraction JPG generation: filename not specified")
        params.rw.transport.ack(params.header)
        return
    if not os.path.exists(filename):
        logger.error("File %s not found", filename)
        params.rw.transport.nack(params.header)
        return
    sizex = params.rw.recipe_step.get("parameters", {}).get("size-x", 400)
    if isinstance(params.message, dict) and params.message.get("size-x"):
        sizex = params.message["size-x"]
    sizey = params.rw.recipe_step.get("parameters", {}).get("size-y", 192)
    if isinstance(params.message, dict) and params.message.get("size-y"):
        sizey = params.message["size-y"]
    output = params.rw.recipe_step.get("parameters", {}).get("output")
    if isinstance(params.message, dict) and params.message.get("output"):
        output = params.message["output"]
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
    if result.returncode:
        logger.error(
            f"Export of {filename} failed with exitcode {result.returncode}:\n"
            + result.stderr.decode("utf8", "replace")
        )
        params.rw.transport.nack(params.header)
        return
    if not os.path.exists(output):
        logger.error("Output file %s not found", output)
        params.rw.transport.nack(params.header)
        return
    with PIL.Image.open(output) as fh:
        fh.thumbnail((sizex, sizey))
        fh.save(output_small)

    logger.info("Created thumbnail %s -> %s -> %s", filename, output, output_small)
    params.rw.transport.ack(params.header)


def do_thumbnail(params):
    """Take a single file and create a smaller version of the same file."""
    filename = params.rw.recipe_step.get("parameters", {}).get("file")
    if isinstance(params.message, dict) and params.message.get("file"):
        filename = params.message["file"]
    if not filename or filename == "None":
        logger.debug("Skipping thumbnail generation: filename not specified")
        params.rw.transport.ack(params.header)
        return
    if not os.path.exists(filename):
        logger.error("File %s not found", filename)
        params.rw.transport.nack(params.header)
        return
    sizex = params.rw.recipe_step.get("parameters", {}).get("size-x", 400)
    if isinstance(params.message, dict) and params.message.get("size-x"):
        sizex = params.message["size-x"]
    sizey = params.rw.recipe_step.get("parameters", {}).get("size-y", 192)
    if isinstance(params.message, dict) and params.message.get("size-y"):
        sizey = params.message["size-y"]
    output = params.rw.recipe_step.get("parameters", {}).get("output")
    if isinstance(params.message, dict) and params.message.get("output"):
        output = params.message["output"]
    if not output:
        # If not set add a 't' in front of the last '.' in the filename
        output = (
            filename[: filename.rindex(".")] + "t" + filename[filename.rindex(".") :]
        )

    with PIL.Image.open(filename) as fh:
        fh.thumbnail((sizex, sizey))
        fh.save(output)

    logger.info("Created thumbnail %s -> %s", filename, output)
    params.rw.transport.ack(params.header)
