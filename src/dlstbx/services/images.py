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


class PluginParameter(NamedTuple):
    rw: workflows.recipe.wrapper.RecipeWrapper
    parameters: Callable[[str], Any]
    message: dict[str, Any]


class DLSImages(CommonService):
    """
    A service that generates images and thumbnails.
    Plugin functions can be registered under the entry point
    'zocalo.services.images.plugins'. The contract is that a plugin function
    takes a single argument of type PluginParameter, and returns a truthy value
    to acknowledge success, and a falsy value to reject the related message.
    Functions may choose to return a list of files that were generated, but
    this is optional at this time.
    """

    # Human readable service name
    _service_name = "DLS Images"

    # Logger name
    _logger_name = "dlstbx.services.images"

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Image service starting")
        self.image_functions: dict[str, Callable] = {
            e.name: e.load()
            for e in pkg_resources.iter_entry_points("zocalo.services.images.plugins")
        }
        workflows.recipe.wrap_subscribe(
            self._transport,
            "images",
            self.image_call,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def image_call(self, rw, header, message):
        """Pass incoming message to the relevant plugin function."""
        command = rw.recipe_step.get("parameters", {}).get("image_command")

        def parameters(key, default=None):
            if isinstance(message, dict) and message.get(key):
                return message[key]
            return rw.recipe_step.get("parameters", {}).get(key, default)

        if command not in self.image_functions:
            self.log.error("Unknown command: %r", command)
            rw.transport.nack(header)
            return

        result = self.image_functions[command](PluginParameter(rw, parameters, message))
        if result:
            rw.transport.ack(header)
        else:
            self.log.error(f"Command {command} resulted in {result}")
            rw.transport.nack(header)


def diffraction(plugin_params: PluginParameter):
    """Take a diffraction data file and transform it into JPEGs."""
    filename = plugin_params.parameters("file")

    imageset_index = 1
    if not filename:
        # 'file' is a filename
        # 'input' is a xia2-type string, may need to remove :x:x suffix
        filename = plugin_params.parameters("input")
        if ":" in filename:
            filename, imageset_index = filename.split(":")[0:2]

    if not filename or filename == "None":
        logger.debug("Skipping diffraction JPG generation: filename not specified")
        return False
    if not os.path.exists(filename):
        logger.error("File %s not found", filename)
        return False
    sizex = plugin_params.parameters("size-x", default=400)
    sizey = plugin_params.parameters("size-y", default=192)
    output = plugin_params.parameters("output")
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
        return False
    if not os.path.exists(output):
        logger.error("Output file %s not found", output)
        return False
    with PIL.Image.open(output) as fh:
        fh.thumbnail((sizex, sizey))
        fh.save(output_small)

    logger.info("Created thumbnail %s -> %s -> %s", filename, output, output_small)
    return [output, output_small]


def thumbnail(plugin_params: PluginParameter):
    """Take a single file and create a smaller version of the same file."""
    filename = plugin_params.parameters("file")
    if not filename or filename == "None":
        logger.debug("Skipping thumbnail generation: filename not specified")
        return False
    if not os.path.exists(filename):
        logger.error("File %s not found", filename)
        return False
    sizex = plugin_params.parameters("size-x", default=400)
    sizey = plugin_params.parameters("size-y", default=192)
    output = plugin_params.parameters("output")
    if not output:
        # If not set add a 't' in front of the last '.' in the filename
        output = (
            filename[: filename.rindex(".")] + "t" + filename[filename.rindex(".") :]
        )

    with PIL.Image.open(filename) as fh:
        fh.thumbnail((sizex, sizey))
        fh.save(output)

    logger.info("Created thumbnail %s -> %s", filename, output)
    return [output]
