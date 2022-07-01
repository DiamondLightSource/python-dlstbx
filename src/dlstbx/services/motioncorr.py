from __future__ import annotations

import procrunner
import workflows.recipe
from workflows.services.common_service import CommonService


class MotionCorr(CommonService):
    """
    A service for motion correcting cryoEM movies using MotionCor2
    """

    # Human readable service name
    _service_name = "DLS MotionCorr"

    # Logger name
    _logger_name = "dlstbx.services.motioncorr"

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Motion correction service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "motioncorr",
            self.motion_correction,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def motion_correction(self, rw, header: dict, message: dict):

        if not rw:
            print(
                "Incoming message is not a recipe message. Simple messages can be valid"
            )
            if (
                not isinstance(message, dict)
                or not message.get("parameters")
                or not message.get("content")
            ):
                self.log.error("Rejected invalid simple message")
                self._transport.nack(header)
                return
            self.log.debug("Received a simple message")

            # Create a wrapper-like object that can be passed to functions
            # as if a recipe wrapper was present.
            class RW_mock:
                def dummy(self, *args, **kwargs):
                    pass

            rw = RW_mock()
            rw.transport = self._transport
            rw.recipe_step = {"parameters": message["parameters"], "output": None}
            rw.environment = {"has_recipe_wrapper": False}
            rw.set_default_channel = rw.dummy
            rw.send = rw.dummy
            message = message["content"]

        self.log.info(message)
        command = ["MotionCor2"]

        def parameters(key: str, default=None):
            if isinstance(message, dict) and message.get(key):
                return message[key]
            return rw.recipe_step.get("parameters", {}).get(key, default)

        if not parameters("movie"):
            self.log.error(
                f"No movie found in motion correction service message: {message}"
            )
            rw.transport.nack(header)
        if not parameters("mrc_out"):
            self.log.error(
                f"No output mrc path found in motion correction service message: {message}"
            )
            rw.transport.nack(header)
        if not parameters("pix_size"):
            self.log.error(
                f"No pixel size found in motion correction parameters: {message}"
            )
            rw.transport.nack(header)
        movie = parameters("movie")
        input_flag = "-InMrc" if movie.endswith(".mrc") else "-InTiff"
        command.extend([input_flag, movie])
        arguments = [
            "-OutMrc",
            parameters("mrc_out"),
            "-Gpu",
            "0",
            "-Patch",
            str(parameters("patch_size", default=5)),
            str(parameters("patch_size", default=5)),
            "-PixSize",
            str(parameters("pix_size")),
        ]
        if parameters("gain_ref"):
            arguments.extend(["-Gain", parameters("gain_ref")])

        command.extend(arguments)
        result = procrunner.run(command)
        if result.returncode:
            self.log.error(
                f"Motion correction of {movie} failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        # Forward results to ctffind
        rw.transport.send(destination='ctffind',
                          message={"parameters":
                              {
                                  "input_image": parameters("mrc_out")
                              }, "content": "dummy"})
        rw.transport.ack(header)
