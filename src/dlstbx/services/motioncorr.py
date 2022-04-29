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
        )

    def motion_correction(self, rw, header: dict, message: dict):
        command = ["MotionCor2"]

        def parameters(key: str, default=None):
            if isinstance(message, dict) and message.get(key):
                return message[key]
            return rw.recipe_step.get("parameters", {}).get(key, default)

        if not message.get("movie"):
            self.log.error(
                f"No movie found in motion correction service message: {message}"
            )
            rw.transport.nack(header)
        if not message.get("mrc_out"):
            self.log.error(
                f"No output mrc path found in motion correction service message: {message}"
            )
            rw.transport.nack(header)
        if not parameters("pix_size"):
            self.log.error(
                f"No pixel size found in motion correction parameters: {message}"
            )
            rw.transport.nack(header)
        input_flag = "-InMrc" if message["movie"].endswith(".mrc") else "-InTiff"
        command.extend([input_flag, message["movie"]])
        arguments = [
            "-OutMrc",
            message["mrc_out"],
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
                f"Motion correction of {message['movie']} failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        rw.transport.ack(header)
