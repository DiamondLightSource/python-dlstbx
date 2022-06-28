from __future__ import annotations

import procrunner
import workflows.recipe
from workflows.services.common_service import CommonService


class CTFFind(CommonService):
    """
    A service for CTF estimating micrographs with CTFFind
    """

    # Human readable service name
    _service_name = "DLS CTFFind"

    # Logger name
    _logger_name = "dlstbx.services.ctffind"

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("CTFFind service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "ctffind",
            self.ctf_find,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def ctf_find(self, rw, header: dict, message: dict):

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
            rw.recipe_step = {"parameters": message["parameters"]}
            rw.environment = {"has_recipe_wrapper": False}
            rw.set_default_channel = rw.dummy
            rw.send = rw.dummy
            message = message["content"]

        self.log.info(message)
        command = ["ctffind"]

        def parameters(key: str, default=None):
            if isinstance(message, dict) and message.get(key):
                return message[key]
            return rw.recipe_step.get("parameters", {}).get(key, default)

        if not parameters("input_image"):
            self.log.error(
                f"No input image found in ctffind service message: {message}"
            )
            rw.transport.nack(header)

        parameters_list = [
            parameters("input_image"),
            parameters("is_movie", default="no"),
            parameters("output_file", default="diagnostic_output.mrc"),
            parameters("pix_size", default="1.0"),
            parameters("voltage", default="300.0"),
            parameters("spher_aber", default="2.70"),
            parameters("ampl_spectrum", default="512"),
            parameters("min_res", default="30.0"),
            parameters("max_res", default="5.0"),
            parameters("min_defocus", default="5000.0"),
            parameters("max_defocus", default="50000.0"),
            parameters("defocus_step", default="100.0"),
            parameters("astigmatism_known", default="no"),
            parameters("slow_search", default="no"),
            parameters("astigmatism_restrain", default="no"),
            parameters("additional_phase_shift", default="no"),
            parameters("expert_options", default="no"),
        ]
        self.log.info(command)
        parameters_string = "\n".join(parameters_list)
        result = procrunner.run(command, stdin=parameters_string.encode("ascii"))
        if result.returncode:
            self.log.error(
                f"CTFFind failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        rw.transport.ack(header)
