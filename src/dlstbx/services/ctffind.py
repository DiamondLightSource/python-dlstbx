from __future__ import annotations

import procrunner
import workflows.recipe
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService

# Possible parameters:
# "input_image" Required
# "output_file" Required
# "pix_size", default="1.0"
# "voltage", default="300.0"
# "spher_aber", default="2.70"
# "ampl_contrast", default="0.8"
# "ampl_spectrum", default="512"
# "min_res", default="30.0"
# "max_res", default="5.0"
# "min_defocus", default="5000.0"
# "max_defocus", default="50000.0"
# "defocus_step", default="100.0"
# "astigmatism_known", default="no"
# "slow_search", default="no"
# "astigmatism_restrain", default="no"
# "additional_phase_shift", default="no"
# "expert_options", default="no"


class CTFParameters(BaseModel):
    pix_size: float
    voltage = 300.0
    spher_aber = 2.7
    ampl_contrast = 0.8
    ampl_spectrum = 512
    min_res = 30.0
    mas_res = 5.0
    min_defocus = 5000.0
    max_defocus = 50000.0
    defocus_step = 100.0
    astigmatism_known = "no"
    slow_search = "no"
    astigmatism_restrain = "no"
    additional_phase_shift = "no"
    expert_options = "no"
    input_image: str = Field(..., min_length=1)
    output_image: str = Field(..., min_length=1)


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

        command = ["ctffind"]

        try:
            ctf_params = CTFParameters(
                **{**rw.recipe_step.get("parameters", {}), **message}
            )
        except (ValidationError, TypeError):
            self.log.warning(
                f"CTF estimation parameter validation failed for message: {message} and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        parameters_list = [
            ctf_params.input_image,
            ctf_params.output_file,
            ctf_params.pix_size,
            ctf_params.voltage,
            ctf_params.spher_aber,
            ctf_params.ampl_contrast,
            ctf_params.ampl_spectrum,
            ctf_params.min_res,
            ctf_params.max_res,
            ctf_params.min_defocus,
            ctf_params.max_defocus,
            ctf_params.defocus_step,
            ctf_params.astigmatism_known,
            ctf_params.slow_search,
            ctf_params.astigmatism_restrain,
            ctf_params.additional_phase_shift,
            ctf_params.expert_options,
        ]

        parameters_string = "\n".join(parameters_list)
        self.log.info(
            f"Input: {ctf_params.input_image} Output: {ctf_params.output_file}"
        )
        result = procrunner.run(command, stdin=parameters_string.encode("ascii"))
        if result.returncode:
            self.log.error(
                f"CTFFind failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

            #ctf_id=full_parameters("ctf_id"),
            #motion_correction_id=full_parameters("motion_correction_id"),
            #auto_proc_program_id=full_parameters("program_id"),
            #box_size_x=full_parameters("box_size_x"),
            #box_size_y=full_parameters("box_size_y"),
            #min_resolution=full_parameters("min_resolution"),
            #max_resolution=full_parameters("max_resolution"),
            #min_defocus=full_parameters("min_defocus"),
            #max_defocus=full_parameters("max_defocus"),
            #astigmatism=full_parameters("astigmatism"),
            #defocus_step_size=full_parameters("defocus_step_size"),
            #astigmatism_angle=full_parameters("astigmatism_angle"),
            #estimated_resolution=full_parameters("estimated_resolution"),
            #estimated_defocus=full_parameters("estimated_defocus"),
            #amplitude_contrast=full_parameters("amplitude_contrast"),
            #cc_value=full_parameters("cc_value"),
            #fft_theoretical_full_path=full_parameters("fft_theoretical_full_path"),
            #comments=full_parameters("comments"),


        # add command, add parameters
        # Forward results to ispyb
        if isinstance(rw, RW_mock):
            rw.transport.send(destination="ispyb_connector",
                          message={
                              "parameters": {"ispyb_command": "insert_ctffind"},
                              "content": {"dummy": "dummy"},
                          },)
        else:
            rw.send_to("ispyb", ispyb_parameters)

        rw.transport.ack(header)
