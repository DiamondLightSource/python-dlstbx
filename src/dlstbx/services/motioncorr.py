from __future__ import annotations

import procrunner
import workflows.recipe
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService
from math import sqrt
import plotly.express as px
from pathlib import Path

# Possible parameters:
# "movie" Required
# "mrc_out" Required
# "pix_size" Required
# "patch_size"
# "gain_ref"
# "ctf" Required


class MotionCorrParameters(BaseModel):
    pix_size: float
    ctf: dict
    movie: str = Field(..., min_length=1)
    mrc_out: str = Field(..., min_length=1)
    patch_size: int = 5
    gain_ref: str = ""

    class Config:
        ignore_extra = True


class MotionCorr(CommonService):
    """
    A service for motion correcting cryoEM movies using MotionCor2
    """

    # Human readable service name
    _service_name = "DLS MotionCorr"

    # Logger name
    _logger_name = "dlstbx.services.motioncorr"

    # Values to extract for ISPyB
    shift_list = []

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

    def parse_mc_output(self, line: str):
        if not line:
            return

        if line.startswith("...... Frame"):
            line_split = line.split(" ")
            self.shift_list.append((line_split[-2], line_split[-1]))


    def motion_correction(self, rw, header: dict, message: dict):

        if not rw:
            if (
                not isinstance(message, dict)
                or not message.get("parameters")
                or not message.get("content")
            ):
                self.log.error("Rejected invalid simple message")
                self._transport.nack(header)
                return

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

        command = ["MotionCor2"]

        try:
            mc_params = MotionCorrParameters(
                {**rw.recipe_step.get("parameters", {}), **message}
            )
        except (ValidationError, TypeError):
            self.log.warning(
                f"Motion correction parameter validation failed for message: {message} and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        movie = mc_params.movie
        input_flag = "-InMrc" if movie.endswith(".mrc") else "-InTiff"
        command.extend([input_flag, movie])
        arguments = [
            "-OutMrc",
            mc_params.mrc_out,
            "-Gpu",
            "0",
            "-Patch",
            str(mc_params.patch_size),
            str(mc_params.patch_size),
            "-PixSize",
            str(mc_params.pix_size),
        ]
        if mc_params.gain_ref:
            arguments.extend(["-Gain", mc_params.gain_ref])

        self.log.info("Input: ", movie, "Output: ", mc_params.mrc_out)

        command.extend(arguments)
        result = procrunner.run(command=command, callback_stdout=self.parse_mc_output)
        if result.returncode:
            self.log.error(
                f"Motion correction of {movie} failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return


        # Forward results to ctffind
        mc_params.ctf["input_image"] = mc_params.mrc_out
        if isinstance(rw, RW_mock):
            rw.transport.send(  # type: ignore
                destination="ctffind",
                message={"parameters": mc_params.ctf, "content": "dummy"},
            )
        else:
            rw.send_to("ctf", mc_params.ctf)


        # Extract results for ispyb
        total_x_shift = sum([item[0] for item in self.shift_list])
        total_y_shift = sum([item[1] for item in self.shift_list])
        total_motion = sqrt(total_x_shift**2 + total_y_shift**2)

        each_total_motion = sqrt(([item][0])**2 + ([item][1])**2 for item in self.shift_list)
        average_motion_per_frame = sum(each_total_motion) / len(self.shift_list)

        drift_plot_x = range(0, len(self.shift_list))
        drift_plot_y = self.shift_list
        fig = px.scatter(x=drift_plot_x, y=drift_plot_y)
        plot_path = Path(mc_params.mrc_out).parent / "drift_plot.json"
        fig.write_json(plot_path)

        ispyb_parameters = {
            "first_frame": 1,
            "last_frame": len(self.shift_list),
            "total_motion": total_motion,
            "average_motion_per_frame": average_motion_per_frame,
            "drift_plot_full_path": plot_path,
            "micrograph_full_path": mc_params.mrc_out,
            "patches_used_x": mc_params.patch_size,
            "patches_used_y": mc_params.patch_size
        }


        # Forward results to ISPyB
        if isinstance(rw, RW_mock):
            rw.transport.send(destination="ispyb_connector",
                              message={
                                  "parameters": ispyb_parameters.update({"ispyb_command": "insert_motion_correction"}),
                                  "content": {"dummy": "dummy"},
                              },)
        else:
            rw.send_to("ispyb", ispyb_parameters)


        # Forward results to murfey
        if isinstance(rw, RW_mock):
            rw.transport.send("murfey", {"corrected_movie": mc_params.mrc_out})
        else:
            rw.send_to("murfey", mc_params.mrc_out)

        rw.transport.ack(header)
        self.shift_list = []
