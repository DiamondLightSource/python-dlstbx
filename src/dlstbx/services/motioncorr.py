from __future__ import annotations

import procrunner
import workflows.recipe
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService

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
        result = procrunner.run(command)
        if result.returncode:
            self.log.error(
                f"Motion correction of {movie} failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        mc_params.ctf["input_image"] = mc_params.mrc_out
        # Forward results to ctffind
        if isinstance(rw, RW_mock):
            rw.transport.send(  # type: ignore
                destination="ctffind",
                message={"parameters": mc_params.ctf, "content": "dummy"},
            )
        else:
            rw.send_to("ctf", mc_params.ctf)

        # add command, add parameters
        # Forward results to ispyb
        ispyb_params = {mc_params.patch_size,
                        mc_params.mrc_out,
                        mc_params.gain_ref,
                        mc_params.pix_size,
                        mc_params.movie}

#    movie_id=full_parameters("movie_id") or movieid, # from recipe
#    auto_proc_program_id=full_parameters("program_id"), # from Murfey
#    image_number=full_parameters("image_number"), # from Murfey
#    first_frame=full_parameters("first_frame"), # from mrc/xml/tiff, use 1 always, from Murfey
#    last_frame=full_parameters("last_frame"), # from mrc/xml/tiff, number_of_frames from movie, number of images in movie, from Murfey
#    dose_per_frame=full_parameters("dose_per_frame"), # from Murfey
#    total_motion=full_parameters("total_motion"), # sum of shifts from MC output
#    average_motion_per_frame=full_parameters("average_motion_per_frame"), # mean of shifts from MC output
#    drift_plot_full_path=full_parameters("drift_plot_full_path"), # make plot in service from Full-frame alignment shift section
#    micrograph_full_path=full_parameters("micrograph_full_path"), # mrc_out
#    micrograph_snapshot_full_path=full_parameters("micrograph_snapshot_full_path"), # pass
#    fft_full_path=full_parameters("fft_full_path"), # pass
#    fft_corrected_full_path=full_parameters("fft_corrected_full_path"), pass
#    patches_used_x=full_parameters("patches_used_x"), # input from Murfey
#    patches_used_y=full_parameters("patches_used_y"), # input from Murfey
#    comments=full_parameters("comments"),

        if isinstance(rw, RW_mock):
            rw.transport.send(destination="ispyb_connector",
                              message={
                              "parameters": {"ispyb_command": "insert_motion_correction"},
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
