from __future__ import annotations

import procrunner
import workflows.recipe
from pydantic import BaseModel, Field
from workflows.services.common_service import CommonService
from pydantic.error_wrappers import ValidationError

# Possible parameters:
# "input_file_list" Required
# "stack_file" Required
# "vol_z" default 1200
# "align"
# "out_bin" default 4
# "tilt_range" (must be a tuple) or "ang_file" Required
# "tilt_axis"
# "tilt_cor"
# "flip_int"
# "flip_vol"
# "wbp"
# "roi_file"
# "patch"
# "kv"
# "align_file"
# "angle_file"
# "align_z"
# "pix_size"
# "init_val"
# "refine_flag"
# "out_imod"
# "out_imod_xf"
# "dark_tol"

class TomoParameters(BaseModel):
    input_file_list: list
    stack_file: Field(..., min_length=1)
    vol_z: int = 1200
    align: int = None
    out_bin: int = 4
    tilt_range: tuple = (None, None)
    tilt_axis: float = None
    tilt_cor: int = None
    flip_int: int = None
    flip_vol: int = None
    wbp: int = None
    roi_file: list = None
    patch: int = None
    kv: int = None
    align_file: str = None
    angle_file: str = None
    align_z: int = None
    pix_size: int = None
    init_val: int = None
    refine_flag: int = None
    out_imod: int = None
    out_imod_xf: int = None
    dark_tol: int or str = None

class TomoAlign(CommonService):
    """
    A service for grouping and aligning tomography tilt-series with Newstack and AreTomo
    """

    # Required parameters: list of files, stack output file name (output file name will be used for both stages)
    # Optional parameters:

    # Human readable service name
    _service_name = "DLS TomoAlign"

    # Logger name
    _logger_name = "dlstbx.services.tomo_align"

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("TomoAlign service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "tomo_align",
            self.tomo_align,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def tomo_align(self, rw, header: dict, message: dict):
        class RW_mock:
            def dummy(self, *args, **kwargs):
                pass

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

            # Create a wrapper-like object that can be passed to functions
            # as if a recipe wrapper was present.

            rw = RW_mock()
            rw.transport = self._transport
            rw.recipe_step = {"parameters": message["parameters"]}
            rw.environment = {"has_recipe_wrapper": False}
            rw.set_default_channel = rw.dummy
            rw.send = rw.dummy
            message = message["content"]

        try:
            tomo_params = TomoParameters(
                **{**rw.recipe_step.get("parameters", {}), **message}
            )
        except (ValidationError, TypeError):
            self.log.warning(
                f"TomoAlign parameter validation failed for message: {message} and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        if not (tomo_params.tilt_range or tomo_params.angle_file):
            self.log.error(
                f"No tilt range or angle file found in tomo_align service message: {message}"
            )
            rw.transport.nack(header)

        if tomo_params.tilt_range and tomo_params.angle_file:
            self.log.error(
                f"Cannot specify both TiltRange and AngFile - tomo_align service message: {message}"
            )
            rw.transport.nack(header)

        newstack_result = self.newstack(tomo_params)
        if newstack_result.returncode:
            self.log.error(
                f"Newstack failed with exitcode {newstack_result.returncode}:\n"
                + newstack_result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        stack_filename_split = tomo_params.stack_file.split(".")
        aretomo_output_file = (
            stack_filename_split[0] + "aretomo." + stack_filename_split[1]
        )

        aretomo_result = self.aretomo(aretomo_output_file, tomo_params)

        if aretomo_result.returncode:
            self.log.error(
                f"AreTomo failed with exitcode {aretomo_result.returncode}:\n"
                + aretomo_result.stderr.decode("utf8", "replace")
            )
            return

        # Extract results for ispyb

        # Forward results to ispyb

        ispyb_parameters = {}
        self.log.info("Sending to ispyb")
        if isinstance(rw, RW_mock):
            rw.transport.send(destination="ispyb_connector",
                              message={
                                  "parameters": ispyb_parameters,
                                  "content": {"dummy": "dummy"},
                              },)
        else:
            rw.send_to("ispyb", ispyb_parameters)
        rw.transport.ack(header)

    def newstack(self, tomo_parameters):
        """
        Sort images by tilt angle
        Construct file containing a list of files
        Run newstack
        """

        def tilt(file_tuple):
            return float(file_tuple[2])
        tomo_parameters.input_file_list.sort(key=tilt)

        # Write a file with a list of .mrcs for input to Newstack
        with open("newstack-fileinlist.txt", "w") as f:
            f.write(f"{len(tomo_parameters.input_file_list)}\n")
            f.write("\n0\n".join(i[0] for i in tomo_parameters.input_file_list))
            f.write("\n0\n")

        newstack_cmd = [
            "newstack",
            "-fileinlist",
            "newstack-fileinlist.txt",
            "-output",
            tomo_parameters.stack_file,
            "-quiet",
        ]
        self.log.info("Running Newstack")
        result = procrunner.run(newstack_cmd)
        return result

    def aretomo(self, output_file, tomo_parameters):
        """
        Run AreTomo on output of Newstack
        """
        aretomo_cmd = [
            "AreTomo",
            "-InMrc",
            tomo_parameters.stack_file,
            "-OutMrc",
            output_file,
            "-VolZ",
            tomo_parameters.vol_z,
            "-OutBin",
            tomo_parameters.out_bin
        ]

        # Required parameters
        if tomo_parameters.tilt_range:
            aretomo_cmd.extend(("-TiltRange", *tomo_parameters.tilt_range))
        elif tomo_parameters.angle_file:
            aretomo_cmd.extend(("-AngFile", tomo_parameters.angle_file))

        # Optional parameters
        optional_aretomo_parameters = {
                              "tilt_axis": "-TiltAxis",
                              "tilt_cor": "-TiltCor",
                              "flip_int": "-FlipInt",
                              "flip_vol": "-FlipVol",
                              "wbp": "-Wbp",
                              "align": "-Align",
                              "roi_file": "-RoiFile",
                              "patch": "-Patch",
                              "kv": "-Kv",
                              "align_file": "-AlnFile",
                              "align_z": "-AlignZ",
                              "pix_size": "-PixSize",
                              "init_val": "initVal",
                              "refine_flag": "refineFlag",
                              "out_imod": "-OutImod",
                              "out_imod_xf": "-OutXf",
                              "dark_tol": "-DarkTol"}

        for k, v in optional_aretomo_parameters.items():
            if getattr(tomo_parameters, k) is not None:
                aretomo_cmd.extend((v, getattr(tomo_parameters, k)))

        self.log.info("Running AreTomo")
        self.log.info(f"Input stack: {tomo_parameters.stack_file} \nOutput file: {output_file}")
        result = procrunner.run(aretomo_cmd)
        return result

        # Tomogram (one per-tilt-series)
        #dataCollectionId=full_parameters("dcid"), # from Murfey
        #autoProcProgramId=full_parameters("program_id"), # from Murfey
        #volumeFile=full_parameters("volume_file"), # outmrc, from inputs
        #stackFile=full_parameters("stack_file"), # inmrc, from inputs
        #sizeX=full_parameters("size_x"), # volume image size, pix
        #sizeY=full_parameters("size_y"), # volume image size, pix
        #sizeZ=full_parameters("size_z"), # volume image size, pix or slices
        #pixelSpacing=full_parameters("pixel_spacing"), # pixel size, from Murfey/inputs
        #residualErrorMean=full_parameters("residual_error_mean"), # calculate from shifts in AreTomo output file?
        #residualErrorSD=full_parameters("residual_error_sd"), # calculate from shifts in AreTomo output file?
        #xAxisCorrection=full_parameters("x_axis_correction"), # TiltCor, from inputs
        #tiltAngleOffset=full_parameters("tilt_angle_offset"), # from aretomo file, tilt offset
        #zShift=full_parameters("z_shift") # VolZ, from inputs

        # TiltImageAlignment (one per movie)
        #movieId=full_parameters("movie_id"), # from Murfey
        #tomogramId=full_parameters("tomogram_id"), # from recipe
        #defocusU=full_parameters("defocus_u"), # don't do - in ctf
        #defocusV=full_parameters("defocus_v"), # don't do - in ctf
        #psdFile=full_parameters("psd_file"), # should be in ctf table but useful so we will insert
        #resolution=full_parameters("resolution"), # don't do - in ctf
        #fitQuality=full_parameters("fit_quality"), # don't do - in ctf
        #refinedMagnification=full_parameters("refined_magnification"), # optional, pass
        #refinedTiltAngle=full_parameters("refined_tilt_angle"), # from aretomo file, tilt angle per image
        #refinedTiltAxis=full_parameters("refinedTiltAxis"), # seems like it should be per tomogram (one num), but is in the tiltimage table per
        # image ??
        #residualError=full_parameters("residual_error") # shift per image?

