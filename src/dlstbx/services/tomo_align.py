from __future__ import annotations

import procrunner
import workflows.recipe
from workflows.services.common_service import CommonService

# Possible parameters:
# "input_file_list" Required
# "stack_file" Required
# "vol_z" default 1200
# "out_bin" default 4
# "tilt_range" (must be a tuple) or "ang_file" Required
# "tile_axis"
# "tilt_cor"
# "flip_int"
# "flip_vol"
# "wbp"
# "roi"
# "roi_file"
# "patch"
# "kv"
# "aln_file"
# "align_z"



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

        def parameters(key: str, default=None):
            if isinstance(message, dict) and message.get(key):
                return message[key]
            return rw.recipe_step.get("parameters", {}).get(key, default)

        if not parameters("input_file_list"):
            self.log.error(
                f"No input files found in tomo_align service message: {message}"
            )
            rw.transport.nack(header)

        if not parameters("stack_file"):
            self.log.error(
                f"No output file found in tomo_align service message: {message}"
            )
            rw.transport.nack(header)

        if not (parameters("tilt_range") or parameters("ang_file")):
            self.log.error(
                f"No tilt range or angle file found in tomo_align service message: {message}"
            )
            rw.transport.nack(header)

        if parameters("tilt_range") and parameters("ang_file"):
            self.log.error(
                f"Cannot specify both TiltRange and AngFile - tomo_align service message: {message}"
            )
            rw.transport.nack(header)

        newstack_result = self.newstack(
            parameters("input_file_list"), parameters("stack_file")
        )
        if newstack_result.returncode:
            self.log.error(
                f"Newstack failed with exitcode {newstack_result.returncode}:\n"
                + newstack_result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        stack_filename_split = parameters("stack_file").split(".")
        aretomo_output_file = (
            stack_filename_split[0] + "aretomo." + stack_filename_split[1]
        )
        aretomo_result = self.aretomo(parameters("stack_file"), aretomo_output_file, parameters)
        if aretomo_result.returncode:
            self.log.error(
                f"AreTomo failed with exitcode {aretomo_result.returncode}:\n"
                + aretomo_result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        rw.transport.ack(header)

    def newstack(self, filein_list_of_tuples, stack_output_file):
        """
        Sort images by tilt angle
        Construct file containing a list of files
        Run newstack
        """

        def tilt(file_tuple):
            return float(file_tuple[2])
        filein_list_of_tuples.sort(key=tilt)

        # Write a file with a list of .mrcs for input to Newstack
        with open("newstack-fileinlist.txt", "w") as f:
            f.write(f"{len(filein_list_of_tuples)}\n")
            f.write("\n0\n".join(i[0] for i in filein_list_of_tuples))
            f.write("\n0\n")

        newstack_cmd = [
            "newstack",
            "-fileinlist",
            "newstack-fileinlist.txt",
            "-output",
            stack_output_file,
            "-quiet",
        ]
        self.log.info("Running Newstack")
        result = procrunner.run(newstack_cmd)
        return result

    def aretomo(self, stack_file, output_file, parameters):
        """
        Run AreTomo on output of Newstack
        """
        aretomo_cmd = [
            "AreTomo",
            "-InMrc",
            stack_file,
            "-OutMrc",
            output_file,
            "-VolZ",
            parameters("vol_z", default="1200"),
            "-OutBin",
            parameters("out_bin", default="4")
        ]

        # Required parameters
        if parameters("tilt_range"):
            aretomo_cmd.extend(("-TiltRange", *parameters("tilt_range")))
        elif parameters("ang_file"):
            aretomo_cmd.extend(("-AngFile", parameters("ang_file")))

        # Optional parameters
        optional_aretomo_parameters = {
                              "tilt_axis": "-TiltAxis",
                              "tilt_cor": "-TiltCor",
                              "flip_int": "-FlipInt",
                              "flip_vol": "-FlipVol",
                              "wbp": "-Wbp",
                              "roi": "-Roi",
                              "roi_file": "-RoiFile",
                              "patch": "-Patch",
                              "kv": "-Kv",
                              "align_file": "-AlignFile",
                              "align_z": "-AlignZ"}

        for k, v in optional_aretomo_parameters.items():
            if parameters(k):
                aretomo_cmd.extend((v, parameters(k)))

        self.log.info("Running AreTomo")
        result = procrunner.run(aretomo_cmd)
        if result.returncode:
            self.log.error(
            f"CTFFind failed with exitcode {result.returncode}:\n"
            + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return
        self.log.info(f"Input stack: {stack_file} \nOutput file: {output_file}")


        # Forward results to ispyb

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

        # multipart message
        # add command, add parameters
        if isinstance(rw, RW_mock):
            rw.transport.send(destination="ispyb_connector",
                              message={
                                  "parameters": {"ispyb_command": "insert_tomogram"},
                                  "content": {"dummy": "dummy"},
                              },)
        else:
            rw.send_to("ispyb", ispyb_parameters)
        rw.transport.ack(header)