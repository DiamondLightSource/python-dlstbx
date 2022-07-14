from __future__ import annotations

import procrunner
import workflows.recipe
from workflows.services.common_service import CommonService

# Possible parameters:
# "input_file_list" Required
# "stack_file" Required
# "vol_z" default 1200
# "out_bin" default 4
# "tilt_range" "ang_file" Required
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

        if not parameters("tilt_range") or parameters("ang_file"):
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
            parameters("out_bin", default="4"),
            "-TiltRange",
            "-60",
            "60",
        ]
        if parameters("tilt_range"):
            aretomo_cmd.extend(("-TiltRange", parameters("tilt_range")))
        elif parameters("ang_file"):
            aretomo_cmd.extend(("-AngFile", parameters("ang_file")))
        if parameters("tile_axis"):
            aretomo_cmd.extend(("-TiltAlis", parameters("tile_axis")))
        if parameters("tilt_cor"):
            aretomo_cmd.extend(("-TiltCor", parameters("tilt_cor")))
        if parameters("flip_int"):
            aretomo_cmd.extend(("-FlipInt", parameters("flip_int")))
        if parameters("flip_vol"):
            aretomo_cmd.extend(("-FlipVol", parameters("flip_vol")))
        if parameters("wbp"):
            aretomo_cmd.extend(("-Wbp", parameters("wbp")))

        if parameters("roi"):
            aretomo_cmd.extend(("-Roi", parameters("roi")))
        if parameters("roi_file"):
            aretomo_cmd.extend(("-RoiFile", parameters("roi_file")))
        if parameters("patch"):
            aretomo_cmd.extend(("-Patch", parameters("patch")))
        if parameters("kv"):
            aretomo_cmd.extend(("-Kv", parameters("kv")))
        if parameters("align_file"):
            aretomo_cmd.extend(("-AlignFile", parameters("align_file")))
        if parameters("align_z"):
            aretomo_cmd.extend(("-AlignZ", parameters("align_z")))


        self.log.info("Running AreTomo")
        result = procrunner.run(aretomo_cmd)
        self.log.info("Input stack: ", stack_file, "Output file: ", output_file)
        return result
