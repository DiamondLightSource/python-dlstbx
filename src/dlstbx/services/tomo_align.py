from __future__ import annotations

import procrunner
import workflows.recipe
from workflows.services.common_service import CommonService


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

        def parameters(key: str, default=None):
            if isinstance(message, dict) and message.get(key):
                return message[key]
            return rw.recipe_step.get("parameters", {}).get(key, default)

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
        aretomo_result = self.aretomo(parameters("stack_file"), aretomo_output_file)
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

        def position(file_tuple):
            return file_tuple[1]

        def tilt(file_tuple):
            return file_tuple[2]

        self.log.info("Setting up Newstack")
        filein_list_of_tuples.sort(key=tilt)
        filein_list_of_tuples.sort(key=position)

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
        self.log.info(newstack_cmd)
        self.log.info("Running Newstack")
        result = procrunner.run(newstack_cmd)
        return result

    def aretomo(self, stack_file, output_file):
        """
        Run AreTomo on output of Newstack
        """
        aretomo_cmd = [
            "AreTomo",  # needed to be AreTomo_1.0.6_Cuda102 for testing, or one of the other available AreTomos
            "-InMrc",
            stack_file,  # "/dls/m07/data/2022/nr29785-36/processing/pipeline_test/stacks/stack001/tomoDLS_001.st",
            "-OutMrc",
            output_file,
            "-VolZ",
            "1200",
            "-OutBin",
            "4",
            "-TiltRange",
            "-60",
            "60",
        ]
        self.log.info("Running AreTomo")
        result = procrunner.run(aretomo_cmd)
        return result
