import logging
import procrunner
import pathlib
import subprocess

import zocalo.wrapper


class DialsWrapper(zocalo.wrapper.BaseWrapper):

    executable = None

    def __init__(self):
        assert self.executable
        self.log = logging.getLogger(f"dlstbx.wrap.{self.executable}")
        super().__init__()

    def construct_commandline(self, params):
        raise NotImplementedError

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        command = self.construct_commandline(params)

        working_directory = pathlib.Path(params["working_directory"])

        # Create working directory with symbolic link
        working_directory.mkdir(parents=True, exist_ok=True)

        self.log.info("command: %s", " ".join(command))
        try:
            result = procrunner.run(
                ["time"] + command,
                timeout=params.get("timeout"),
                raise_timeout_exception=True,
                working_directory=working_directory,
            )
        except subprocess.TimeoutExpired as te:
            success = False
            self.log.warning(f"{self.executable} timed out: {te.timeout}\n  {te.cmd}")
            self.log.debug(te.stdout)
            self.log.debug(te.stderr)
        else:
            success = not result.returncode
            if success:
                self.log.info(f"{self.executable} successful")
            else:
                self.log.info(
                    f"{self.executable} failed with exitcode {result.returncode}"
                )
                self.log.debug(result.stdout)
                self.log.debug(result.stderr)
        return success
