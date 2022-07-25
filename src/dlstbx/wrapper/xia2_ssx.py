from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional

import pydantic

import dlstbx.util
import dlstbx.util.symlink
from dlstbx.util import ChainMapWithReplacement
from dlstbx.wrapper import Wrapper

logger = logging.getLogger("dlstbx.srap.xia2.ssx")


class Xia2SsxParams(pydantic.BaseModel):
    template: Path
    unit_cell: Optional[
        tuple[
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
        ]
    ] = None
    spacegroup: Optional[str] = None

    @pydantic.validator("unit_cell", pre=True)
    def check_unit_cell(cls, v):
        if isinstance(v, str):
            v = v.replace(",", " ").split()
        v = tuple(float(v) for v in v)
        return v


class Payload(pydantic.BaseModel):
    working_directory: Path
    results_directory: Path
    create_symlink: Optional[Path] = None
    timeout: Optional[pydantic.PositiveFloat] = None


class Xia2SsxWrapper(Wrapper):
    name = "xia2.ssx"

    def construct_commandline(self, params: Xia2SsxParams):
        command = [
            "dev.xia2.ssx",
            f"template={params.template}",
        ]
        if params.unit_cell:
            command.append("unit_cell=%s,%s,%s,%s,%s,%s" % params.unit_cell)
        if params.spacegroup:
            command.append(f"space_group={params.spacegroup}")
        return command

    def run(self):
        job_parameters = self.recwrap.recipe_step["job_parameters"]
        params_d = ChainMapWithReplacement(
            job_parameters.get("xia2.ssx", {}),
            job_parameters.get("ispyb_parameters", {}),
        )

        try:
            xia2_ssx_params = Xia2SsxParams(**params_d)
            params = Payload(**job_parameters)
        except (Exception, pydantic.ValidationError) as e:
            logger.error(e, exc_info=True)
            raise

        # Create working directory with symbolic link
        params.working_directory.mkdir(parents=True, exist_ok=True)
        if params.create_symlink:
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(params.working_directory), params.create_symlink
            )

        command = self.construct_commandline(xia2_ssx_params)
        try:
            start_time = time.perf_counter()
            result = subprocess.run(
                command,
                timeout=params.timeout,
                cwd=params.working_directory,
            )
            runtime = time.perf_counter() - start_time
            logger.info(f"xia2.ssx took {runtime} seconds")
            self._runtime_hist.observe(runtime)
        except subprocess.TimeoutExpired as te:
            success = False
            logger.warning(f"xia2.ssx timed out: {te.timeout}\n  {te.cmd}")
            logger.debug(te.stdout)
            logger.debug(te.stderr)
            self._timeout_counter.inc()
        else:
            success = not result.returncode
            if success:
                logger.info("xia2.ssx successful")
            else:
                logger.info(f"xia2.ssx failed with exitcode {result.returncode}")
                logger.debug(result.stdout)
                logger.debug(result.stderr)

        if success:
            # copy output files to result directory
            params.results_directory.mkdir(parents=True, exist_ok=True)
            if params.create_symlink:
                dlstbx.util.symlink.create_parent_symlink(
                    str(params.results_directory), params.create_symlink
                )

            # Probably need to be a little more selective here...
            for f in params.working_directory.iterdir():
                if f.is_file():
                    shutil.copy(f, params.results_directory)

        if success:
            self._success_counter.inc()
        else:
            self._failure_counter.inc()

        return success
