from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Literal, Optional

import pydantic

from dlstbx.wrapper import Wrapper

SENSOFIT_PYTHON = Path("/dls_sw/apps/sensofit/env/bin/python")


class SensoFitParameters(pydantic.BaseModel):
    cxw_file: Path
    results_subdir: str = "sensofit_results"
    mode: Literal["dk", "ode", "both"] = "ode"  # dk is fast, ode is full
    n_parallel_jobs: Optional[int] = None
    python: Path = SENSOFIT_PYTHON

    @pydantic.field_validator("mode", mode="before")
    @classmethod
    def _default_unset_mode(cls, value):
        """Fall back to the default when the recipe placeholder went unfilled.

        The dispatcher leaves an undefined {item} exactly as it found it, so a
        run without -s mode= arrives here as the string "{mode}".
        """
        return "ode" if value == "{mode}" else value

    # Supplied by hand for now. Eventually these will come from metadata
    # alongside the .cxw file, which will also identify the protein target.
    proposal: Optional[str] = None
    visit: Optional[str] = None


class SensoFitWrapper(Wrapper):
    """Run sensofit kinetic fitting over a single Creoptix .cxw file."""

    _logger_name = "dlstbx.wrap.sensofit"
    name = "sensofit"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = SensoFitParameters(**self.recwrap.recipe_step["job_parameters"])
        cxw = params.cxw_file
        if not cxw.is_file():
            self.log.error("No such .cxw file: %s", cxw)
            return False

        # Results live beside the input
        results_dir = cxw.parent / params.results_subdir
        results_dir.mkdir(parents=True, exist_ok=True)

        command = [
            str(params.python),
            "-u",
            "-m",
            "sensofit",
            str(cxw),
            "--mode",
            params.mode,
            "--output",
            str(results_dir),
        ]
        if params.n_parallel_jobs:
            command += ["--n-parallel-jobs", str(params.n_parallel_jobs)]

        self.log.info("Running sensofit: %s", " ".join(command))
        start = time.time()
        result = subprocess.run(command, cwd=results_dir)
        runtime = time.time() - start
        success = result.returncode == 0

        joblog = (
            f"slurm-{os.environ['SLURM_JOB_ID']}.out"
            if os.environ.get("SLURM_JOB_ID")
            else "the job log"
        )

        if success:
            self.log.info(
                "sensofit finished %s in %.1f seconds, results in %s",
                cxw.name,
                runtime,
                results_dir,
            )
        else:
            self.log.error(
                "sensofit failed on %s with exit code %d after %.1f seconds, see %s",
                cxw.name,
                result.returncode,
                runtime,
                joblog,
            )

        self.recwrap.send_to(
            "email",
            "\n".join(
                [
                    f"sensofit {'finished' if success else 'FAILED'} for {cxw.name}",
                    "",
                    f"Input:   {cxw}",
                    f"Results: {results_dir}",
                    f"Log:     {cxw.parent / joblog}",
                    "",
                    "Files produced:",
                    *(f"  {p.name}" for p in sorted(results_dir.iterdir())),
                ]
            ),
        )

        # PLACEHOLDER: move the results to the visit's labxchem processed directory once
        # the .cxw metadata carries sufficient info in order to link.
        if params.proposal and params.visit:
            destination = (
                Path("/dls/labxchem/data")
                / params.proposal
                / params.visit
                / "processed"
                / "auto"
            )
            self.log.warning(
                "PLACEHOLDER: would deposit %s to %s", results_dir, destination
            )

        return success
