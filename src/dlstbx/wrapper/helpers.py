from __future__ import annotations

import os
import subprocess
from pathlib import Path


def run_dials_estimate_resolution(
    filepaths: list[Path], working_directory: Path, extra_args: list[str] = None
) -> dict[str, float]:

    command = ["dials.estimate_resolution"] + [os.fspath(p) for p in filepaths]
    if extra_args:
        command.extend(extra_args)
    result = subprocess.run(
        command,
        cwd=working_directory,
    )
    if result.returncode:
        return {}

    log_file = Path(working_directory / "dials.estimate_resolution.log")
    resolution_limits: dict[str, float] = {}
    for line in log_file.read_text().splitlines():
        if line.startswith("Resolution "):
            metric = line.split(":")[0].split(" ")[-1]
            value = float(line.split(":")[-1].strip())
            resolution_limits[metric] = value

    return resolution_limits
