from __future__ import annotations

import os
import shutil
import subprocess
from copy import deepcopy
from pathlib import Path

import py


def run_dials_estimate_resolution(
    filepaths: list[Path], working_directory: Path, extra_args: list[str] | None = None
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


def copy_results(working_directory, results_directory, skip_copy, logger):
    def ignore_func(directory, files):
        ignore_list = deepcopy(skip_copy)
        pth = py.path.local(directory)
        for f in files:
            fp = pth.join(f)
            if not fp.check():
                ignore_list.append(f)
                continue
            if os.path.islink(fp):
                dest = os.readlink(fp)
                if not os.path.isfile(dest):
                    ignore_list.append(f)
        return ignore_list

    shutil.copytree(
        working_directory,
        results_directory,
        symlinks=False,
        ignore_dangling_symlinks=True,
        ignore=ignore_func,
    )
    src_pth_esc = r"\/".join(os.path.dirname(working_directory).split(os.sep))
    dest_pth_esc = r"\/".join(os.path.dirname(results_directory).split(os.sep))
    sed_command = (
        r"find %s -type f -exec grep -Iq . {} \; -and -exec sed -i 's/%s/%s/g' {} +"
        % (results_directory, src_pth_esc, dest_pth_esc)
    )
    logger.info(f"Running sed command: {sed_command}")
    try:
        subprocess.call([sed_command], shell=True)
    except Exception:
        logger.warning("Failed to run sed command to update paths", exc_info=True)
