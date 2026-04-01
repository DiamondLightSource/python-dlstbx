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


def fix_tmp_paths_in_logs(working_directory, results_directory, logger, uuid=None):
    src_paths_esc = [
        r"\/".join(os.path.dirname(working_directory).split(os.sep)),
    ]
    if uuid:
        src_paths_esc.append(rf"\/tmp\/{uuid}")
    dest_pth_esc = r"\/".join(os.path.dirname(results_directory).split(os.sep))
    for pth in src_paths_esc:
        sed_command = (
            r"find %s -type f -exec grep -Iq . {} \; -and -exec sed -ci 's/%s/%s/g' {} +"
            % (results_directory, pth, dest_pth_esc)
        )
        logger.info(f"Running sed command: {sed_command}")
        try:
            subprocess.call([sed_command], shell=True)
        except Exception:
            logger.warning("Failed to run sed command to update paths", exc_info=True)


def copy_results(working_directory, results_directory, skip_copy, uuid, logger):
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
        symlinks=True,
        ignore_dangling_symlinks=False,
        ignore=ignore_func,
    )
    fix_tmp_paths_in_logs(working_directory, results_directory, logger, uuid)


def fix_acl_mask(subprocess_directory, results_directory, logger):
    # Fix ACL mask for files extracted from .tar archive
    # Using m:rwX resets mask for files as well, unclear why.
    # Hence, running find to apply mask to files and directories separately
    for ft, msk in (("d", "m:rwx"), ("f", "m:rw")):
        setfacl_command = r"find %s -type %s -exec setfacl -m %s '{}' ';'" % (
            results_directory,
            ft,
            msk,
        )
        logger.info(f"Running command to fix ACLs: {setfacl_command}")
        result = subprocess.run(
            [
                setfacl_command,
            ],
            cwd=subprocess_directory,
            shell=True,
        )
        if not result.returncode:
            logger.info(f"Resetting ALC mask to {msk} in {results_directory}")
        else:
            logger.error(f"Failed to reset ALC mask to {msk} in {results_directory}")


def get_file_from_autoprocscaling_info(
    autoprocscaling_info: dict, input_pattern: str
) -> Path:
    attachments = autoprocscaling_info.get("attachments", {})
    for filename in attachments:
        if input_pattern in filename:
            filepath = Path(filename).resolve()
            if not filepath.is_file():
                raise ValueError(
                    "Could not find data file %s to process", input_pattern
                )
            return filepath
    raise ValueError("Input data file matching %s not available", input_pattern)
