from __future__ import annotations

import logging
import os
import subprocess
import sys
import time

logger = logging.getLogger("dlstbx.align_crystal")


def _run_command(args):
    logger.info(f"Running command: {' '.join(args)}")
    start_time = time.time()
    result = subprocess.run(args, capture_output=True, text=True)
    logger.info(result.stdout)
    logger.info(result.stderr)
    logger.info(
        f"Command '{' '.join(args)}' exited with returncode '{result.returncode}' after {(time.time() - start_time):.1f} seconds\n"
    )

    if result.returncode:
        return False
    return True


def align_crystal(image_files, nproc=None):
    if not _run_command(["dials.import"] + image_files):
        return False

    command = ["dials.find_spots", "imported.expt"]
    if nproc:
        command.append("nproc=%s" % nproc)
    if not _run_command(command):
        return False

    # could iteratively try different indexing options until one succeeds
    if not _run_command(
        ["dials.index", "imported.expt", "strong.refl", "indexing.method=fft1d"]
    ):
        return False

    if not _run_command(["dials.refine", "indexed.expt", "indexed.refl"]):
        return False

    if not _run_command(
        ["dials.integrate", "refined.expt", "refined.refl", "profile.fitting=False"]
    ):
        return False

    if not _run_command(["dials.symmetry", "integrated.expt", "integrated.refl"]):
        return False

    return _run_command(["dials.align_crystal", "symmetrized.expt"])


def run(args=None):
    if not args:
        args = sys.argv[1:]
    # setup logging
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    fh = logging.FileHandler("dlstbx.align_crystal.log")
    fh.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    logger.addHandler(ch)
    logger.addHandler(fh)

    image_files = [f for f in args if os.path.isfile(f)]
    assert image_files
    if not align_crystal(image_files):
        sys.exit("crystal alignment failed")


if __name__ == "__main__":
    run()
