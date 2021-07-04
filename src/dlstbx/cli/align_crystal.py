import json
import logging
import os
import shutil
import sys

import procrunner

logger = logging.getLogger("dlstbx.align_crystal")


def _run_command(args):
    logger.info("command: %s", " ".join(args))
    result = procrunner.run(args, print_stdout=False, print_stderr=False)
    logger.info(
        "exited with returncode %s after %s seconds",
        result.returncode,
        result["runtime"],
    )
    if result.returncode:
        logger.info(result.stdout)
        logger.info(result.stderr)
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

    if not _run_command(
        ["dials.refine_bravais_settings", "indexed.expt", "indexed.refl"]
    ):
        return False
    with open("bravais_summary.json") as f:
        d = json.load(f)
    solutions = {int(k): v for k, v in d.items()}
    for k in solutions:
        solutions[k]["experiments_file"] = "bravais_setting_%d.expt" % k
    soln = solutions[max(solutions)]

    if not _run_command(
        ["dials.reindex", "indexed.refl", 'change_of_basis_op="%s"' % soln["cb_op"]]
    ):
        return False
    shutil.copyfile(soln["experiments_file"], "reindexed.expt")

    return _run_command(["dials.align_crystal", "reindexed.expt"])


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
