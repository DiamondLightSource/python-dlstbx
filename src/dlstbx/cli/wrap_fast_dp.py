import glob
import json
import logging
import os
import shutil
import sys

import procrunner

logger = logging.getLogger("dlstbx.wrap_fast_dp")


def run(args=None):
    if not args:
        args = sys.argv[1:]
    assert len(args) >= 2, len(args)
    recipe_pointer = args[0]
    recipe_file = args[1]
    assert os.path.isfile(recipe_file), recipe_file
    with open(recipe_file, "rb") as f:
        recipe = json.load(f)

    fast_dp_recipe = recipe[recipe_pointer]

    # setup the fast_dp command line FIXME work out unit cell, spacegroup,
    # -j -J values

    command = ["fast_dp", "-a", "S"]
    filename = None
    params = fast_dp_recipe["job_parameters"]
    for param, values in params["fast_dp"].items():
        if param == "image":
            tokens = values.split(":")
            filename = tokens[0]
            start, end = int(tokens[1]), int(tokens[2])
            command.extend(["-1", str(start), "-N", str(end)])

    assert filename is not None
    command.append(filename)
    # run fast_dp in working directory

    cwd = os.path.abspath(os.curdir)

    working_directory = params["working_directory"]
    if not os.path.exists(working_directory):
        os.makedirs(working_directory)
    os.chdir(working_directory)
    logger.info("command: %s", " ".join(command))
    logger.info("working directory: %s" % working_directory)
    result = procrunner.run(
        command, timeout=params.get("timeout"), print_stdout=False, print_stderr=False
    )

    logger.info("command: %s", " ".join(result["command"]))
    logger.info("timeout: %s", result["timeout"])
    logger.info("time_start: %s", result["time_start"])
    logger.info("time_end: %s", result["time_end"])
    logger.info("runtime: %s", result["runtime"])
    logger.info("exitcode: %s", result["exitcode"])
    logger.debug(result["stdout"])
    logger.debug(result["stderr"])

    # copy output files to result directory

    results_directory = params["results_directory"]
    if not os.path.exists(results_directory):
        os.makedirs(results_directory)

    # FIXME decide what useful results should be copied over for a useful
    # fast_dp job

    for f in glob.glob(os.path.join(working_directory, "*.*")):
        shutil.copy(f, results_directory)

    os.chdir(cwd)


def main():
    logging.basicConfig(level=logging.DEBUG)
    run()
