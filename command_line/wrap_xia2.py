from __future__ import absolute_import, division, print_function

import glob
import json
import logging
import os
import shutil

import procrunner

logger = logging.getLogger("dlstbx.wrap_xia2")


def run(args):
    assert len(args) >= 2, len(args)
    recipe_pointer = args[0]
    recipe_file = args[1]
    assert os.path.isfile(recipe_file), recipe_file
    with open(recipe_file, "rb") as f:
        recipe = json.load(f)

    xia2_recipe = recipe[recipe_pointer]

    # setup the xia2 command line

    command = ["xia2"]
    params = xia2_recipe["job_parameters"]
    for param, values in params["xia2"].iteritems():
        # this is single xia2 task to ignore the other images
        if param == "images":
            continue
        if param == "image":
            param = "image"
            values = values.split(",")
        if not isinstance(values, (list, tuple)):
            values = [values]
        for v in values:
            command.append("%s=%s" % (param, v))

    # run xia2 in working directory

    cwd = os.path.abspath(os.curdir)

    working_directory = params["working_directory"]
    if not os.path.exists(working_directory):
        os.makedirs(working_directory)
    os.chdir(working_directory)
    result = procrunner.run_process(
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

    for f in os.listdir(working_directory):
        src = os.path.join(working_directory, f)
        dst = os.path.join(results_directory, f)
        if os.path.isfile(src):
            logger.debug("Copying %s to %s" % (src, dst))
            shutil.copyfile(src, dst)
        elif f in ("DataFiles", "LogFiles") and os.path.isdir(src):
            logger.debug("Copying %s to %s recursively" % (src, dst))
            shutil.copytree(src, dst)

    os.chdir(cwd)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    import sys

    run(sys.argv[1:])
