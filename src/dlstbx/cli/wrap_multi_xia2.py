import glob
import json
import logging
import os
import shutil
import sys

import procrunner

logger = logging.getLogger("dlstbx.wrap_multi_xia2")


def run(args=None):
    if not args:
        args = sys.argv[1:]
    assert len(args) >= 2, len(args)
    recipe_pointer = args[0]
    recipe_file = args[1]

    assert os.path.isfile(recipe_file), recipe_file
    with open(recipe_file, "rb") as f:
        recipe = json.load(f)

    multi_xia2_recipe = recipe[recipe_pointer]

    # setup the multi_xia2 command line

    command = ["xia2"]
    params = multi_xia2_recipe["job_parameters"]
    if "multi_xia2" in params:
        job_parameters = params["multi_xia2"]
    else:
        job_parameters = params["xia2"]
    for param, values in job_parameters.items():
        if param == "images":
            param = "image"
            values = values.split(",")
        if not isinstance(values, (list, tuple)):
            values = [values]
        for v in values:
            command.append(f"{param}={v}")
    if params.get("ispyb_parameters"):
        if params["ispyb_parameters"].get("d_min"):
            command.append(
                "xia2.settings.resolution.d_min=%s"
                % params["ispyb_parameters"]["d_min"]
            )
        if params["ispyb_parameters"].get("spacegroup"):
            command.append(
                "xia2.settings.space_group=%s"
                % params["ispyb_parameters"]["spacegroup"]
            )
        if params["ispyb_parameters"].get("unit_cell"):
            command.append(
                "xia2.settings.unit_cell=%s" % params["ispyb_parameters"]["unit_cell"]
            )

    # run xia2 in working directory

    cwd = os.path.abspath(os.curdir)

    working_directory = params["working_directory"]
    if not os.path.exists(working_directory):
        os.makedirs(working_directory)
    os.chdir(working_directory)
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

    for subdir in ("DataFiles", "LogFiles"):
        src = os.path.join(working_directory, subdir)
        dst = os.path.join(results_directory, subdir)
        if os.path.exists(src):
            logger.debug(f"Copying {src} to {dst}")
            shutil.copytree(src, dst)
        else:
            logger.warning("Expected output directory does not exist: %s", src)

    for f in glob.glob(os.path.join(working_directory, "*.*")):
        shutil.copy(f, results_directory)

    os.chdir(cwd)


def main():
    logging.basicConfig(level=logging.DEBUG)
    run()
