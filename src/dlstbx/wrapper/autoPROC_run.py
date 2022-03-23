from __future__ import annotations

import logging
import os
from pathlib import Path

import procrunner
from dials.util.mp import available_cores
from dxtbx.model.experiment_list import ExperimentListFactory
from dxtbx.serialize import xds

from dlstbx.util.iris import get_objects_from_s3
from dlstbx.util.merging_statistics import get_merging_statistics
from dlstbx.wrapper import Wrapper

logger = logging.getLogger("zocalo.wrap.autoPROC_run")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
    "FONTCONFIG_PATH": "",
    "FONTCONFIG_FILE": "",
}


def construct_commandline(params, working_directory=None, image_directory=None):
    """Construct autoPROC command line.
    Takes job parameter dictionary, returns array."""

    image_directory_input = image_directory
    images = params["images"]
    pname = params["autoproc"].get("pname")
    xname = params["autoproc"].get("xname")
    beamline = params["beamline"]
    nproc = params["autoproc"].get("nproc", available_cores())

    # prefix = image_template.split("#")[0]
    # crystal = prefix.replace("_", "").replace(" ", "").replace("-", "")

    command = [
        "process",
        "-xml",
        "autoPROC_XdsKeyword_MAXIMUM_NUMBER_OF_PROCESSORS=12",
        "-M",
        "HighResCutOnCChalf",
        'autoPROC_CreateSummaryImageHrefLink="no"',
        'autoPROC_Summary2Base64_Run="yes"',
        'StopIfSubdirExists="no"',
        "-d",
        str(working_directory),
        "-nthreads",
        f"{nproc}",
    ]
    if pname:
        command.append(f"pname={pname}")
    if xname:
        command.append(f"xname={xname}")

    # If any keywords defined in the following macros are also defined after
    # the macro on the command line, then the value on the command line "wins"
    if beamline == "i23":
        macro = "DiamondI23"
    elif beamline == "i04":
        macro = "DiamondI04"
    else:
        macro = None

    if macro is not None:
        command.extend(["-M", macro])

    untrusted_rectangles = []

    hdf5_mode = False

    for i, image in enumerate(images.split(",")):
        first_image_or_master_h5, image_first, image_last = image.split(":")

        if first_image_or_master_h5.endswith(".h5"):
            template = first_image_or_master_h5
            hdf5_mode = True
        else:
            from dxtbx.sequence_filenames import template_regex

            template, n_digits = template_regex(first_image_or_master_h5)

        if not image_directory_input:
            image_directory, image_template = os.path.split(template)
        else:
            image_directory = image_directory_input
            _, image_template = os.path.split(template)

        # ensure unique identifier if multiple sweeps
        prefix = image_template.split("_master.h5")[0].split("#")[0]
        idn = f"x{i}" + prefix.replace("_", "").replace(" ", "").replace("-", "")
        command.extend(
            [
                "-Id",
                ",".join(
                    (
                        idn,
                        os.fspath(image_directory),
                        image_template,
                        image_first,
                        image_last,
                    )
                ),
            ]
        )

        # Update image directory path in case processing is run on the cloud
        first_image_or_master_h5 = os.path.join(
            image_directory, os.path.basename(first_image_or_master_h5)
        )

        # This assumes that all datasets have the same untrusted rectangles
        untrusted_rectangles = get_untrusted_rectangles(
            first_image_or_master_h5,
            macro=macro,
        )
        if beamline == "i04-1":
            untrusted_rectangles.append("774 1029 1356 1613")

        if beamline == "i24" and first_image_or_master_h5.endswith(".cbf"):
            # i24 can run in tray mode (horizontal gonio) or pin mode
            # (vertical gonio)
            with open(first_image_or_master_h5, "rb") as f:
                for line in f.readlines():
                    if b"Oscillation_axis" in line and b"+SLOW" in line:
                        command.append(
                            'autoPROC_XdsKeyword_ROTATION_AXIS="0.000000 -1.000000  0.000000"'
                        )
                        break
                    elif b"Oscillation_axis" in line and b"+FAST" in line:
                        command.append(
                            'autoPROC_XdsKeyword_ROTATION_AXIS="-1.000000  0.000000  0.000000"'
                        )
                        break

    if hdf5_mode:
        command.append("DistributeBackgroundImagesForHdf5=no")
        plugin_name = "durin-plugin.so"
        hdf5_lib = ""
        for d in os.environ["PATH"].split(os.pathsep):
            if os.path.exists(os.path.join(d, plugin_name)):
                hdf5_lib = "autoPROC_XdsKeyword_LIB=%s" % os.path.join(d, plugin_name)
        if not hdf5_lib:
            logger.warning("Couldn't find plugin %s in PATH" % plugin_name)
        if hdf5_lib:
            command.append(hdf5_lib)

    if untrusted_rectangles:
        command.append(
            'autoPROC_XdsKeyword_UNTRUSTED_RECTANGLE="%s"'
            % " | ".join(untrusted_rectangles)
        )

    if params.get("ispyb_parameters"):
        if params["ispyb_parameters"].get("d_min"):
            reshigh = params["ispyb_parameters"]["d_min"]
            reslow = 1000
            command.extend(["-R", f"{reslow}", f"{reshigh}"])
        if params["ispyb_parameters"].get("spacegroup"):
            command.append("symm=%s" % params["ispyb_parameters"]["spacegroup"])
        if params["ispyb_parameters"].get("unit_cell"):
            command.append(
                'cell="%s"' % params["ispyb_parameters"]["unit_cell"].replace(",", " ")
            )
        if params["ispyb_parameters"].get("small_molecule"):
            command.extend(
                [
                    "-M",
                    "SmallMolecules",
                ]
            )

    return command


def get_untrusted_rectangles(first_image_or_master_h5, macro=None):
    rectangles = []

    if macro:
        # Parse any existing untrusted rectangles out of the macro
        with open(
            os.path.expandvars(f"$autoPROC_home/autoPROC/macros/{macro}.macro")
        ) as f:
            for line in f.readlines():
                line = line.strip()
                if line.strip().startswith("autoPROC_XdsKeyword_UNTRUSTED_RECTANGLE="):
                    rectangles.append(line.split("=")[-1].strip('"'))

    # Now add any untrusted rectangles defined in the dxtbx model
    expts = ExperimentListFactory.from_filenames([str(first_image_or_master_h5)])
    to_xds = xds.to_xds(expts[0].imageset)
    for panel, (x0, _, y0, _) in zip(to_xds.get_detector(), to_xds.panel_limits):
        for f0, s0, f1, s1 in panel.get_mask():
            rectangles.append(
                "%d %d %d %d" % (f0 + x0 - 1, f1 + x0, s0 + y0 - 1, s1 + y0)
            )

    return rectangles


class autoPROCRunWrapper(Wrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)
        procrunner_directory = working_directory / params["create_symlink"]
        procrunner_directory.mkdir(parents=True, exist_ok=True)
        image_directory = None

        if "s3_urls" in self.recwrap.environment:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            try:
                get_objects_from_s3(
                    working_directory, self.recwrap.environment.get("s3_urls"), logger
                )
            except Exception:
                logger.exception(
                    "Exception raised while downloading files from S3 object store"
                )
                return False
            # We only want to override the image_directory when running in The Cloud,
            # as only then will the images have been copied locally. Otherwise use the
            # original image_directory.
            image_directory = working_directory

        command = construct_commandline(
            params,
            working_directory=procrunner_directory,
            image_directory=image_directory,
        )

        # disable control sequence parameters from autoPROC output
        # https://www.globalphasing.com/autoproc/wiki/index.cgi?RunningAutoProcAtSynchrotrons#settings
        logger.info("command: %s", " ".join(command))
        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            environment_override={"autoPROC_HIGHLIGHT": "no", **clean_environment},
            working_directory=str(procrunner_directory),
        )

        success = not result["exitcode"] and not result["timeout"]
        if success:
            logger.info("autoPROC successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "autoPROC failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"])
            logger.debug(result["stderr"])

        autoproc_log = procrunner_directory / "autoPROC.log"
        autoproc_log.write_bytes(result["stdout"])

        # HTCondor resolves symlinks while transferring data and doesn't support symlinks to directories
        if "s3_urls" in self.recwrap.environment:
            for tmp_file in procrunner_directory.rglob("*"):
                if (
                    tmp_file.is_symlink() and tmp_file.is_dir()
                ) or tmp_file.suffix == ".h5":
                    tmp_file.unlink(True)

        # cd $jobdir
        # tar -xzvf summary.tar.gz

        # Visit=`basename ${3}`
        ## put history into the log files
        # echo "Attempting to add history to mtz files"
        # find $jobdir -name '*.mtz' -exec /dls_sw/apps/mx-scripts/misc/AddHistoryToMTZ.sh $Beamline $Visit {} $2 autoPROC \;

        if success:
            json_file = procrunner_directory / "iotbx-merging-stats.json"
            scaled_unmerged_mtz = procrunner_directory / "aimless_unmerged.mtz"
            if scaled_unmerged_mtz.is_file():
                json_file.write_text(
                    get_merging_statistics(str(scaled_unmerged_mtz)).as_json()
                )

        # move summary_inlined.html to summary.html
        inlined_html = procrunner_directory / "summary_inlined.html"
        if inlined_html.is_file():
            inlined_html.rename(procrunner_directory / "summary.html")

        return success
