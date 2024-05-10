from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
import xml.etree.ElementTree
from pathlib import Path
from typing import Any

from dials.util.system import CPU_COUNT
from dxtbx.model.experiment_list import ExperimentListFactory
from dxtbx.serialize import xds

import dlstbx.util.symlink
from dlstbx.util import iris
from dlstbx.util.merging_statistics import get_merging_statistics
from dlstbx.wrapper import Wrapper
from dlstbx.wrapper.helpers import run_dials_estimate_resolution

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
    "FONTCONFIG_PATH": "",
    "FONTCONFIG_FILE": "",
}


def read_autoproc_xml(xml_file: Path, logger: logging.LoggerAdapter):
    if not xml_file.is_file():
        logger.info(f"Expected file {xml_file} missing")
        return False
    logger.debug(f"Reading autoPROC results from {xml_file}")

    def make_dict_from_tree(element_tree):
        """Traverse the given XML element tree to convert it into a dictionary.

        :param element_tree: An XML element tree
        :type element_tree: xml.etree.ElementTree
        :rtype: dict
        """

        def internal_iter(tree, accum):
            """Recursively iterate through the elements of the tree accumulating
            a dictionary result.

            :param tree: The XML element tree
            :type tree: xml.etree.ElementTree
            :param accum: Dictionary into which data is accumulated
            :type accum: dict
            :rtype: dict
            """
            if tree is None:
                return accum
            if len(tree):
                accum[tree.tag] = {}
                for each in tree:
                    result = internal_iter(each, {})
                    if each.tag in accum[tree.tag]:
                        if not isinstance(accum[tree.tag][each.tag], list):
                            accum[tree.tag][each.tag] = [accum[tree.tag][each.tag]]
                        accum[tree.tag][each.tag].append(result[each.tag])
                    else:
                        accum[tree.tag].update(result)
            else:
                accum[tree.tag] = tree.text
            return accum

        return internal_iter(element_tree, {})

    try:
        xml_dict = make_dict_from_tree(xml.etree.ElementTree.parse(xml_file).getroot())
    except Exception as e:
        logger.error(
            f"Could not read autoPROC file from {xml_file}: {e}",
            exc_info=True,
        )
        return False

    if "AutoProcContainer" not in xml_dict:
        logger.error(f"No AutoProcContainer in autoPROC log file {xml_file}")
        return False

    xml_dict = xml_dict["AutoProcContainer"]
    appa = xml_dict.get("AutoProcProgramContainer", {}).get("AutoProcProgramAttachment")
    if appa is not None and not isinstance(appa, list):
        # If AutoPROC reports only a single attachment then it is not presented as a list
        xml_dict["AutoProcProgramContainer"]["AutoProcProgramAttachment"] = [appa]
    return xml_dict


def construct_commandline(
    params: dict,
    logger: logging.LoggerAdapter,
    working_directory: Path | None = None,
    input_image_directory: os.PathLike[str] | None = None,
):
    """Construct autoPROC command line.
    Takes job parameter dictionary, returns array."""

    if not working_directory:
        working_directory = params["working_directory"]
    images = params["images"]
    pname = params["autoproc"].get("pname")
    xname = params["autoproc"].get("xname")
    beamline = params["beamline"]
    nproc = params["autoproc"].get("nproc", CPU_COUNT)

    command = [
        "process",
        "-xml",
        "-M",
        "HighResCutOnCChalf",
        "-M",
        "ReportingInlined",
        'AutoProcSmallFootprint="yes"',
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

        image_directory = input_image_directory
        if image_directory is None:
            image_directory, image_template = os.path.split(template)
        else:
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

        if image_directory:
            first_image_or_master_h5_name = Path(first_image_or_master_h5).name
            first_image_or_master_h5 = str(
                Path(image_directory) / first_image_or_master_h5_name
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
            logger.warning("Couldn't find plugin %s in PATH", plugin_name)
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


def get_untrusted_rectangles(first_image_or_master_h5: str, macro: str | None = None):
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


class autoPROCWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.autoPROC"
    name = "autoPROC"

    def send_results_to_ispyb(
        self,
        autoproc_xml: dict,
        success: bool,
        special_program_name: str | None = None,
        attachments: list[tuple[str, Path, str, int]] | None = None,
        res_i_sig_i_2: float | None = None,
    ):
        ispyb_command_list = []

        if special_program_name:
            # Overwrite ispyb_autoprocprogram_id in the recipe wrapper
            # environment for this step and any eventual downstream ones
            params = self.recwrap.recipe_step["job_parameters"]
            rpid = params.get("ispyb_process", "")
            if not rpid.isdigit():
                rpid = ""
            ispyb_command_list.append(
                {
                    "ispyb_command": "register_processing",
                    "program": special_program_name,
                    "cmdline": special_program_name,
                    "environment": "",
                    "rpid": rpid,
                    "store_result": "ispyb_autoprocprogram_id",
                }
            )

        autoproc_version = (
            autoproc_xml["AutoProcProgramContainer"]["AutoProcProgram"][
                "processingPrograms"
            ]
            .split("(")[1]
            .split()[0]
        )
        self.log.debug(f"autoPROC version: {autoproc_version}")

        # Step 1: Add new record to AutoProc, keep the AutoProcID
        if auto_proc := autoproc_xml.get("AutoProc"):
            if isinstance(auto_proc, list):
                # For multiple sweeps autoPROC duplicates this container
                auto_proc = auto_proc[0]
            ispyb_command_list.append(
                {
                    "ispyb_command": "write_autoproc",
                    "autoproc_id": None,
                    "store_result": "ispyb_autoproc_id",
                    "spacegroup": auto_proc["spaceGroup"],
                    "refinedcell_a": auto_proc["refinedCell_a"],
                    "refinedcell_b": auto_proc["refinedCell_b"],
                    "refinedcell_c": auto_proc["refinedCell_c"],
                    "refinedcell_alpha": auto_proc["refinedCell_alpha"],
                    "refinedcell_beta": auto_proc["refinedCell_beta"],
                    "refinedcell_gamma": auto_proc["refinedCell_gamma"],
                }
            )
        else:
            success = False
            self.log.info("AutoProc record missing from AutoProc xml file")

        # Step 2: Store scaling results, linked to the AutoProcID
        #         Keep the AutoProcScalingID
        APSC = autoproc_xml.get("AutoProcScalingContainer", {})
        if isinstance(APSC, list):
            # For multiple sweeps autoPROC duplicates this container
            APSC = APSC[0]
        if "AutoProcScalingStatistics" in APSC:
            insert_scaling: dict[str, Any] = {
                "ispyb_command": "insert_scaling",
                "autoproc_id": "$ispyb_autoproc_id",
                "store_result": "ispyb_autoprocscaling_id",
            }
            for statistics in APSC["AutoProcScalingStatistics"]:
                insert_scaling[statistics["scalingStatisticsType"]] = {
                    "anom_completeness": statistics["anomalousCompleteness"],
                    "anom_multiplicity": statistics["anomalousMultiplicity"],
                    "cc_anom": statistics["ccAnomalous"],
                    "cc_half": statistics["ccHalf"],
                    "completeness": statistics["completeness"],
                    "mean_i_sig_i": statistics["meanIOverSigI"],
                    "multiplicity": statistics["multiplicity"],
                    "n_tot_obs": statistics["nTotalObservations"],
                    "n_tot_unique_obs": statistics["nTotalUniqueObservations"],
                    "r_meas_all_iplusi_minus": statistics["rMeasAllIPlusIMinus"],
                    "r_meas_within_iplusi_minus": statistics["rMeasWithinIPlusIMinus"],
                    "r_merge": statistics["rMerge"],
                    "r_pim_all_iplusi_minus": statistics["rPimAllIPlusIMinus"],
                    "r_pim_within_iplusi_minus": statistics["rPimWithinIPlusIMinus"],
                    "res_i_sig_i_2": (
                        res_i_sig_i_2
                        if statistics["scalingStatisticsType"] == "overall"
                        else None
                    ),
                    "res_lim_high": statistics["resolutionLimitHigh"],
                    "res_lim_low": statistics["resolutionLimitLow"],
                }
            ispyb_command_list.append(insert_scaling)
        else:
            success = False
            self.log.info(
                "AutoProcScalingStatistics record missing from AutoProc xml file"
            )

        # Step 3: Store integration results, linking them to ScalingID
        if APIC := APSC.get("AutoProcIntegrationContainer"):
            if isinstance(APIC, dict):  # Make it a list regardless
                APIC = [APIC]
            for n, container in enumerate(APIC):
                int_result = container["AutoProcIntegration"]
                integration = {
                    "ispyb_command": "upsert_integration",
                    "scaling_id": "$ispyb_autoprocscaling_id",
                    "beam_vec_x": int_result["beamVectorX"],
                    "beam_vec_y": int_result["beamVectorY"],
                    "beam_vec_z": int_result["beamVectorZ"],
                    "cell_a": int_result["cell_a"],
                    "cell_b": int_result["cell_b"],
                    "cell_c": int_result["cell_c"],
                    "cell_alpha": int_result["cell_alpha"],
                    "cell_beta": int_result["cell_beta"],
                    "cell_gamma": int_result["cell_gamma"],
                    "start_image_no": int_result["startImageNumber"],
                    "end_image_no": int_result["endImageNumber"],
                    "rot_axis_x": int_result["rotationAxisX"],
                    "rot_axis_y": int_result["rotationAxisY"],
                    "rot_axis_z": int_result["rotationAxisZ"],
                    # autoPROC swaps X and Y compared to what we expect
                    "refined_xbeam": int_result["refinedYBeam"],
                    "refined_ybeam": int_result["refinedXBeam"],
                    "refined_detector_dist": int_result["refinedDetectorDistance"],
                }
                if autoproc_version < "20210420":
                    # autoPROC reports beam centre in px rather than mm
                    params = self.recwrap.recipe_step["job_parameters"]
                    image_template = params["autoproc"]["image_template"]
                    if image_template.endswith(".h5"):
                        px_to_mm = 0.075
                    else:
                        px_to_mm = 0.172
                    if autoproc_version == "20200918":
                        # Known bug in this version of autoPROC:
                        # We reported the direct beam position in pixels within that
                        # ISPyB-compatible XML file, when other programs apparently
                        # reported it in mm. So we wanted to change it to mm as well,
                        # but did that fix in two places at the same time ...
                        # resulting in multiplying by (pixelsize)**2 instead of just
                        # pixelsize.
                        px_to_mm = 1 / px_to_mm
                    for beam_direction in ("refined_xbeam", "refined_ybeam"):
                        if integration[beam_direction]:
                            integration[beam_direction] = (
                                float(integration[beam_direction]) * px_to_mm
                            )
                        self.log.debug(
                            f"{beam_direction}: {integration[beam_direction]}"
                        )

                if n > 0 or special_program_name:
                    # make sure only the first integration of the original program
                    # uses the integration ID initially created in the recipe before
                    # processing started, and all subsequent integration results
                    # are written to a new record
                    integration["integration_id"] = None
            ispyb_command_list.append(integration)
        else:
            success = False
            self.log.info(
                "AutoProcIntegrationContainer record missing from AutoProc xml file"
            )

        if attachments:
            for filename, dirname, filetype, importance_rank in attachments:
                ispyb_command_list.append(
                    {
                        "ispyb_command": "add_program_attachment",
                        "program_id": "$ispyb_autoprocprogram_id",
                        "file_name": filename,
                        "file_path": os.fspath(dirname),
                        "file_type": filetype,
                        "importance_rank": importance_rank,
                    }
                )

        if special_program_name:
            if success:
                ispyb_command_list.append(
                    {
                        "ispyb_command": "update_processing_status",
                        "program_id": "$ispyb_autoprocprogram_id",
                        "message": "processing successful",
                        "status": "success",
                    }
                )
            else:
                ispyb_command_list.append(
                    {
                        "ispyb_command": "update_processing_status",
                        "program_id": "$ispyb_autoprocprogram_id",
                        "message": "processing failure",
                        "status": "failure",
                    }
                )

        if not ispyb_command_list:
            self.log.warning("no results to send to ISPyB")
            return False
        self.log.info(
            "Sending %d commands to ISPyB: %s",
            len(ispyb_command_list),
            str(ispyb_command_list),
        )
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        return success

    def setup(self, working_directory: Path, params: dict):

        # Create working directory with symbolic link
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory, params["create_symlink"], levels=1
            )

        if singularity_image := params.get("singularity_image"):
            try:
                iris.write_singularity_script(working_directory, singularity_image)
                self.recwrap.environment.update(
                    {"singularity_image": singularity_image}
                )
            except Exception:
                self.log.exception("Error writing singularity script")
                return False

        return True

    def run_autoPROC(self, working_directory: Path, params: dict):

        subprocess_directory = working_directory / "autoPROC"
        subprocess_directory.mkdir(parents=True, exist_ok=True)
        input_image_directory = None

        if s3_urls := self.recwrap.environment.get("s3_urls"):
            # Logger for recording data transfer rates from S3 Echo object store
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.log.logger.addHandler(handler)
            self.log.logger.setLevel(logging.DEBUG)
            try:
                iris.get_objects_from_s3(working_directory, s3_urls, self.log)
            except Exception:
                self.log.exception(
                    "Exception raised while downloading files from S3 object store"
                )
                return False
            # We only want to override the image_directory when running in The Cloud,
            # as only then will the images have been copied locally. Otherwise use the
            # original image_directory.
            input_image_directory = working_directory

        command = construct_commandline(
            params,
            self.log,
            working_directory=subprocess_directory,
            input_image_directory=input_image_directory,
        )

        # disable control sequence parameters from autoPROC output
        # https://www.globalphasing.com/autoproc/wiki/index.cgi?RunningAutoProcAtSynchrotrons#settings
        self.log.info("command: %s", " ".join(command))
        with (subprocess_directory / "autoPROC.log").open("w") as fp:
            try:
                start_time = time.perf_counter()
                result = subprocess.run(
                    command,
                    timeout=params.get("timeout"),
                    env=dict(os.environ, autoPROC_HIGHLIGHT="no"),
                    cwd=subprocess_directory,
                    text=True,
                    stdout=fp,
                )
                runtime = time.perf_counter() - start_time
                self.log.info(f"autoPROC took {runtime} seconds")
                self._runtime_hist.observe(runtime)
            except subprocess.TimeoutExpired as te:
                success = False
                self.log.warning(f"autoPROC timed out: {te.timeout}\n  {te.cmd}")
                self.log.debug(te.stdout)
                self.log.debug(te.stderr)
                self._timeout_counter.inc()
            else:
                success = not result.returncode
                if success:
                    self.log.info("autoPROC successful")
                else:
                    self.log.info(f"autoPROC failed with exitcode {result.returncode}")
                    self.log.debug(result.stdout)
                    self.log.debug(result.stderr)

        if success:
            json_file = subprocess_directory / "iotbx-merging-stats.json"
            scaled_unmerged_mtz = subprocess_directory / "aimless_unmerged.mtz"
            if scaled_unmerged_mtz.is_file():
                json_file.write_text(
                    get_merging_statistics(os.fspath(scaled_unmerged_mtz)).as_json()
                )

        if params.get("s3echo"):
            minio_client = iris.get_minio_client(params["s3echo"]["configuration"])
            bucket_name = params["s3echo"].get("bucket", "autoproc")
            try:
                iris.store_results_in_s3(
                    minio_client,
                    bucket_name,
                    params["rpid"],
                    subprocess_directory,
                    self.log,
                )
            except Exception:
                success = False
                self.log.info(
                    "Error while trying to save autoPROC processing results to S3 Echo",
                    exc_info=True,
                )

        return success

    def report(self, working_directory: Path, params: dict, success: bool):

        working_directory = working_directory / "autoPROC"
        if not working_directory.is_dir():
            self.log.error(f"autoPROC working directory {working_directory} not found.")
            return False

        # attempt to read autoproc XML droppings
        autoproc_xml = read_autoproc_xml(working_directory / "autoPROC.xml", self.log)
        staraniso_xml = read_autoproc_xml(
            working_directory / "autoPROC_staraniso.xml", self.log
        )

        # copy output files to result directory
        results_directory = Path(params["results_directory"]) / "autoPROC"
        results_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(results_directory), params["create_symlink"]
            )

        copy_extensions = {
            ".cif",
            ".dat",
            ".HKL",
            ".html",
            ".log",
            ".LP",
            ".mtz",
            ".pdf",
            ".sca",
            ".stats",
        }
        keep = {"summary.tar.gz": "result", "iotbx-merging-stats.json": "graph"}
        if autoproc_xml:
            for entry in autoproc_xml.get("AutoProcProgramContainer", {}).get(
                "AutoProcProgramAttachment", []
            ):
                keep[entry["fileName"]] = {"log": "log"}.get(
                    entry["fileType"].lower(), "result"
                )
        else:
            success = False
        if staraniso_xml:
            for entry in staraniso_xml.get("AutoProcProgramContainer", {}).get(
                "AutoProcProgramAttachment", []
            ):
                keep[entry["fileName"]] = {"log": "log"}.get(
                    entry["fileType"].lower(), "result"
                )
        for filename in working_directory.glob("staraniso*ell"):
            keep[filename.name] = "result"
        allfiles = []  # flat list
        anisofiles = []  # tuples of file name, dir name, file type
        attachments = []  # tuples of file name, dir name, file type
        for filename in working_directory.iterdir():
            keep_as = keep.get(
                filename.name,
                "result" if filename.suffix in copy_extensions else None,
            )
            if not keep_as:
                continue
            destination = results_directory / filename.name
            self.log.debug(f"Copying {filename} to {destination}")
            shutil.copy2(filename, destination, follow_symlinks=False)

            # Fix symlinks to point to a file in the processing directory
            if destination.is_symlink():
                processed_filename = results_directory / filename.resolve().relative_to(
                    working_directory
                )
                destination.unlink(True)
                destination.symlink_to(
                    processed_filename.relative_to(destination.parent)
                )

            if filename.name not in keep:
                continue  # only copy file, do not register in ISPyB
            importance_rank = {
                "truncate-unique.mtz": 1,
                "staraniso_alldata-unique.mtz": 1,
                "summary.html": 1,
                "summary_inlined.html": 1,
            }.get(filename.name, 2)
            if "staraniso" in filename.name:
                anisofiles.append(
                    (
                        destination.name,
                        destination.parent,
                        keep_as,
                        importance_rank,
                    )
                )
            else:
                if keep_as == "log":
                    # also record log files for staraniso
                    anisofiles.append(
                        (
                            destination.name,
                            destination.parent,
                            keep_as,
                            importance_rank,
                        )
                    )
                attachments.append(
                    (
                        destination.name,
                        destination.parent,
                        keep_as,
                        importance_rank,
                    )
                )
                allfiles.append(os.fspath(destination))
        if allfiles:
            self.record_result_all_files({"filelist": allfiles})
        else:
            success = False

        # Calculate the resolution at which the mean merged I/sig(I) = 2
        # Why? Because https://jira.diamond.ac.uk/browse/LIMS-104
        res_i_sig_i_2 = None
        alldata_unmerged_mtz = working_directory / "aimless_alldata_unmerged.mtz"
        if alldata_unmerged_mtz.is_file():
            try:
                resolution_limits = run_dials_estimate_resolution(
                    [alldata_unmerged_mtz],
                    working_directory,
                    extra_args=["misigma=2"],
                )
                res_i_sig_i_2 = resolution_limits.get("Mn(I/sig)")
            except Exception as e:
                self.log.warning(
                    f"dials.estimate_resolution failure: {e}", exc_info=True
                )

        if autoproc_xml:
            success = self.send_results_to_ispyb(
                autoproc_xml,
                success,
                attachments=attachments,
                res_i_sig_i_2=res_i_sig_i_2,
            )
        if staraniso_xml:
            self.send_results_to_ispyb(
                staraniso_xml,
                success,
                special_program_name="autoPROC+STARANISO",
                attachments=anisofiles,
            )

        return success

    def run(self):

        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = dict(self.recwrap.recipe_step["job_parameters"])

        # Create working directory with symbolic link
        working_directory = Path(params.get("working_directory", os.getcwd()))
        working_directory.mkdir(parents=True, exist_ok=True)

        # Adjust all paths if a spacegroup is set in ISPyB
        if params.get("ispyb_parameters"):
            if (
                params["ispyb_parameters"].get("spacegroup")
                and "/" not in params["ispyb_parameters"]["spacegroup"]
            ):
                if "create_symlink" in params:
                    params["create_symlink"] += (
                        "-" + params["ispyb_parameters"]["spacegroup"]
                    )

        stage = params.get("stage")
        assert stage in {None, "setup", "run", "report"}
        success = True

        if stage in {None, "setup"}:
            success = self.setup(working_directory, params)

        if stage in {None, "run"} and success:
            success = self.run_autoPROC(working_directory, params)

        if stage in {None, "report"}:
            success = self.report(working_directory, params, success)
            if success:
                self._success_counter.inc()

        if not success:
            self._failure_counter.inc()

        return success
