import logging
import os
import xml.etree.ElementTree

import procrunner
import py
import zocalo.wrapper
from dxtbx.model.experiment_list import ExperimentListFactory
from dxtbx.serialize import xds

import dlstbx.util.symlink
from dlstbx.util.merging_statistics import get_merging_statistics

logger = logging.getLogger("dlstbx.wrap.autoPROC")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
    "FONTCONFIG_PATH": "",
    "FONTCONFIG_FILE": "",
}


def read_autoproc_xml(xml_file):
    if not xml_file.check(file=1, exists=1):
        logger.info("Expected file %s missing", xml_file.strpath)
        return False
    logger.debug("Reading autoPROC results from %s", xml_file.strpath)

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
        xml_dict = make_dict_from_tree(
            xml.etree.ElementTree.parse(xml_file.strpath).getroot()
        )
    except Exception as e:
        logger.error(
            "Could not read autoPROC file from %s: %s",
            xml_file.strpath,
            e,
            exc_info=True,
        )
        return False

    if "AutoProcContainer" not in xml_dict:
        logger.error("No AutoProcContainer in autoPROC log file %s", xml_file.strpath)
        return False

    xml_dict = xml_dict["AutoProcContainer"]
    appa = xml_dict.get("AutoProcProgramContainer", {}).get("AutoProcProgramAttachment")
    if appa is not None and not isinstance(appa, list):
        # If AutoPROC reports only a single attachment then it is not presented as a list
        xml_dict["AutoProcProgramContainer"]["AutoProcProgramAttachment"] = [appa]
    return xml_dict


class autoPROCWrapper(zocalo.wrapper.BaseWrapper):
    def send_results_to_ispyb(
        self, autoproc_xml, special_program_name=None, attachments=None
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
        logger.debug(f"autoPROC version: {autoproc_version}")

        # Step 1: Add new record to AutoProc, keep the AutoProcID
        if "AutoProc" in autoproc_xml:
            ispyb_command_list.append(
                {
                    "ispyb_command": "write_autoproc",
                    "autoproc_id": None,
                    "store_result": "ispyb_autoproc_id",
                    "spacegroup": autoproc_xml["AutoProc"]["spaceGroup"],
                    "refinedcell_a": autoproc_xml["AutoProc"]["refinedCell_a"],
                    "refinedcell_b": autoproc_xml["AutoProc"]["refinedCell_b"],
                    "refinedcell_c": autoproc_xml["AutoProc"]["refinedCell_c"],
                    "refinedcell_alpha": autoproc_xml["AutoProc"]["refinedCell_alpha"],
                    "refinedcell_beta": autoproc_xml["AutoProc"]["refinedCell_beta"],
                    "refinedcell_gamma": autoproc_xml["AutoProc"]["refinedCell_gamma"],
                }
            )
        else:
            logger.info("AutoProc record missing from AutoProc xml file")

        # Step 2: Store scaling results, linked to the AutoProcID
        #         Keep the AutoProcScalingID
        if "AutoProcScalingStatistics" in autoproc_xml.get(
            "AutoProcScalingContainer", {}
        ):
            insert_scaling = {
                "ispyb_command": "insert_scaling",
                "autoproc_id": "$ispyb_autoproc_id",
                "store_result": "ispyb_autoprocscaling_id",
            }
            for statistics in autoproc_xml["AutoProcScalingContainer"][
                "AutoProcScalingStatistics"
            ]:
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
                    "res_lim_high": statistics["resolutionLimitHigh"],
                    "res_lim_low": statistics["resolutionLimitLow"],
                }
            ispyb_command_list.append(insert_scaling)
        else:
            logger.info(
                "AutoProcScalingStatistics record missing from AutoProc xml file"
            )

        # Step 3: Store integration results, linking them to ScalingID
        if "AutoProcIntegrationContainer" in autoproc_xml.get(
            "AutoProcScalingContainer", {}
        ):
            APIC = autoproc_xml["AutoProcScalingContainer"][
                "AutoProcIntegrationContainer"
            ]
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
                        logger.debug(f"{beam_direction}: {integration[beam_direction]}")

                if n > 0 or special_program_name:
                    # make sure only the first integration of the original program
                    # uses the integration ID initially created in the recipe before
                    # processing started, and all subsequent integration results
                    # are written to a new record
                    integration["integration_id"] = None
            ispyb_command_list.append(integration)
        else:
            logger.info(
                "AutoProcIntegrationContainer record missing from AutoProc xml file"
            )

        if attachments:
            for filename, dirname, filetype, importance_rank in attachments:
                ispyb_command_list.append(
                    {
                        "ispyb_command": "add_program_attachment",
                        "program_id": "$ispyb_autoprocprogram_id",
                        "file_name": filename,
                        "file_path": dirname,
                        "file_type": filetype,
                        "importance_rank": importance_rank,
                    }
                )

        if special_program_name:
            ispyb_command_list.append(
                {
                    "ispyb_command": "update_processing_status",
                    "program_id": "$ispyb_autoprocprogram_id",
                    "message": "processing successful",
                    "status": "success",
                }
            )

        if not ispyb_command_list:
            logger.warning("no results to send to ISPyB")
            return False
        logger.info(
            "Sending %d commands to ISPyB: %s",
            len(ispyb_command_list),
            str(ispyb_command_list),
        )
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        return True

    def construct_commandline(self, params):
        """Construct autoPROC command line.
        Takes job parameter dictionary, returns array."""

        working_directory = params["working_directory"]
        image_template = params["autoproc"]["image_template"]
        image_directory = params["autoproc"]["image_directory"]
        image_first = params["autoproc"]["image_first"]
        image_last = params["autoproc"]["image_last"]
        image_pattern = params["image_pattern"]
        project = params["autoproc"].get("project")
        crystal = params["autoproc"].get("crystal")

        beamline = params["beamline"]

        prefix = image_template.split("#")[0]
        crystal = prefix.replace("_", "").replace(" ", "").replace("-", "")

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
            working_directory,
        ]
        if project:
            command.append(f"pname={project}")
        if crystal:
            command.append(f"xname={crystal}")

        # If any keywords defined in the following macros are also defined after
        # the macro on the command line, then the value on the command line "wins"
        if beamline == "i23":
            self._macro = "DiamondI23"
        elif beamline == "i04":
            self._macro = "DiamondI04"
        else:
            self._macro = None

        if self._macro is not None:
            command.extend(["-M", self._macro])

        if image_template.endswith(".h5"):
            command.extend(
                [
                    "-h5",
                    os.path.join(image_directory, image_template),
                    "DistributeBackgroundImagesForHdf5=no",
                ]
            )
            plugin_name = "durin-plugin.so"
            hdf5_lib = ""
            for d in os.environ["PATH"].split(os.pathsep):
                if os.path.exists(os.path.join(d, plugin_name)):
                    hdf5_lib = "autoPROC_XdsKeyword_LIB=%s" % os.path.join(
                        d, plugin_name
                    )
            if not hdf5_lib:
                logger.warning("Couldn't find plugin %s in PATH" % plugin_name)
            if hdf5_lib:
                command.append(hdf5_lib)
            untrusted_rectangles = self.get_untrusted_rectangles(
                os.path.join(image_directory, image_template)
            )
            if untrusted_rectangles:
                command.append(
                    'autoPROC_XdsKeyword_UNTRUSTED_RECTANGLE="%s"'
                    % " | ".join(untrusted_rectangles)
                )

        else:
            command.extend(
                [
                    "-Id",
                    ",".join(
                        (
                            crystal,
                            image_directory,
                            image_template,
                            image_first,
                            image_last,
                        )
                    ),
                ]
            )
            first_image_path = os.path.join(
                image_directory, image_pattern % int(image_first)
            )
            untrusted_rectangles = self.get_untrusted_rectangles(first_image_path)
            if untrusted_rectangles:
                command.append(
                    'autoPROC_XdsKeyword_UNTRUSTED_RECTANGLE="%s"'
                    % " | ".join(untrusted_rectangles)
                )
            if beamline == "i24":
                # i24 can run in tray mode (horizontal gonio) or pin mode
                # (vertical gonio)
                with open(first_image_path, "rb") as f:
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

        if params.get("ispyb_parameters"):
            if params["ispyb_parameters"].get("d_min"):
                # Can we set just d_min alone?
                # -R <reslow> <reshigh>
                pass
            if params["ispyb_parameters"].get("spacegroup"):
                command.append("symm=%s" % params["ispyb_parameters"]["spacegroup"])
            if params["ispyb_parameters"].get("unit_cell"):
                command.append(
                    "cell=%s"
                    % params["ispyb_parameters"]["unit_cell"].replace(",", " ")
                )

        return command

    def get_untrusted_rectangles(self, first_image):
        rectangles = []

        if self._macro is not None:
            # Parse any existing untrusted rectangles out of the macro
            with open(
                os.path.expandvars(
                    "$autoPROC_home/autoPROC/macros/%s.macro" % self._macro
                )
            ) as f:
                for line in f.readlines():
                    line = line.strip()
                    if line.strip().startswith(
                        "autoPROC_XdsKeyword_UNTRUSTED_RECTANGLE="
                    ):
                        rectangles.append(line.split("=")[-1].strip('"'))

        # Now add any untrusted rectangles defined in the dxtbx model
        expts = ExperimentListFactory.from_filenames([str(first_image)])
        to_xds = xds.to_xds(expts[0].imageset)
        for panel, (x0, _, y0, _) in zip(to_xds.get_detector(), to_xds.panel_limits):
            for f0, s0, f1, s1 in panel.get_mask():
                rectangles.append(
                    "%d %d %d %d" % (f0 + x0 - 1, f1 + x0, s0 + y0 - 1, s1 + y0)
                )

        return rectangles

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

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
                # only runs without space group are shown in SynchWeb overview
                params["synchweb_ticks"] = None

        command = self.construct_commandline(params)

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        # Create working directory with symbolic link
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory.strpath, params["create_symlink"]
            )

        # Create SynchWeb ticks hack file.
        # For this we need to create the results directory and symlink immediately.
        if params.get("synchweb_ticks"):
            logger.debug("Setting SynchWeb status to swirl")
            if params.get("create_symlink"):
                results_directory.ensure(dir=True)
                dlstbx.util.symlink.create_parent_symlink(
                    results_directory.strpath, params["create_symlink"]
                )
            py.path.local(params["synchweb_ticks"]).ensure()

        # disable control sequence parameters from autoPROC output
        # https://www.globalphasing.com/autoproc/wiki/index.cgi?RunningAutoProcAtSynchrotrons#settings
        logger.info("command: %s", " ".join(command))
        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            environment_override={"autoPROC_HIGHLIGHT": "no", **clean_environment},
            working_directory=working_directory.strpath,
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
            logger.debug(result["stdout"].decode("latin1"))
            logger.debug(result["stderr"].decode("latin1"))

        working_directory.join("autoPROC.log").write(result["stdout"].decode("latin1"))

        # cd $jobdir
        # tar -xzvf summary.tar.gz

        # Visit=`basename ${3}`
        ## put history into the log files
        # echo "Attempting to add history to mtz files"
        # find $jobdir -name '*.mtz' -exec /dls_sw/apps/mx-scripts/misc/AddHistoryToMTZ.sh $Beamline $Visit {} $2 autoPROC \;

        if success:
            json_file = working_directory.join("iotbx-merging-stats.json")
            scaled_unmerged_mtz = working_directory.join("aimless_unmerged.mtz")
            if scaled_unmerged_mtz.check():
                json_file.write(
                    get_merging_statistics(str(scaled_unmerged_mtz.strpath)).as_json()
                )

        # move summary_inlined.html to summary.html
        inlined_html = working_directory.join("summary_inlined.html")
        if inlined_html.check():
            inlined_html.move(working_directory.join("summary.html"))

        # attempt to read autoproc XML droppings
        autoproc_xml = read_autoproc_xml(working_directory.join("autoPROC.xml"))
        staraniso_xml = read_autoproc_xml(
            working_directory.join("autoPROC_staraniso.xml")
        )

        # copy output files to result directory
        results_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                results_directory.strpath, params["create_symlink"]
            )

        copy_extensions = {
            ".dat",
            ".HKL",
            ".html",
            ".log",
            ".LP",
            ".mtz",
            ".pdf",
            ".sca",
        }
        keep = {"summary.tar.gz": "result", "iotbx-merging-stats.json": "graph"}
        if autoproc_xml:
            for entry in autoproc_xml.get("AutoProcProgramContainer", {}).get(
                "AutoProcProgramAttachment", []
            ):
                keep[entry["fileName"]] = {"log": "log"}.get(
                    entry["fileType"].lower(), "result"
                )
        if staraniso_xml:
            for entry in staraniso_xml.get("AutoProcProgramContainer", {}).get(
                "AutoProcProgramAttachment", []
            ):
                keep[entry["fileName"]] = {"log": "log"}.get(
                    entry["fileType"].lower(), "result"
                )
        allfiles = []  # flat list
        anisofiles = []  # tuples of file name, dir name, file type
        attachments = []  # tuples of file name, dir name, file type
        for filename in working_directory.listdir():
            keep_as = keep.get(filename.basename, filename.ext in copy_extensions)
            if not keep_as:
                continue
            destination = results_directory.join(filename.basename)
            logger.debug("Copying %s to %s", filename.strpath, destination.strpath)
            filename.copy(destination)
            if filename.basename not in keep:
                continue  # only copy file, do not register in ISPyB
            importance_rank = {
                "truncate-unique.mtz": 1,
                "staraniso_alldata-unique.mtz": 1,
                "summary.html": 1,
            }.get(filename.basename, 2)
            if "staraniso" in filename.basename:
                anisofiles.append(
                    (
                        destination.basename,
                        destination.dirname,
                        keep_as,
                        importance_rank,
                    )
                )
            else:
                if keep_as == "log":
                    # also record log files for staraniso
                    anisofiles.append(
                        (
                            destination.basename,
                            destination.dirname,
                            keep_as,
                            importance_rank,
                        )
                    )
                attachments.append(
                    (
                        destination.basename,
                        destination.dirname,
                        keep_as,
                        importance_rank,
                    )
                )
                allfiles.append(destination.strpath)
        if allfiles:
            self.record_result_all_files({"filelist": allfiles})

        if success and autoproc_xml:
            success = self.send_results_to_ispyb(autoproc_xml, attachments=attachments)
        if success and staraniso_xml:
            success = self.send_results_to_ispyb(
                staraniso_xml,
                special_program_name="autoPROC+STARANISO",
                attachments=anisofiles,
            )

        # Update SynchWeb ticks hack file.
        if params.get("synchweb_ticks"):
            if success:
                logger.debug("Setting SynchWeb status to success")
                py.path.local(params["synchweb_ticks"]).write(
                    """
            The purpose of this file is only
            to signal to SynchWeb that the
            data were successfully processed.

            # magic string: %s
            """
                    % params.get("synchweb_ticks_magic")
                )
            else:
                logger.debug("Setting SynchWeb status to failure")
                py.path.local(params["synchweb_ticks"]).write(
                    """
            The purpose of this file is only
            to signal to SynchWeb that the
            data processing has failed.
            """
                )

        return success
