import logging
import shutil
import xml.etree.ElementTree
from pathlib import Path

import zocalo.wrapper

import dlstbx.util.symlink
from dlstbx.util.iris import remove_objects_from_s3

logger = logging.getLogger("zocalo.wrap.autoPROC_results")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
    "FONTCONFIG_PATH": "",
    "FONTCONFIG_FILE": "",
}


def read_autoproc_xml(xml_file):
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
        xml_dict = make_dict_from_tree(
            xml.etree.ElementTree.parse(str(xml_file)).getroot()
        )
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


class autoPROCResultsWrapper(zocalo.wrapper.BaseWrapper):
    def send_results_to_ispyb(
        self, autoproc_xml, special_program_name=None, attachments=None
    ):
        ispyb_command_list = []

        success = True
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
            success = False

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
            success = False

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
            success = False

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
            logger.warning("no results to send to ISPyB")
            success = False
        else:
            logger.info(
                "Sending %d commands to ISPyB: %s",
                len(ispyb_command_list),
                str(ispyb_command_list),
            )
            self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        return success

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

        working_directory = Path(params["working_directory"])
        results_directory = Path(params["results_directory"])

        if "s3_urls" in self.recwrap.environment:
            try:
                remove_objects_from_s3(
                    params.get("create_symlink").lower(),
                    self.recwrap.environment.get("s3_urls"),
                )
            except Exception:
                logger.exception(
                    "Exception raised while trying to remove files from S3 object store."
                )

        # copy output files to result directory
        results_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                str(results_directory), params["create_symlink"]
            )

        if not working_directory.is_dir():
            logger.error(
                f"autoPROC working directory {str(working_directory)} not found."
            )
            return False

        # attempt to read autoproc XML droppings
        autoproc_xml = read_autoproc_xml(working_directory / "autoPROC.xml")
        staraniso_xml = read_autoproc_xml(working_directory / "autoPROC_staraniso.xml")

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
        for filename in working_directory.iterdir():
            keep_as = keep.get(filename.name, filename.suffix in copy_extensions)
            if not keep_as:
                continue
            destination = results_directory / filename.name
            logger.debug(f"Copying {filename} to {destination}")
            shutil.copy(filename, destination)
            if filename.name not in keep:
                continue  # only copy file, do not register in ISPyB
            importance_rank = {
                "truncate-unique.mtz": 1,
                "staraniso_alldata-unique.mtz": 1,
                "summary.html": 1,
            }.get(filename.name, 2)
            if "staraniso" in filename.name:
                anisofiles.append(
                    (
                        destination.name,
                        str(destination.parent),
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
                            str(destination.parent),
                            keep_as,
                            importance_rank,
                        )
                    )
                attachments.append(
                    (
                        destination.name,
                        str(destination.parent),
                        keep_as,
                        importance_rank,
                    )
                )
                allfiles.append(str(destination))
        if allfiles:
            self.record_result_all_files({"filelist": allfiles})

        success = False
        if autoproc_xml:
            success_autoproc = self.send_results_to_ispyb(
                autoproc_xml, attachments=attachments
            )
            success = success or success_autoproc
        if staraniso_xml:
            success_staraniso = self.send_results_to_ispyb(
                staraniso_xml,
                special_program_name="autoPROC+STARANISO",
                attachments=anisofiles,
            )
            success = success or success_staraniso
        return success
