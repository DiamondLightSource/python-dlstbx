import logging
import zocalo
from pathlib import Path
import dlstbx.util
import procrunner
from shutil import copyfile
import xml.etree.ElementTree as ET
import json


logger = logging.getLogger("dlstbx.wrap.best")


class BESTWrapper(zocalo.wrapper.BaseWrapper):
    def xml_to_dict(self, all_names):

        tree = ET.ElementTree(ET.fromstring(self.xml_string))
        root = tree.getroot()
        assert root.tag == "edna_tables", root.tag

        summary_values = {}

        for table in root.findall("table"):
            table_name = table.attrib.get("name")
            table_idx = table.attrib.get("index")
            if table_name in all_names:
                table_key = (table_name, table_idx)
                summary_values[table_key] = {}
                for l in table.findall("list"):
                    list_name = l.attrib.get("name")
                    list_idx = l.attrib.get("index")
                    if list_name in all_names[table_name]:
                        list_key = (list_name, list_idx)
                        summary_values[table_key][list_key] = {}
                        for item in l.findall("item"):
                            name = item.attrib.get("name")
                            if name in all_names[table_name][list_name]:
                                summary_values[table_key][list_key][name] = item.text
        return summary_values

    def send_results_to_ispyb(self, params):
        ispyb_command_list = []

        # Step 1: Add new record to Screening table, keep the ScreeningId
        # screening_params: ['id', 'dcgid', 'dcid', 'programversion', 'shortcomments', 'comments']
        d = {
            "ispyb_command": "insert_screening",
            "dcid": params["dcid"],
            "dcgid": params["dcgid"],
            "programversion": "BEST 5.1",
            "shortcomments": "BEST anomalous"
            if params["best"]["anomalous"]
            else "BEST native",
            "comments": "Running BEST via Zocalo",
            "store_result": "ispyb_screening_id",
        }
        ispyb_command_list.append(d)

        # Step 2: Store screeningInput results, linked to the screeningId
        #         Keep the screeningInputId
        d = {
            "ispyb_command": "insert_screening_input",
            "screening_id": "$ispyb_screening_id",
            "beamx": params["beamX"],
            "beamy": params["beamY"],
            "store_result": "ispyb_screening_input_id",
        }
        ispyb_command_list.append(d)

        # Step 3: Store screeningOutput results, linked to the screeningId
        #         Keep the screeningOutputId
        d = {
            "program": "BEST 5.1",
            "ispyb_command": "insert_screening_output",
            "screening_id": "$ispyb_screening_id",
            "store_result": "ispyb_screening_output_id",
        }
        ispyb_command_list.append(d)

        # Step 4: Store screeningOutputLattice results, linked to the screeningOutputId
        #         Keep the screeningOutputLatticeId
        all_names = {
            "general_inform": {
                "crystal_parameters": [
                    "cell_a",
                    "cell_b",
                    "cell_c",
                    "cell_alpha",
                    "cell_beta",
                    "cell_gamma",
                ]
            }
        }
        res = self.xml_to_dict(all_names)
        res = res[("general_inform", "1")][("crystal_parameters", "1")]
        d = {
            "spacegroup": params["spacegroup"],
            "unitcella": res["cell_a"],
            "unitcellb": res["cell_b"],
            "unitcellc": res["cell_c"],
            "unitcellalpha": res["cell_alpha"],
            "unitcellbeta": res["cell_beta"],
            "unitcellgamma": res["cell_gamma"],
            "ispyb_command": "insert_screening_output_lattice",
            "screening_output_id": "$ispyb_screening_output_id",
            "store_result": "ispyb_screening_output_lattice_id",
        }
        ispyb_command_list.append(d)

        # Step 5: Store screeningStrategy results, linked to the screeningOutputId
        #         Keep the screeningStrategyId
        all_names = {
            "general_inform": {
                "ranking_resolution": [
                    "dmin",
                ]
            },
            "data_collection_strategy": {
                "summary": [
                    "resolution",
                    "resolution_reasoning",
                    "completeness",
                    "redundancy",
                    "transmission",
                    "total_exposure_time",
                ],
                "collection_run": [
                    "phi_start",
                    "number_of_images",
                    "phi_width",
                    "exposure_time",
                    "overlaps",
                ],
            },
        }
        res = self.xml_to_dict(all_names)
        dc_summary = res[("data_collection_strategy", "1")][("summary", "1")]
        dc_col = res[("data_collection_strategy", "1")][("collection_run", "1")]
        d = {
            "program": "BEST 5.1",
            "ispyb_command": "insert_screening_strategy",
            "resolution": dc_summary["resolution"],
            "completeness": dc_summary["completeness"],
            "multiplicity": dc_summary["redundancy"],
            "anomalous": params["best"]["anomalous"],
            "rankingresolution": res[("general_inform", "1")][
                ("ranking_resolution", "1")
            ]["dmin"],
            "transmission": dc_summary["transmission"],
            "screening_output_id": "$ispyb_screening_output_id",
            "store_result": "ispyb_screening_strategy_id",
        }
        ispyb_command_list.append(d)

        # Step 6: Store screeningStrategyWedge results, linked to the screeningStrategyId
        #         Keep the screeningStrategyWedgeId
        d = {
            "wedgenumber": 1,
            "resolution": dc_summary["resolution"],
            "completeness": dc_summary["completeness"],
            "multiplicity": dc_summary["redundancy"],
            "wavelength": params["wavelength"],
            "ispyb_command": "insert_screening_strategy_wedge",
            "screening_strategy_id": "$ispyb_screening_strategy_id",
            "store_result": "ispyb_screening_strategy_wedge_id",
            "comments": dc_summary["resolution_reasoning"],
        }
        ispyb_command_list.append(d)

        # Step 7: Store screeningStrategySubWedge results, linked to the screeningStrategyWedgeId
        #         Keep the screeningStrategySubWedgeId
        num_subwedge = len(res[("data_collection_strategy", "1")]) - 1
        for (name, idx), dc_col in res[("data_collection_strategy", "1")].items():
            if name != "collection_run":
                continue
            axisend = float(dc_col["phi_start"]) + int(
                dc_col["number_of_images"]
            ) * float(dc_col["phi_width"])
            d = {
                "subwedgenumber": idx,
                "rotationaxis": params["rotationAxis"],
                "axisstart": dc_col["phi_start"],
                "axisend": f"{axisend}",
                "exposuretime": dc_col["exposure_time"],
                "oscillationrange": dc_col["phi_width"],
                "noimages": dc_col["number_of_images"],
                "comments": f"BEST SubWedge{idx}/{num_subwedge}",
                "ispyb_command": "insert_screening_strategy_sub_wedge",
                "screening_strategy_wedge_id": "$ispyb_screening_strategy_wedge_id",
                "store_result": f"ispyb_screening_strategy_sub_wedge_id_{idx}",
            }
            ispyb_command_list.append(d)

        if ispyb_command_list:
            logger.debug(f"Sending {json.dumps(ispyb_command_list)}")
            self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
            logger.info(f"Sent {len(ispyb_command_list)} commands to ISPyB")
        else:
            logger.warning("No commands to send to ISPyB")

        return True

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]
        if params.get("ispyb_parameters"):
            data_path = params["ispyb_parameters"]["data"]
        else:
            data_path = params["data"]

        working_directory = Path(params["working_directory"])
        # Create working directory with symbolic link
        working_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            try:
                levels = params["levels_symlink"]
                dlstbx.util.symlink.create_parent_symlink(
                    str(working_directory), params["create_symlink"], levels=levels
                )
            except KeyError:
                dlstbx.util.symlink.create_parent_symlink(
                    str(working_directory), params["create_symlink"]
                )
        command = [params["best_command"]]
        for flg, label in [
            ("-f", "detector"),
            ("-t", "exposure_time"),
            ("-q", "minimize_total_time"),
            ("-or", "orientations"),
            ("-r", "aimed_resolution"),
            ("-i2s", "i_over_sigma"),
            ("-DMAX", "maximum_dose"),
            ("-T", "total_exposure_time"),
            ("-a", "anomalous"),
            ("-GpS", "dose_rate"),
            ("-sh", "shape"),
            ("-su", "susceptibility"),
            ("-C", "completeness"),
            ("-R", "redundancy"),
            ("-plan", "plan_file"),
            ("-g", "plots"),
            ("-o", "plots_mtv"),
            ("-e", "complexity_level"),
            ("-dna", "dna"),
            ("-S", "speed"),
            ("-M", "min_exposure_time"),
        ]:
            value = params["best"].get(label)
            if value is True:
                command.append(flg)
            elif value:
                command.extend([flg, str(value)])
        data_path = Path(data_path) / params["crystal"] / "SAD" / "SWEEP1" / "integrate"
        corr_path = str(data_path / "CORRECT.LP")
        bkg_path = str(data_path / "BKGPIX.cbf")
        hkl_path = str(data_path / "XDS_ASCII.HKL")
        command.extend(["-xds", corr_path, bkg_path, hkl_path])
        logger.info(f"Running BEST command: {' '.join(command)}")
        try:
            result = procrunner.run(
                command,
                timeout=params["timeout"],
                working_directory=working_directory,
            )
            assert result["exitcode"] == 0
            assert result["timeout"] is False
        except AssertionError:
            logger.exception("Process returned an error code when running BEST")
            return False
        except Exception:
            logger.exception("Running BEST has failed")
            return False

        try:
            best_logfile = working_directory / params["best_logfile"]
            best_logfile.write_text(result["stdout"].decode("latin1"))
        except Exception:
            logger.warning(f"Failed to write BEST output to {str(best_logfile)}")

        xml_file = working_directory / params["best"]["dna"]
        if xml_file.is_file():
            with open(xml_file, "r") as fh:
                self.xml_string = fh.read()
            if "</edna_tables>" not in self.xml_string:
                self.xml_string = "\n".join((self.xml_string, "</edna_tables>"))
            logger.info("sending results to ispyb")
            self.send_results_to_ispyb(params)
        else:
            logger.exception(f"Expected output file does not exist: {xml_file}")
            return False

        if params.get("results_directory"):
            results_directory = Path(params["results_directory"])
            results_directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"Copying BEST results to {str(results_directory)}")
            keep_ext = {
                "*.log": "log",
                "*.xml": "result",
                "*.dat": "result",
                "*.gle": "graph",
                "*.mtv": "graph",
            }
            for pattern, filetype in keep_ext.items():
                for filename in working_directory.rglob(pattern):
                    destination = results_directory / filename.name
                    try:
                        copyfile(filename, destination)
                        self.record_result_individual_file(
                            {
                                "file_path": str(results_directory),
                                "file_name": filename.name,
                                "file_type": filetype,
                            }
                        )
                    except Exception:
                        logger.exception(
                            f"Error copying {filename} into the results directory {results_directory}"
                        )
        else:
            logger.debug("Result directory not specified")
        return True
