from __future__ import absolute_import, division, print_function

import json
import logging
import os
import py

import procrunner
import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.mosflm_strategy")


class MosflmStrategyWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])
        logger.info("working_directory: %s" % working_directory.strpath)
        working_directory.ensure(dir=1)

        # Set SynchWeb to swirl
        results_directory.join("strategy_native.log").ensure()

        if params["image_pattern"].endswith(".h5"):
            self.snowflake2cbf()

        image_directory = params["image_directory"]
        image_pattern = params["image_pattern"]
        image_first = int(params["image_first"])
        image_file_name = os.path.join(image_directory, image_pattern % image_first)
        commands = ["som.strategy", image_file_name]
        space_group = params.get("spacegroup")
        if space_group is not None:
            commands.append(space_group)
        logger.info("command: %s", " ".join(commands))
        result = procrunner.run(
            commands,
            timeout=params.get("timeout", 3600),
            working_directory=working_directory.strpath,
        )
        if result["exitcode"]:
            logger.info("exitcode: %s", result["exitcode"])
            logger.info(result["stdout"])
            logger.info(result["stderr"])
        logger.info("timeout: %s", result["timeout"])
        logger.info("runtime: %s", result["runtime"])

        if working_directory.join("mosflm_index.mat").check():
            self.recwrap.send_to(
                "indexing-solution",
                {"index.mat": working_directory.join("mosflm_index.mat").strpath},
            )
        if (
            not working_directory.join("strategy_native.log").check()
            or working_directory.join("strategy_native.log").size() == 0
        ):
            result["exitcode"] = 1
            working_directory.join("strategy_native.log").write(
                "failed to determine strategy"
            )

        strategy_dat = working_directory.join("strategy.dat")
        if strategy_dat.check():
            results = self.parse_strategy_dat(strategy_dat)
            self.recwrap.send_to("strategy", results)
            # insert results into database
            self.send_screening_result_to_ispyb(params["dcid"], results)

        # copy output files to result directory
        logger.info(
            "Copying results from %s to %s"
            % (working_directory.strpath, results_directory.strpath)
        )
        for f in working_directory.listdir():
            if not f.basename.startswith("."):
                f.copy(results_directory)

        return result["exitcode"] == 0

    def parse_strategy_dat(self, strategy_dat):
        lines = strategy_dat.readlines(cr=False)
        tokens = [line.strip().split(",") for line in lines]
        logger.debug(tokens)

        return {
            "unit_cell": tokens[0][1:7],
            "space_group": tokens[0][7],
            "mosaicity": tokens[0][8],
            "strategy_native": {
                "anomalous": False,
                "comments": "MOSFLM native",
                "axisstart": tokens[1][1],
                "axisend": tokens[1][2],
                "oscillationrange": tokens[1][3],
                "noimages": tokens[1][4],
                "completeness": tokens[1][5],
                "resolution": tokens[1][6],
            },
            "strategy_anomalous": {
                "anomalous": True,
                "comments": "MOSFLM anomalous",
                "axisstart": tokens[2][1],
                "axisend": tokens[2][2],
                "oscillationrange": tokens[2][3],
                "noimages": tokens[2][4],
                "completeness": tokens[2][5],
                "resolution": tokens[2][6],
            },
        }

    def snowflake2cbf(self):
        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = py.path.local(params["working_directory"])
        tmpdir = working_directory.join("image-tmp")
        tmpdir.ensure(dir=True)
        master_h5 = os.path.join(params["image_directory"], params["image_pattern"])
        prefix = params["image_pattern"].split("master.h5")[0]
        params["image_pattern"] = prefix + "%04d.cbf"
        logger.info("Image pattern: %s", params["image_pattern"])
        logger.info(
            "Converting %s to %s" % (master_h5, tmpdir.join(params["image_pattern"]))
        )
        result = procrunner.run(
            ["dlstbx.snowflake2cbf", master_h5, params["image_pattern"]],
            working_directory=tmpdir.strpath,
            timeout=params.get("timeout", 3600),
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("timeout: %s", result["timeout"])
        logger.info("time_start: %s", result["time_start"])
        logger.info("time_end: %s", result["time_end"])
        logger.info("runtime: %s", result["runtime"])
        logger.info("exitcode: %s", result["exitcode"])
        params["orig_image_directory"] = params["image_directory"]
        params["image_directory"] = tmpdir.strpath

    def send_screening_result_to_ispyb(self, dcid, results):

        # example strategy.dat output:
        #
        # character,57.910,57.910,149.806,90.000,90.000,90.000,P4,0.55
        # mosflm native,182.0,272.0,1.2,75,1.00,1.47
        # mosflm anomalous,182.0,272.0,1.2,75,0.92,1.47

        assert dcid > 0, "Invalid data collection ID given."

        ispyb_command_list = []

        for i, strategy in enumerate(
            [results["strategy_native"], results["strategy_anomalous"]]
        ):

            # Step 1: Add new record to Screening table, keep the ScreeningId
            d = {
                "dcid": dcid,
                "programversion": "mosflm",
                "shortcomments": strategy["comments"],
                "ispyb_command": "insert_screening",
                "store_result": "ispyb_screening_id_%i" % i,
            }
            ispyb_command_list.append(d)

            # Step 2: Store screeningOutput results, linked to the screeningId
            #         Keep the screeningOutputId
            d = {
                "program": "mosflm",
                "indexingsuccess": 1,
                "strategysuccess": 1,
                "ispyb_command": "insert_screening_output",
                "screening_id": "$ispyb_screening_id_%i" % i,
                "store_result": "ispyb_screening_output_id_%i" % i,
            }
            ispyb_command_list.append(d)

            # Step 3: Store screeningOutputLattice results, linked to the screeningOutputId
            #         Keep the screeningOutputLatticeId
            d = {
                "spacegroup": results["space_group"],
                "unitcella": results["unit_cell"][0],
                "unitcellb": results["unit_cell"][1],
                "unitcellc": results["unit_cell"][2],
                "unitcellalpha": results["unit_cell"][3],
                "unitcellbeta": results["unit_cell"][4],
                "unitcellgamma": results["unit_cell"][5],
                "ispyb_command": "insert_screening_output_lattice",
                "screening_output_id": "$ispyb_screening_output_id_%i" % i,
                "store_result": "ispyb_screening_output_lattice_id_%i" % i,
            }
            ispyb_command_list.append(d)

            # Step 4: Store screeningStrategy results, linked to the screeningOutputId
            #         Keep the screeningStrategyId
            d = {
                "program": "mosflm",
                "anomalous": strategy["anomalous"],
                "ispyb_command": "insert_screening_strategy",
                "screening_output_id": "$ispyb_screening_output_id_%i" % i,
                "store_result": "ispyb_screening_strategy_id_%i" % i,
            }
            ispyb_command_list.append(d)

            # Step 5: Store screeningStrategyWedge results, linked to the screeningStrategyId
            #         Keep the screeningStrategyWedgeId
            d = {
                "wedgenumber": 1,
                "resolution": strategy["resolution"],
                "completeness": strategy["completeness"],
                "noimages": strategy["noimages"],
                "comments": strategy["comments"],
                "ispyb_command": "insert_screening_strategy_wedge",
                "screening_strategy_id": "$ispyb_screening_strategy_id_%i" % i,
                "store_result": "ispyb_screening_strategy_wedge_id_%i" % i,
            }
            ispyb_command_list.append(d)

            # Step 6: Store screeningStrategySubWedge results, linked to the screeningStrategyWedgeId
            #         Keep the screeningStrategySubWedgeId
            d = {
                "subwedgenumber": 1,
                "rotationaxis": "omega",
                "comments": strategy["comments"],
                "ispyb_command": "insert_screening_strategy_sub_wedge",
                "screening_strategy_wedge_id": "$ispyb_screening_strategy_wedge_id_%i"
                % i,
                "store_result": "ispyb_screening_strategy_sub_wedge_id_%i" % i,
            }
            for k in (
                "resolution",
                "axisstart",
                "axisend",
                "oscillationrange",
                "noimages",
                "completeness",
            ):
                d[k] = strategy[k]
            ispyb_command_list.append(d)

        logger.info("Sending %s", json.dumps(ispyb_command_list, indent=2))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))
