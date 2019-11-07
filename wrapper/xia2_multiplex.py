from __future__ import absolute_import, division, print_function

import logging
import itertools
import py

import dlstbx.util.symlink
import procrunner
import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.xia2.multiplex")


class Xia2MultiplexWrapper(zocalo.wrapper.BaseWrapper):
    def send_results_to_ispyb(self, z):
        ispyb_command_list = []

        # Step 1: Add new record to AutoProc, keep the AutoProcID
        register_autoproc = {
            "ispyb_command": "write_autoproc",
            "autoproc_id": None,
            "store_result": "ispyb_autoproc_id",
            "spacegroup": z["spacegroup"],
            "refinedcell_a": z["unit_cell"][0],
            "refinedcell_b": z["unit_cell"][1],
            "refinedcell_c": z["unit_cell"][2],
            "refinedcell_alpha": z["unit_cell"][3],
            "refinedcell_beta": z["unit_cell"][4],
            "refinedcell_gamma": z["unit_cell"][5],
        }
        ispyb_command_list.append(register_autoproc)

        # Step 2: Store scaling results, linked to the AutoProcID
        #         Keep the AutoProcScalingID
        insert_scaling = z["scaling_statistics"]
        insert_scaling.update(
            {
                "ispyb_command": "insert_scaling",
                "autoproc_id": "$ispyb_autoproc_id",
                "store_result": "ispyb_autoprocscaling_id",
            }
        )
        ispyb_command_list.append(insert_scaling)

        # Step 3: Store integration result, linked to the ScalingID
        integration = {
            "ispyb_command": "upsert_integration",
            "scaling_id": "$ispyb_autoprocscaling_id",
            "cell_a": z["unit_cell"][0],
            "cell_b": z["unit_cell"][1],
            "cell_c": z["unit_cell"][2],
            "cell_alpha": z["unit_cell"][3],
            "cell_beta": z["unit_cell"][4],
            "cell_gamma": z["unit_cell"][5],
            #'refined_xbeam': z['refined_beam'][0],
            #'refined_ybeam': z['refined_beam'][1],
        }
        ispyb_command_list.append(integration)

        logger.info("Sending %s", str(ispyb_command_list))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

    def construct_commandline(self, params):
        """Construct xia2.multiplex command line.
       Takes job parameter dictionary, returns array."""

        command = ["xia2.multiplex"]

        appids = params["appids"]

        import ispyb

        with ispyb.open(
            "/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg"
        ) as ispyb_conn:
            data_files = itertools.chain.from_iterable(
                self.get_data_files_for_appid(appid, ispyb_conn)
                for appid in appids
                if appid is not None
            )
            for f in data_files:
                command.append(f.strpath)

        return command

    def get_data_files_for_appid(self, appid, ispyb_conn):
        data_files = []
        logger.info("Retrieving program attachment for appid %s", appid)
        attachments = ispyb_conn.mx_processing.retrieve_program_attachments_for_program_id(
            appid
        )
        for item in attachments:
            if item["fileType"] == "Result":
                if (
                    item["fileName"].endswith(
                        ("experiments.json", "reflections.pickle", ".expt", ".refl")
                    )
                    and "_scaled." not in item["fileName"]
                ):
                    data_files.append(
                        py.path.local(item["filePath"]).join(item["fileName"])
                    )
        logger.info("Found the following files for appid %s:", appid)
        logger.info(list(data_files))
        if len(data_files) != 2:
            logger.warning(
                "Expected to find exactly 2 data files for appid %s (found %s)",
                appid,
                len(data_files),
            )
            return []
        return data_files

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        # Adjust all paths if a spacegroup is set in ISPyB
        if params.get("ispyb_parameters"):
            if (
                params["ispyb_parameters"].get("spacegroup")
                and "/" not in params["ispyb_parameters"]["spacegroup"]
            ):
                for parameter in (
                    "working_directory",
                    "results_directory",
                    "create_symlink",
                ):
                    if parameter in params:
                        params[parameter] += (
                            "-" + params["ispyb_parameters"]["spacegroup"]
                        )
            if params["ispyb_parameters"].get("appids"):
                params["appids"] = params["ispyb_parameters"]["appids"].split(",")

        assert len(params.get("appids", [])) > 1

        command = self.construct_commandline(params)

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])

        # Create working directory with symbolic link
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory.strpath, params["create_symlink"]
            )

        # run xia2.multiplex in working directory

        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            working_directory=working_directory.strpath,
        )

        logger.info("command: %s", " ".join(result["command"]))
        logger.info("timeout: %s", result["timeout"])
        logger.info("time_start: %s", result["time_start"])
        logger.info("time_end: %s", result["time_end"])
        logger.info("runtime: %s", result["runtime"])
        logger.info("exitcode: %s", result["exitcode"])
        logger.debug(result["stdout"])
        logger.debug(result["stderr"])

        json_file = working_directory.join("iotbx-merging-stats.json")
        scaled_unmerged_mtz = working_directory.join("scaled_unmerged.mtz")
        if scaled_unmerged_mtz.check():

            import iotbx.merging_statistics

            i_obs = iotbx.merging_statistics.select_data(
                str(scaled_unmerged_mtz.strpath), data_labels=None
            )

            def get_merging_statistics(i_obs):
                result = iotbx.merging_statistics.dataset_statistics(
                    i_obs=i_obs,
                    n_bins=20,
                    anomalous=False,
                    use_internal_variance=False,
                    eliminate_sys_absent=False,
                    assert_is_not_unique_set_under_symmetry=False,
                )
                return result

            merging_stats = get_merging_statistics(
                i_obs.customized_copy(anomalous_flag=False)
            )
            anom_merging_stats = get_merging_statistics(
                i_obs.customized_copy(anomalous_flag=True)
            )

            with json_file.open("wb") as fh:
                fh.write(merging_stats.as_json())

            ispyb_d = {
                "commandline": " ".join(result["command"]),
                "spacegroup": i_obs.space_group().type().lookup_symbol(),
                "unit_cell": list(i_obs.unit_cell().parameters()),
                "scaling_statistics": {},
            }
            for stats, bin in (
                ("overall", merging_stats.overall),
                ("innerShell", merging_stats.bins[0]),
                ("outerShell", merging_stats.bins[-1]),
            ):
                ispyb_d["scaling_statistics"][stats] = {
                    "anom_completeness": bin.anom_completeness,
                    "anom_multiplicity": bin.mean_redundancy,
                    "cc_anom": bin.cc_anom,
                    "cc_half": bin.cc_one_half,
                    "completeness": bin.completeness,
                    "mean_i_sig_i": bin.i_over_sigma_mean,
                    "multiplicity": bin.mean_redundancy,
                    "n_tot_obs": bin.n_obs,
                    "n_tot_unique_obs": bin.n_uniq,
                    "r_meas_all_iplusi_minus": bin.r_meas,
                    "r_merge": bin.r_merge,
                    "res_lim_high": bin.d_min,
                    "res_lim_low": bin.d_max,
                }
            for stats, bin in (
                ("overall", anom_merging_stats.overall),
                ("innerShell", anom_merging_stats.bins[0]),
                ("outerShell", anom_merging_stats.bins[-1]),
            ):
                ispyb_d["scaling_statistics"][stats].update(
                    {
                        "anom_completeness": bin.anom_completeness,
                        "anom_multiplicity": bin.mean_redundancy,
                        "cc_anom": bin.cc_anom,
                        "r_meas_all_iplusi_minus": bin.r_meas,
                    }
                )
            self.send_results_to_ispyb(ispyb_d)

        # copy output files to result directory
        results_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                results_directory.strpath, params["create_symlink"]
            )

        keep_ext = {
            ".png": None,
            ".log": "log",
            ".json": None,
            ".pickle": None,
            ".expt": None,
            ".refl": None,
            ".mtz": None,
            ".html": "log",
        }
        keep = {
            "scaled.mtz": "result",
            "scaled_unmerged.mtz": "result",
            "multiplex.expt": "result",
            "multiplex.refl": "result",
            "iotbx-merging-stats.json": "graph",
        }

        # Record these log files first so they appear at the top of the list
        # of attachments in SynchWeb
        primary_log_files = [
            working_directory.join("xia2.multiplex.html"),
            working_directory.join("xia2.multiplex.log"),
        ]

        allfiles = []
        for filename in primary_log_files + working_directory.listdir():
            filetype = keep_ext.get(filename.ext)
            if filename.basename in keep:
                filetype = keep[filename.basename]
            if filetype is None:
                continue
            destination = results_directory.join(filename.basename)
            if destination.strpath in allfiles:
                # We've already seen this file above
                continue
            logger.debug("Copying %s to %s" % (filename.strpath, destination.strpath))
            allfiles.append(destination.strpath)
            filename.copy(destination)
            if filetype:
                self.record_result_individual_file(
                    {
                        "file_path": destination.dirname,
                        "file_name": destination.basename,
                        "file_type": filetype,
                    }
                )
        if allfiles:
            self.record_result_all_files({"filelist": allfiles})

        return result["exitcode"] == 0
