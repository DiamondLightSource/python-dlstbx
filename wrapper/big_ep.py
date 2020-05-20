import logging
import py
from datetime import datetime

import procrunner
import zocalo.wrapper
import tempfile
import glob
import os
import json
import shutil
import subprocess
import time

logger = logging.getLogger("dlstbx.wrap.big_ep")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
}


class BigEPWrapper(zocalo.wrapper.BaseWrapper):
    def construct_commandline(self, params, working_directory):
        """Construct big_ep command line.
       Takes job parameter dictionary, returns array."""

        dcid = params["dcid"]

        if "data" not in params:
            try:
                params["data"] = params.get(
                    "ispyb_parameters", self.recwrap.environment
                )["data"]
            except KeyError:
                logger.debug("Input data file not available")

        try:
            sequence = params["protein_info"]["sequence"]
            if sequence:
                seq_filename = os.path.join(
                    working_directory, "seq_{}.fasta".format(dcid)
                )
                from iotbx.bioinformatics import fasta_sequence

                with open(seq_filename, "w") as fp:
                    fp.write(fasta_sequence(sequence).format(80))
                params["seq_file"] = seq_filename
        except Exception:
            logger.debug("Cannot read protein sequence information for dcid %s", dcid)

        try:
            params["atom_type"] = params["diffraction_plan_info"]["anomalousscatterer"]
        except Exception:
            logger.debug("Anomalous scatterer info for dcid %s not available", dcid)

        try:
            params.update(params["energy_scan_info"])
        except Exception:
            logger.debug("Energy scan data relevant for dcid %s not found", dcid)

        command = [
            "big_ep",
            "fast_ep={}".format(params["fast_ep_directory"]),
        ]
        for parameter in (
            "data",
            "atom_type",
            "seq_file",
            "edge_position",
            "qsub_project",
        ):
            if parameter in params:
                command.append(
                    "{parameter}={value}".format(
                        parameter=parameter, value=params[parameter]
                    )
                )
        try:
            command.extend(
                [
                    "{}.fp={}".format(params["edge_position"], params["fp"]),
                    "{}.fpp={}".format(params["edge_position"], params["fpp"]),
                ]
            )
        except KeyError:
            pass
        try:
            fp = tempfile.NamedTemporaryFile(dir=working_directory)
            bigep_script = os.path.join(
                working_directory, "run_bigep_{}.sh".format(os.path.basename(fp.name))
            )
            fp.close()
            with open(bigep_script, "w") as fp:
                fp.writelines(
                    [
                        "#!/bin/bash\n",
                        ". /etc/profile.d/modules.sh\n",
                        "module purge\n",
                        "module load big_ep\n",
                        " ".join(command),
                    ]
                )
        except OSError:
            logger.exception(
                "Could not create big_ep script file in the working directory"
            )
            return False

        return command, bigep_script

    def send_command_to_ispyb(self, params, bigep_command, xml_file):
        with xml_file.open("w") as fp:
            fp.writelines(
                [
                    "<PhasingContainer>",
                    "<PhasingAnalysis>",
                    "<recordTimeStamp>%s</recordTimeStamp>" % params["timestamp"],
                    "</PhasingAnalysis>",
                    "<PhasingProgramRun>",
                    "<phasingCommandLine>%s</phasingCommandLine>" % bigep_command,
                    "<phasingPrograms>big_ep</phasingPrograms>",
                    "</PhasingProgramRun>",
                    "</PhasingContainer>",
                ]
            )

        command = [
            "python",
            "/dls_sw/apps/mx-scripts/dbserver/src/phasing2ispyb.py",
            "-s",
            "sci-serv3",
            "-p",
            "2611",
            "-i",
            xml_file.strpath,
            "-f",
            params["data"],
        ]

        logger.info("Running command: %s", " ".join(command))
        result = procrunner.run(
            command,
            timeout=params.get("timeout"),
            print_stdout=True,
            print_stderr=True,
            working_directory=params["working_directory"],
            environment_override=clean_environment,
        )
        logger.info(
            "phasing2ispyb terminated after %.1f seconds with exitcode %s and timeout %s",
            result["runtime"],
            result["exitcode"],
            result["timeout"],
        )
        success = not result["exitcode"] and not result["timeout"]
        return success

    def get_map_model_from_json(self, json_path):
        try:
            abs_json_path = os.path.join(json_path, "big_ep_model_ispyb.json")
            result = {"json": abs_json_path}
            with open(abs_json_path, "r") as json_file:
                msg_json = json.load(json_file)
            result.update({k: msg_json[k] for k in ["pdb", "map", "mtz"]})
            return result
        except Exception:
            logger.debug(
                "Couldn't read map/model data from %s", abs_json_path, exc_info=True
            )

    def get_pipeline_paths(self, working_directory):
        paths = [
            p
            for p in glob.glob(os.path.join(working_directory, "*", "*", "*"))
            if os.path.isdir(p)
        ]

        autosharp_path = next(iter(filter(lambda p: "autoSHARP" in p, paths)))
        autosol_path = next(iter(filter(lambda p: "AutoSol" in p, paths)))
        crank2_path = next(iter(filter(lambda p: "crank2" in p, paths)))

        return [autosharp_path, autosol_path, crank2_path]

    def write_coot_script(self, working_directory):
        coot_script = [
            "set_map_radius(20.0)",
            "set_dynamic_map_sampling_on()",
            "set_dynamic_map_size_display_on()",
        ]

        for map_model_path in self.get_pipeline_paths(working_directory):
            try:
                f = self.get_map_model_from_json(map_model_path)
                if os.path.isfile(f["pdb"]):
                    coot_script.append(
                        'read_pdb("{}")'.format(
                            os.path.relpath(f["pdb"], working_directory)
                        )
                    )
                if os.path.isfile(f["map"]):
                    coot_script.append(
                        'handle_read_ccp4_map("{}", 0)'.format(
                            os.path.relpath(f["map"], working_directory)
                        )
                    )
            except Exception:
                continue
        with open(os.path.join(working_directory, "models.py"), "wt") as fp:
            fp.write(os.linesep.join(coot_script))
        with open(os.path.join(working_directory, "big_ep_coot.sh"), "wt") as fp:
            fp.write(
                os.linesep.join(
                    [
                        "#!/bin/sh",
                        "module purge",
                        "module load ccp4",
                        "coot --python models.py --no-guano",
                    ]
                )
            )

    def copy_results(self, working_directory, results_directory):
        def ignore_func(directory, files):
            ignore_list = [".launch", ".recipewrap"]
            pth = py.path.local(directory)
            for f in files:
                fp = pth.join(f)
                if not fp.check():
                    ignore_list.append(f)
            return ignore_list

        shutil.copytree(working_directory, results_directory, ignore=ignore_func)
        src_pth_esc = r"\/".join(working_directory.split(os.sep))
        dest_pth_esc = r"\/".join(results_directory.split(os.sep))
        sed_command = (
            r"find %s -type f -exec grep -Iq . {} \; -and -exec sed -i 's/%s/%s/g' {} +"
            % (results_directory, src_pth_esc, dest_pth_esc)
        )
        try:
            subprocess.call([sed_command], shell=True)
        except Exception:
            logger.debug("Failed to run sed command to update paths", exc_info=True)

    def send_results_to_ispyb(self, results_directory):
        result = False
        pipeline_paths = self.get_pipeline_paths(results_directory)
        for map_model_path in pipeline_paths:
            try:
                f = self.get_map_model_from_json(map_model_path)
                for fp in f.values():
                    if os.path.isfile(fp):
                        self.record_result_individual_file(
                            {
                                "file_path": os.path.dirname(fp),
                                "file_name": os.path.basename(fp),
                                "file_type": "Result",
                            }
                        )
                        result = True
            except Exception:
                continue

        js_settings = next(
            iter(
                glob.glob(
                    os.path.join(results_directory, "*", "*", "big_ep_settings.json")
                )
            )
        )
        self.record_result_individual_file(
            {
                "file_path": os.path.dirname(js_settings),
                "file_name": os.path.basename(js_settings),
                "file_type": "result",
            }
        )

        log_files = ["LISTautoSHARP.html", "phenix_autobuild.log", "crank2.log"]
        for pth, fp in zip(pipeline_paths, log_files):
            self.record_result_individual_file(
                {"file_path": pth, "file_name": fp, "file_type": "log"}
            )
        return result

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        self.recwrap.environment.update(params["ispyb_parameters"])

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])
        ispyb_working_directory = py.path.local(params["ispyb_working_directory"])
        ispyb_results_directory = py.path.local(params["ispyb_results_directory"])

        # Create working directory with symbolic link
        dt = datetime.now()
        dt_stamp = dt.strftime("%Y%m%d_%H%M%S")
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            big_ep_path = ispyb_working_directory.join("..", "big_ep")
            big_ep_path.ensure(dir=True)
            while True:
                try:
                    symlink_path = big_ep_path.join(dt_stamp)
                    symlink_path.mksymlinkto(ispyb_working_directory.join("big_ep"))
                    break
                except py.error.EEXIST:
                    logger.debug("Symlink %s already exists", symlink_path.strpath)
                    time.sleep(1)
                    dt = datetime.now()
                    dt_stamp = dt.strftime("%Y%m%d_%H%M%S")

        # Create big_ep directory to update status in Synchweb
        if "devel" not in params:
            ispyb_results_directory.ensure(dir=True)
            big_ep_path = ispyb_results_directory.join("..", "big_ep")
            big_ep_path.ensure(dir=True)
            if params.get("create_symlink"):
                symlink_path = big_ep_path.join(dt_stamp)
                try:
                    symlink_path.mksymlinkto(ispyb_results_directory.join("big_ep"))
                except py.error.EEXIST:
                    logger.debug("Symlink %s already exists", symlink_path.strpath)

        params["timestamp"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        command, bigep_script = self.construct_commandline(
            params, working_directory.strpath
        )
        str_command = " ".join(command)
        logger.info("command: %s", str_command)

        if "xml" in params:
            xml_file = working_directory.join(params["xml"])
            result = self.send_command_to_ispyb(params, str_command, xml_file)
            if not result:
                logger.error(
                    "Running phasing2ispyb.py script returned non-zero exit code"
                )

        result = procrunner.run(
            ["sh", bigep_script],
            timeout=params.get("timeout"),
            working_directory=working_directory,
        )
        if result["exitcode"] or result["timeout"]:
            logger.info("timeout: %s", result["timeout"])
            logger.info("exitcode: %s", result["exitcode"])
            logger.debug(result["stdout"])
            logger.debug(result["stderr"])
        logger.info("runtime: %s", result["runtime"])

        try:
            self.write_coot_script(working_directory.strpath)
        except Exception:
            logger.debug(
                "Couldn't write Coot scripts to %s",
                working_directory.strpath,
                exc_info=True,
            )

        if "devel" in params:
            return result["exitcode"] == 0
        else:
            self.copy_results(working_directory.strpath, results_directory.strpath)
            return self.send_results_to_ispyb(results_directory.strpath)
