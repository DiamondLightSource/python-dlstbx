from __future__ import absolute_import, division, print_function

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

        from dlstbx.ispybtbx import ispybtbx

        ispyb_conn = ispybtbx()
        edge_data = ispyb_conn.get_edge_data(dcid)
        params.update(edge_data)
        sequence = ispyb_conn.get_sequence(dcid)
        if sequence:
            seq_filename = os.path.join(working_directory, "seq_{}.fasta".format(dcid))
            from iotbx.bioinformatics import fasta_sequence

            with open(seq_filename, "w") as fp:
                fp.write(fasta_sequence(sequence).format(80))
            params["sequence"] = seq_filename

        command = [
            "{}/big_ep".format(os.environ["BIG_EP_BIN"]),
            "fast_ep={}".format(params["fast_ep_directory"]),
        ]
        for key, parameter in (
            ("data", "data"),
            ("atom_type", "atom_type"),
            ("sequence", "seq_file"),
            ("edge_position", "edge_position"),
        ):
            if key in params:
                command.append(
                    "{parameter}={value}".format(parameter=parameter, value=params[key])
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
        except IOError:
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

    def write_coot_script(self, working_directory):
        def get_map_model_from_json(json_path):

            try:
                abs_json_path = os.path.join(json_path, "big_ep_model_ispyb.json")
                with open(abs_json_path, "r") as json_file:
                    msg_json = json.load(json_file)
                return {"pdb": msg_json["pdb"], "map": msg_json["map"]}
            except Exception:
                logger.info("Couldn't read map/model data from %s", abs_json_path)

        paths = [
            p
            for p in glob.glob(os.path.join(working_directory, "*", "*", "*"))
            if os.path.isdir(p)
        ]

        autosharp_path = next(iter(filter(lambda p: "autoSHARP" in p, paths)))
        autosol_path = next(iter(filter(lambda p: "AutoSol" in p, paths)))
        crank2_path = next(iter(filter(lambda p: "crank2" in p, paths)))

        coot_script = [
            "set_map_radius(20.0)",
            "set_dynamic_map_sampling_on()",
            "set_dynamic_map_size_display_on()",
        ]

        for map_model_path in [autosharp_path, autosol_path, crank2_path]:
            try:
                f = get_map_model_from_json(map_model_path)
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
            logger.info("Failed to run sed command to update paths")

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        if "BIG_EP_BIN" not in os.environ:
            logger.error("Environment not configured to run big_ep")
            return False

        params = self.recwrap.recipe_step["job_parameters"]
        dt = datetime.now()
        params["timestamp"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        dt_stamp = dt.strftime("%Y%m%d_%H%M%S")

        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])
        ispyb_working_directory = py.path.local(params["ispyb_working_directory"])
        ispyb_results_directory = py.path.local(params["ispyb_results_directory"])

        # Create working directory with symbolic link
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            big_ep_path = ispyb_working_directory.join("..", "big_ep")
            big_ep_path.ensure(dir=True)
            os.symlink(
                ispyb_working_directory.join("big_ep").strpath,
                big_ep_path.join(dt_stamp).strpath,
            )
        # Create big_ep directory to update status in Synchweb
        if "devel" not in params and params.get("create_symlink"):
            ispyb_results_directory.ensure(dir=True)
            big_ep_path = ispyb_results_directory.join("..", "big_ep")
            big_ep_path.ensure(dir=True)

        command, bigep_script = self.construct_commandline(
            params, working_directory.strpath
        )
        str_command = " ".join(command)
        logger.info("command: %s", str_command)

        if "xml" in params:
            xml_file = working_directory.join(params["xml"])
            self.send_command_to_ispyb(params, str_command, xml_file)

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
            logger.info("Couldn't write Coot scripts to %s", working_directory.strpath)

        if "devel" not in params:
            self.copy_results(working_directory.strpath, results_directory.strpath)
            if params.get("create_symlink"):
                big_ep_path = ispyb_results_directory.join("..", "big_ep")
                os.symlink(
                    ispyb_results_directory.join("big_ep").strpath,
                    big_ep_path.join(dt_stamp).strpath,
                )
        return result["exitcode"] == 0
