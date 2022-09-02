from __future__ import annotations

import json
from pprint import pformat

import py
import xmltodict

import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper


class FastEPReportWrapper(Wrapper):

    _logger_name = "zocalo.wrap.fast_ep_report"

    def send_results_to_ispyb(self, xml_file):
        params = self.recwrap.recipe_step["job_parameters"]

        scaling_id = params.get("ispyb_parameters", params).get("scaling_id", None)
        if not str(scaling_id).isdigit():
            self.log.error(
                f"Can not write results to ISPyB: no scaling ID set ({scaling_id})"
            )
            return False
        scaling_id = int(scaling_id)
        self.log.info(
            f"Inserting fast_ep phasing results from {xml_file} into ISPyB for scaling_id {scaling_id}"
        )

        with open(xml_file) as fh:
            phasing_results = xmltodict.parse(fh.read())

        self.log.info(
            f"Sending {phasing_results} phasing results commands to ISPyB for scaling_id {scaling_id}"
        )
        self.recwrap.send_to(
            "ispyb",
            {
                "phasing_results": phasing_results,
                "scaling_id": scaling_id,
            },
        )
        return True

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = py.path.local(params["working_directory"])
        try:
            results_directory = py.path.local(params["results_directory"])
        except KeyError:
            self.log.info("Results directory not specified")

        # Create working directory with symbolic link
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory.strpath, params["create_symlink"]
            )

        # Send results to topaz for hand determination
        fast_ep_data_json = working_directory.join("fast_ep_data.json")
        if fast_ep_data_json.check():
            with fast_ep_data_json.open("r") as fp:
                fast_ep_data = json.load(fp)
            with working_directory.join("fast_ep.log").open("r") as fp:
                for line in fp:
                    if "Unit cell:" in line:
                        cell_info = tuple(float(v) for v in line.split()[2:])
                        break
            best_sg = fast_ep_data["_spacegroup"][0]
            best_solv = "{0:.2f}".format(fast_ep_data["solv"])
            original_hand = working_directory.join(best_solv, "sad.phs")
            inverted_hand = working_directory.join(best_solv, "sad_i.phs")
            hkl_data = working_directory.join(best_solv, "sad.hkl")
            fa_data = working_directory.join(best_solv, "sad_fa.hkl")
            res_data = working_directory.join(best_solv, "sad_fa.res")
            topaz_data = {
                "original_phase_file": original_hand.strpath,
                "inverse_phase_file": inverted_hand.strpath,
                "hkl_file": hkl_data.strpath,
                "fa_file": fa_data.strpath,
                "res_file": res_data.strpath,
                "space_group": best_sg,
                "cell_info": cell_info,
                "best_solvent": best_solv,
            }
            self.log.info("Topaz data: %s", pformat(topaz_data))
            self.recwrap.send_to("topaz", topaz_data)
        else:
            self.log.warning(
                "fast_ep failed. Results file %s unavailable", fast_ep_data_json.strpath
            )
            return False

        # Create results directory and symlink if they don't already exist
        try:
            results_directory.ensure(dir=True)
            if params.get("create_symlink"):
                dlstbx.util.symlink.create_parent_symlink(
                    results_directory.strpath, params["create_symlink"]
                )

            self.log.info("Copying fast_ep results to %s", results_directory.strpath)
            keep_ext = {
                ".cif": "result",
                ".error": "log",
                ".hkl": "result",
                ".html": "log",
                ".ins": "result",
                ".json": "result",
                ".lst": "log",
                ".mtz": "result",
                ".pdb": "result",
                ".png": None,
                ".sca": "result",
                ".sh": None,
                ".xml": False,
            }
            keep = {"fast_ep.log": "log", "shelxc.log": "log"}
            allfiles = []
            for filename in working_directory.listdir():
                filetype = keep_ext.get(filename.ext)
                if filename.basename in keep:
                    filetype = keep[filename.basename]
                if filetype is None:
                    continue
                destination = results_directory.join(filename.basename)
                filename.copy(destination)
                allfiles.append(destination.strpath)
                if filetype:
                    self.record_result_individual_file(
                        {
                            "file_path": destination.dirname,
                            "file_name": destination.basename,
                            "file_type": filetype,
                        }
                    )

            if "xml" in params["fast_ep"]:
                xml_file = working_directory.join(params["fast_ep"]["xml"])
                if xml_file.check():
                    xml_data = working_directory.join(params["fast_ep"]["xml"]).read()
                    self.log.info("Sending fast_ep phasing results to ISPyB")
                    xml_file.write(
                        xml_data.replace(
                            working_directory.strpath, results_directory.strpath
                        )
                    )
                    result_ispyb = self.send_results_to_ispyb(xml_file.strpath)
                    if not result_ispyb:
                        self.log.error(
                            "Running phasing2ispyb.py script returned non-zero exit code"
                        )
                else:
                    self.log.info(
                        "fast_ep failed, no .xml output, thus not reporting to ISPyB"
                    )
                    return False
        except NameError:
            self.log.info(
                "Copying fast_ep results ignored. Results directory unavailable."
            )

        return True
