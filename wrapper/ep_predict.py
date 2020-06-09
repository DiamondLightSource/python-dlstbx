import zocalo.wrapper
import logging
import py
import json
import tempfile
import os
import procrunner
from pprint import pformat
from functools import reduce

from dlstbx.command_line import ep_predict

logger = logging.getLogger("dlstbx.wrap.ep_predict")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
}


class EPPredictWrapper(zocalo.wrapper.BaseWrapper):
    def get_xia2_meric_keys(self, params):
        return {
            "stats": {
                "loc": [
                    "_crystals",
                    params["crystal"],
                    "_scaler",
                    "_scalr_statistics",
                    f'["{params["project"]}", "{params["crystal"]}", "SAD"]',
                ],
                "keys": [
                    "Low resolution limit",
                    "Anomalous slope",
                    "Anomalous correlation",
                    "dI/s(dI)",
                    "dF/F",
                ],
            },
            "wavelength": {
                "loc": ["_crystals", params["crystal"], "_wavelengths", "SAD"],
                "keys": ["_wavelength",],
            },
        }

    def read_anomalous_metrics(self, json_file, params):
        with json_file.open() as fp:
            json_data = json.load(fp)

        metrics_data = []
        metric_keys = self.get_xia2_meric_keys(params)
        for el in ["stats", "wavelength"]:
            input_stats = reduce(
                lambda c, k: c.get(k, {}), metric_keys[el]["loc"], json_data
            )
            try:
                metrics_data.extend(
                    [input_stats.get(i, 0)[0] for i in metric_keys[el]["keys"]]
                )
            except TypeError:
                metrics_data.extend(
                    [input_stats.get(i, 0) for i in metric_keys[el]["keys"]]
                )
        try:
            metrics_data[-1] = params["energy_scan_info"]["fpp"]
        except KeyError:
            el = params["diffraction_plan_info"]["anomalousscatterer"]
            if metrics_data[-1] < 2.8:
                from cctbx.eltbx import sasaki as tbl_fpfdp
            else:
                from cctbx.eltbx import henke as tbl_fpfdp
            tbl = tbl_fpfdp.table(el)
            f = tbl.at_angstrom(metrics_data[-1])
            metrics_data[-1] = f.fdp()

        return metrics_data

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = py.path.local(params["working_directory"])
        working_directory.ensure(dir=True)

        json_file = py.path.local(params["data"])
        try:
            metrics = self.read_anomalous_metrics(json_file, params)
            fmt_metrix = " ".join(["{:.5f}".format(v) for v in metrics])
        except Exception:
            logger.exception("Error reading input parameters. Aborting.")
            return False

        fmt_script_path = ep_predict.__file__
        # Avoid preompiled module incompatibility between Python 2 & 3
        fmt_script_path = (
            fmt_script_path[:-1]
            if fmt_script_path.endswith(".pyc")
            else fmt_script_path
        )
        try:
            fp = tempfile.NamedTemporaryFile(dir=working_directory.strpath)
            predict_script = working_directory.join(
                "run_ep_predict_{}.sh".format(os.path.basename(fp.name))
            )
            fp.close()
            with predict_script.open("w") as fp:
                fp.writelines(
                    [
                        "#!/bin/bash\n",
                        ". /etc/profile.d/modules.sh\n",
                        "module purge\n",
                        "module load python/3.8\n",
                        "python {} {} {} {} {}\n".format(
                            fmt_script_path,
                            params["classifier"],
                            params["output_file"],
                            params["threshold"],
                            fmt_metrix,
                        ),
                    ]
                )
        except OSError:
            logger.exception(
                "Could not create prediction script file in the working directory"
            )
            return False
        try:
            result = procrunner.run(
                ["sh", predict_script.strpath],
                timeout=params["timeout"],
                working_directory=working_directory,
                environment_override=clean_environment,
            )
            assert result["exitcode"] == 0
            assert result["timeout"] is False
        except AssertionError:
            logger.exception(
                "Process returned an error code when running prediction script"
            )
            return False
        except Exception:
            logger.exception("Running prediction script has failed")
            return False
        # Create results directory if it doesn't already exist
        json_result = py.path.local(params["output_file"])
        if json_result.check():
            try:
                results_directory = py.path.local(params["results_directory"])
                results_directory.ensure(dir=True)
                logger.info(
                    "Copying ep_predict results to %s", results_directory.strpath
                )
                destination = results_directory.join(json_result.basename)
                json_result.copy(destination)
                self.record_result_individual_file(
                    {
                        "file_path": destination.dirname,
                        "file_name": destination.basename,
                        "file_type": "result",
                    }
                )
            except Exception:
                logger.info(
                    "Error copying files into the results directory %s",
                    results_directory.strpath,
                )
        else:
            logger.info("Results file %s not found", json_result.strpath)
            return False

        email_message = pformat(
            {
                "Results": json.loads(json_result.read()),
                "Output file": json_result.strpath,
                "Classifier": params["classifier"],
                "Input data": params["data"],
                "Diffraction plan": params["diffraction_plan_info"],
            }
        )
        self.recwrap.send_to("ep_predict-mail", email_message)
        return True
