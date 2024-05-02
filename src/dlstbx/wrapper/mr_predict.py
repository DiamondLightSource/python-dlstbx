from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
from pprint import pformat
from shutil import copyfile

import procrunner

from dlstbx.util import mr_predict, mr_utils
from dlstbx.wrapper import Wrapper

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
}


class MRPredictWrapper(Wrapper):

    _logger_name = "dlstbx.wrap.mr_predict"

    def run_phaser_ellg(self, working_directory, tag, params, timeout):
        for key in (
            "hklin",
            "input_pdb",
            "seq_indent",
            "seq_file",
            "number_molecules",
            "resolution",
            "spacegroup",
        ):
            if not params.get(key):
                self.log.info(f"Cannot read {key} from MrBUMP logfile")
                return None
        phaser_script = [
            "phaser << eof\n",
            "TITLe mr_predict eLLG calculation\n",
            "MODE MR_ELLG\n",
            f"HKLIn {params['hklin']}\n",
            "LABIn F=F SIGF=SIGF\n",
            f"ENSEmble {tag} PDB {params['input_pdb']} IDENtity {params['seq_indent'] / 100.0}\n",
            f"COMPosition PROTein SEQuence {params['seq_file']} NUM {params['number_molecules']}\n",
            f"ROOT {tag}\n",
            f"RESOlution {params['resolution']}\n",
            f"SPACegroup {params['spacegroup']}\n",
            "eof",
        ]

        try:
            fp = tempfile.NamedTemporaryFile(dir=working_directory)
            sfx = Path(fp.name).stem
            phaser_ellg_script = working_directory / f"run_phaser_ellg_{sfx}.sh"
            fp.close()
            with open(phaser_ellg_script, "w") as fp:
                fp.writelines(
                    [
                        "#!/bin/bash\n",
                        ". /etc/profile.d/modules.sh\n",
                        "module purge\n",
                        "module load ccp4\n",
                    ]
                    + phaser_script,
                )
        except OSError:
            self.log.exception(
                "Could not create phaser script file in the working directory"
            )
            return False
        try:
            result = procrunner.run(
                ["sh", str(phaser_ellg_script)],
                timeout=timeout,
                raise_timeout_exception=True,
                working_directory=working_directory,
            )
            assert result.returncode == 0
            phaser_ellg_log = result.stdout.decode("latin1")
        except subprocess.TimeoutExpired:
            self.log.warning(f"Phaser eLLG script runtime exceeded timeout {timeout}")
            phaser_ellg_log = None
        except AssertionError:
            self.log.warning(
                "Process returned an error code when running Phaser eLLG script"
            )
            phaser_ellg_log = None
        except Exception:
            self.log.warning("Running Phaser eLLG script has failed")
            phaser_ellg_log = None
        finally:
            self.log.debug(result.stdout.decode("latin1"))
            self.log.debug(result.stderr.decode("latin1"))
        return phaser_ellg_log

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)

        fmt_script_path = mr_predict.__file__
        mrbump_logfile = Path(params["data"])
        output_file = Path(params["output_file"])
        metrics = mr_utils.get_mrbump_metrics(mrbump_logfile)
        for tag, model_params in metrics.items():
            phaser_ellg_log = self.run_phaser_ellg(
                working_directory, tag, model_params, params["timeout"]
            )
            try:
                model_params["results"]["eLLG"] = mr_utils.get_phaser_ellg(
                    phaser_ellg_log
                )
            except Exception:
                model_params["results"]["eLLG"] = None

        commands = []
        log_files = []
        for key, model in metrics.items():
            if not model["results"]["eLLG"]:
                continue
            try:
                fmt_metrix = " ".join(
                    [
                        f"{v:.3f}"
                        for v in (
                            model["results"]["eLLG"],
                            model["seq_indent"],
                            model["molecular_weight"],
                        )
                    ]
                )
            except Exception:
                self.log.info(
                    f"Error reading mr_predict input parameters for model {key}"
                )
                continue
            mr_logfile = Path(output_file.parent) / Path(
                output_file.stem + f"_{key}" + output_file.suffix
            )
            log_files.append(mr_logfile)
            commands.append(
                f"python {fmt_script_path} {params['classifier']} "
                f"{mr_logfile} {params['threshold']} {fmt_metrix}\n"
            )
        if not log_files:
            self.log.info("No MR log files found for running MR prediction script")
            return False
        try:
            fp = tempfile.NamedTemporaryFile(dir=working_directory)
            sfx = Path(fp.name).stem
            predict_script = working_directory / f"run_mr_predict_{sfx}.sh"
            fp.close()
            with open(predict_script, "w") as fp:
                fp.writelines(
                    [
                        "#!/bin/bash\n",
                        ". /etc/profile.d/modules.sh\n",
                        "module purge\n",
                        "module load python/3.7\n",
                    ]
                    + commands,
                )
        except OSError:
            self.log.exception(
                "Could not create mr_predict script file in the working directory"
            )
            return False
        try:
            result = procrunner.run(
                ["sh", str(predict_script)],
                timeout=params["timeout"],
                raise_timeout_exception=True,
                working_directory=working_directory,
                environment_override=clean_environment,
            )
            assert result.returncode == 0
        except subprocess.TimeoutExpired:
            self.log.exception(
                f"mr_predict script runtime exceeded timeout {params['timeout']}"
            )
            return False
        except AssertionError:
            self.log.exception(
                "Process returned an error code when running MR prediction script"
            )
            return False
        except Exception:
            self.log.exception("Running mr_predict script has failed")
            return False

        if params.get("results_directory"):
            results_directory = Path(params["results_directory"])
            results_directory.mkdir(parents=True, exist_ok=True)
            for mr_logfile in log_files:
                # Create results directory if it doesn't already exist
                mr_result = Path(mr_logfile)
                if mr_result.is_file():
                    try:
                        self.log.info(
                            f"Copying mr_predict results to {results_directory}"
                        )
                        destination = results_directory / mr_result.name
                        copyfile(mr_result, destination)
                        self.record_result_individual_file(
                            {
                                "file_path": str(results_directory),
                                "file_name": mr_result.name,
                                "file_type": "result",
                                "importance_rank": 1,
                            }
                        )
                    except Exception:
                        self.log.info(
                            f"Error copying files into the results directory {results_directory}"
                        )
                else:
                    self.log.info(f"Results file {mr_logfile} not found")
                    return False

        email_message = {
            "Classifier": params["classifier"],
            "Input data": params["data"],
            "Tag": params["program"],
            "MrBUMP": metrics,
        }
        email_message["Results"] = [
            {
                "json_file": str(mr_logfile),
                "prediction": json.loads(mr_logfile.read_text()),
            }
            for mr_logfile in log_files
        ]

        self.recwrap.send_to("mr_predict-mail", pformat(email_message))
        return True
