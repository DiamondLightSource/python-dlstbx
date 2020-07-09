import zocalo.wrapper
import logging
import json
import tempfile
import procrunner
from pprint import pformat

from dlstbx.command_line import mr_predict
from pathlib import Path
from shutil import copyfile

logger = logging.getLogger("dlstbx.wrap.mr_predict")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
}


class MRPredictWrapper(zocalo.wrapper.BaseWrapper):
    def get_mrbump_metrics(self, mrbump_logfile):
        mrbump_log = [l for l in mrbump_logfile.read_text().split("\n")]
        for line in mrbump_log:
            if "Molecular Weight (daltons)" in line:
                mw = float(line.split(":")[-1])
                break
        iter_log = iter(mrbump_log)
        models = {}
        for line in iter_log:
            if "Template Model" == line[:14]:
                model_label = line.split(":")[-1].strip()
                for next_line in iter_log:
                    if "Estimated sequence identity" in next_line:
                        seq_ident = float(next_line.split(":")[-1]) * 100.0
                        models[model_label] = {
                            "molecular_weight": mw,
                            "seq_indent": seq_ident,
                        }
                        break
        iter_log = iter(mrbump_log)
        for line in iter_log:
            if "MrBUMP Summary" in line:
                for final_line in iter_log:
                    if "Phaser_LLG" in final_line and "Model_Name" in final_line:
                        labels = [v for v in final_line.split(" ") if v][:7]
                        for next_line in iter_log:
                            try:
                                final_values = [v for v in next_line.split(" ") if v]
                                (model_name, mr_program, solution_type) = tuple(
                                    final_values[:3]
                                )
                                (
                                    phaser_llg,
                                    phaser_tfg,
                                    final_rfact,
                                    final_rfree,
                                ) = tuple(float(v) for v in final_values[3:7])
                                model_name = model_name.strip()
                                models[model_name]["results"] = dict(
                                    zip(
                                        labels,
                                        (
                                            model_name,
                                            mr_program,
                                            solution_type,
                                            phaser_llg,
                                            phaser_tfg,
                                            final_rfact,
                                            final_rfree,
                                        ),
                                    )
                                )
                            except ValueError:
                                break
        return models

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)

        fmt_script_path = mr_predict.__file__
        # Avoid preompiled module incompatibility between Python 2 & 3
        fmt_script_path = (
            fmt_script_path[:-1]
            if fmt_script_path.endswith(".pyc")
            else fmt_script_path
        )
        mrbump_logfile = Path(params["data"])
        output_file = Path(params["output_file"])
        metrics = self.get_mrbump_metrics(mrbump_logfile)
        commands = []
        log_files = []
        for key, model in metrics.items():
            try:
                fmt_metrix = " ".join(
                    [
                        "{:.3f}".format(v)
                        for v in (
                            model["results"]["Phaser_LLG"],
                            model["seq_indent"],
                            model["molecular_weight"],
                        )
                    ]
                )
            except Exception:
                logger.exception("Error reading mr_predict input parameters. Aborting.")
                return False
            mr_logfile = Path(output_file.parent) / Path(
                output_file.stem + f"_{key}" + output_file.suffix
            )
            log_files.append(mr_logfile)
            commands.append(
                f"python {fmt_script_path} {params['classifier']} "
                f"{mr_logfile} {params['threshold']} {fmt_metrix}\n"
            )
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
            logger.exception(
                "Could not create mr_predict script file in the working directory"
            )
            return False
        try:
            result = procrunner.run(
                ["sh", str(predict_script)],
                timeout=params["timeout"],
                working_directory=working_directory,
                environment_override=clean_environment,
            )
            assert result["exitcode"] == 0
            assert result["timeout"] is False
        except AssertionError:
            logger.exception(
                "Process returned an error code when running MR prediction script"
            )
            return False
        except Exception:
            logger.exception("Running mr_predict script has failed")
            return False

        if params.get("results_directory"):
            results_directory = Path(params["results_directory"])
            results_directory.mkdir(parents=True, exist_ok=True)
            for mr_logfile in log_files:
                # Create results directory if it doesn't already exist
                mr_result = Path(mr_logfile)
                if mr_result.is_file():
                    try:
                        logger.info(
                            f"Copying mr_predict results to {results_directory}"
                        )
                        destination = results_directory / mr_result.name
                        copyfile(mr_result, destination)
                        self.record_result_individual_file(
                            {
                                "file_path": str(results_directory),
                                "file_name": mr_result.name,
                                "file_type": "result",
                            }
                        )
                    except Exception:
                        logger.info(
                            f"Error copying files into the results directory {results_directory}"
                        )
                else:
                    logger.info(f"Results file {mr_logfile} not found")
                    return False

        email_message = {
            "Classifier": params["classifier"],
            "Input data": params["data"],
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
