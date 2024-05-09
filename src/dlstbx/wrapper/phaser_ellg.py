"""
Example of a basic wrapper script
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from pprint import pformat

import procrunner

from dlstbx.util import mr_utils
from dlstbx.wrapper import Wrapper


class PhasereLLGWrapper(Wrapper):

    _logger_name = "dlstbx.wrap.phaser_ellg"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)

        mrbump_logfile = Path(params["data"])
        metrics = mr_utils.get_mrbump_metrics(mrbump_logfile)
        self.log.info(pformat(metrics))

        for tag, model_params in metrics.items():
            phaser_script = [
                "phaser << eof\n",
                "TITLe mr_predict eLLG calculation\n",
                "MODE MR_ELLG\n",
                f"HKLIn {model_params['hklin']}\n",
                "LABIn F=F SIGF=SIGF\n",
                f"ENSEmble {tag} PDB {model_params['input_pdb']} IDENtity {model_params['seq_indent'] / 100.0}\n",
                f"COMPosition PROTein SEQuence {model_params['seq_file']} NUM {model_params['number_molecules']}\n",
                f"ROOT {tag}\n",  # not the default
                f"RESOlution {model_params['resolution']}\n",
                f"SPACegroup {model_params['spacegroup']}\n",
                "eof",
            ]

            try:
                fp = tempfile.NamedTemporaryFile(dir=working_directory)
                sfx = Path(fp.name).stem
                phaser_ellg_script = (
                    working_directory / f"run_phaser_ellgmr_predict_{sfx}.sh"
                )
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
                    timeout=params["timeout"],
                    working_directory=working_directory,
                )
                assert result["exitcode"] == 0
                assert result["timeout"] is False
            except AssertionError:
                self.log.exception(
                    "Process returned an error code when running Phaser eLLG script"
                )
                return False
            except Exception:
                self.log.exception("Running Phaser eLLG script has failed")
                return False
        return True
