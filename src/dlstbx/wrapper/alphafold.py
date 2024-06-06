from __future__ import annotations

import pathlib
import subprocess
from typing import List, Optional

import procrunner
import pydantic
from iotbx.bioinformatics import fasta_sequence

from dlstbx.wrapper import Wrapper


class AlphaFoldParameters(pydantic.BaseModel):
    sequence: str = pydantic.Field(..., regex="[A-Z]+")
    protein_id: int = pydantic.Field(..., gt=0)
    protein_name: str
    working_directory: pathlib.Path
    timeout: Optional[float] = pydantic.Field(None, gt=0)


class AlphaFoldWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.alphafold"

    def send_results_to_ispyb(self, pdb_files: List[pathlib.Path], protein_id: int):
        ispyb_command = {
            "ispyb_command": "insert_pdb_files",
            "protein_id": protein_id,
            "pdb_files": [str(p) for p in pdb_files],
            "source": "AlphaFold",
        }
        self.log.info("Sending %s to ISPyB", str(ispyb_command))
        self.recwrap.send_to("ispyb", ispyb_command)
        return True

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = AlphaFoldParameters(**self.recwrap.recipe_step["job_parameters"])
        working_directory = params.working_directory
        seq_filename = working_directory / f"seq_{params.protein_id}.fasta"
        seq_filename.write_text(
            fasta_sequence(params.sequence, name=params.protein_name).format(80)
        )

        stdin = "\n".join(
            [
                ". /etc/profile.d/modules.sh",
                "module load alphafold",
                f"alphafold --fasta_paths={seq_filename}",
            ]
        )

        try:
            result = procrunner.run(
                ["/bin/bash"],
                stdin=stdin.encode("latin-1"),
                timeout=params.timeout,
                raise_timeout_exception=True,
                working_directory=params.working_directory,
                print_stdout=True,
                print_stderr=True,
                environment_override={
                    "LD_LIBRARY_PATH": "",
                    "LOADEDMODULES": "",
                    "PYTHONPATH": "",
                    "_LMFILES_": "",
                },
            )
        except subprocess.TimeoutExpired as te:
            success = False
            self.log.warning(f"AlphaFold timed out: {te.timeout}\n  {te.cmd}")
            self.log.debug(te.stdout)
            self.log.debug(te.stderr)
        else:
            success = not result.returncode
            if success:
                self.log.info("AlphaFold successful")
            else:
                self.log.info(f"AlphaFold failed with exitcode {result.returncode}")
                self.log.debug(result.stdout)
                self.log.debug(result.stderr)

        subdir = params.working_directory / f"seq_{params.protein_id}"
        ranked_pdbs = sorted(subdir.glob("ranked_*.pdb"))
        if not ranked_pdbs:
            self.log.warning(f"No ranked_*.pdb files found in {subdir}")
            return False

        self.send_results_to_ispyb(ranked_pdbs, params.protein_id)

        return success
