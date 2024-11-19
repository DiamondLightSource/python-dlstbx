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
                "DATABASE_DIR=/dls/mx-scratch/alphafold-db",
                "INSTALL_DIR=/dls_sw/apps/alphafold/alphafoldv2.3.2",
                "BIN_DIR=/dls_sw/apps/alphafold/alphafoldv2.3.2/env/bin",
                "module load mamba",
                "mamba activate /dls_sw/apps/alphafold/alphafoldv2.3.2/env",
                "python ${INSTALL_DIR}/run_alphafold.py \
                    --data_dir=$DATABASE_DIR \
                    --uniref90_database_path=$DATABASE_DIR/uniref90/uniref90.fasta \
                    --mgnify_database_path=$DATABASE_DIR/mgnify/mgy_clusters_2022_05.fa \
                    --bfd_database_path=$DATABASE_DIR/bfd/bfd_metaclust_clu_complete_id30_c90_final_seq.sorted_opt \
                    --uniref30_database_path=$DATABASE_DIR/uniref30/UniRef30_2021_03 \
                    --pdb70_database_path=$DATABASE_DIR/pdb70/pdb70 \
                    --template_mmcif_dir=$DATABASE_DIR/pdb_mmcif/mmcif_files \
                    --obsolete_pdbs_path=$DATABASE_DIR/pdb_mmcif/obsolete.dat \
                    --model_preset=monomer \
                    --max_template_date=3000-01-01 \
                    --db_preset=full_dbs \
                    --jackhmmer_binary_path=${BIN_DIR}/jackhmmer \
                    --hhsearch_binary_path=${BIN_DIR}/hhsearch \
                    --hhblits_binary_path=${BIN_DIR}/hhblits \
                    --kalign_binary_path=${BIN_DIR}/kalign \
                    --fasta_paths={seq_filename} \
                    --use_gpu_relax=TRUE",
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
