from __future__ import annotations

import subprocess
from pathlib import Path

from dlstbx.wrapper import Wrapper


class LigandRestraintsWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.ligandrestraints"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]
        processing_dir = Path(params.get("processing_directory"))
        dtag = params.get("dtag")

        model_dir = processing_dir / "auto" / "analysis" / "model_building"
        dataset_dir = model_dir / dtag
        compound_dir = dataset_dir / "compound"

        smiles_files = list(compound_dir.glob("*.smiles"))
        if len(smiles_files) == 0:
            self.log.error(
                f"No .smiles file present in {compound_dir}, cannot continue for dtag {dtag}"
            )
            return False
        elif len(smiles_files) > 1:
            self.log.error(
                f"Multiple .smiles files found in {compound_dir}: {smiles_files}, warning for dtag {dtag}"
            )
            return False

        smiles_file = smiles_files[0]
        CompoundCode = smiles_file.stem

        restraints_log = dataset_dir / "restraints.log"
        attachments = [restraints_log]
        restraints_command = f"grade2 --in {smiles_file} --itype smi --out {CompoundCode} -f > {restraints_log}"

        try:
            subprocess.run(
                restraints_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=compound_dir,
                check=True,
                timeout=60 * 60,
            )
        except subprocess.CalledProcessError as e:
            self.log.error(
                f"Ligand restraint generation command: '{restraints_command}' failed for dataset {dtag}"
            )
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            self.send_attachments_to_ispyb(attachments)
            return False

        (compound_dir / f"{CompoundCode}.restraints.cif").rename(
            compound_dir / f"{CompoundCode}.cif"
        )
        (compound_dir / f"{CompoundCode}.xyz.pdb").rename(
            compound_dir / f"{CompoundCode}.pdb"
        )

        self.log.info(f"Restraints generated successfully for dtag {dtag}")
        self.send_attachments_to_ispyb(attachments)
        return True

    def send_attachments_to_ispyb(self, attachments):
        for f in attachments:
            if f.exists():
                file_type = "Log" if f.suffix in (".log", ".out") else "Result"
                importance_rank = 2 if file_type == "Log" else 1
                try:
                    self.record_result_individual_file(
                        {
                            "file_path": str(f.parents[0]),
                            "file_name": f.name,
                            "file_type": file_type,
                            "importance_rank": importance_rank,
                        }
                    )
                    self.log.info(f"Uploaded {f.name} as an attachment")
                except Exception:
                    self.log.warning(
                        f"Could not attach {f.name} to ISPyB", exc_info=True
                    )
