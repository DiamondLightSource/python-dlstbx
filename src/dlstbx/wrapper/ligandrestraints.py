from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from dlstbx.wrapper import Wrapper

RESTRAINTS_PROGRAMS = ("grade2", "acedrg")


class LigandRestraintsWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.ligandrestraints"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]
        analysis_dir = Path(params.get("analysis_directory"))
        dtag = params.get("dtag")
        program = str(params.get("restraints_program") or "grade2").lower()

        if program not in RESTRAINTS_PROGRAMS:
            self.log.error(
                f"Unknown restraints program {program!r} for dtag {dtag}, expected one of {RESTRAINTS_PROGRAMS}"
            )
            return False

        model_dir = analysis_dir / "model_building"
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

        if program == "acedrg":
            restraints_command = f"acedrg -i {smiles_file} -r LIG -o {CompoundCode} > {restraints_log} 2>&1"
        else:
            restraints_command = f"grade2 --in {smiles_file} --itype smi --out {CompoundCode} -f > {restraints_log}"

        self.log.info(f"Generating restraints with {program} for dtag {dtag}")

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

        if program == "grade2":
            # grade2 names its outputs slightly differently
            for suffix, target in (("restraints.cif", "cif"), ("xyz.pdb", "pdb")):
                source = compound_dir / f"{CompoundCode}.{suffix}"
                if source.exists():
                    source.rename(compound_dir / f"{CompoundCode}.{target}")
        else:
            # acedrg leaves its per-conformer tmp directory behind
            shutil.rmtree(compound_dir / f"{CompoundCode}_TMP", ignore_errors=True)

        missing = [
            f"{CompoundCode}.{extension}"
            for extension in ("cif", "pdb")
            if not (compound_dir / f"{CompoundCode}.{extension}").exists()
        ]
        if missing:
            self.log.error(
                f"{program} did not produce {', '.join(missing)} for dataset {dtag}"
            )
            self.send_attachments_to_ispyb(attachments)
            return False

        self.log.info(
            f"Restraints generated successfully with {program} for dtag {dtag}"
        )
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
