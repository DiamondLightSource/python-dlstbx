from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from dlstbx.util.mvs.helpers import save_cropped_map
from dlstbx.util.mvs.viewer_pipedream import gen_html_pipedream
from dlstbx.util.pipedream_xchem_helpers import (
    process_pdb_file,
    save_dataset_metadata,
)
from dlstbx.wrapper import Wrapper


class PipedreamWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pipedream_xchem"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]

        processing_dir = Path(params.get("processing_directory"))
        auto_dir = processing_dir / "auto"
        analysis_dir = Path(auto_dir / "analysis")
        pipedream_dir = analysis_dir / "pipedream"
        model_dir = analysis_dir / "model_building"
        dtag = params.get("dtag")
        smiles = params.get("smiles")

        dataset_dir = model_dir / dtag
        out_dir = pipedream_dir / dtag

        dimple_pdb = dataset_dir / "dimple.pdb"
        dimple_mtz = dataset_dir / "dimple.mtz"
        upstream_mtz = dataset_dir / f"{dtag}.free.mtz"

        self.log.info(f"Processing dtag: {dtag}")

        compound_dir = dataset_dir / "compound"

        smiles_files = list(compound_dir.glob("*.smiles"))

        if len(smiles_files) == 0:
            self.log.error(
                f"No .smiles file present in {compound_dir}, cannot continue for dtag {dtag}"
            )
            return False
        elif len(smiles_files) > 1:
            self.log.error(
                f"Multiple .smiles files found in in {compound_dir}: {smiles_files}, warning for dtag {dtag}"
            )
            return False

        smiles_file = smiles_files[0]
        CompoundCode = smiles_file.stem

        # Restraints (grade2) were generated upstream by the ligand-restraints job.
        ligand_cif = compound_dir / f"{CompoundCode}.cif"
        attachments = []

        self.log.info(f"Removing crystallisation components from pdb file for {dtag}")
        process_pdb_file(dimple_pdb, self.log)
        self.log.info(f"Launching pipedream for {dtag}")

        # -------------------------------------------------------
        # Pipedream

        pipedream_log = out_dir / "summary.out"
        attachments.extend([pipedream_log, ligand_cif])

        pipedream_command = f"/dls_sw/apps/GPhL/BUSTER/20250717/scripts/pipedream \
            -nolmr \
            -hklin {upstream_mtz} \
            -xyzin {dimple_pdb} \
            -hklref {dimple_mtz} \
            -d {out_dir} \
            -mrefine TLSbasic,WaterUpdatePkmaps \
            -keepwater \
            -remediate \
            -sidechainrebuild \
            -runpepflip \
            -rhocommands \
            -xclusters \
            -nochirals \
            -rhofit {str(ligand_cif)}"

        try:
            subprocess.run(
                pipedream_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=pipedream_dir,
                check=True,
                timeout=params.get("timeout-minutes") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"Pipedream command: '{pipedream_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)

            with open(out_dir / "stderr.out", "w") as stderr:
                stderr.write(e.stderr)

            attachments.extend([out_dir / "stderr.out"])
            self.send_attachments_to_ispyb(attachments)
            return False

        self.log.info(f"Pipedream finished successfully for dtag {dtag}")

        # -------------------------------------------------------

        report_dir = out_dir / f"report-{CompoundCode}"
        postrefine_dir = out_dir / f"postrefine-{CompoundCode}"
        refine_mtz = postrefine_dir / "refine.mtz"
        refine_pdb = postrefine_dir / "refine.pdb"
        buster_report = report_dir / "report.pdf"

        pipedream_summary = out_dir / "pipedream_summary.json"
        save_dataset_metadata(
            str(pipedream_dir),
            str(compound_dir),
            str(out_dir),
            CompoundCode,
            smiles,
            pipedream_command,
            dtag,
            self.log,
        )

        pictures_dir = report_dir / "ligand/pictures"
        ligand_gifs = list(pictures_dir.glob("*.gif"))  # multiple

        attachments.extend(
            [buster_report, refine_mtz, refine_pdb, pipedream_summary, *ligand_gifs]
        )

        try:
            with open(pipedream_summary, "r") as f:
                data = json.load(f)
                reslo = (
                    data.get("dataprocessing", {})
                    .get("inputdata", {})
                    .get("reslo", None)
                )
                reshi = (
                    data.get("dataprocessing", {})
                    .get("inputdata", {})
                    .get("reshigh", None)
                )
        except Exception as e:
            self.log.info(f"Can't continue with pipedream postprocessing: {e}")
            self.send_attachments_to_ispyb(attachments)
            return True

        # Post-processing: Generate maps and run edstats
        map_2fofc = postrefine_dir / "refine_2fofc.map"
        map_fofc = postrefine_dir / "refine_fofc.map"

        try:
            os.system(f"gemmi sf2map --sample 5 {str(refine_mtz)} {map_2fofc} 2>&1")
            os.system(f"gemmi sf2map --sample 5 {str(refine_mtz)} {map_fofc} 2>&1")
        except Exception as e:
            self.log.debug(f"Cannot continue with pipedream postprocessing: {e}")
            self.send_attachments_to_ispyb(attachments)
            return True

        try:
            cropped_map = save_cropped_map(
                str(refine_pdb), str(map_2fofc), "LIG", radius=6
            )
            mvs_html = gen_html_pipedream(
                str(refine_pdb),
                cropped_map,
                resname="LIG",
                outdir=out_dir,
                dtag=dtag,
                smiles=smiles,
            )
            attachments.extend([mvs_html])
        except Exception as e:
            self.log.debug(f"Exception generating mvs html: {e}")

        if reslo is None or reshi is None:
            self.log.debug(
                "Can't continue with pipedream postprocessing: resolution range None"
            )
            self.send_attachments_to_ispyb(attachments)
            return True

        # Run edstats if both maps exist and resolution range is found
        if not map_2fofc.exists() or not map_fofc.exists():
            self.log.debug(
                "Can't continue with pipedream postprocessing: maps not found"
            )
            self.send_attachments_to_ispyb(attachments)
            return True

        edstats_out = postrefine_dir / "edstats.out"
        edstats_command = f"module load ccp4; edstats XYZIN {refine_pdb} MAPIN1 {map_2fofc} MAPIN2 {map_fofc} OUT {str(edstats_out)}"
        stdin_text = f"RESLO={reslo}\nRESHI={reshi}\nEND\n"

        try:
            subprocess.run(
                edstats_command,
                input=stdin_text,
                text=True,
                capture_output=True,
                check=True,
                shell=True,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"Edstats command: '{edstats_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            self.send_attachments_to_ispyb(attachments)
            return True

        self.log.info(f"Pipedream postprocessing finished successfully for dtag {dtag}")

        attachments.extend([edstats_out])
        self.send_attachments_to_ispyb(attachments)
        return True

    def send_attachments_to_ispyb(self, attachments):
        for f in attachments:
            if f.exists():
                if f.suffix == ".out":
                    file_type = "Log"
                    importance_rank = 2
                elif f.suffix == ".log":
                    file_type = "Log"
                    importance_rank = 2
                else:
                    file_type = "Result"
                    importance_rank = 1
                try:
                    result_dict = {
                        "file_path": str(f.parents[0]),
                        "file_name": f.name,
                        "file_type": file_type,
                        "importance_rank": importance_rank,
                    }
                    self.record_result_individual_file(result_dict)
                    self.log.info(f"Uploaded {f.name} as an attachment")

                except Exception:
                    self.log.warning(
                        f"Could not attach {f.name} to ISPyB", exc_info=True
                    )
