from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path

import portalocker

import dlstbx.util.symlink
from dlstbx.util.mvs.helpers import save_cropped_map
from dlstbx.util.mvs.viewer_pipedream import gen_html_pipedream
from dlstbx.wrapper import Wrapper


class PipedreamWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pipedream_xchem"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.info(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]

        # database_path = Path(params.get("database_path"))
        processed_dir = Path(params.get("processed_directory"))
        analysis_dir = Path(processed_dir / "analysis")
        pipedream_dir = analysis_dir / "pipedream"
        model_dir = pipedream_dir / "model_building"
        dtag = params.get("dtag")
        smiles = params.get("smiles")

        dataset_dir = model_dir / dtag
        out_dir = pipedream_dir / dtag

        dimple_pdb = dataset_dir / "dimple.pdb"
        dimple_mtz = dataset_dir / "dimple.mtz"
        upstream_mtz = dataset_dir / f"{dtag}.free.mtz"

        if pipeline_final_params := params.get("pipeline-final", []):
            final_directory = Path(pipeline_final_params["path"])
            final_directory.mkdir(parents=True, exist_ok=True)
            if params.get("create_symlink"):
                dlstbx.util.symlink.create_parent_symlink(
                    final_directory, params.get("create_symlink")
                )

        self.log.info(f"Processing dtag: {dtag}")

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
                f"Multiple .smiles files found in in {compound_dir}: {smiles_files}, warning for dtag {dtag}"
            )
            return False

        smiles_file = smiles_files[0]
        CompoundCode = smiles_file.stem

        # -------------------------------------------------------
        # Ligand restraint generation

        restraints_log = dataset_dir / "restraints.log"
        attachments = [restraints_log]  # synchweb attachments

        restraints_command = f"grade2 --in {smiles_file} --itype smi --out {CompoundCode} -f > {restraints_log}"

        try:
            subprocess.run(
                restraints_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=compound_dir,
                check=True,
                timeout=30 * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(
                f"Ligand restraint generation command: '{restraints_command}' failed for dataset {dtag}"
            )
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            self.send_attachments_to_ispyb(attachments, final_directory)
            return False

        restraints = compound_dir / f"{CompoundCode}.restraints.cif"
        restraints.rename(compound_dir / f"{CompoundCode}.cif")
        ligand_pdb = compound_dir / f"{CompoundCode}.xyz.pdb"
        ligand_pdb.rename(compound_dir / f"{CompoundCode}.pdb")

        ligand_cif = compound_dir / f"{CompoundCode}.cif"
        self.log.info(f"Restraints generated succesfully for dtag {dtag}")

        self.log.info(f"Removing crystallisation components from pdb file for {dtag}")
        self.process_pdb_file(dimple_pdb)
        self.log.info(f"Launching pipedream for {dtag}")

        # -------------------------------------------------------
        # Pipedream

        pipedream_log = out_dir / "pipedream.log"
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

            with open(pipedream_log, "w") as log_file:
                log_file.write(e.stdout)

            with open(out_dir / "pipedream.stderr", "w") as stderr:
                stderr.write(e.stderr)

            attachments.extend([out_dir / "pipedream.stderr"])
            self.send_attachments_to_ispyb(attachments, final_directory)
            return False

        self.log.info(f"Pipedream finished successfully for dtag {dtag}")

        # -------------------------------------------------------

        report_dir = out_dir / f"report-{CompoundCode}"
        buster_report = report_dir / "report.pdf"

        postrefine_dir = out_dir / f"postrefine-{CompoundCode}"
        refine_mtz = postrefine_dir / "refine.mtz"
        refine_pdb = postrefine_dir / "refine.pdb"

        pipedream_summary = f"{out_dir}/pipedream_summary.json"
        self.save_dataset_metadata(
            str(pipedream_dir),
            str(compound_dir),
            str(out_dir),
            CompoundCode,
            smiles,
            pipedream_command,
            dtag,
        )

        attachments.extend([buster_report, refine_mtz, refine_pdb, pipedream_summary])

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
            self.send_attachments_to_ispyb(attachments, final_directory)
            return True

        # Post-processing: Generate maps and run edstats
        map_2fofc = postrefine_dir / "refine_2fofc.map"
        map_fofc = postrefine_dir / "refine_fofc.map"

        try:
            os.system(f"gemmi sf2map --sample 5 {str(refine_mtz)} {map_2fofc} 2>&1")
            os.system(f"gemmi sf2map --sample 5 {str(refine_mtz)} {map_fofc} 2>&1")
        except Exception as e:
            self.log.debug(f"Cannot continue with pipedream postprocessing: {e}")
            self.send_attachments_to_ispyb(attachments, final_directory)
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
            self.send_attachments_to_ispyb(attachments, final_directory)
            return True

        # Run edstats if both maps exist and resolution range is found
        if not map_2fofc.exists() or not map_fofc.exists():
            self.log.debug(
                "Can't continue with pipedream postprocessing: maps not found"
            )
            self.send_attachments_to_ispyb(attachments, final_directory)
            return True

        edstats_command = f"module load ccp4; edstats XYZIN {refine_pdb} MAPIN1 {map_2fofc} MAPIN2 {map_fofc} OUT {str(postrefine_dir / 'edstats.out')}"
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
            self.send_attachments_to_ispyb(attachments, final_directory)
            return True

        self.log.info(f"Pipedream postprocessing finished successfully for dtag {dtag}")
        self.send_attachments_to_ispyb(attachments, final_directory)
        return True

    def process_pdb_file(self, dimple_pdb: Path):
        if dimple_pdb.exists():
            with open(dimple_pdb, "r", encoding="utf-8") as f:
                lines = f.readlines()

            # Count removals by component type
            original_count = len(lines)
            components_to_remove = ["DMS", "EDO", "GOL", "SO4", "PO4", "PEG"]
            removed_counts = dict.fromkeys(components_to_remove, 0)

            kept_lines = []
            for line in lines:
                if any(res in line for res in components_to_remove):
                    # Count which component was found
                    for comp in components_to_remove:
                        if comp in line:
                            removed_counts[comp] += 1
                            break
                else:
                    kept_lines.append(line)

            # Write cleaned file
            with open(dimple_pdb, "w", encoding="utf-8") as f:
                f.writelines(kept_lines)

            removed_total = original_count - len(kept_lines)
            if removed_total > 0:
                component_summary = ", ".join(
                    [
                        f"{comp}: {count}"
                        for comp, count in removed_counts.items()
                        if count > 0
                    ]
                )
                self.log.debug(f"Removed {removed_total} lines. ({component_summary})")

        else:
            self.log.debug(f"Dimple pdb {dimple_pdb} does not exist")
            return True

    def save_dataset_metadata(
        self,
        pipedream_dir,
        input_dir,
        output_dir,
        compound_code,
        smiles_string,
        pipedream_cmd,
        dtag,
    ):
        metadata = {
            "Input_dir": input_dir,
            "CompoundCode": compound_code,
            "PipedreamDirectory": output_dir,
            "ReportHTML": f"{output_dir}/report-{compound_code}/index.html",
            "LigandReportHTML": f"{output_dir}/report-{compound_code}/ligand/index.html",
            "ExpectedSummary": f"{output_dir}/pipedream_summary.json",
            "PipedreamCommand": pipedream_cmd,
            "ExpectedCIF": os.path.join(input_dir, f"{compound_code}.cif"),
            "ExpectedPDB": os.path.join(input_dir, f"{compound_code}.pdb"),
            "InputSMILES": smiles_string,
        }

        output_yaml = {}
        output_yaml[dtag] = metadata
        json_file = f"{pipedream_dir}/Pipedream_output.json"

        # Acquire a lock
        with portalocker.Lock(json_file, "a", timeout=5):
            if os.path.exists(json_file) and os.path.getsize(json_file) > 0:
                with open(json_file, "r", encoding="utf-8") as f:
                    try:
                        data = json.load(f)
                    except Exception as e:
                        self.log.debug(
                            f"Cannot continue with pipedream postprocessing: {e}"
                        )
            else:
                data = {}

            data.update(output_yaml)

            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

    def send_attachments_to_ispyb(self, attachments, final_directory):
        for f in attachments:
            if f.suffix == ".html":
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".mtz":
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".cif":
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".pdb":
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".pdf":
                file_type = "Result"
                importance_rank = 1
            elif f.suffix == ".log":
                file_type = "Log"
                importance_rank = 2
            else:
                continue
            try:
                shutil.copy(f, final_directory)
                result_dict = {
                    "file_path": str(final_directory),
                    "file_name": f.name,
                    "file_type": file_type,
                    "importance_rank": importance_rank,
                }
                self.record_result_individual_file(result_dict)
                self.log.info(f"Uploaded {f.name} as an attachment")

            except Exception:
                self.log.warning(f"Could not attach {f.name} to ISPyB", exc_info=True)

    def update_data_source(self, db_dict, dtag, database_path):
        sql = (
            "UPDATE mainTable SET "
            + ", ".join([f"{k} = :{k}" for k in db_dict])
            + f" WHERE CrystalName = '{dtag}'"
        )
        conn = sqlite3.connect(database_path)
        # conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute(sql, db_dict)
        conn.commit()

    # Integrate back with XCE via datasource
    # db_dict = {}
    # db_dict["DimplePANDDAwasRun"] = True
    # # db_dict["DimplePANDDAreject"] = False
    # db_dict["DimplePANDDApath"] = str(auto_panddas_dir / "processed_datasets")

    # try:
    #     self.update_data_source(db_dict, dtag, database_path)
    #     self.log.info(f"Updated sqlite database for dataset {dtag}")
    # except Exception as e:
    #     self.log.info(f"Could not update sqlite database for dataset {dtag}: {e}")
