from __future__ import annotations

import os
import pathlib
import shutil
import subprocess

import pandas as pd

from dlstbx.wrapper import Wrapper


class PanDDAWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pandda"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.debug(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]

        head_dir = pathlib.Path(params["head_directory"])
        processing_dir = head_dir / "processing"
        table_dir = pathlib.Path(params["table_directory"])
        res_limit = params["res_limit"]
        completeness_limit = params["completeness_limit"]

        df_echo = self.make_dispensing_df(processing_dir, table_dir)
        df_ispyb = pd.read_csv(processing_dir / "ispyb.csv")
        dfmerged = pd.merge(df_echo, df_ispyb, how="outer", on=["barcode", "location"])

        # lost_frags = dfmerged[dfmerged["filePath"].isna()]
        # entries with smiles but no dcid, record these, also dcid but no smiles

        acronyms = dfmerged[dfmerged["acronym"].notna()]["acronym"].unique()
        # allow for multiple protein targets?

        # filter multiplex data based on resolution and completeness limits & select best multiplex dataset
        df_final = dfmerged[
            (dfmerged["resolutionLimitHigh"] <= res_limit)
            & (dfmerged["completeness"] >= completeness_limit)
        ]

        # one entry per ligand
        df_final = (
            df_final.sort_values("completeness", ascending=False)
            .drop_duplicates("dataCollectionId")  # dataCollectionId, Smiles
            .sort_index()
        )

        outpath = head_dir / "processing/analysis/datasets_for_pandda.csv"
        df_final.to_csv(outpath)  # save df

        for j in range(len(acronyms)):
            if (
                len(df_final[df_final["acronym"] == acronyms[j]])
                < params["min_datasets"]
            ):
                df_final = df_final[df_final["acronym"] != acronyms[j]]
                self.log.info(
                    f"Aborting PanDDA processing for {acronyms[j]}. Insufficient number of datsets"
                )

        # create the directory structure required for PanDDA analysis
        for index, row in df_final.iterrows():
            well, acr, library, source_well, vn = (
                row["location"],
                row["acronym"],
                row["Library Barcode"],
                row["Source Well"],
                int(row["visit_number"]),
            )
            multiplex_path = pathlib.Path(str(row["filePath"]))
            well_dir = (
                processing_dir
                / "analysis"
                / f"model_building_{acr}"
                / f"{acr}-{vn}-x{well}"  # label by vn & well
            )
            compound_dir = well_dir / "compound"
            pathlib.Path(compound_dir).mkdir(parents=True, exist_ok=True)
            shutil.copyfile(
                multiplex_path / "dimple/final.pdb",
                well_dir / f"{acr}-{vn}-x{well}.dimple.pdb",
            )
            shutil.copyfile(
                multiplex_path / "dimple/final.mtz",
                well_dir / f"{acr}-{vn}-x{well}.dimple.mtz",
            )

            cif_dir = (
                table_dir / "pdb_cif" / f"{library}"
            )  # central pdb_cif dir for fragment library
            pdb = cif_dir / f"{source_well}" / "ligand.xyz.pdb"
            cif = cif_dir / f"{source_well}" / "ligand.restraints.cif"

            if pdb.exists() and cif.exists():
                shutil.copyfile(pdb, well_dir / "ligand.pdb")
                shutil.copyfile(cif, well_dir / "ligand.cif")
                # shutil.copyfile(pdb, compound_dir / "ligand.pdb")
                # shutil.copyfile(cif, compound_dir / "ligand.cif")
            else:
                self.log.info(
                    f"No ligand pdb/cif file found for well {well}, ligand library {library}, skipping..."
                )  # or subprocess acedrg here to create ligand files? acedrg -i {well_dir}/lig.smi -o {well_dir}/lig

        for acr in acronyms:
            pandda_command = f"module load ccp4/7.0.078; \
             module load pymol/1.8.2.0-py2.7; \
             pandda.analyse data_dirs={processing_dir}/'analysis/model_building_{acr}/*' pdb_style='*.dimple.pdb' out_dir={processing_dir}/'analysis/pandda_{acr}' cpus=36 low_resolution_completeness=none"

            # pandda2_command = f"source /dls/science/groups/i04-1/software/pandda_2_gemmi/act_experimental; \
            #  conda activate pandda2_ray; \
            #  python -u /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/scripts/pandda.py --local_cpus=36 --data_dirs={processing_dir}/'analysis/model_building_{acr}' --out_dir={processing_dir}/'analysis/pandda2_{acr}'  > pandda2.log "

            try:
                result = subprocess.run(
                    pandda_command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    cwd=processing_dir,
                    check=True,
                    timeout=params.get("timeout") * 60,
                )

            except subprocess.CalledProcessError as e:
                self.log.error(f"PanDDA process '{pandda_command}' failed")
                self.log.info(e.stdout)
                self.log.error(e.stderr)
                return False

        with open(processing_dir / "pandda.log", "w") as log_file:
            log_file.write(result.stdout)

        self.log.info("Sending results to ISPyB")
        self.send_attachments_to_ispyb(processing_dir, acronyms)

        self.log.info("PanDDA script finished")
        return True

    def send_attachments_to_ispyb(self, processing_dir, acronymns):  # change
        for acr in acronymns:
            html_dir = (
                processing_dir / f"analysis/pandda_{acr}/analyses/html_summaries"
            )  # pandda
            # log_dir = processing_dir / f"analysis/pandda_{acr}/logs"

            for f in html_dir.iterdir():
                file_type = "Result"
                importance_rank = 1
                try:
                    result_dict = {
                        "file_path": str(processing_dir),
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

    def make_dispensing_df(self, processing_dir, table_dir):
        echo_files = []
        echo_dir = processing_dir / "echo"
        for file in echo_dir.iterdir():
            if "Echo.csv" in file.name:
                echo_files.append(file)

        df = pd.DataFrame(data=None)  # init
        for echo_file in echo_files:
            # self.log.info(f"{echo_file} found")
            echo_path = echo_dir / echo_file
            dfecho = pd.read_csv(echo_path)
            plate_type = echo_file.parts[-1].split("_")[4]  # change this
            dfecho["Plate Type"] = plate_type
            df = pd.concat([df, dfecho])

        ID, SMILES, WELL = ([] for i in range(3))
        for index, row in df.iterrows():
            sourcewell = row["Source Well"]
            destinationwell = row["Destination Well"]
            library = row["Library Barcode"]
            plate_type = "platedefinition_" + row["Plate Type"]
            for library_file in os.listdir(table_dir):
                if library in library_file:
                    library_path = table_dir / library_file
                    dflib = pd.read_csv(library_path)

                    match = dflib.loc[dflib["Well"] == sourcewell]
                    ID.append(match["ID"].item())
                    SMILES.append(match["Smile"].item())

                if plate_type in library_file:
                    platedef_path = table_dir / library_file
                    dfdef = pd.read_csv(platedef_path)

                    match = dfdef.loc[dfdef["Destination Well"] == destinationwell]
                    WELL.append(match["Data Well"].item().split("_")[1])

        df["Catalog ID"] = ID
        df["Smiles"] = SMILES
        df["ISPyB Well"] = WELL
        df["Experiment ID"] = df["Catalog ID"] + "/" + df["Transfer Volume"].astype(str)
        df = df.rename(columns={"Plate Barcode": "barcode", "ISPyB Well": "location"})
        df.location = pd.to_numeric(df.location)

        return df
