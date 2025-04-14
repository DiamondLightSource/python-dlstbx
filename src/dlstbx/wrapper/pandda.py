from __future__ import annotations

import os
import pathlib
import shutil
import subprocess

import pandas as pd

import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper


class PanDDAWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pandda"

    # def send_attachments_to_ispyb(self, pipeline_directory):
    #     for f in pipeline_directory.iterdir():

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.debug(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )

        params = self.recwrap.recipe_step["job_parameters"]

        processing_dir = pathlib.Path(params["processing_directory"])
        table_dir = pathlib.Path(params["table_directory"])
        # working_directory = pathlib.Path(params["working_directory"])
        # working_directory.mkdir(parents=True, exist_ok=True)
        # results_directory = pathlib.Path(params["results_directory"])
        # results_directory.mkdir(parents=True, exist_ok=True)

        res_limit = params["res_limit"]
        completeness_limit = params["completeness_limit"]

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
            plate_type = echo_file.parts[-1].split("_")[4]
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
        df_ispyb = pd.read_csv(processing_dir / "ispyb.csv")
        dfmerged = pd.merge(df, df_ispyb, how="outer", on=["barcode", "location"])

        # lost_frags = dfmerged[dfmerged["filePath"].isna()]
        # entries with smiles but no dcid, record these

        acronyms = dfmerged[dfmerged["acronym"].notna()]["acronym"].unique()
        # the non na protein acronyms

        # filter multiplex data based on res and completeness limits &  select best multiplex dataset per dcid
        df_final = dfmerged[
            (dfmerged["resolutionLimitHigh"] < res_limit)
            & (dfmerged["completeness"] > completeness_limit)
        ]

        # one entry per smiles or dcid?
        df_final = (
            df_final.sort_values("resolutionLimitHigh", ascending=False)
            .drop_duplicates("Smiles")  # dataCollectionId
            .sort_index()
        )

        for j in range(len(acronyms)):
            if (
                len(df_final[df_final["acronym"] == acronyms[j]])
                < params["min_datasets"]
            ):
                df_final = df_final[df_final["acronym"] != acronyms[j]]
                self.log.info(
                    f"Aborting PanDDA processing for target {acronyms[j]}. Insufficient number of datsets"
                )

        # create the directory structure required for panddas analysis
        for index, row in df_final.iterrows():
            well, acr, library, source_well, vn = (
                row["location"],
                row["acronym"],
                row["Library Barcode"],
                row["Source Well"],
                row["visit_number"],
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
                multiplex_path / "dimple/final.pdb", well_dir / f"x{well}_dimple.pdb"
            )
            shutil.copyfile(
                multiplex_path / "dimple/final.mtz", well_dir / f"x{well}_dimple.mtz"
            )

            cif_dir = (
                table_dir / "pdb_cif" / f"{library}"
            )  # central pdb_cif dir for library
            pdb = cif_dir / f"{source_well}" / "ligand.xyz.pdb"
            cif = cif_dir / f"{source_well}" / "ligand.restraints.cif"

            if pdb.exists() and cif.exists():
                shutil.copyfile(pdb, compound_dir / "ligand.pdb")
                shutil.copyfile(cif, compound_dir / "ligand.cif")
            else:
                self.log.info(
                    f"No ligand pdb/cif file found for well {well}, ligand library {library}, skipping..."
                )  # or subprocess acedrg here to create ligand files? acedrg -i A1/lig.smi -o A1/lig

        for acr in acronyms:
            pandda_command = f"source /dls/science/groups/i04-1/software/pandda_2_gemmi/act_experimental; \
             conda activate pandda2_ray; \
             python -u /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/scripts/pandda.py --local_cpus=36 --data_dirs={processing_dir}/'analysis/model_building_{acr}' --out_dir={processing_dir}/'analysis/pandda2_{acr}' "

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

        # shutil.copytree(
        #     working_directory,
        #     results_directory,
        #     dirs_exist_ok=True,
        #     ignore=ignore_patterns(".*"),
        # )

        # if params.get("create_symlink"):
        #     dlstbx.util.symlink.create_parent_symlink(
        #         os.fspath(working_directory), params["create_symlink"]
        #     )
        #     dlstbx.util.symlink.create_parent_symlink(
        #         os.fspath(results_directory), params["create_symlink"]
        #     )

        self.log.info("Sending results to ISPyB")
        self.send_attachments_to_ispyb(processing_dir)

        self.log.info("PanDDA script finished")
        return True
