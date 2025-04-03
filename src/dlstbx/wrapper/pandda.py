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

        working_directory = pathlib.Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)
        results_directory = pathlib.Path(params["results_directory"])
        results_directory.mkdir(parents=True, exist_ok=True)

        res_limit = float(3.00)  # make into recipe parameters
        completeness_limit = float(90)

        query = "Echo.csv"
        echo_files = []
        echo_dir = processing_dir / "echo"
        for file in echo_dir.iterdir():
            if query in file.name:
                echo_files.append(file)

        df = pd.DataFrame(data=None)

        for echo_file in echo_files:
            print(f"{echo_file} found.")
            echo_path = echo_dir / echo_file
            dfecho = pd.read_csv(echo_path)
            plate_type = echo_file.parts[
                -1
            ].split(
                "_"
            )[
                4
            ]  # better to query Container.containerType and put in echofile before this step
            dfecho["Plate Type"] = plate_type
            df = pd.concat([df, dfecho])

        ID = []
        SMILES = []
        WELL = []
        table_dir = "/dls/science/groups/mx/vmxi/tables"

        for index, row in df.iterrows():
            sourcewell = row["Source Well"]
            destinationwell = row["Destination Well"]
            library = row[
                "Library Barcode"
            ]  # library looks to be the same as library barcode
            plate_type = "platedefinition_" + row["Plate Type"]
            for library_file in os.listdir(table_dir):
                if library in library_file:
                    library_path = table_dir + "/" + library_file
                    dflib = pd.read_csv(library_path)

                    match = dflib.loc[dflib["Well"] == sourcewell]
                    ID.append(match["ID"].item())
                    SMILES.append(match["Smile"].item())

                if plate_type in library_file:
                    platedef_path = table_dir + "/" + library_file
                    dfdef = pd.read_csv(platedef_path)

                    match = dfdef.loc[dfdef["Destination Well"] == destinationwell]
                    WELL.append(match["Data Well"].item().split("_")[1])

        print(" \nFragment information found.\n ")
        df["Catalog ID"] = ID
        df["Smiles"] = SMILES
        df["ISPyB Well"] = WELL
        df["Experiment ID"] = df["Catalog ID"] + "/" + df["Transfer Volume"].astype(str)
        df.rename(columns={"Plate Barcode": "barcode", "ISPyB Well": "location"})

        df2 = pd.read_csv(processing_dir / "ispyb.csv")
        dfmerged = pd.merge(df, df2, how="outer", on=["barcode", "location"])

        dfmerged[dfmerged["filePath"].isna()]  # entries with smiles but no dcid, record

        # check multiplex dataset quality
        multiplex_data = []
        for index, row in dfmerged.iterrows():
            multiplex_dir = Path(str(row["filePath"]))
            if (multiplex_dir / "xia2.multiplex.log").exists():
                with open(multiplex_dir / "xia2.multiplex.log") as f:
                    for line in f:  # why search through all lines
                        if "cluster_" in line:
                            if len(line.split()) > 5:  # added this line for now to fix
                                number_of_datasets_in_cluster = line.split()[1]
                        if "Completeness" in line:
                            if len(line.split()) == 4:
                                completeness = line.split()[1]
                        if "High resolution limit  " in line:
                            high_res_limit = line.split()[
                                5
                            ]  # this gets the overall limit not the high res limit which would be [5], correct?

                if (
                    float(completeness) >= completeness_limit
                    and float(high_res_limit) <= res_limit
                ):
                    multiplex_data.append(True)
                else:
                    multiplex_data.append(False)
            else:
                multiplex_data.append(False)

        dfmerged["Multiplex quality"] = multiplex_data
        df_final = dfmerged[dfmerged["Multiplex quality"] == True]  # filter data

        pandda_datasets = len(df_final)
        if pandda_datasets < 50:  # make into param?
            self.log.error(
                f"Aborting PanDDA processing. There are a total of {pandda_datasets} that meet the quality criteria which is less than the required amount of 50"
            )
            return False

        # create the directory structure required for panddas analysis
        for index, row in df_final.iterrows():
            well = row["location"]
            multiplex_path = Path(str(row["filePath"]))
            Path(
                processing_dir / "analysis" / "model_building" / f"Mac-x{well}/compound"
            ).mkdir(parents=True, exist_ok=True)
            dirpath = processing_dir / "analysis" / "model_building" / f"Mac-x{well}"
            shutil.copyfile(
                multiplex_path / "dimple/final.pdb", dirpath / f"x{well}_dimple.pdb"
            )
            shutil.copyfile(
                multiplex_path / "dimple/final.mtz", dirpath / f"x{well}_dimple.mtz"
            )

            library = row["Library Barcode"]
            source_well = row["Source Well"]
            cif_dir = pathlib.Path("")  # make a central cif,smiles dir for each library
            shutil.copyfile(
                cif_dir / "source_well" / "ligand.pdb",
                processing_dir
                / "analysis"
                / "model_building"
                / f"Mac-x{well}"
                / "compound"
                / "ligand.pdb",
            )
            shutil.copyfile(
                cif_dir / "source_well" / "ligand.cif",
                processing_dir
                / "analysis"
                / "model_building"
                / f"Mac-x{well}"
                / "compound"
                / "ligand.cif",
            )

        pandda_command = ""

        try:
            result = subprocess.run(
                pandda_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=working_directory,
                check=True,
                timeout=params.get("timeout") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"PanDDA process '{pandda_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        with open(working_directory / "pandda.log", "w") as log_file:
            log_file.write(result.stdout)

        # shutil.copytree(
        #     working_directory,
        #     results_directory,
        #     dirs_exist_ok=True,
        #     ignore=ignore_patterns(".*"),
        # )

        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(working_directory), params["create_symlink"]
            )
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(results_directory), params["create_symlink"]
            )

        self.log.info("Sending results to ISPyB")
        self.send_attachments_to_ispyb(processing_dir)

        self.log.info("PanDDA script finished")
        return True
