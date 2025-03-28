from __future__ import annotations

import os
import pathlib
import re
import subprocess

import pandas as pd

import dlstbx.util.symlink
from dlstbx.wrapper import Wrapper


class PanDDAWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.pandda"

    # def send_attachments_to_ispyb(self, pipeline_directory, min_cc_keep):
    #     for f in pipeline_directory.iterdir():
    #         if f.stem.endswith("final") and CC >= min_cc_keep:
    #             file_type = "Result"
    #             importance_rank = 1
    #         elif f.suffix == ".log":
    #             file_type = "Log"
    #             importance_rank = 2
    #         else:
    #             continue
    #         try:
    #             result_dict = {
    #                 "file_path": str(pipeline_directory),
    #                 "file_name": f.name,
    #                 "file_type": file_type,
    #                 "importance_rank": importance_rank,
    #             }
    #             self.record_result_individual_file(result_dict)
    #             self.log.info(f"Uploaded {f.name} as an attachment")
    #         except Exception:
    #             self.log.warning(f"Could not attach {f.name} to ISPyB", exc_info=True)

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

        res_limit = float(3.00)
        completeness_limit = float(90)

        echo_dir = working_directory + "/echo"
        list_dir = os.listdir(echo_dir)

        df = pd.DataFrame(
            columns=[
                "Catalog ID",
                "Smiles",
                "Library",
                "Library Plate",
                "Source Well",
                "Destination Well",
                "Plate Type",
                "ISPyB Well",
                "Transfer Volume",
                "Destination Well X Offset",
                "Destination Well Y Offset",
                "Experiment ID",
                "Well ID",
                "All Well",
                "Plate Barcode",
                "Proposal",
                "Visit",
                "Protein",
                "Dataset ID",
                "Sample Name",
                "Data Directory",
                "All Well Path",
                "All Crystal Path",
                "All Multiplex Path",
                "Selected Multiplex Path",
                "Selected Dimple Path",
                "Output Path",
            ]
        )

        query = "Echo.csv"
        echo_files = []

        for file_name in sorted(list_dir, reverse=False):
            if query in file_name:
                echo_files.append(file_name)

        for echo_file in echo_files:
            print(f"{echo_file} found.")
            echo_path = echo_dir + "/" + echo_file
            dfecho = pd.read_csv(echo_path)
            protein_acr = echo_file.split("_")[0]
            dfecho["Protein"] = protein_acr
            proposal = re.split(r"_|-", echo_file)[3]
            dfecho["Proposal"] = proposal
            visit = re.split(r"_|-", echo_file)[4]
            dfecho["Visit"] = visit
            plate_barcode = echo_file.split("_")[1]
            dfecho["Plate Barcode"] = plate_barcode
            data_dir = "/dls/mx/data/" + proposal + "/" + proposal + "-" + visit
            dfecho["Data Directory"] = data_dir
            library_plate = echo_file.split("_")[3]
            dfecho["Library Plate"] = library_plate
            library = re.split(r"_|-", echo_file)[5]
            dfecho["Library"] = library
            plate_type = echo_file.split("_")[4]
            dfecho["Plate Type"] = plate_type
            df = pd.concat([df, dfecho])

        # rewritten this block and checked output is the same as original
        ID = []
        SMILES = []
        WELL = []
        table_dir = "/dls/science/groups/mx/vmxi/tables"

        for index, row in df.iterrows():
            sourcewell = row["Source Well"]
            destinationwell = row["Destination Well"]
            library = row["Library"]
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
                    WELL.append(match["Data Well"].item())

        print(" \nFragment information found.\n ")

        pandda_command = "print('pandda command')"

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
            self.log.error(f"Ligand_fit process '{pandda_command}' failed")
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

        # self.log.info("Sending results to ISPyB")
        # self.send_attachments_to_ispyb(pipeline_directory)

        # CC = self.pull_CC_from_log(pipeline_directory)
        # if CC >= min_cc_keep:
        #     self.log.info("Ligand_fitting pipeline finished successfully")
        #     return True
        # else:
        #     self.log.info(
        #         f"Ligand_fitting pipeline finished but ligand fitting CC ({CC}) did not meet quality threshold ({min_cc_keep})"
        #     )
        #     return False
