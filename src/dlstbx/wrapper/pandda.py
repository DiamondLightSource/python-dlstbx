from __future__ import annotations

import glob
import os
import pathlib
import shutil
import subprocess

import pandas as pd
from iotbx.reflection_file_reader import any_reflection_file

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
        # entries with smiles but no dcid, record these! also dcid but no smiles

        acronyms = dfmerged[dfmerged["acronym"].notna()]["acronym"].unique()
        acr = acronyms[0]  # assume 1 target

        # filter multiplex data based on resolution/completeness limits & select best multiplex dataset
        df_final = dfmerged[
            (dfmerged["resolutionLimitHigh"] <= res_limit)
            & (dfmerged["completeness"] >= completeness_limit)
        ]

        df_final = (
            df_final.sort_values("resolutionLimitHigh", ascending=False)  # completeness
            .drop_duplicates("Smiles")  # dataCollectionId
            .sort_index()
        )

        outpath = processing_dir / "analysis/datasets_for_pandda.csv"
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

        # Create directory structure for PanDDA analysis
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
            os.symlink(
                multiplex_path / "dimple/final.pdb",
                well_dir / f"{acr}-{vn}-x{well}.dimple.pdb",
            )
            os.symlink(
                multiplex_path / "dimple/final.mtz",
                well_dir / f"{acr}-{vn}-x{well}.dimple.mtz",
            )
            os.symlink(multiplex_path / "scaled.mtz", well_dir / "scaled.mtz")

            # get pre-made ligand files from central directory, use symlinks?
            cif_dir = table_dir / "pdb_cif" / f"{library}"
            pdb = cif_dir / f"{source_well}" / "ligand.xyz.pdb"
            cif = cif_dir / f"{source_well}" / "ligand.restraints.cif"

            if pdb.exists() and cif.exists():
                shutil.copyfile(pdb, well_dir / "ligand.pdb")
                shutil.copyfile(cif, well_dir / "ligand.cif")
                shutil.copyfile(pdb, compound_dir / "ligand.pdb")
                shutil.copyfile(cif, compound_dir / "ligand.cif")
            else:
                self.log.info(
                    f"No ligand pdb/cif file found for location {well}, ligand library {library}, skipping..."
                )  # or subprocess acedrg here to create ligand files? acedrg -i {well_dir}/lig.smi -o {well_dir}/lig

        # PanDDA pre-run
        self.log.info("Running PanDDA pre-run")
        pandda_dir = processing_dir / f"analysis/pandda_{acr}"

        pandda_command = f"module load ccp4/7.0.078; \
            module load pymol/1.8.2.0-py2.7; \
            pandda.analyse data_dirs={processing_dir}/'analysis/model_building_{acr}/*' pdb_style='*.dimple.pdb' out_dir={pandda_dir} cpus=36 write_average_map=all max_new_datasets=100 low_resolution_completeness=none"

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

        # Build new ground state apo model from PanDDA pre-run
        self.log.info("Selecting best ground state model from PanDDA pre-run")
        datasetList = self.find_highest_resolution_datasets(pandda_dir)
        datasetListwithoutEvent = self.get_datasets_without_event_map(
            pandda_dir, datasetList
        )
        lowestRfree = self.select_dataset_with_lowest_Rfree(
            pandda_dir, datasetListwithoutEvent
        )
        lowestRfreedir = pandda_dir / "processed_datasets" / lowestRfree
        self.log.info(f"Lowest Rfree dataset: {lowestRfree}")
        pdb, mtz, ccp4map = self.link_pdb_mtz_files(pandda_dir, lowestRfree)
        resolution, average_mtz = self.convert_mean_map_to_mtz(ccp4map, mtz)

        self.log.info("Building new ground state model")
        phenix_command = f"module load phenix; \
         phenix.real_space_refine {pdb} {ccp4map} resolution={resolution}"

        try:
            result = subprocess.run(
                phenix_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=lowestRfreedir,
                check=True,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"PanDDA process '{phenix_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        # Re-run dimple using new ground-state model as reference
        self.log.info("Re-running dimple using new ground-state model as reference")
        apo_model = pdb.replace(".pdb", "_real_space_refined_000.pdb")

        for dir in (processing_dir / f"analysis/model_building_{acr}").iterdir():
            regex = "final.dimple"
            prefix = dir.parts[-1] + f".{regex}"
            mtz_file = f"{dir}/scaled.mtz"
            dimple_command = f"module load ccp4; \
            dimple {mtz_file} {apo_model} {dir} --hklout={prefix}.mtz --xyzout={prefix}.pdb "

            try:
                result = subprocess.run(
                    dimple_command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=360,
                )

            except subprocess.CalledProcessError as e:
                # self.log.error(f"PanDDA process '{dimple_command}' failed")
                self.log.info(e.stdout)
                self.log.error(e.stderr)

        # Run PanDDA2
        self.log.info("Running PanDDA2")
        pandda2_dir = processing_dir / f"analysis/pandda2_{acr}"

        pandda2_command = f"source /dls/science/groups/i04-1/software/pandda_2_gemmi/act_experimental; \
         conda activate pandda2_ray; \
         python -u /dls/science/groups/i04-1/conor_dev/pandda_2_gemmi/scripts/pandda.py --local_cpus=36 --data_dirs={processing_dir}/'analysis/model_building_{acr}' --max_rfree=0.3 --pdb_regex='*{regex}.pdb' --mtz_regex='*{regex}.mtz' --out_dir={pandda2_dir}  > pandda2.log "

        try:
            result = subprocess.run(
                pandda2_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=processing_dir,
                check=True,
                timeout=params.get("timeout") * 60,
            )

        except subprocess.CalledProcessError as e:
            self.log.error(f"PanDDA process '{pandda2_command}' failed")
            self.log.info(e.stdout)
            self.log.error(e.stderr)
            return False

        self.log.info("Sending results to ISPyB")
        self.send_attachments_to_ispyb(processing_dir)

        self.log.info("PanDDA pipeline finished")
        return True

    # XCE helper scripts
    def find_highest_resolution_datasets(self, panddaDir):
        found = False
        datasetList = []
        for logFile in glob.glob(os.path.join(panddaDir, "logs", "*.log")):
            for n, line in enumerate(open(logFile)):
                if (
                    line.startswith("Statistical Electron Density Characterisation")
                    and len(line.split()) == 6
                ):
                    found = True
                    foundLine = n
                if found and n >= foundLine + 3:
                    if line.startswith("---"):
                        break
                    else:
                        tmpLine = (
                            line.replace(" ", "")
                            .replace("\t", "")
                            .replace("\n", "")
                            .replace("\r", "")
                        )
                        for item in tmpLine.split(","):
                            if item != "":
                                datasetList.append(item)
        return datasetList

    def get_datasets_without_event_map(self, panddaDir, datasetList):
        datasetListwithoutEvent = []
        for dataset in datasetList:
            noEvent = True
            for files in glob.glob(
                os.path.join(panddaDir, "processed_datasets", dataset, "*")
            ):
                if "event" in files:
                    noEvent = False
                    break
            if noEvent:
                datasetListwithoutEvent.append(dataset)
        print(datasetListwithoutEvent)
        return datasetListwithoutEvent

    def select_dataset_with_lowest_Rfree(self, panddaDir, datasetListwithoutEvent):
        datasetList = []
        lowestRfree = ""
        for dataset in datasetListwithoutEvent:
            pdbfile = os.path.join(
                panddaDir,
                "processed_datasets",
                dataset,
                dataset + "-pandda-input.pdb",
            )
            if os.path.isfile(pdbfile):
                for line in open(pdbfile):
                    if line.startswith(
                        "REMARK   3   FREE R VALUE                     :"
                    ):
                        Rfree = line.split()[6]
                        break
                try:
                    datasetList.append([dataset, float(Rfree)])
                except ValueError:
                    pass
        if datasetList:
            lowestRfree = min(datasetList, key=lambda x: x[1])[0]
        return lowestRfree

    def link_pdb_mtz_files(self, panddaDir, lowestRfree):
        lowestRfreedir = panddaDir / "processed_datasets" / lowestRfree

        pdbfile = lowestRfreedir / f"{lowestRfree}-pandda-input.pdb"
        mtzfile = lowestRfreedir / f"{lowestRfree}-pandda-input.mtz"
        mapfile = lowestRfreedir / f"{lowestRfree}-ground-state-average-map.native.ccp4"

        if pdbfile.exists():
            os.symlink(pdbfile, lowestRfreedir / f"{lowestRfree}-ground-state.pdb")

        if mtzfile.exists():
            os.symlink(mtzfile, lowestRfreedir / f"{lowestRfree}-ground-state.mtz")

        if mapfile.exists():
            os.symlink(
                mapfile,
                lowestRfreedir / f"{lowestRfree}-ground-state-mean-map.native.ccp4",
            )

        return str(pdbfile), str(mtzfile), str(mapfile)

    def convert_mean_map_to_mtz(self, emap, mtz):
        print("converting ground-state-mean-map to MTZ")
        cmd = (
            "mapmask MAPIN %s MAPOUT %s << eof\n"
            % (emap, emap.replace(".ccp4", ".P1.ccp4"))
            + " XYZLIM CELL\n"
            " PAD 0.0\n"
            " SYMMETRY 1\n"
            "eof\n"
        )
        os.system(cmd)
        hkl = any_reflection_file(file_name=mtz)
        miller_arrays = hkl.as_miller_arrays()
        mtz = miller_arrays[0]
        resolution = str(round(mtz.d_min(), 2))
        infile = emap.replace(".ccp4", ".P1.ccp4")
        outfile = emap.replace(".ccp4", ".mtz")
        cmd = (
            "module load phenix\n"  # phenix/1.20
            f"phenix.map_to_structure_factors {infile} d_min={resolution} output_file_name={outfile}"
        )
        os.system(cmd)
        return resolution, outfile

    def send_attachments_to_ispyb(self, processing_dir):  # fix
        html_dir = processing_dir / f"analysis/pandda_Mac1/analyses/html_summaries"
        # log_dir = processing_dir / f"analysis/pandda_{acr}/logs"

        for f in html_dir.iterdir():
            file_type = "Result"
            importance_rank = 1
            try:
                result_dict = {
                    "file_path": str(html_dir),
                    "file_name": f.name,
                    "file_type": file_type,
                    "importance_rank": importance_rank,
                }
                self.record_result_individual_file(result_dict)
                self.log.info(f"Uploaded {f.name} as an attachment")
            except Exception:
                self.log.warning(f"Could not attach {f.name} to ISPyB", exc_info=True)

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
