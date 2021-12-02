import configparser
import copy
import enum
import itertools
import logging
import os
import pathlib
import re
import shutil
from dataclasses import dataclass
from typing import List, Tuple

import procrunner
import zocalo.wrapper

import dlstbx.util.symlink

logger = logging.getLogger("dlstbx.wrap.dimple")


class DimpleWrapper(zocalo.wrapper.BaseWrapper):
    def send_results_to_ispyb(self):
        log_file = self.results_directory / "dimple.log"
        if not log_file.is_file():
            logger.error(
                "Can not insert dimple results into ISPyB: dimple.log not found"
            )
            return False
        log = configparser.RawConfigParser()
        log.read(log_file)

        scaling_id = self.params.get("ispyb_parameters", self.params).get(
            "scaling_id", []
        )
        assert (
            len(scaling_id) == 1
        ), f"Exactly one scaling id must be provided: {scaling_id}"
        scaling_id = scaling_id[0]
        if not str(scaling_id).isdigit():
            logger.error(
                f"Can not write results to ISPyB: no scaling ID set ({scaling_id})"
            )
            return False
        scaling_id = int(scaling_id)
        logger.debug(
            f"Inserting dimple phasing results from {self.results_directory} into ISPyB for scaling_id {scaling_id}"
        )

        ispyb_command_list = []

        starttime = log.get(log.sections()[1], "start_time")
        endtime = log.get(log.sections()[-1], "end_time")
        try:
            msg = " ".join(log.get("find-blobs", "info").split()[:4])
        except configparser.NoSectionError:
            msg = "Unmodelled blobs not found"
        dimple_args = log.get("workflow", "args").split()

        insert_mxmr_run = {
            "ispyb_command": "insert_mxmr_run",
            "store_result": "ispyb_mxmr_run_id",
            "scaling_id": scaling_id,
            "pipeline": "dimple",
            "logfile": log_file,
            "success": 1,
            "starttime": starttime,
            "endtime": endtime,
            "rfreestart": log.getfloat("refmac5 restr", "ini_free_r"),
            "rfreeend": log.getfloat("refmac5 restr", "free_r"),
            "rstart": log.getfloat("refmac5 restr", "ini_overall_r"),
            "rend": log.getfloat("refmac5 restr", "overall_r"),
            "message": msg,
            "rundir": self.results_directory,
            "inputmtzfile": dimple_args[0],
            "inputcoordfile": dimple_args[1],
            "outputmtzfile": self.results_directory / "final.mtz",
            "outputcoordfile": self.results_directory / "final.pdb",
            "cmdline": (
                log.get("workflow", "prog")
                + " "
                + log.get("workflow", "args").replace("\n", " ")
            ),
        }
        ispyb_command_list.append(insert_mxmr_run)

        for n in (1, 2):
            if (self.results_directory / f"/blob{n}v1.png").is_file():
                insert_mxmr_run_blob = {
                    "ispyb_command": "insert_mxmr_run_blob",
                    "mxmr_run_id": "$ispyb_mxmr_run_id",
                    "view1": f"blob{n}v1.png",
                    "view2": f"blob{n}v2.png",
                    "view3": f"blob{n}v3.png",
                }
                ispyb_command_list.append(insert_mxmr_run_blob)

        logger.debug("Sending %s", str(ispyb_command_list))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        return True

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.params = self.recwrap.recipe_step["job_parameters"]
        self.working_directory = pathlib.Path(self.params["working_directory"])
        self.results_directory = pathlib.Path(self.params["results_directory"])
        self.working_directory.mkdir(parents=True)

        mtz = self.params.get("ispyb_parameters", self.params.get("dimple", {})).get(
            "data", []
        )
        if not mtz:
            logger.error("Could not identify on what data to run")
            return False

        assert len(mtz) == 1, "Exactly one data file data file must be provided: %s" % (
            mtz
        )
        mtz = os.path.abspath(mtz[0])
        if not os.path.exists(mtz):
            logger.error("Could not find data file %s to process", mtz)
            return False
        pdb = self.params.get("ispyb_parameters", {}).get("pdb") or self.params.get(
            "dimple", {}
        ).get("pdb")
        if not pdb:
            logger.error("Not running dimple as no PDB file available")
            return False

        pdb = copy.deepcopy(pdb)  # otherwise we could modify the array in the recipe
        for i, code_or_file in enumerate(pdb):
            if not os.path.isfile(code_or_file) and len(code_or_file) == 4:
                local_pdb_copy = pathlib.Path(
                    f"/dls/science/groups/scisoft/PDB/{code_or_file[1:3].lower()}/pdb{code_or_file.lower()}.ent.gz"
                )
                if local_pdb_copy.is_file():
                    code_or_file = local_pdb_copy
                    logger.debug(f"Using local PDB {local_pdb_copy}")
            if code_or_file.is_file():
                shutil.copy(code_or_file, self.working_directory)
                pdb[i] = self.working_directory / code_or_file.name
        command = (
            ["dimple", mtz]
            + pdb
            + [
                self.working_directory,
                # '--dls-naming',
                "--anode",
                "-fpng",
            ]
        )

        if self.params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(self.working_directory), self.params["create_symlink"]
            )

        # Create SynchWeb ticks hack file. This will be deleted or replaced later.
        # For this we need to create the results directory and its symlink immediately.
        if self.params.get("synchweb_ticks") and self.params.get(
            "ispyb_parameters", {}
        ).get("set_synchweb_status"):
            logger.debug("Setting SynchWeb status to swirl")
            if self.params.get("create_symlink"):
                self.results_directory.mkdir(parents=True)
                dlstbx.util.symlink.create_parent_symlink(
                    os.fspath(self.results_directory), self.params["create_symlink"]
                )
                mtzsymlink = os.path.join(
                    os.path.dirname(mtz), self.params["create_symlink"]
                )
                if not os.path.exists(mtzsymlink):
                    deltapath = self.results_directory.relative_to(os.path.dirname(mtz))
                    os.symlink(deltapath, mtzsymlink)
            pathlib.Path(self.params["synchweb_ticks"]).mkdir(parents=True)

        logger.info("command: %s", " ".join(map(str, command)))
        result = procrunner.run(
            command,
            working_directory=self.working_directory,
            timeout=self.params.get("timeout"),
        )
        success = not result["exitcode"] and not result["timeout"]
        if success:
            logger.info("dimple successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "dimple failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"].decode("latin1"))
            logger.debug(result["stderr"].decode("latin1"))

        # Hack to workaround dimple returning successful exitcode despite 'Giving up'
        success &= b"Giving up" not in result.stdout

        logger.info(f"Copying DIMPLE results to {self.results_directory}")
        self.results_directory.mkdir(parents=True)
        if self.params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(self.results_directory), self.params["create_symlink"]
            )
            mtzsymlink = os.path.join(
                os.path.dirname(mtz), self.params["create_symlink"]
            )
            if not os.path.exists(mtzsymlink):
                deltapath = self.results_directory.relative_to(os.path.dirname(mtz))
                os.symlink(deltapath, mtzsymlink)
        for f in self.working_directory.iterdir():
            if f.name.startswith("."):
                continue
            if any(f.suffix == skipext for skipext in (".pickle", ".r3d")):
                continue
            shutil.copy(f, self.results_directory)

        # Replace tmp working_directory with results_directory in coot scripts
        filenames = [
            self.results_directory / f for f in ("coot.sh", "anom-coot.sh")
        ] + list(self.results_directory.glob("*blob*-coot.py"))
        for path in filenames:
            if path.is_file():
                logger.debug("Replacing tmp paths in %s", path)
                path.write_text(
                    path.read_text().replace(
                        os.fspath(self.working_directory),
                        os.fspath(self.results_directory),
                    )
                )
        if success:
            logger.info("Sending dimple results to ISPyB")
            success = self.send_results_to_ispyb()

        # Record AutoProcAttachments (SCI-9692)
        attachments = {
            self.results_directory / "final.mtz": ("result", 1),
            self.results_directory / "final.pdb": ("result", 1),
            self.results_directory / "dimple.log": ("log", 2),
            self.results_directory / "screen.log": ("log", 1),
        }
        attachments.update(
            {
                log_file: ("log", 1)
                for log_file in itertools.chain(
                    self.results_directory.glob("[0-9]*-find-blobs.log"),
                    self.results_directory.glob("[0-9]*-refmac5_restr.log"),
                )
            }
        )
        logger.info(attachments)
        for file_name, (file_type, importance_rank) in attachments.items():
            if file_name.is_file():
                self.record_result_individual_file(
                    {
                        "file_path": file_name.parent,
                        "file_name": file_name.name,
                        "file_type": file_type,
                        "importance_rank": importance_rank,
                    }
                )

        # Update SynchWeb tick hack file
        if self.params.get("synchweb_ticks") and self.params.get(
            "ispyb_parameters", {}
        ).get("set_synchweb_status"):
            if success:
                logger.debug("Removing SynchWeb hack file")
                pathlib.Path(self.params["synchweb_ticks"]).unlink()
            else:
                logger.debug("Updating SynchWeb hack file to failure")
                pathlib.Path(self.params["synchweb_ticks"]).write_text(
                    "This file is used as a flag to synchweb to show the processing has failed"
                )

        return success


class MapType(enum.Enum):
    ANOMALOUS = "anomalous"
    DIFFERENCE = "difference"


@dataclass
class Atom:
    name: str
    chain_id: str
    res_seq: int
    res_name: str


@dataclass
class Blob:
    xyz: Tuple[float, float, float]
    height: float
    occupancy: float
    nearest_atom: Atom
    nearest_atom_distance: float
    map_type: MapType


ATOM_NAME_RE = re.compile(r"([\w]+)_([A-Z]):([A-Z]+)([0-9]+)")


def get_blobs_from_anode_log(log_file: pathlib.Path) -> List[Blob]:
    blobs = []
    with log_file.open() as fh:
        in_strongest_peaks_section = False
        for line in fh.readlines():
            line = line.strip()
            if line == "Strongest unique anomalous peaks":
                in_strongest_peaks_section = True
                continue
            if in_strongest_peaks_section and line.startswith("S"):
                tokens = line.split()
                if len(tokens) == 8:
                    x, y, z, height, occupancy, distance = map(float, tokens[1:7])
                    atom = tokens[7]
                    m = ATOM_NAME_RE.match(atom)
                    if m:
                        name, chain_id, res_name, res_seq = m.groups()
                        nearest_atom = Atom(
                            name=name,
                            chain_id=chain_id,
                            res_name=res_name,
                            res_seq=res_seq,
                        )
                        blobs.append(
                            Blob(
                                xyz=(x, y, z),
                                height=height,
                                occupancy=occupancy,
                                nearest_atom=nearest_atom,
                                nearest_atom_distance=distance,
                                map_type="anomalous",
                            )
                        )
    return blobs


if __name__ == "__main__":
    import sys

    log_file = pathlib.Path(sys.argv[1])
    blobs = get_blobs_from_anode_log(log_file=log_file)
    print(blobs)
