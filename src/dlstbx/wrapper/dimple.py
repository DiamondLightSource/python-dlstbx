from __future__ import annotations

import configparser
import copy
import itertools
import json
import os
import pathlib
import re
import shutil
from typing import List

import dateutil.parser
import gemmi
import procrunner

import dlstbx.util.symlink
from dlstbx import schemas
from dlstbx.util import ChainMapWithReplacement
from dlstbx.wrapper import Wrapper


class DimpleWrapper(Wrapper):

    _logger_name = "dlstbx.wrap.dimple"

    def send_results_to_ispyb(self):
        log_file = self.results_directory / "dimple.log"
        if not log_file.is_file():
            self.log.error(
                "Can not insert dimple results into ISPyB: dimple.log not found"
            )
            return False
        log = configparser.RawConfigParser()
        log.read(log_file)

        scaling_id = self.params.get("scaling_id", [])
        assert (
            len(scaling_id) == 1
        ), f"Exactly one scaling id must be provided: {scaling_id}"
        scaling_id = scaling_id[0]
        program_id = self.params.get("program_id")
        self.log.debug(
            f"Inserting dimple phasing results from {self.results_directory} into ISPyB for scaling_id {scaling_id}"
        )

        start_time = log.get(log.sections()[1], "start_time")
        end_time = log.get(log.sections()[-1], "end_time")
        try:
            msg = " ".join(log.get("find-blobs", "info").split()[:4])
        except configparser.NoSectionError:
            msg = "Unmodelled blobs not found"
        dimple_args = log.get("workflow", "args").split()

        app = schemas.AutoProcProgram(
            command_line=(
                log.get("workflow", "prog")
                + " "
                + log.get("workflow", "args").replace("\n", " ")
            ),
            programs="dimple",
            status=1,
            message=msg,
            start_time=dateutil.parser.parse(start_time),
            end_time=dateutil.parser.parse(end_time),
        )

        mxmrrun = schemas.MXMRRun(
            auto_proc_scaling_id=scaling_id,
            auto_proc_program_id=program_id,
            rfree_start=log.getfloat("refmac5 restr", "ini_free_r"),
            rfree_end=log.getfloat("refmac5 restr", "free_r"),
            rwork_start=log.getfloat("refmac5 restr", "ini_overall_r"),
            rwork_end=log.getfloat("refmac5 restr", "overall_r"),
        )

        input_mtz = pathlib.Path(dimple_args[0])
        input_pdb = pathlib.Path(dimple_args[1])
        # Record AutoProcAttachments (SCI-9692)
        result_files = {
            self.results_directory
            / "final.mtz": (schemas.AttachmentFileType.RESULT, 1),
            self.results_directory
            / "final.pdb": (schemas.AttachmentFileType.RESULT, 1),
            self.results_directory / "screen.log": (schemas.AttachmentFileType.LOG, 1),
            input_mtz: (schemas.AttachmentFileType.INPUT, 2),
            input_pdb: (schemas.AttachmentFileType.INPUT, 2),
            log_file: (schemas.AttachmentFileType.LOG, 2),
        }
        result_files.update(
            {
                log_file: (schemas.AttachmentFileType.LOG, 2)
                for log_file in itertools.chain(
                    self.results_directory.glob("[0-9]*-find-blobs.log"),
                    self.results_directory.glob("[0-9]*-refmac5_restr.log"),
                )
            }
        )
        attachments = [
            schemas.Attachment(
                file_type=ftype,
                file_path=f.parent,
                file_name=f.name,
                timestamp=dateutil.parser.parse(end_time),
                importance_rank=importance_rank,
            )
            for f, (ftype, importance_rank) in result_files.items()
            if f.is_file()
        ]

        blobs = []
        find_blobs_log = next(
            self.results_directory.glob("[0-9]*-find-blobs.log"), None
        )
        cell = get_cell_from_mtz(input_mtz)
        if find_blobs_log:
            blobs = get_blobs_from_find_blobs_log(find_blobs_log)
            for i in range(min(len(blobs), 2)):
                n = i + 1
                if (self.results_directory / f"blob{n}v1.png").is_file():
                    blob = blobs[n - 1]
                    blob.filepath = self.results_directory
                    blob.view1 = f"blob{n}v1.png"
                    blob.view2 = f"blob{n}v2.png"
                    blob.view3 = f"blob{n}v3.png"

        anom_blobs = []
        anode_log = self.results_directory / "anode.lsa"
        if anode_log:
            anom_blobs = get_blobs_from_anode_log(anode_log, cell)
            for i in range(min(len(anom_blobs), 2)):
                n = i + 1
                if (self.results_directory / f"anom-blob{n}v1.png").is_file():
                    blob = anom_blobs[n - 1]
                    blob.filepath = self.results_directory
                    blob.view1 = f"anom-blob{n}v1.png"
                    blob.view2 = f"anom-blob{n}v2.png"
                    blob.view3 = f"anom-blob{n}v3.png"
            anode_result_files = {
                self.results_directory / "anode.pha": schemas.AttachmentFileType.RESULT,
                self.results_directory
                / "anode_fa.res": schemas.AttachmentFileType.RESULT,
                anode_log: schemas.AttachmentFileType.LOG,
            }
            attachments.extend(
                [
                    schemas.Attachment(
                        file_type=ftype,
                        file_path=f.parent,
                        file_name=f.name,
                        timestamp=dateutil.parser.parse(end_time),
                    )
                    for f, ftype in anode_result_files.items()
                    if f.is_file()
                ]
            )

        ispyb_results = {
            "ispyb_command": "insert_dimple_results",
            "mxmrrun": json.loads(mxmrrun.json()),
            "blobs": [json.loads(b.json()) for b in blobs + anom_blobs],
            "auto_proc_program": json.loads(app.json()),
            "attachments": [json.loads(att.json()) for att in attachments],
        }

        self.log.debug("Sending %s", str(ispyb_results))
        self.recwrap.send_to("ispyb", ispyb_results)
        return True

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        self.params = ChainMapWithReplacement(
            self.recwrap.recipe_step["job_parameters"].get("ispyb_parameters", {}),
            self.recwrap.recipe_step["job_parameters"].get("dimple", {}),
            self.recwrap.recipe_step["job_parameters"],
            substitutions=self.recwrap.environment,
        )

        self.working_directory = pathlib.Path(self.params["working_directory"])
        self.results_directory = pathlib.Path(self.params["results_directory"])
        self.working_directory.mkdir(parents=True, exist_ok=True)

        mtz = self.params.get("data", [])
        if not mtz:
            self.log.error("Could not identify on what data to run")
            return False

        assert len(mtz) == 1, "Exactly one data file data file must be provided: %s" % (
            mtz
        )
        mtz = pathlib.Path(mtz[0]).resolve()
        if not mtz.is_file():
            self.log.error("Could not find data file %s to process", mtz)
            return False
        pdb = self.params.get("pdb")
        if not pdb:
            self.log.error("Not running dimple as no PDB file available")
            return False

        pdb = copy.deepcopy(pdb)  # otherwise we could modify the array in the recipe
        for i, code_or_file in enumerate(pdb):
            if not os.path.isfile(code_or_file) and len(code_or_file) == 4:
                local_pdb_copy = pathlib.Path(
                    f"/dls/science/groups/scisoft/PDB/{code_or_file[1:3].lower()}/pdb{code_or_file.lower()}.ent.gz"
                )
                if local_pdb_copy.is_file():
                    code_or_file = local_pdb_copy
                    self.log.debug(f"Using local PDB {local_pdb_copy}")
            if os.path.isfile(code_or_file):
                shutil.copy(code_or_file, self.working_directory)
                pdb[i] = self.working_directory / os.path.basename(code_or_file)
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

        self.log.info("command: %s", " ".join(map(str, command)))
        result = procrunner.run(
            command,
            working_directory=self.working_directory,
            timeout=self.params.get("timeout"),
        )
        success = not result["exitcode"] and not result["timeout"]
        if success:
            self.log.info("dimple successful, took %.1f seconds", result["runtime"])
        else:
            self.log.info(
                "dimple failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            self.log.debug(result["stdout"].decode("latin1"))
            self.log.debug(result["stderr"].decode("latin1"))

        # Hack to workaround dimple returning successful exitcode despite 'Giving up'
        success &= b"Giving up" not in result.stdout

        self.log.info(f"Copying DIMPLE results to {self.results_directory}")
        self.results_directory.mkdir(parents=True, exist_ok=True)
        if self.params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(self.results_directory), self.params["create_symlink"]
            )
            mtzsymlink = mtz.parent / self.params["create_symlink"]
            if not mtzsymlink.exists():
                deltapath = os.path.relpath(self.results_directory, mtz.parent)
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
                self.log.debug("Replacing tmp paths in %s", path)
                path.write_text(
                    path.read_text().replace(
                        os.fspath(self.working_directory),
                        os.fspath(self.results_directory),
                    )
                )
        if success:
            self.log.info("Sending dimple results to ISPyB")
            success = self.send_results_to_ispyb()

        return success


ATOM_NAME_RE = re.compile(r"([\w]+)_([A-Z]):([A-Z]+)([0-9]+)")


def get_blobs_from_anode_log(
    log_file: pathlib.Path, cell: gemmi.UnitCell
) -> List[schemas.Blob]:
    blobs = []
    with log_file.open() as fh:
        in_strongest_peaks_section = False
        for line in fh.readlines():
            line = line.strip()
            if line == "Strongest unique anomalous peaks":
                in_strongest_peaks_section = True
                continue
            if in_strongest_peaks_section and len(tokens := line.split()) == 8:
                x, y, z, height, occupancy, distance = map(float, tokens[1:7])
                atom = tokens[7]
                m = ATOM_NAME_RE.match(atom)
                if m:
                    name, chain_id, res_name, res_seq = m.groups()
                    nearest_atom = schemas.Atom(
                        name=name,
                        chain_id=chain_id,
                        res_name=res_name,
                        res_seq=res_seq,
                    )
                    blobs.append(
                        schemas.Blob(
                            xyz=tuple(cell.orthogonalize(gemmi.Fractional(x, y, z))),
                            height=height,
                            occupancy=occupancy,
                            nearest_atom=nearest_atom,
                            nearest_atom_distance=distance,
                            map_type="anomalous",
                        )
                    )
    return blobs


def get_blobs_from_find_blobs_log(log_file: pathlib.Path) -> List[schemas.Blob]:
    blobs = []
    with log_file.open() as fh:
        for line in fh.readlines():
            if line.startswith("#"):
                tokens = (
                    line.replace("(", "").replace(")", "").replace(",", " ").split()
                )
                blobs.append(
                    schemas.Blob(
                        xyz=tuple(float(x) for x in tokens[6:9]),
                        height=float(tokens[5]),
                        map_type="difference",
                    )
                )
    return blobs


def get_cell_from_mtz(mtz_file: pathlib.Path) -> gemmi.UnitCell:
    return gemmi.read_mtz_file(os.fspath(mtz_file)).cell
