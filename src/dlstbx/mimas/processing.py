from __future__ import annotations

import os
import pathlib
from pprint import pformat, pprint
from typing import Dict, List, Optional, Tuple

import pydantic

from dlstbx import mimas
from dlstbx.mimas.core import (
    is_anomalous,
    is_characterization,
    is_industrial_visit,
    is_mx_beamline,
    is_phasing,
    is_rotation,
)
from dlstbx.mimas.i19 import is_i19
from dlstbx.mimas.specification import EventSpecification, TargetSpecification
from dlstbx.util.pdb import PDBFileOrCode, trim_pdb_bfactors

is_processing = EventSpecification(mimas.MimasEvent.PROCESSING)
is_alphafold = TargetSpecification(mimas.MimasTarget.ALPHAFOLD)
is_big_ep = TargetSpecification(mimas.MimasTarget.BIG_EP)
is_big_ep_launcher = TargetSpecification(mimas.MimasTarget.BIG_EP_LAUNCHER)
is_dimple = TargetSpecification(mimas.MimasTarget.DIMPLE)
is_fast_ep = TargetSpecification(mimas.MimasTarget.FAST_EP)
is_mrbump = TargetSpecification(mimas.MimasTarget.MRBUMP)
is_shelxt = TargetSpecification(mimas.MimasTarget.SHELXT)
is_multiplex = TargetSpecification(mimas.MimasTarget.MULTIPLEX)


class DimpleParameters(pydantic.BaseModel):
    dcid: int = pydantic.Field(gt=0)
    experiment_type: str
    scaling_id: int = pydantic.Field(gt=0)
    mtz: pathlib.Path | Dict[str, pathlib.Path]
    pdb: list[PDBFileOrCode]
    automatic: Optional[bool] = False
    comment: Optional[str] = None
    symlink: str = pydantic.Field(default="")


@mimas.match_specification(
    is_rotation & ~is_characterization & is_processing & is_dimple & is_mx_beamline
)
def handle_dimple(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    """Trigger a dimple job for a given data collection.
    Identify any PDB files or PDB codes associated with the given data collection.
    - PDB codes or file contents stored in the ISPyB PDB table and linked with
      the given data collection. Any files defined in the database will be copied
      into a subdirectory inside `pdb_tmpdir`, where the subdirectory name will be
      a hash of the file contents.
    - PDB files (with `.pdb` extension) stored in the directory optionally provided
      by the `user_pdb_directory` recipe parameter.
    If any PDB files or codes are identified, then new ProcessingJob,
    ProcessingJobImageSweep and ProcessingJobParameter will be created, and the
    resulting processingJobId will be sent to the `processing_recipe` queue.
    Recipe parameters:
    - target: set this to "dimple"
    - dcid: the dataCollectionId for the given data collection
    - comment: a comment to be stored in the ProcessingJob.comment field
    - automatic: boolean value passed to ProcessingJob.automatic field
    - scaling_id: autoProcScalingId that the dimple results should be linked to
    - mtz: the input mtz reflection file for dimple
    - user_pdb_directory: optionally look for PDB files in this directory
    - pdb_tmpdir: temporary location to write the contents of PDB files stored
        in the database
    Minimal recipe parameters:
    {
        "target": "dimple",
        "dcid": 123456,
        "comment": "DIMPLE triggered by automatic xia2-dials",
        "automatic": True,
        "scaling_id": 654321,
        "user_pdb_directory": "/path/to/user_pdb",
        "mtz": "/path/to/scaled.mtz",
        "pdb_tmpdir": "/path/to/pdb_tmpdir",
    }
    """
    if pdb_files_or_codes := scenario.pdb_files_or_codes:
        pdb_files = [str(p) for p in pdb_files_or_codes]
        pprint(pdb_files)
    tasks: list[mimas.Invocation] = []

    tasks.extend(
        [
            mimas.MimasISPyBJobInvocation(
                DCID=scenario.DCID,
                autostart=True,
                recipe="postprocessing-dimple",
                source="automatic",
                comment=str(scenario.comment),
                displayname="DIMPLE",
                parameters=(
                    mimas.MimasISPyBParameter(key="data", value=str(scenario.mtz)),
                    mimas.MimasISPyBParameter(
                        key="scaling_id", value=str(scenario.autoprocscaling_id)
                    ),
                    mimas.MimasISPyBParameter(
                        key="create_symlink", value=scenario.tag or "dimple"
                    ),
                    *(
                        mimas.MimasISPyBParameter(key="pdb", value=pdb_file)
                        for pdb_file in pdb_files
                    ),
                ),
                triggervariables=(
                    mimas.MimasISPyBTriggerVariable(
                        "ispyb_autoprocscalingid", str(scenario.autoprocscaling_id)
                    ),
                ),
            ),
        ]
    )
    return tasks


@mimas.match_specification(is_processing & is_shelxt & is_i19)
def handle_shelxt(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    """Trigger a shelxt job for a given data collection."""
    tasks: list[mimas.Invocation] = [
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=False,
            recipe="postprocessing-shelxt",
            source="automatic",
            comment=str(scenario.comment),
            displayname="shelxt",
            parameters=(
                mimas.MimasISPyBParameter(
                    key="scaling_id", value=str(scenario.autoprocscaling_id)
                ),
            ),
            triggervariables=(
                mimas.MimasISPyBTriggerVariable(
                    "ispyb_autoprocscalingid", str(scenario.autoprocscaling_id)
                ),
            ),
        ),
    ]
    return tasks


@mimas.match_specification(
    is_rotation & ~is_characterization & is_processing & is_mrbump & is_mx_beamline
)
def handle_mrbump(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    tasks: list[mimas.Invocation] = []
    if not scenario.pdb_files_or_codes:
        return tasks
    all_pdb_files: set[PDBFileOrCode] = {
        pdb_param
        for pdb_param in scenario.pdb_files_or_codes
        if (pdb_param.filepath and pathlib.Path(pdb_param.filepath).is_file())
    }
    triggervars: Tuple[mimas.MimasISPyBTriggerVariable, ...] = (
        mimas.MimasISPyBTriggerVariable(
            "ispyb_autoprocscalingid", str(scenario.autoprocscaling_id)
        ),
    )
    recipe_name = "postprocessing-mrbump"
    print(f"scenario cloudburting: {pformat(scenario.cloudbursting)}")
    if scenario.cloudbursting:
        for el in scenario.cloudbursting:
            print(f"scenario element: {pformat(el)}")
            if el["cloud_spec"].is_satisfied_by(scenario) and any(
                r in recipe_name for r in el["recipes"]
            ):
                recipe_name = "postprocessing-mrbump-cloud"
                triggervars += (
                    mimas.MimasISPyBTriggerVariable("statistic-cluster", "iris"),
                )
                break
    for pdb_files in {(), tuple(all_pdb_files)}:
        mrbump_parameters = {
            "hklin": str(scenario.mtz),
            "scaling_id": scenario.autoprocscaling_id,
        }
        if pdb_files:
            mrbump_parameters["dophmmer"] = "False"
            mrbump_parameters["mdlunmod"] = "True"
        pdb_localfiles = []
        for pdb_file in pdb_files:
            if not pdb_file.filepath:
                continue
            filepath = pathlib.Path(pdb_file.filepath)
            if pdb_file.source == "AlphaFold":
                trimmed = filepath.with_name(
                    filepath.stem + "_trimmed" + filepath.suffix
                )
                trim_pdb_bfactors(
                    os.fspath(filepath),
                    os.fspath(trimmed),
                    atom_selection="bfactor > 70",
                    set_b_iso=20,
                )
                filepath = trimmed
            pdb_localfiles.append(os.fspath(filepath))

        tasks.append(
            mimas.MimasISPyBJobInvocation(
                DCID=scenario.DCID,
                autostart=False,
                recipe=recipe_name,
                source="automatic",
                comment=str(scenario.comment),
                displayname="MrBUMP",
                parameters=(
                    *(
                        mimas.MimasISPyBParameter(key=key, value=str(val))
                        for key, val in mrbump_parameters.items()
                    ),
                    *(
                        mimas.MimasISPyBParameter(key="localfile", value=pdb_file)
                        for pdb_file in pdb_localfiles
                    ),
                ),
                triggervariables=triggervars,
            ),
        )
    return tasks


@mimas.match_specification(
    is_rotation
    & ~is_characterization
    & is_processing
    & is_fast_ep
    & is_mx_beamline
    & is_anomalous
)
def handle_fast_ep(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    triggervars: Tuple[mimas.MimasISPyBTriggerVariable, ...] = (
        mimas.MimasISPyBTriggerVariable(
            "ispyb_autoprocscalingid", str(scenario.autoprocscaling_id)
        ),
    )
    recipe_name = "postprocessing-fast-ep"
    print(f"scenario cloudburting: {pformat(scenario.cloudbursting)}")
    if scenario.cloudbursting:
        for el in scenario.cloudbursting:
            print(f"scenario element: {pformat(el)}")
            if el["cloud_spec"].is_satisfied_by(scenario) and any(
                r in recipe_name for r in el["recipes"]
            ):
                recipe_name += "-cloud"
                triggervars += (
                    mimas.MimasISPyBTriggerVariable("statistic-cluster", "iris"),
                )
                break
    tasks: list[mimas.Invocation] = [
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=False,
            recipe=recipe_name,
            source="automatic",
            comment=str(scenario.comment),
            displayname="fast_ep",
            parameters=(
                mimas.MimasISPyBParameter(key="data", value=str(scenario.mtz)),
                mimas.MimasISPyBParameter(
                    key="scaling_id", value=str(scenario.autoprocscaling_id)
                ),
            ),
            triggervariables=triggervars,
        ),
    ]
    return tasks


@mimas.match_specification(
    is_processing & is_big_ep & is_mx_beamline & ~is_industrial_visit & is_phasing
)
def handle_big_ep(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    bigep_path_ext = {
        "autoPROC": "autoPROC/ap-run",
        "autoPROC+STARANISO": "autoPROC-STARANISO/ap-run",
        "xia2 3dii": "xia2/3dii-run",
        "xia2 dials": "xia2/dials-run",
        "xia2 3dii (multi)": "multi-xia2/3dii",
        "xia2 dials (multi)": "multi-xia2/dials",
        "xia2.multiplex": "xia2.multiplex",
    }
    path_ext = bigep_path_ext.get(str(scenario.tag))
    if path_ext and scenario.spacegroup:
        path_ext += "-" + str(scenario.spacegroup)
    triggervars: Tuple[mimas.MimasISPyBTriggerVariable, ...] = (
        mimas.MimasISPyBTriggerVariable(
            "ispyb_autoprocscalingid", str(scenario.autoprocscaling_id)
        ),
        mimas.MimasISPyBTriggerVariable("path_ext", str(path_ext)),
    )
    recipe_name = "postprocessing-big-ep"
    print(f"scenario cloudburting: {pformat(scenario.cloudbursting)}")
    if scenario.cloudbursting:
        for el in scenario.cloudbursting:
            print(f"scenario element: {pformat(el)}")
            if el["cloud_spec"].is_satisfied_by(scenario) and any(
                r in recipe_name for r in el["recipes"]
            ):
                recipe_name += "-cloud"
                triggervars += (
                    mimas.MimasISPyBTriggerVariable("statistic-cluster", "iris"),
                )
                break
    tasks: list[mimas.Invocation] = []

    if not scenario.mtz or not scenario.scaled_unmerged_mtz:
        return tasks

    tasks.append(
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=False,
            recipe=recipe_name,
            source="automatic",
            comment=str(scenario.comment),
            displayname="big_ep",
            parameters=(
                mimas.MimasISPyBParameter(
                    key="data", value=str(scenario.mtz.resolve())
                ),
                mimas.MimasISPyBParameter(
                    key="scaled_unmerged_mtz",
                    value=str(scenario.scaled_unmerged_mtz.resolve()),
                ),
                mimas.MimasISPyBParameter(
                    key="program_id", value=str(scenario.autoprocprogram_id)
                ),
                mimas.MimasISPyBParameter(
                    key="scaling_id", value=str(scenario.autoprocscaling_id)
                ),
                mimas.MimasISPyBParameter(
                    key="upstream_source", value=str(scenario.upstream_source)
                ),
            ),
            triggervariables=triggervars,
        )
    )
    return tasks


@mimas.match_specification(
    is_processing
    & is_big_ep_launcher
    & is_mx_beamline
    & ~is_industrial_visit
    & is_phasing
)
def handle_big_ep_launcher(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    raise NotImplementedError()


@mimas.match_specification(
    is_rotation & ~is_characterization & is_processing & is_alphafold & is_mx_beamline
)
def handle_alphafold(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    raise NotImplementedError()


@mimas.match_specification(
    is_rotation & ~is_characterization & is_processing & is_multiplex & is_mx_beamline
)
def handle_multiplex(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    raise NotImplementedError()
