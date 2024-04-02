from __future__ import annotations

from typing import List, Tuple

from dlstbx import mimas
from dlstbx.mimas.core import (
    is_eiger,
    is_end,
    is_end_group,
    is_pilatus,
    is_serial,
    is_start,
    xia2_dials_absorption_params,
)
from dlstbx.mimas.specification import BeamlineSpecification

is_i19 = BeamlineSpecification("i19-1") | BeamlineSpecification("i19-2")


XIA2_DIALS_DAC_PARAMS: Tuple[mimas.MimasISPyBParameter, ...] = (
    mimas.MimasISPyBParameter(key="dynamic_shadowing", value="true"),
    mimas.MimasISPyBParameter(key="ice_rings.filter", value="true"),
    mimas.MimasISPyBParameter(
        key="ice_rings.unit_cell", value="3.1652,3.1652,3.1652,90,90,90"
    ),
    mimas.MimasISPyBParameter(key="ice_rings.space_group", value="Im-3m"),
    mimas.MimasISPyBParameter(key="ice_rings.width", value="0.01"),
    mimas.MimasISPyBParameter(key="scan_varying", value="true"),
    mimas.MimasISPyBParameter(key="resolution_range", value="999,15"),
    mimas.MimasISPyBParameter(key="keep_all_reflections", value="false"),
    mimas.MimasISPyBParameter(key="cc_half", value="none"),
    mimas.MimasISPyBParameter(key="isigma", value="2"),
)


@mimas.match_specification(is_i19 & is_start & is_pilatus)
def handle_i19_start_pilatus(
    scenario: mimas.MimasScenario, **kwargs
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="per-image-analysis-rotation-i19"
        )
    ]


@mimas.match_specification(is_i19 & is_end & is_pilatus)
def handle_i19_end_pilatus(
    scenario: mimas.MimasScenario, **kwargs
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
        for recipe in ("archive-cbfs", "processing-rlv", "strategy-screen19")
    ]


@mimas.match_specification(is_i19 & is_end & is_eiger & is_serial)
def handle_i19_end_eiger_serial(
    scenario: mimas.MimasScenario, **kwargs
) -> List[mimas.Invocation]:
    tasks: list[mimas.Invocation] = []
    #     mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
    #     for recipe in (
    #         "per-image-analysis-rotation-serial-cs05r",
    #         "processing-rlv-eiger",
    #         "generate-diffraction-preview",
    #         "strategy-screen19-eiger",
    #     )
    # ]

    ParamTuple = Tuple[mimas.MimasISPyBParameter, ...]
    extra_params: List[ParamTuple] = [()]
    if scenario.spacegroup:
        # Space group is set, run xia2 with space group
        spacegroup = scenario.spacegroup.string
        symmetry_parameters: ParamTuple = (
            mimas.MimasISPyBParameter(key="spacegroup", value=spacegroup),
        )
        if scenario.unitcell:
            symmetry_parameters += (
                mimas.MimasISPyBParameter(
                    key="unit_cell", value=scenario.unitcell.string
                ),
            )
        extra_params.append(symmetry_parameters)

    # Find the scenario sweep that matches this DCID
    dcid_sweep = [
        x for x in scenario.getsweepslistfromsamedcg if x.DCID == scenario.DCID
    ]
    assert len(dcid_sweep) == 1

    for params in extra_params:
        tasks.extend(
            [
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe="autoprocessing-multi-xia2-smallmolecule-nexus",
                    source="automatic",
                    sweeps=tuple(dcid_sweep),
                    displayname="xia2 dials",
                    parameters=(
                        *params,
                        *xia2_dials_absorption_params(scenario),
                    ),
                ),
            ]
        )
    return tasks


@mimas.match_specification(is_i19 & is_end_group & is_eiger & is_serial)
def handle_i19_end_group_eiger_serial(
    scenario: mimas.MimasScenario, **kwargs
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="autoprocessing-i19serial-groupend"
        )
    ]


@mimas.match_specification(is_i19 & is_end & is_eiger & ~is_serial)
def handle_i19_end_eiger(
    scenario: mimas.MimasScenario, **kwargs
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
        for recipe in (
            "archive-nexus",
            "processing-rlv-eiger",
            "generate-diffraction-preview",
            "per-image-analysis-rotation-swmr-i19",
            "strategy-screen19-eiger",
        )
    ]


@mimas.match_specification(is_i19 & is_end & ~is_serial)
def handle_i19_end(scenario: mimas.MimasScenario, **kwargs) -> List[mimas.Invocation]:
    tasks: List[mimas.Invocation] = [
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
        )
    ]

    ParamTuple = Tuple[mimas.MimasISPyBParameter, ...]
    extra_params: List[ParamTuple] = [()]
    if scenario.spacegroup:
        # Space group is set, run xia2 with space group
        spacegroup = scenario.spacegroup.string
        symmetry_parameters: ParamTuple = (
            mimas.MimasISPyBParameter(key="spacegroup", value=spacegroup),
        )
        if scenario.unitcell:
            symmetry_parameters += (
                mimas.MimasISPyBParameter(
                    key="unit_cell", value=scenario.unitcell.string
                ),
            )
        extra_params.append(symmetry_parameters)

    # if the scenario is flagged as high pressure, add the dials high pressure flags
    xia2_dials_extra_params: ParamTuple = ()
    if scenario.dcclass == mimas.MimasDCClass.DIAMOND_ANVIL_CELL:
        xia2_dials_extra_params = (*XIA2_DIALS_DAC_PARAMS,)

    for params in extra_params:
        tasks.extend(
            [
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe=(
                        "autoprocessing-multi-xia2-smallmolecule"
                        if scenario.detectorclass is mimas.MimasDetectorClass.PILATUS
                        else "autoprocessing-multi-xia2-smallmolecule-nexus"
                    ),
                    source="automatic",
                    sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    displayname="xia2 dials",
                    parameters=(
                        *params,
                        *xia2_dials_absorption_params(scenario),
                        *xia2_dials_extra_params,
                    ),
                ),
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe=(
                        "autoprocessing-multi-xia2-smallmolecule-dials-aiml"
                        if scenario.detectorclass is mimas.MimasDetectorClass.PILATUS
                        else "autoprocessing-multi-xia2-smallmolecule-d-a-nexus"
                    ),
                    source="automatic",
                    sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    displayname="xia2 dials-aimless",
                    parameters=(
                        *params,
                        *xia2_dials_extra_params,
                    ),
                ),
            ]
        )

    return tasks
