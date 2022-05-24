from __future__ import annotations

from typing import List, Tuple

from dlstbx import mimas
from dlstbx.mimas.core import (
    is_eiger,
    is_end,
    is_pilatus,
    is_start,
    xia2_dials_absorption_params,
)
from dlstbx.mimas.specification import BeamlineSpecification

is_i19 = BeamlineSpecification("i19-1") | BeamlineSpecification("i19-2")


@mimas.match_specification(is_i19 & is_start & is_pilatus)
def handle_i19_start_pilatus(
    scenario: mimas.MimasScenario, **kwargs
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="per-image-analysis-rotation"
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


@mimas.match_specification(is_i19 & is_start & is_eiger)
def handle_i19_start_eiger(
    scenario: mimas.MimasScenario, **kwargs
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID,
            recipe="per-image-analysis-rotation-swmr-i19",
        )
    ]


@mimas.match_specification(is_i19 & is_end & is_eiger)
def handle_i19_end_eiger(
    scenario: mimas.MimasScenario, **kwargs
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
        for recipe in (
            "archive-nexus",
            "processing-rlv-eiger",
            "generate-diffraction-preview",
            "strategy-screen19-eiger",
        )
    ]


@mimas.match_specification(is_i19 & is_end)
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

    for params in extra_params:
        tasks.extend(
            [
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe="autoprocessing-multi-xia2-smallmolecule"
                    if scenario.detectorclass is mimas.MimasDetectorClass.PILATUS
                    else "autoprocessing-multi-xia2-smallmolecule-nexus",
                    source="automatic",
                    sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    parameters=(
                        *params,
                        *xia2_dials_absorption_params(scenario),
                    ),
                ),
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe="autoprocessing-multi-xia2-smallmolecule-dials-aiml"
                    if scenario.detectorclass is mimas.MimasDetectorClass.PILATUS
                    else "autoprocessing-multi-xia2-smallmolecule-d-a-nexus",
                    source="automatic",
                    sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    parameters=params,
                ),
            ]
        )

    return tasks
