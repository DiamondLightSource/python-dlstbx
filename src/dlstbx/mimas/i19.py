from typing import List

from dlstbx import mimas
from dlstbx.mimas.core import (
    is_eiger,
    is_end,
    is_i19,
    is_pilatus,
    is_start,
    xia2_dials_absorption_params,
)


@mimas.match_specification(is_i19 & is_start & is_pilatus)
def handle_i19_start_pilatus(
    scenario: mimas.MimasScenario,
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="per-image-analysis-rotation"
        )
    ]


@mimas.match_specification(is_i19 & is_start & is_eiger)
def handle_i19_start_eiger(
    scenario: mimas.MimasScenario,
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID,
            recipe="per-image-analysis-rotation-swmr-i19",
        )
    ]


@mimas.match_specification(is_i19 & is_end & is_pilatus)
def handle_i19_end_pilatus(
    scenario: mimas.MimasScenario,
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
        for recipe in ("archive-cbfs", "processing-rlv", "strategy-screen19")
    ]


@mimas.match_specification(is_i19 & is_end & is_eiger)
def handle_i19_end_eiger(
    scenario: mimas.MimasScenario,
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
def handle_i19_end(scenario: mimas.MimasScenario) -> List[mimas.Invocation]:
    tasks = [
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
        ),
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=True,
            recipe="autoprocessing-multi-xia2-smallmolecule"
            if scenario.detectorclass is mimas.MimasDetectorClass.PILATUS
            else "autoprocessing-multi-xia2-smallmolecule-nexus",
            source="automatic",
            sweeps=tuple(scenario.getsweepslistfromsamedcg),
            parameters=xia2_dials_absorption_params(scenario),
        ),
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=True,
            recipe="autoprocessing-multi-xia2-smallmolecule-dials-aiml"
            if scenario.detectorclass is mimas.MimasDetectorClass.PILATUS
            else "autoprocessing-multi-xia2-smallmolecule-d-a-nexus",
            source="automatic",
            sweeps=tuple(scenario.getsweepslistfromsamedcg),
        ),
    ]

    if scenario.spacegroup:
        # Space group is set, run xia2 with space group
        spacegroup = scenario.spacegroup.string
        symmetry_parameters = (
            mimas.MimasISPyBParameter(key="spacegroup", value=spacegroup),
        )
        if scenario.unitcell:
            symmetry_parameters += (
                mimas.MimasISPyBParameter(
                    key="unit_cell", value=scenario.unitcell.string
                ),
            )

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
                        *symmetry_parameters,
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
                    parameters=symmetry_parameters,
                ),
            ]
        )

    return tasks
