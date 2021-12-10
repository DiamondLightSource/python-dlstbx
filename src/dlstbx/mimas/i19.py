import dlstbx.mimas
from dlstbx.mimas.core import (
    HandleScenarioReturnType,
    handle_scenario,
    is_eiger,
    is_end,
    is_i19,
    is_pilatus,
    is_start,
    xia2_dials_absorption_params,
)


@handle_scenario(is_i19 & is_start & is_pilatus)
def handle_i19_start_pilatus(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="per-image-analysis-rotation"
        )
    ]


@handle_scenario(is_i19 & is_start & is_eiger)
def handle_i19_start_eiger(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID,
            recipe="per-image-analysis-rotation-swmr-i19",
        )
    ]


@handle_scenario(is_i19 & is_end & is_pilatus)
def handle_i19_end_pilatus(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
        for recipe in ("archive-cbfs", "processing-rlv", "strategy-screen19")
    ]


@handle_scenario(is_i19 & is_end & is_eiger)
def handle_i19_end_eiger(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
        for recipe in (
            "archive-nexus",
            "processing-rlv-eiger",
            "generate-diffraction-preview",
            "strategy-screen19-eiger",
        )
    ]


@handle_scenario(is_i19 & is_end)
def handle_i19_end(scenario: dlstbx.mimas.MimasScenario) -> HandleScenarioReturnType:
    tasks = [
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
        ),
        dlstbx.mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=True,
            recipe="autoprocessing-multi-xia2-smallmolecule"
            if scenario.detectorclass is dlstbx.mimas.MimasDetectorClass.PILATUS
            else "autoprocessing-multi-xia2-smallmolecule-nexus",
            source="automatic",
            sweeps=tuple(scenario.getsweepslistfromsamedcg),
            parameters=xia2_dials_absorption_params(scenario),
        ),
        dlstbx.mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=True,
            recipe="autoprocessing-multi-xia2-smallmolecule-dials-aiml"
            if scenario.detectorclass is dlstbx.mimas.MimasDetectorClass.PILATUS
            else "autoprocessing-multi-xia2-smallmolecule-d-a-nexus",
            source="automatic",
            sweeps=tuple(scenario.getsweepslistfromsamedcg),
        ),
    ]

    if scenario.spacegroup:
        # Space group is set, run xia2 with space group
        spacegroup = scenario.spacegroup.string
        symmetry_parameters = (
            dlstbx.mimas.MimasISPyBParameter(key="spacegroup", value=spacegroup),
        )
        if scenario.unitcell:
            symmetry_parameters += (
                dlstbx.mimas.MimasISPyBParameter(
                    key="unit_cell", value=scenario.unitcell.string
                ),
            )

        tasks.extend(
            [
                dlstbx.mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe="autoprocessing-multi-xia2-smallmolecule"
                    if scenario.detectorclass is dlstbx.mimas.MimasDetectorClass.PILATUS
                    else "autoprocessing-multi-xia2-smallmolecule-nexus",
                    source="automatic",
                    sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    parameters=(
                        *symmetry_parameters,
                        *xia2_dials_absorption_params(scenario),
                    ),
                ),
                dlstbx.mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe="autoprocessing-multi-xia2-smallmolecule-dials-aiml"
                    if scenario.detectorclass is dlstbx.mimas.MimasDetectorClass.PILATUS
                    else "autoprocessing-multi-xia2-smallmolecule-d-a-nexus",
                    source="automatic",
                    sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    parameters=symmetry_parameters,
                ),
            ]
        )

    return tasks
