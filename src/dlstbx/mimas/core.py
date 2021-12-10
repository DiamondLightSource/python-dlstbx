import functools
from typing import Callable, List, Tuple, Union

import dlstbx.mimas
from dlstbx.mimas.specification import (
    BeamlineSpecification,
    DCClassSpecification,
    DetectorClassSpecification,
    EventSpecification,
    ScenarioSpecification,
    VisitSpecification,
)

HandleScenarioReturnType = List[
    Union[dlstbx.mimas.MimasISPyBJobInvocation, dlstbx.mimas.MimasRecipeInvocation]
]


class ScenarioHandler:
    handlers = []

    @classmethod
    def register(cls, scenario_specfication: ScenarioSpecification) -> Callable:
        def outer_wrapper(handler: Callable) -> Callable:
            @functools.wraps(handler)
            def inner_wrapper(scenario: dlstbx.mimas.MimasScenario):
                if scenario_specfication.is_satisfied_by(scenario):
                    print(
                        f"Specification {scenario_specfication} is satisfied by :\n  {scenario}"
                    )
                    return functools.partial(handler, scenario)

            cls.handlers.append(inner_wrapper)
            return inner_wrapper

        return outer_wrapper

    @classmethod
    def handle(cls, scenario: dlstbx.mimas.MimasScenario) -> HandleScenarioReturnType:
        tasks: HandleScenarioReturnType = []
        for handler in reversed(cls.handlers):
            handle_scenario = handler(scenario)
            if handle_scenario:
                tasks.extend(handle_scenario())
        return tasks


is_i19 = BeamlineSpecification("i19-1") | BeamlineSpecification("i19-2")
is_vmxi = BeamlineSpecification("i02-2")
i19_or_vmxi = is_i19 | is_vmxi
is_pilatus = DetectorClassSpecification(dlstbx.mimas.MimasDetectorClass.PILATUS)
is_eiger = DetectorClassSpecification(dlstbx.mimas.MimasDetectorClass.EIGER)
is_start = EventSpecification(dlstbx.mimas.MimasEvent.START)
is_end = EventSpecification(dlstbx.mimas.MimasEvent.END)
is_gridscan = DCClassSpecification(dlstbx.mimas.MimasDCClass.GRIDSCAN)
is_rotation = DCClassSpecification(dlstbx.mimas.MimasDCClass.ROTATION)
is_screening = DCClassSpecification(dlstbx.mimas.MimasDCClass.SCREENING)


def xia2_dials_absorption_params(
    scenario: dlstbx.mimas.MimasScenario,
) -> Tuple[dlstbx.mimas.MimasISPyBParameter]:
    # Decide absorption_level for xia2-dials jobs
    if scenario.anomalous_scatterer:
        absorption_level = "high"
    else:
        absorption_level = "medium"
    return (
        dlstbx.mimas.MimasISPyBParameter(
            key="absorption_level", value=absorption_level
        ),
    )


@ScenarioHandler.register(is_pilatus & is_gridscan & is_start & ~i19_or_vmxi)
def handle_pilatus_gridscan_start(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="archive-cbfs"),
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="per-image-analysis-gridscan"
        ),
    ]


@ScenarioHandler.register(is_pilatus & ~is_gridscan & is_start & ~i19_or_vmxi)
def handle_pilatus_not_gridscan_start(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="archive-cbfs"),
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="per-image-analysis-rotation"
        ),
    ]


@ScenarioHandler.register(is_eiger & is_start & ~i19_or_vmxi)
def handle_eiger_start(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    if scenario.dcclass is dlstbx.mimas.MimasDCClass.GRIDSCAN:
        return [
            dlstbx.mimas.MimasRecipeInvocation(
                DCID=scenario.DCID, recipe="per-image-analysis-gridscan-swmr"
            )
        ]
    else:
        return [
            dlstbx.mimas.MimasRecipeInvocation(
                DCID=scenario.DCID, recipe="per-image-analysis-rotation-swmr"
            )
        ]


@ScenarioHandler.register(is_eiger & is_end & ~i19_or_vmxi)
def handle_eiger_end(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    stopped = scenario.runstatus == "DataCollection Stopped"
    tasks = [
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
        ),
        dlstbx.mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="archive-nexus"),
    ]
    if not stopped:
        tasks.append(
            dlstbx.mimas.MimasRecipeInvocation(
                DCID=scenario.DCID, recipe="generate-diffraction-preview"
            )
        )
    return tasks


@ScenarioHandler.register(is_pilatus & is_end & ~i19_or_vmxi)
def handle_pilatus_end(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
        )
    ]


@ScenarioHandler.register(is_pilatus & is_end & is_screening & ~i19_or_vmxi)
def handle_pilatus_screening_end(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="strategy-edna"),
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="strategy-mosflm"
        ),
    ]


@ScenarioHandler.register(is_eiger & is_screening & is_end & ~i19_or_vmxi)
def handle_eiger_screening(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
        for recipe in (
            "strategy-align-crystal",
            "strategy-mosflm",
            "strategy-edna-eiger",
        )
    ]


@ScenarioHandler.register(is_pilatus & is_screening & is_end & ~i19_or_vmxi)
def handle_pilatus_screening(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
        for recipe in (
            "strategy-mosflm",
            "strategy-edna",
        )
    ]


def has_related_data_collections(scenario: dlstbx.mimas.MimasScenario):
    return (
        scenario.dcclass is dlstbx.mimas.MimasDCClass.ROTATION
        and scenario.getsweepslistfromsamedcg
        and any(
            sweep.DCID != scenario.DCID for sweep in scenario.getsweepslistfromsamedcg
        )
    )


@ScenarioHandler.register(is_rotation & is_end & ~i19_or_vmxi)
def handle_rotation_end(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:

    tasks = [
        # RLV
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID,
            recipe="processing-rlv"
            if scenario.detectorclass is dlstbx.mimas.MimasDetectorClass.PILATUS
            else "processing-rlv-eiger",
        ),
    ]

    extra_params = [()]
    if scenario.spacegroup:
        spacegroup = scenario.spacegroup.string
        # if spacegroup == "P1211":
        #     spacegroup = "P21"  # I04-1 hothothotfix for 20190508 only
        # if spacegroup == "C1211":
        #     spacegroup = "C2"  # I04-1 hothothotfix for 20190510 only
        # if spacegroup == "C121":
        #     spacegroup = "C2"  # I03 hothothotfix for 20190510 only

        if scenario.unitcell:
            spacegroup_params = (
                dlstbx.mimas.MimasISPyBParameter(key="spacegroup", value=spacegroup),
                dlstbx.mimas.MimasISPyBParameter(
                    key="unit_cell", value=scenario.unitcell.string
                ),
            )
        else:
            spacegroup_params = (
                dlstbx.mimas.MimasISPyBParameter(key="spacegroup", value=spacegroup),
            )

        extra_params.append(spacegroup_params)

        # Only run fast_dp with spacegroup set
        tasks.append(
            dlstbx.mimas.MimasISPyBJobInvocation(
                DCID=scenario.DCID,
                autostart=True,
                recipe="autoprocessing-fast-dp"
                if scenario.detectorclass is dlstbx.mimas.MimasDetectorClass.PILATUS
                else "autoprocessing-fast-dp-eiger",
                source="automatic",
                parameters=(
                    dlstbx.mimas.MimasISPyBParameter(
                        key="spacegroup", value=spacegroup
                    ),
                ),
            )
        )
    else:
        # Only run fast_dp without spacegroup set
        tasks.append(
            dlstbx.mimas.MimasISPyBJobInvocation(
                DCID=scenario.DCID,
                autostart=True,
                recipe="autoprocessing-fast-dp"
                if scenario.detectorclass is dlstbx.mimas.MimasDetectorClass.PILATUS
                else "autoprocessing-fast-dp-eiger",
                source="automatic",
            )
        )

    for params in extra_params:
        tasks.extend(
            [
                # xia2-dials
                dlstbx.mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=scenario.preferred_processing == "xia2/DIALS",
                    recipe="autoprocessing-xia2-dials"
                    if scenario.detectorclass is dlstbx.mimas.MimasDetectorClass.PILATUS
                    else "autoprocessing-xia2-dials-eiger-cluster",
                    source="automatic",
                    parameters=(
                        dlstbx.mimas.MimasISPyBParameter(
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
                        *params,
                        *xia2_dials_absorption_params(scenario),
                    ),
                ),
                # xia2-3dii
                dlstbx.mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=scenario.preferred_processing == "xia2/XDS",
                    recipe="autoprocessing-xia2-3dii"
                    if scenario.detectorclass is dlstbx.mimas.MimasDetectorClass.PILATUS
                    else "autoprocessing-xia2-3dii-eiger-cluster",
                    source="automatic",
                    parameters=(
                        dlstbx.mimas.MimasISPyBParameter(
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
                        *params,
                    ),
                ),
                # autoPROC
                dlstbx.mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=scenario.preferred_processing == "autoPROC",
                    recipe="autoprocessing-autoPROC"
                    if scenario.detectorclass is dlstbx.mimas.MimasDetectorClass.PILATUS
                    else "autoprocessing-autoPROC-eiger-cluster",
                    source="automatic",
                    parameters=params,
                ),
            ]
        )

        if has_related_data_collections(scenario):
            tasks.extend(
                [
                    # xia2-dials
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,
                        recipe="autoprocessing-multi-xia2-dials"
                        if scenario.detectorclass
                        is dlstbx.mimas.MimasDetectorClass.PILATUS
                        else "autoprocessing-multi-xia2-dials-eiger-cluster",
                        source="automatic",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                            *params,
                            *xia2_dials_absorption_params(scenario),
                        ),
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    ),
                    # xia2-3dii
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,
                        recipe="autoprocessing-multi-xia2-3dii"
                        if scenario.detectorclass
                        is dlstbx.mimas.MimasDetectorClass.PILATUS
                        else "autoprocessing-multi-xia2-3dii-eiger-cluster",
                        source="automatic",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                            *params,
                        ),
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    ),
                ]
            )

    return tasks


@ScenarioHandler.register(is_vmxi & is_start)
def handle_vmxi_start_event(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    # Do nothing
    return []


@ScenarioHandler.register(is_vmxi & is_end)
def handle_vmxi_end_event(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
        ),
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="generate-diffraction-preview"
        ),
        dlstbx.mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="archive-nexus"),
    ]


@ScenarioHandler.register(is_vmxi & is_end & is_gridscan)
def handle_vmxi_gridscan(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="vmxi-spot-counts-per-image"
        )
    ]


@ScenarioHandler.register(is_vmxi & is_end & is_rotation)
def handle_vmxi_rotation_scan(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        # Per-image analysis
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="vmxi-per-image-analysis"
        ),
        # fast_dp
        dlstbx.mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=True,
            recipe="autoprocessing-fast-dp-eiger",
            source="automatic",
        ),
        # xia2-dials
        dlstbx.mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=scenario.preferred_processing == "xia2/DIALS",
            recipe="autoprocessing-xia2-dials-eiger",
            source="automatic",
            parameters=(
                dlstbx.mimas.MimasISPyBParameter(
                    key="resolution.cc_half_significance_level",
                    value="0.1",
                ),
                dlstbx.mimas.MimasISPyBParameter(key="remove_blanks", value="true"),
                dlstbx.mimas.MimasISPyBParameter(key="failover", value="true"),
                *xia2_dials_absorption_params(scenario),
            ),
        ),
        # xia2-3dii
        dlstbx.mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=scenario.preferred_processing == "xia2/XDS",
            recipe="autoprocessing-xia2-3dii-eiger",
            source="automatic",
            parameters=(
                dlstbx.mimas.MimasISPyBParameter(
                    key="resolution.cc_half_significance_level",
                    value="0.1",
                ),
            ),
        ),
        # autoPROC
        dlstbx.mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=scenario.preferred_processing == "autoPROC",
            recipe="autoprocessing-autoPROC-eiger",
            source="automatic",
        ),
    ]


@ScenarioHandler.register(is_i19 & is_start & is_pilatus)
def handle_i19_start_pilatus(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="per-image-analysis-rotation"
        )
    ]


@ScenarioHandler.register(is_i19 & is_start & is_eiger)
def handle_i19_start_eiger(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID,
            recipe="per-image-analysis-rotation-swmr-i19",
        )
    ]


@ScenarioHandler.register(is_i19 & is_end & is_pilatus)
def handle_i19_end_pilatus(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
        for recipe in ("archive-cbfs", "processing-rlv", "strategy-screen19")
    ]


@ScenarioHandler.register(is_i19 & is_end & is_eiger)
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


@ScenarioHandler.register(is_i19 & is_end)
def handle_i19(scenario: dlstbx.mimas.MimasScenario) -> HandleScenarioReturnType:
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


CLOUD_VISITS = {
    "cm",
    "nt28218",
    "mx23694",
    "mx30951",
    "lb28215",
    "mx23459-213",
}


is_cloud = (
    VisitSpecification(CLOUD_VISITS)
    & is_end
    & is_rotation
    & BeamlineSpecification("i03")
    & BeamlineSpecification("i04-1")
)


@ScenarioHandler.register(is_cloud)
def handle_cloud(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        # xia2-dials
        dlstbx.mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=True,
            recipe="autoprocessing-xia2-dials-eiger-cloud",
            source="automatic",
            parameters=(
                dlstbx.mimas.MimasISPyBParameter(
                    key="resolution.cc_half_significance_level", value="0.1"
                ),
                *xia2_dials_absorption_params(scenario),
            ),
        ),
        # xia2-3dii
        dlstbx.mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=True,
            recipe="autoprocessing-xia2-3dii-eiger-cluster",
            source="automatic",
            parameters=(
                dlstbx.mimas.MimasISPyBParameter(
                    key="resolution.cc_half_significance_level", value="0.1"
                ),
            ),
        ),
        # autoPROC
        dlstbx.mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=True,
            recipe="autoprocessing-autoPROC-eiger-cluster",
            source="automatic",
        ),
    ]


def run(
    scenario: dlstbx.mimas.MimasScenario,
) -> List[
    Union[dlstbx.mimas.MimasRecipeInvocation, dlstbx.mimas.MimasISPyBJobInvocation]
]:
    return ScenarioHandler.handle(scenario)
