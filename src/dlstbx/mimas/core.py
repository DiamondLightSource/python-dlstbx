from __future__ import annotations

from typing import List, Tuple

from dlstbx import mimas
from dlstbx.mimas.specification import (
    BeamlineSpecification,
    DCClassSpecification,
    DetectorClassSpecification,
    EventSpecification,
)

MX_BEAMLINES = {"i02-1", "i02-2", "i03", "i04", "i04-1", "i23", "i24"}
is_vmxi = BeamlineSpecification("i02-2")
is_mx_beamline = BeamlineSpecification(beamlines=MX_BEAMLINES)
is_pilatus = DetectorClassSpecification(mimas.MimasDetectorClass.PILATUS)
is_eiger = DetectorClassSpecification(mimas.MimasDetectorClass.EIGER)
is_start = EventSpecification(mimas.MimasEvent.START)
is_end = EventSpecification(mimas.MimasEvent.END)
is_gridscan = DCClassSpecification(mimas.MimasDCClass.GRIDSCAN)
is_rotation = DCClassSpecification(mimas.MimasDCClass.ROTATION)
is_screening = DCClassSpecification(mimas.MimasDCClass.SCREENING)


XIA2_DIALS_COPPER_RINGS_PARAMS: Tuple[mimas.MimasISPyBParameter, ...] = (
    mimas.MimasISPyBParameter(
        key="ice_rings.unit_cell", value="3.615,3.615,3.615,90,90,90"
    ),
    mimas.MimasISPyBParameter(key="ice_rings.space_group", value="fm-3m"),
    mimas.MimasISPyBParameter(key="ice_rings.width", value="0.01"),
    mimas.MimasISPyBParameter(key="ice_rings.filter", value="true"),
)


def xia2_dials_absorption_params(
    scenario: mimas.MimasScenario,
) -> Tuple[mimas.MimasISPyBParameter]:
    # Decide absorption_level for xia2-dials jobs
    absorption_level = "high" if scenario.anomalous_scatterer else "medium"
    return (mimas.MimasISPyBParameter(key="absorption_level", value=absorption_level),)


@mimas.match_specification(
    is_pilatus & is_gridscan & is_start & is_mx_beamline & ~is_vmxi
)
def handle_pilatus_gridscan_start(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="archive-cbfs"),
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="per-image-analysis-gridscan"
        ),
    ]


@mimas.match_specification(
    is_pilatus & ~is_gridscan & is_start & is_mx_beamline & ~is_vmxi
)
def handle_pilatus_not_gridscan_start(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="archive-cbfs"),
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="per-image-analysis-rotation"
        ),
    ]


@mimas.match_specification(is_eiger & is_start & is_mx_beamline & ~is_vmxi)
def handle_eiger_start(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    suffix = "-vmxm" if scenario.beamline == "i02-1" else ""
    recipe = (
        f"per-image-analysis-gridscan-swmr{suffix}"
        if scenario.dcclass is mimas.MimasDCClass.GRIDSCAN
        else f"per-image-analysis-rotation-swmr{suffix}"
    )
    return [mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)]


@mimas.match_specification(is_eiger & is_end & is_mx_beamline & ~is_vmxi)
def handle_eiger_end(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    stopped = scenario.runstatus == "DataCollection Stopped"
    tasks: List[mimas.Invocation] = [
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
        ),
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="archive-nexus"),
    ]
    if not stopped:
        tasks.append(
            mimas.MimasRecipeInvocation(
                DCID=scenario.DCID, recipe="generate-diffraction-preview"
            )
        )
    return tasks


@mimas.match_specification(is_pilatus & is_end & is_mx_beamline & ~is_vmxi)
def handle_pilatus_end(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
        )
    ]


@mimas.match_specification(is_eiger & is_screening & is_end & is_mx_beamline & ~is_vmxi)
def handle_eiger_screening(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
        for recipe in (
            "strategy-align-crystal",
            "strategy-mosflm",
            "strategy-edna-eiger",
        )
    ]


@mimas.match_specification(
    is_pilatus & is_screening & is_end & is_mx_beamline & ~is_vmxi
)
def handle_pilatus_screening(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
        for recipe in (
            "strategy-mosflm",
            "strategy-edna",
        )
    ]


def has_related_data_collections(scenario: mimas.MimasScenario):
    return (
        scenario.dcclass is mimas.MimasDCClass.ROTATION
        and scenario.getsweepslistfromsamedcg
        and any(
            sweep.DCID != scenario.DCID for sweep in scenario.getsweepslistfromsamedcg
        )
    )


@mimas.match_specification(is_rotation & is_end & is_mx_beamline & ~is_vmxi)
def handle_rotation_end(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:

    suffix = (
        "-eiger" if scenario.detectorclass is mimas.MimasDetectorClass.EIGER else ""
    )
    tasks: List[mimas.Invocation] = [
        # RLV
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID,
            recipe=f"processing-rlv{suffix}",
        ),
    ]

    ParamTuple = Tuple[mimas.MimasISPyBParameter, ...]
    extra_params: List[ParamTuple] = [()]
    if scenario.spacegroup:
        spacegroup = scenario.spacegroup.string
        # if spacegroup == "P1211":
        #     spacegroup = "P21"  # I04-1 hothothotfix for 20190508 only
        # if spacegroup == "C1211":
        #     spacegroup = "C2"  # I04-1 hothothotfix for 20190510 only
        # if spacegroup == "C121":
        #     spacegroup = "C2"  # I03 hothothotfix for 20190510 only

        if scenario.spacegroup:
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

        # Only run fast_dp with spacegroup set
        tasks.append(
            mimas.MimasISPyBJobInvocation(
                DCID=scenario.DCID,
                autostart=True,
                recipe=f"autoprocessing-fast-dp{suffix}",
                source="automatic",
                parameters=(
                    mimas.MimasISPyBParameter(key="spacegroup", value=spacegroup),
                ),
            )
        )
    else:
        # Only run fast_dp without spacegroup set
        tasks.append(
            mimas.MimasISPyBJobInvocation(
                DCID=scenario.DCID,
                autostart=True,
                recipe=f"autoprocessing-fast-dp{suffix}",
                source="automatic",
            )
        )

    xia2_dials_beamline_extra_params: ParamTuple = ()
    if scenario.beamline == "i02-1":
        xia2_dials_beamline_extra_params = (
            *XIA2_DIALS_COPPER_RINGS_PARAMS,
            mimas.MimasISPyBParameter(key="remove_blanks", value="true"),
            mimas.MimasISPyBParameter(key="failover", value="true"),
        )

    suffix = (
        "-eiger-cluster"
        if scenario.detectorclass is mimas.MimasDetectorClass.EIGER
        else ""
    )
    for params in extra_params:
        tasks.extend(
            [
                # xia2-dials
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=scenario.preferred_processing == "xia2/DIALS",
                    recipe=f"autoprocessing-xia2-dials{suffix}",
                    source="automatic",
                    parameters=(
                        mimas.MimasISPyBParameter(
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
                        *params,
                        *xia2_dials_beamline_extra_params,
                        *xia2_dials_absorption_params(scenario),
                    ),
                ),
                # xia2-3dii
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=scenario.preferred_processing == "xia2/XDS",
                    recipe=f"autoprocessing-xia2-3dii{suffix}",
                    source="automatic",
                    parameters=(
                        mimas.MimasISPyBParameter(
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
                        *params,
                    ),
                ),
                # autoPROC
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=scenario.preferred_processing == "autoPROC",
                    recipe=f"autoprocessing-autoPROC{suffix}",
                    source="automatic",
                    parameters=params,
                ),
            ]
        )

        if has_related_data_collections(scenario):
            tasks.extend(
                [
                    # xia2-dials
                    mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,
                        recipe=f"autoprocessing-multi-xia2-dials{suffix}",
                        source="automatic",
                        parameters=(
                            mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                            *params,
                            *xia2_dials_absorption_params(scenario),
                        ),
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    ),
                    # xia2-3dii
                    mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,
                        recipe=f"autoprocessing-multi-xia2-3dii{suffix}",
                        source="automatic",
                        parameters=(
                            mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                            *params,
                        ),
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    ),
                ]
            )

    return tasks
