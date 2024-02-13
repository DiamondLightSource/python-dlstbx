from __future__ import annotations

from typing import List, Tuple

import zocalo.configuration

from dlstbx import mimas
from dlstbx.mimas.specification import (
    BeamlineSpecification,
    DCClassSpecification,
    DetectorClassSpecification,
    EventSpecification,
)

MX_BEAMLINES = {"i02-1", "i02-2", "i03", "i04", "i04-1", "i23", "i24"}
is_vmxi = BeamlineSpecification("i02-2")
is_i03 = BeamlineSpecification("i03")
is_i04_1 = BeamlineSpecification("i04-1")
is_i04 = BeamlineSpecification("i04")
is_i24 = BeamlineSpecification("i24")
is_mx_beamline = BeamlineSpecification(beamlines=MX_BEAMLINES)
is_pilatus = DetectorClassSpecification(mimas.MimasDetectorClass.PILATUS)
is_eiger = DetectorClassSpecification(mimas.MimasDetectorClass.EIGER)
is_start = EventSpecification(mimas.MimasEvent.START)
is_end = EventSpecification(mimas.MimasEvent.END)
is_start_group = EventSpecification(mimas.MimasEvent.START_GROUP)
is_end_group = EventSpecification(mimas.MimasEvent.END_GROUP)
is_gridscan = DCClassSpecification(mimas.MimasDCClass.GRIDSCAN)
is_serial_fixed = DCClassSpecification(mimas.MimasDCClass.SERIAL_FIXED)
is_serial_jet = DCClassSpecification(mimas.MimasDCClass.SERIAL_JET)
is_serial = is_serial_fixed | is_serial_jet
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
    is_pilatus & ~is_gridscan & ~is_serial & is_start & is_mx_beamline & ~is_vmxi
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


@mimas.match_specification(is_eiger & is_end & is_mx_beamline & ~is_vmxi & ~is_serial)
def handle_eiger_end_i03(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    suffix_grid = "-swmr-vmxm" if scenario.beamline == "i02-1" else "-i03"
    suffix_rot = "-vmxm" if scenario.beamline == "i02-1" else ""
    recipe = (
        f"per-image-analysis-gridscan{suffix_grid}"
        if scenario.dcclass is mimas.MimasDCClass.GRIDSCAN
        else f"per-image-analysis-rotation-swmr{suffix_rot}"
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


@mimas.match_specification(is_pilatus & is_end & is_mx_beamline & ~is_vmxi & ~is_serial)
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
    zc: zocalo.configuration.Configuration,
    **kwargs,
) -> List[mimas.Invocation]:
    suffix = suffix_pref = (
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
                displayname="fast_dp",
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
                displayname="fast_dp",
            )
        )

    xia2_dials_beamline_extra_params: ParamTuple = ()
    if scenario.beamline == "i02-1":
        xia2_dials_beamline_extra_params = (
            *XIA2_DIALS_COPPER_RINGS_PARAMS,
            mimas.MimasISPyBParameter(key="remove_blanks", value="true"),
            mimas.MimasISPyBParameter(key="failover", value="true"),
        )

    triggervars_pref: Tuple[mimas.MimasISPyBTriggerVariable, ...] = ()
    triggervars: Tuple[mimas.MimasISPyBTriggerVariable, ...] = ()
    cloud_recipes: set[str] = set()
    if scenario.cloudbursting and "eiger" in suffix:
        for el in scenario.cloudbursting:
            if el["cloud_spec"].is_satisfied_by(scenario):
                suffix = "-eiger-cloud"
                cloud_recipes.update(el.get("recipes", ["autoprocessing"]))
                triggervars = (
                    mimas.MimasISPyBTriggerVariable("statistic-cluster", "iris"),
                )

    ppl_autostart: dict[str, bool] = {}
    ppl_suffix: dict[str, str] = {}
    ppl_triggervars: dict[str, Tuple[mimas.MimasISPyBTriggerVariable, ...]] = {}
    for ppl, recipe in (
        ("xia2/DIALS", "autoprocessing-xia2-dials"),
        ("xia2/XDS", "autoprocessing-xia2-3dii"),
        ("autoPROC", "autoprocessing-autoPROC"),
        ("mxia2/DIALS", "autoprocessing-multi-xia2-dials"),
        ("mxia2/XDS", "autoprocessing-multi-xia2-3dii"),
    ):
        ppl_autostart[ppl] = False
        ppl_suffix[ppl] = suffix_pref
        ppl_triggervars[ppl] = triggervars_pref
        if scenario.preferred_processing == ppl:
            ppl_autostart[ppl] = True
        elif any(r in recipe for r in cloud_recipes):
            ppl_suffix[ppl] = suffix
            ppl_triggervars[ppl] = triggervars

    for params in extra_params:
        tasks.extend(
            [
                # xia2-dials
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=ppl_autostart["xia2/DIALS"],
                    recipe=f"autoprocessing-xia2-dials{ppl_suffix['xia2/DIALS']}",
                    source="automatic",
                    displayname="xia2 dials",
                    parameters=(
                        mimas.MimasISPyBParameter(
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
                        *params,
                        *xia2_dials_beamline_extra_params,
                        *xia2_dials_absorption_params(scenario),
                    ),
                    triggervariables=ppl_triggervars["xia2/DIALS"],
                ),
                # xia2-3dii
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=ppl_autostart["xia2/XDS"],
                    recipe=f"autoprocessing-xia2-3dii{ppl_suffix['xia2/XDS']}",
                    source="automatic",
                    displayname="xia2 3dii",
                    parameters=(
                        mimas.MimasISPyBParameter(
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
                        *params,
                    ),
                    triggervariables=ppl_triggervars["xia2/XDS"],
                ),
                # autoPROC
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=ppl_autostart["autoPROC"],
                    recipe=f"autoprocessing-autoPROC{ppl_suffix['autoPROC']}",
                    source="automatic",
                    displayname="autoPROC",
                    parameters=params,
                    triggervariables=ppl_triggervars["autoPROC"],
                ),
            ]
        )

        if has_related_data_collections(scenario):
            tasks.extend(
                [
                    # xia2-dials
                    mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,  # no priority processing for multi-xia2
                        recipe=f"autoprocessing-multi-xia2-dials{ppl_suffix['mxia2/DIALS']}",
                        source="automatic",
                        displayname="xia2 dials (multi)",
                        parameters=(
                            mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                            *params,
                            *xia2_dials_absorption_params(scenario),
                        ),
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                        triggervariables=ppl_triggervars["mxia2/DIALS"],
                    ),
                    # xia2-3dii
                    mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,  # no priority processing for multi-xia2
                        recipe=f"autoprocessing-multi-xia2-3dii{ppl_suffix['mxia2/XDS']}",
                        source="automatic",
                        displayname="xia2 3dii (multi)",
                        parameters=(
                            mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                            *params,
                        ),
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                        triggervariables=ppl_triggervars["mxia2/XDS"],
                    ),
                ]
            )

    return tasks
