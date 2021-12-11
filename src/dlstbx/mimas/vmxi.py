import dlstbx.mimas
from dlstbx.mimas.core import (
    HandleScenarioReturnType,
    is_end,
    is_gridscan,
    is_rotation,
    is_start,
    is_vmxi,
    match_specification,
    xia2_dials_absorption_params,
)


@match_specification(is_vmxi & is_start)
def handle_vmxi_start(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    # Do nothing
    return []


@match_specification(is_vmxi & is_end)
def handle_vmxi_end(
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


@match_specification(is_vmxi & is_end & is_gridscan)
def handle_vmxi_gridscan(
    scenario: dlstbx.mimas.MimasScenario,
) -> HandleScenarioReturnType:
    return [
        dlstbx.mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="vmxi-spot-counts-per-image"
        )
    ]


@match_specification(is_vmxi & is_end & is_rotation)
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
