from __future__ import annotations

from typing import List

from dlstbx import mimas
from dlstbx.mimas.core import (
    is_end,
    is_gridscan,
    is_rotation,
    is_start,
    is_vmxi,
    xia2_dials_absorption_params,
)


@mimas.match_specification(is_vmxi & is_start)
def handle_vmxi_start(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    # Do nothing
    return []


@mimas.match_specification(is_vmxi & is_end)
def handle_vmxi_end(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
        ),
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="generate-diffraction-preview"
        ),
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="archive-nexus"),
    ]


@mimas.match_specification(is_vmxi & is_end & is_gridscan)
def handle_vmxi_gridscan(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="vmxi-spot-counts-per-image"
        )
    ]


@mimas.match_specification(is_vmxi & is_end & is_rotation)
def handle_vmxi_rotation_scan(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    return [
        # Per-image analysis
        mimas.MimasRecipeInvocation(
            DCID=scenario.DCID, recipe="vmxi-per-image-analysis"
        ),
        # fast_dp
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=True,
            recipe="autoprocessing-fast-dp-eiger",
            source="automatic",
            displayname="fast_dp",
        ),
        # xia2-dials
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=scenario.preferred_processing == "xia2/DIALS",
            recipe="autoprocessing-xia2-dials-eiger",
            source="automatic",
            displayname="xia2 dials",
            parameters=(
                mimas.MimasISPyBParameter(
                    key="resolution.cc_half_significance_level",
                    value="0.1",
                ),
                mimas.MimasISPyBParameter(key="remove_blanks", value="true"),
                mimas.MimasISPyBParameter(key="failover", value="true"),
                *xia2_dials_absorption_params(scenario),
            ),
        ),
        # xia2-3dii
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=scenario.preferred_processing == "xia2/XDS",
            recipe="autoprocessing-xia2-3dii-eiger",
            source="automatic",
            displayname="xia2 3dii",
            parameters=(
                mimas.MimasISPyBParameter(
                    key="resolution.cc_half_significance_level",
                    value="0.1",
                ),
            ),
        ),
        # autoPROC
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=scenario.preferred_processing == "autoPROC",
            recipe="autoprocessing-autoPROC-eiger",
            source="automatic",
            displayname="autoPROC",
        ),
    ]
