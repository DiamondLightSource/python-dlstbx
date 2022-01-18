from __future__ import annotations

from typing import List

import zocalo.configuration

from dlstbx import mimas
from dlstbx.mimas.core import (
    is_end,
    is_mx_beamline,
    is_rotation,
    is_vmxi,
    xia2_dials_absorption_params,
)
from dlstbx.mimas.specification import BeamlineSpecification, VisitSpecification


@mimas.match_specification(is_rotation & is_end & is_mx_beamline & ~is_vmxi)
def handle_cloud(
    scenario: mimas.MimasScenario,
) -> List[mimas.Invocation]:

    tasks: List[mimas.Invocation] = []

    zc = zocalo.configuration.from_file()
    zc.activate()

    if not zc.storage:
        return tasks

    visit_pattern = set(zc.storage.get("zocalo.mimas.cloud.visit_pattern", []))
    cloud_beamlines = set(zc.storage.get("zocalo.mimas.cloud.beamlines", []))
    cloud_recipes = set(zc.storage.get("zocalo.mimas.cloud.recipes", []))

    cloud_spec = VisitSpecification(visit_pattern) & BeamlineSpecification(
        beamlines=cloud_beamlines
    )

    if cloud_spec.is_satisfied_by(scenario):
        if "autoprocessing-xia2-dials-eiger-cloud" in cloud_recipes:
            tasks.append(
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe="autoprocessing-xia2-dials-eiger-cloud",
                    source="automatic",
                    parameters=(
                        mimas.MimasISPyBParameter(
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
                        *xia2_dials_absorption_params(scenario),
                    ),
                )
            )

        if "autoprocessing-xia2-3dii-eiger-cloud" in cloud_recipes:
            tasks.append(
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe="autoprocessing-xia2-3dii-eiger-cloud",
                    source="automatic",
                    parameters=(
                        mimas.MimasISPyBParameter(
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
                    ),
                )
            )

        if "autoprocessing-autoPROC-eiger-cloud" in cloud_recipes:
            tasks.append(
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe="autoprocessing-autoPROC-eiger-cloud",
                    source="automatic",
                )
            )

    return tasks
