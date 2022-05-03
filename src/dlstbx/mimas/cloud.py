from __future__ import annotations

from typing import List, Tuple

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
    *,
    zc: zocalo.configuration.Configuration,
) -> List[mimas.Invocation]:

    tasks: List[mimas.Invocation] = []

    if not zc.storage:
        return tasks

    for group in zc.storage.get("zocalo.mimas.cloud", []):
        cloud_spec = VisitSpecification(
            set(group.get("visit_pattern", []))
        ) & BeamlineSpecification(beamlines=set(group.get("beamlines", [])))
        cloud_recipes = set(group.get("recipes", []))
        if cloud_spec.is_satisfied_by(scenario):
            ParamTuple = Tuple[mimas.MimasISPyBParameter, ...]
            extra_params: List[ParamTuple] = [()]
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

            for params in extra_params:
                if "autoprocessing-xia2-dials-eiger-cloud" in cloud_recipes:
                    tasks.append(
                        mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            recipe="autoprocessing-xia2-dials-eiger-cloud",
                            source="automatic",
                            parameters=(
                                mimas.MimasISPyBParameter(
                                    key="resolution.cc_half_significance_level",
                                    value="0.1",
                                ),
                                *params,
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
                                    key="resolution.cc_half_significance_level",
                                    value="0.1",
                                ),
                                *params,
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
                            parameters=params,
                        )
                    )

    return tasks
