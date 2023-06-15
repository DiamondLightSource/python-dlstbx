from __future__ import annotations

import time
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
    cluster_stats: dict,
) -> List[mimas.Invocation]:
    def on_cloudbursting(group: dict):
        """
        Forward message to trigger if number of waiting jobs doesn't exceed
        the predefined threshold.
        """
        if group.get("cloudbursting", False):
            if (cluster_stats["jobs_waiting"] < cluster_stats["max_jobs_waiting"]) and (
                cluster_stats["last_cluster_update"] > (time.time() - 300)
            ):
                return False
        return True

    tasks: List[mimas.Invocation] = []

    if not zc.storage:
        return tasks

    for group in zc.storage.get("zocalo.mimas.cloud", []):
        cloud_spec = VisitSpecification(
            set(group.get("visit_pattern", []))
        ) & BeamlineSpecification(beamlines=set(group.get("beamlines", [])))
        cloud_recipes = set(group.get("recipes", []))
        if on_cloudbursting(group) and cloud_spec.is_satisfied_by(scenario):
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
                            displayname="xia2 dials",
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
                            displayname="xia2 3dii",
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
                            displayname="autoPROC",
                            parameters=params,
                        )
                    )

    return tasks
