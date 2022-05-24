from __future__ import annotations

from typing import List, Tuple

from dlstbx import mimas
from dlstbx.mimas.core import (
    is_end,
    xia2_dials_absorption_params,
)
from dlstbx.mimas.specification import BeamlineSpecification

is_i15 = BeamlineSpecification("i15")

@mimas.match_specification(is_i15 & is_end)
def handle_i15_end(scenario: mimas.MimasScenario, **kwargs) -> List[mimas.Invocation]:
    tasks: List[mimas.Invocation] = [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=recipe)
        for recipe in (
            "generate-crystal-thumbnails",
            "processing-rlv",
            "strategy-screen19",
            "per-image-analysis-rotation",
        )
    ]

    ParamTuple = Tuple[mimas.MimasISPyBParameter, ...]
    extra_params: List[ParamTuple] = [()]
    if scenario.spacegroup:
        # Space group is set, run xia2 with space group
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
        tasks.extend(
            [
                mimas.MimasRecipeInvocation(
                    DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
                ),
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe="autoprocessing-multi-xia2-smallmolecule",
                    source="automatic",
                    sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    parameters=(
                        *params,
                        *xia2_dials_absorption_params(scenario),
                    ),
                ),
                mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe="autoprocessing-multi-xia2-smallmolecule-dials-aiml",
                    source="automatic",
                    sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    parameters=params,
                ),
            ]
        )

    return tasks
