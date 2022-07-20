from __future__ import annotations

from typing import List, Tuple

from dlstbx import mimas
from dlstbx.mimas.core import is_end, is_pilatus, is_serial_fixed


@mimas.match_specification(is_pilatus & is_end & is_serial_fixed)
def handle_pilatus_serial_fixed_end(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    ParamTuple = Tuple[mimas.MimasISPyBParameter, ...]
    symmetry_parameters: ParamTuple = ()
    if scenario.spacegroup:
        spacegroup = scenario.spacegroup.string
        symmetry_parameters += (
            mimas.MimasISPyBParameter(key="spacegroup", value=spacegroup),
        )
        if scenario.unitcell:
            symmetry_parameters += (
                mimas.MimasISPyBParameter(
                    key="unit_cell", value=scenario.unitcell.string
                ),
            )
    return [
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=True,
            recipe="autoprocessing-xia2-ssx-pilatus",
            source="automatic",
            sweeps=tuple(scenario.getsweepslistfromsamedcg),
            parameters=symmetry_parameters,
        ),
    ]
