from __future__ import annotations

from typing import List, Tuple

from dlstbx import mimas
from dlstbx.mimas.core import is_eiger, is_end, is_pilatus, is_serial, is_start


@mimas.match_specification(is_pilatus & is_end & is_serial)
def handle_pilatus_serial_end(
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


@mimas.match_specification(is_eiger & is_end & is_serial)
def handle_eiger_serial_end(
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
            recipe="autoprocessing-xia2-ssx-eiger",
            source="automatic",
            sweeps=tuple(scenario.getsweepslistfromsamedcg),
            parameters=symmetry_parameters,
        ),
    ]


@mimas.match_specification(is_pilatus & is_start & is_serial)
def handle_pilatus_serial_start(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="pia-index-ssx-pilatus"),
    ]


@mimas.match_specification(is_eiger & is_start & is_serial)
def handle_eiger_serial_start(
    scenario: mimas.MimasScenario,
    **kwargs,
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="pia-index-ssx-swmr"),
    ]
