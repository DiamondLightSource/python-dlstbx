from __future__ import annotations

from typing import List

from dlstbx import mimas
from dlstbx.mimas.core import is_end, is_rotation, xia2_dials_absorption_params
from dlstbx.mimas.specification import BeamlineSpecification, VisitSpecification

CLOUD_VISITS = {
    "cm",
    "nt28218",
    "mx",
}


is_cloud = (
    VisitSpecification(CLOUD_VISITS)
    & is_end
    & is_rotation
    & BeamlineSpecification("i03")
)


@mimas.match_specification(is_cloud)
def handle_cloud(
    scenario: mimas.MimasScenario,
) -> List[mimas.Invocation]:
    return [
        # xia2-dials
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
        ),
        # xia2-3dii
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=True,
            recipe="autoprocessing-xia2-3dii-eiger-cluster",
            source="automatic",
            parameters=(
                mimas.MimasISPyBParameter(
                    key="resolution.cc_half_significance_level", value="0.1"
                ),
            ),
        ),
        # autoPROC
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            autostart=True,
            recipe="autoprocessing-autoPROC-eiger-cluster",
            source="automatic",
        ),
    ]
