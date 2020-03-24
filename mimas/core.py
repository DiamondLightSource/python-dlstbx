from typing import List, Union

import dlstbx.mimas


def run(
    scenario: dlstbx.mimas.MimasScenario,
) -> List[
    Union[dlstbx.mimas.MimasRecipeInvocation, dlstbx.mimas.MimasISPyBJobInvocation]
]:
    tasks = []

    if scenario.event == dlstbx.mimas.MimasEvent.START:
        if scenario.beamline in ("i02-1", "i04-1", "i23", "i24", "p45"):
            tasks.append(
                dlstbx.mimas.MimasRecipeInvocation(
                    DCID=scenario.DCID,
                    recipe="archive-per-image-analysis-eiger-streamdump",
                )
            )
            if scenario.isitagridscan:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="per-image-analysis-gridscan"
                    )
                )
            else:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="per-image-analysis-rotation"
                    )
                )
            if "processing-autoproc" in scenario.default_recipes:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=True,
                        comment="",
                        displayname="",
                        parameters=(),
                        recipe="autoprocessing-autoPROC",
                        source="automatic",
                        sweeps=(),
                        triggervariables=(),
                    )
                )
            spacegroup = scenario.spacegroup
            if spacegroup == "P1211":
                spacegroup = "P21"  # I04-1 hothothotfix for 20190508 only
            if spacegroup == "C1211":
                spacegroup = "C2"  # I04-1 hothothotfix for 20190510 only
            if spacegroup == "C121":
                spacegroup = "C2"  # I03 hothothotfix for 20190510 only
            if spacegroup:
                unitcell = scenario.unitcell
                if unitcell:
                    unitcell = unitcell.replace(" ", ",")
                # Space group is set, run fast_dp, xia2 and autoPROC with space group
                if "processing-fast-dp" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=(
                                dlstbx.mimas.MimasISPyBParameter(
                                    key="spacegroup", value=spacegroup
                                ),
                            ),
                            recipe="autoprocessing-fast-dp",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if "processing-autoproc" in scenario.default_recipes:
                    if unitcell:
                        parameters = (
                            dlstbx.mimas.MimasISPyBParameter(
                                key="spacegroup", value=spacegroup
                            ),
                            dlstbx.mimas.MimasISPyBParameter(
                                key="unit_cell", value=unitcell
                            ),
                        )
                    else:
                        parameters = (
                            dlstbx.mimas.MimasISPyBParameter(
                                key="spacegroup", value=spacegroup
                            ),
                        )
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-autoPROC",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
            else:
                # Space group is not set, only run fast_dp
                if "processing-fast-dp" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=(),
                            recipe="autoprocessing-fast-dp",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )

        if scenario.beamline == "i02-2":
            pass  # nothing defined

        if scenario.beamline in ("i03", "i04") and scenario.isitagridscan:
            tasks.append(
                dlstbx.mimas.MimasRecipeInvocation(
                    DCID=scenario.DCID, recipe="per-image-analysis-eiger-streamdump"
                )
            )

        if scenario.beamline in ("i19-1", "i19-2"):
            if scenario.isitagridscan:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="per-image-analysis-gridscan"
                    )
                )
            else:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="per-image-analysis-rotation"
                    )
                )

    return tasks
