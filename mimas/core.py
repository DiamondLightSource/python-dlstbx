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

    if scenario.event == dlstbx.mimas.MimasEvent.END:
        if scenario.beamline in ("i02-1", "i04-1", "i23", "i24", "p45"):
            recipes = set(scenario.default_recipes) - {
                "processing-xia2-dials",
                "processing-xia2-3dii",
                "processing-multi-xia2-dials",
                "processing-multi-xia2-3dii",
                "processing-autoproc",
                "processing-fast-dp",
                "per-image-analysis-gridscan",
                "per-image-analysis-rotation",
            }
            for r in recipes:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe=r)
                )
            tasks.append(
                dlstbx.mimas.MimasRecipeInvocation(
                    DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
                )
            )
            # Always run xia2 and autoPROC without space group set
            if "processing-xia2-dials" in scenario.default_recipes:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=True,
                        comment="",
                        displayname="",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                        recipe="autoprocessing-xia2-dials",
                        source="automatic",
                        sweeps=(),
                        triggervariables=(),
                    )
                )
            if "processing-xia2-3dii" in scenario.default_recipes:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=True,
                        comment="",
                        displayname="",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                        recipe="autoprocessing-xia2-3dii",
                        source="automatic",
                        sweeps=(),
                        triggervariables=(),
                    )
                )
            if "processing-multi-xia2-dials" in scenario.default_recipes:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=True,
                        comment="",
                        displayname="",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                        recipe="autoprocessing-multi-xia2-dials",
                        source="automatic",
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                        triggervariables=(),
                    )
                )
            if "processing-multi-xia2-3dii" in scenario.default_recipes:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=True,
                        comment="",
                        displayname="",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                        recipe="autoprocessing-multi-xia2-3dii",
                        source="automatic",
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                        triggervariables=(),
                    )
                )
            spacegroup = scenario.spacegroup
            if spacegroup == "P1211":
                spacegroup = "P21"  # I04-1 hothothotfix for 20190508 only
            if spacegroup:
                # Space group is set, run xia2 and autoPROC with space group
                unitcell = scenario.unitcell
                if unitcell:
                    unitcell = unitcell.replace(" ", ",")
                    parameters = (
                        dlstbx.mimas.MimasISPyBParameter(
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
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
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
                        dlstbx.mimas.MimasISPyBParameter(
                            key="spacegroup", value=spacegroup
                        ),
                    )

                if "processing-xia2-dials" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-xia2-dials",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if "processing-xia2-3dii" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-xia2-3dii",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if "processing-multi-xia2-dials" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-multi-xia2-dials",
                            source="automatic",
                            sweeps=tuple(scenario.getsweepslistfromsamedcg),
                            triggervariables=(),
                        )
                    )
                if "processing-multi-xia2-3dii" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-multi-xia2-3dii",
                            source="automatic",
                            sweeps=tuple(scenario.getsweepslistfromsamedcg),
                            triggervariables=(),
                        )
                    )

        if scenario.beamline == "i02-2":
            gridscan = "per-image-analysis-gridscan" in scenario.default_recipes
            gridscan = (
                gridscan or "vmxi-spot-counts-per-image" in scenario.default_recipes
            )
            ishdf = "#" in "dcid[image_pattern]"  # I guess this is exactly wrong
            tasks.append(
                dlstbx.mimas.MimasRecipeInvocation(
                    DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
                )
            )
            tasks.append(
                dlstbx.mimas.MimasRecipeInvocation(
                    DCID=scenario.DCID, recipe="generate-diffraction-preview"
                )
            )
            if ishdf:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="archive-nexus"
                    )
                )
            if gridscan:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="vmxi-spot-counts-per-image"
                    )
                )
            else:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="vmxi-per-image-analysis"
                    )
                )
                # Always run xia2 and autoPROC without space group set
                if "processing-fast-dp" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=(),
                            recipe="autoprocessing-fast-dp-eiger",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )

                if "processing-xia2-dials" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=(
                                dlstbx.mimas.MimasISPyBParameter(
                                    key="remove_blanks", value="true"
                                ),
                            ),
                            recipe="autoprocessing-xia2-dials-i04",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if "processing-xia2-3dii" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=(),
                            recipe="autoprocessing-xia2-3dii-i04",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
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
                            recipe="autoprocessing-autoPROC-eiger",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )

        if scenario.beamline in ("i03", "i04",):
            gridscan = "per-image-analysis-gridscan" in scenario.default_recipes
            stopped = scenario.runstatus == "DataCollection Stopped"
            strategy = any("strategy" in dr for dr in scenario.default_recipes)
            tasks.append(
                dlstbx.mimas.MimasRecipeInvocation(
                    DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
                )
            )
            if not stopped:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="generate-diffraction-preview"
                    )
                )
            tasks.append(
                dlstbx.mimas.MimasRecipeInvocation(
                    DCID=scenario.DCID, recipe="archive-nexus"
                )
            )
            if not gridscan:
                # skipped static gridscan analysis in favour of TOM algorithm
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="per-image-analysis-eiger-rotation"
                    )
                )
            if strategy and not gridscan:
                for recipe in (
                    "strategy-align-crystal",
                    "strategy-mosflm",
                    "strategy-edna-i04",
                ):
                    tasks.append(
                        dlstbx.mimas.MimasRecipeInvocation(
                            DCID=scenario.DCID, recipe=recipe
                        )
                    )
            # Always run xia2 and autoPROC without space group set

            if "processing-xia2-dials" in scenario.default_recipes:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=True,
                        comment="",
                        displayname="",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                        recipe="autoprocessing-xia2-dials-i04",
                        source="automatic",
                        sweeps=(),
                        triggervariables=(),
                    )
                )
            if "processing-xia2-3dii" in scenario.default_recipes:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=True,
                        comment="",
                        displayname="",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                        recipe="autoprocessing-xia2-3dii-i04",
                        source="automatic",
                        sweeps=(),
                        triggervariables=(),
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
                        recipe="autoprocessing-autoPROC-eiger",
                        source="automatic",
                        sweeps=(),
                        triggervariables=(),
                    )
                )
            if "processing-multi-xia2-dials" in scenario.default_recipes:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=True,
                        comment="",
                        displayname="",
                        parameters=(),
                        recipe="autoprocessing-multi-xia2-dials-eiger",
                        source="automatic",
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                        triggervariables=(),
                    )
                )
            if "processing-multi-xia2-3dii" in scenario.default_recipes:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=True,
                        comment="",
                        displayname="",
                        parameters=(),
                        recipe="autoprocessing-multi-xia2-3dii-eiger",
                        source="automatic",
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                        triggervariables=(),
                    )
                )

            #   ################ Determine SG and UC ####################
            spacegroup = scenario.spacegroup
            if spacegroup == "P1211":
                spacegroup = "P21"  # I04-1 hothothotfix for 20190508 only
            if spacegroup == "C1211":
                spacegroup = "C2"  # I04-1 hothothotfix for 20190510 only
            if spacegroup == "C121":
                spacegroup = "C2"  # I03 hothothotfix for 20190510 only
            if spacegroup:
                # Space group is set, run xia2 and autoPROC with space group

                unitcell = scenario.unitcell
                if unitcell:
                    unitcell = unitcell.replace(" ", ",")
                    parameters = (
                        dlstbx.mimas.MimasISPyBParameter(
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
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
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
                        dlstbx.mimas.MimasISPyBParameter(
                            key="spacegroup", value=spacegroup
                        ),
                    )

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
                            recipe="autoprocessing-fast-dp-eiger",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if "processing-xia2-dials" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-xia2-dials-i04",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if "processing-xia2-3dii" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-xia2-3dii-i04",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if "processing-multi-xia2-dials" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-multi-xia2-dials-eiger",
                            source="automatic",
                            sweeps=tuple(scenario.getsweepslistfromsamedcg),
                            triggervariables=(),
                        )
                    )
                if "processing-multi-xia2-3dii" in scenario.default_recipes:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-multi-xia2-3dii-eiger",
                            source="automatic",
                            sweeps=tuple(scenario.getsweepslistfromsamedcg),
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
                            recipe="autoprocessing-autoPROC-eiger",
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
                            recipe="autoprocessing-fast-dp-eiger",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )

        if scenario.beamline in ("i19-1", "i19-2"):
            for recipe in (
                "archive-cbfs",
                "strategy-screen19",
                "processing-rlv",
                "generate-crystal-thumbnails",
            ):
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe=recipe
                    )
                )

            tasks.append(
                dlstbx.mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    comment="",
                    displayname="",
                    parameters=(),
                    recipe="autoprocessing-multi-xia2-smallmolecule",
                    source="automatic",
                    sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    triggervariables=(),
                )
            )
            tasks.append(
                dlstbx.mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    comment="",
                    displayname="",
                    parameters=(),
                    recipe="autoprocessing-multi-xia2-smallmolecule-dials-aiml",
                    source="automatic",
                    sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    triggervariables=(),
                )
            )

    return tasks
