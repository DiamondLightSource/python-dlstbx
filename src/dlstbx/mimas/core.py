from typing import List, Union

import dlstbx.mimas


def run(
    scenario: dlstbx.mimas.MimasScenario,
) -> List[
    Union[dlstbx.mimas.MimasRecipeInvocation, dlstbx.mimas.MimasISPyBJobInvocation]
]:
    tasks = []

    multi_xia2: bool = False
    if (
        scenario.dcclass is dlstbx.mimas.MimasDCClass.ROTATION
        and scenario.getsweepslistfromsamedcg
        and any(
            sweep.DCID != scenario.DCID for sweep in scenario.getsweepslistfromsamedcg
        )
    ):
        multi_xia2 = True

    if scenario.event is dlstbx.mimas.MimasEvent.START:
        if scenario.beamline in ("i19-1", "i19-2"):
            # i19 is a special case
            tasks.append(
                dlstbx.mimas.MimasRecipeInvocation(
                    DCID=scenario.DCID, recipe="per-image-analysis-rotation"
                )
            )

        elif scenario.beamline == "i02-2":
            # VMXi is also a special case
            pass  # nothing defined

        elif scenario.detectorclass.name == "PILATUS":
            if scenario.dcclass is dlstbx.mimas.MimasDCClass.GRIDSCAN:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="archive-cbfs"
                    )
                )
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="per-image-analysis-gridscan"
                    )
                )
            else:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="archive-cbfs"
                    )
                )
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="per-image-analysis-rotation"
                    )
                )

        elif scenario.detectorclass.name == "EIGER":
            if scenario.dcclass is dlstbx.mimas.MimasDCClass.GRIDSCAN:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID,
                        recipe="per-image-analysis-gridscan-swmr",
                    )
                )
            else:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID,
                        recipe="per-image-analysis-rotation-swmr",
                    )
                )

    if scenario.event is dlstbx.mimas.MimasEvent.END:

        if scenario.beamline in ("i19-1", "i19-2"):
            # i19 is a special case
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
                    recipe="autoprocessing-multi-xia2-smallmolecule",
                    source="automatic",
                    sweeps=tuple(scenario.getsweepslistfromsamedcg),
                )
            )
            tasks.append(
                dlstbx.mimas.MimasISPyBJobInvocation(
                    DCID=scenario.DCID,
                    autostart=True,
                    recipe="autoprocessing-multi-xia2-smallmolecule-dials-aiml",
                    source="automatic",
                    sweeps=tuple(scenario.getsweepslistfromsamedcg),
                )
            )

        elif scenario.beamline == "i02-2":
            # VMXi is also a special case
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
            tasks.append(
                dlstbx.mimas.MimasRecipeInvocation(
                    DCID=scenario.DCID, recipe="archive-nexus"
                )
            )
            if scenario.dcclass is dlstbx.mimas.MimasDCClass.GRIDSCAN:
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
                if scenario.dcclass is dlstbx.mimas.MimasDCClass.ROTATION:
                    # fast_dp
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            recipe="autoprocessing-fast-dp-eiger",
                            source="automatic",
                        )
                    )
                    # xia2-dials
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "xia2/DIALS",
                            recipe="autoprocessing-xia2-dials-eiger",
                            source="automatic",
                            parameters=(
                                dlstbx.mimas.MimasISPyBParameter(
                                    key="resolution.cc_half_significance_level",
                                    value="0.1",
                                ),
                                dlstbx.mimas.MimasISPyBParameter(
                                    key="remove_blanks", value="true"
                                ),
                                dlstbx.mimas.MimasISPyBParameter(
                                    key="failover", value="true"
                                ),
                            ),
                        )
                    )
                    # xia2-3dii
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "xia2/XDS",
                            recipe="autoprocessing-xia2-3dii-eiger",
                            source="automatic",
                            parameters=(
                                dlstbx.mimas.MimasISPyBParameter(
                                    key="resolution.cc_half_significance_level",
                                    value="0.1",
                                ),
                            ),
                        )
                    )
                    # autoPROC
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "autoPROC",
                            recipe="autoprocessing-autoPROC-eiger",
                            source="automatic",
                        )
                    )

        elif scenario.detectorclass is dlstbx.mimas.MimasDetectorClass.PILATUS:
            if scenario.dcclass is dlstbx.mimas.MimasDCClass.SCREENING:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="strategy-edna"
                    )
                )
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="strategy-mosflm"
                    )
                )

            tasks.append(
                dlstbx.mimas.MimasRecipeInvocation(
                    DCID=scenario.DCID, recipe="generate-crystal-thumbnails"
                )
            )
            if scenario.dcclass is dlstbx.mimas.MimasDCClass.ROTATION:
                # RLV
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="processing-rlv"
                    )
                )
                # xia2-dials
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=scenario.preferred_processing == "xia2/DIALS",
                        recipe="autoprocessing-xia2-dials",
                        source="automatic",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                    )
                )
                # xia2-3dii
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=scenario.preferred_processing == "xia2/XDS",
                        recipe="autoprocessing-xia2-3dii",
                        source="automatic",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                    )
                )
                # autoPROC
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=scenario.preferred_processing == "autoPROC",
                        recipe="autoprocessing-autoPROC",
                        source="automatic",
                    )
                )

            if multi_xia2:
                # xia2-dials
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,
                        recipe="autoprocessing-multi-xia2-dials",
                        source="automatic",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    )
                )
                # xia2-3dii
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,
                        recipe="autoprocessing-multi-xia2-3dii",
                        source="automatic",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    )
                )
            if scenario.spacegroup:
                # Space group is set, run xia2 and autoPROC with space group
                spacegroup = scenario.spacegroup.string
                if spacegroup == "P1211":
                    spacegroup = "P21"  # I04-1 hothothotfix for 20190508 only
                if scenario.unitcell:
                    parameters = (
                        dlstbx.mimas.MimasISPyBParameter(
                            key="spacegroup", value=spacegroup
                        ),
                        dlstbx.mimas.MimasISPyBParameter(
                            key="unit_cell", value=scenario.unitcell.string
                        ),
                    )
                else:
                    parameters = (
                        dlstbx.mimas.MimasISPyBParameter(
                            key="spacegroup", value=spacegroup
                        ),
                    )
                xia2_parameters = (
                    dlstbx.mimas.MimasISPyBParameter(
                        key="resolution.cc_half_significance_level", value="0.1"
                    ),
                    *parameters,
                )
                if scenario.dcclass is dlstbx.mimas.MimasDCClass.ROTATION:
                    # xia2-dials
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "xia2/DIALS",
                            recipe="autoprocessing-xia2-dials",
                            source="automatic",
                            parameters=xia2_parameters,
                        )
                    )
                    # xia2-3dii
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "xia2/XDS",
                            recipe="autoprocessing-xia2-3dii",
                            source="automatic",
                            parameters=xia2_parameters,
                        )
                    )
                    # autoPROC
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "autoPROC",
                            recipe="autoprocessing-autoPROC",
                            source="automatic",
                            parameters=parameters,
                        )
                    )
                    # fast_dp
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            recipe="autoprocessing-fast-dp",
                            source="automatic",
                            parameters=(
                                dlstbx.mimas.MimasISPyBParameter(
                                    key="spacegroup", value=spacegroup
                                ),
                            ),
                        )
                    )
                if multi_xia2:
                    # xia2-dials
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=False,
                            recipe="autoprocessing-multi-xia2-dials",
                            source="automatic",
                            parameters=parameters,
                            sweeps=tuple(scenario.getsweepslistfromsamedcg),
                        )
                    )
                    # xia2-3dii
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=False,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-multi-xia2-3dii",
                            source="automatic",
                            sweeps=tuple(scenario.getsweepslistfromsamedcg),
                            triggervariables=(),
                        )
                    )
            else:
                # Space group is not set, only run fast_dp
                # (xia2 and autoPROC have already been accounted for above)
                if scenario.dcclass is dlstbx.mimas.MimasDCClass.ROTATION:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            recipe="autoprocessing-fast-dp",
                            source="automatic",
                        )
                    )

        elif scenario.detectorclass.name == "EIGER":
            stopped = scenario.runstatus == "DataCollection Stopped"
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

            if scenario.dcclass is dlstbx.mimas.MimasDCClass.SCREENING:
                for recipe in (
                    "strategy-align-crystal",
                    "strategy-mosflm",
                    "strategy-edna-eiger",
                ):
                    tasks.append(
                        dlstbx.mimas.MimasRecipeInvocation(
                            DCID=scenario.DCID, recipe=recipe
                        )
                    )
            # Always run xia2 and autoPROC without space group set

            if scenario.dcclass is dlstbx.mimas.MimasDCClass.ROTATION:
                # xia2-dials
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=scenario.preferred_processing == "xia2/DIALS",
                        recipe="autoprocessing-xia2-dials-eiger",
                        source="automatic",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                    )
                )
                # xia2-3dii
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=scenario.preferred_processing == "xia2/XDS",
                        recipe="autoprocessing-xia2-3dii-eiger",
                        source="automatic",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                    )
                )
                # autoPROC
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=scenario.preferred_processing == "autoPROC",
                        recipe="autoprocessing-autoPROC-eiger",
                        source="automatic",
                    )
                )
            if multi_xia2:
                # xia2-dials
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,
                        recipe="autoprocessing-multi-xia2-dials-eiger",
                        source="automatic",
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    )
                )
                # xia2-3dii
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,
                        recipe="autoprocessing-multi-xia2-3dii-eiger",
                        source="automatic",
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                    )
                )

            if scenario.spacegroup:
                # Space group is set, run xia2 and autoPROC with space group
                spacegroup = scenario.spacegroup.string
                if spacegroup == "P1211":
                    spacegroup = "P21"  # I04-1 hothothotfix for 20190508 only
                if spacegroup == "C1211":
                    spacegroup = "C2"  # I04-1 hothothotfix for 20190510 only
                if spacegroup == "C121":
                    spacegroup = "C2"  # I03 hothothotfix for 20190510 only

                if scenario.unitcell:
                    parameters = (
                        dlstbx.mimas.MimasISPyBParameter(
                            key="resolution.cc_half_significance_level", value="0.1"
                        ),
                        dlstbx.mimas.MimasISPyBParameter(
                            key="spacegroup", value=spacegroup
                        ),
                        dlstbx.mimas.MimasISPyBParameter(
                            key="unit_cell", value=scenario.unitcell.string
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

                if scenario.dcclass is dlstbx.mimas.MimasDCClass.ROTATION:
                    # fast_dp
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            recipe="autoprocessing-fast-dp-eiger",
                            source="automatic",
                            parameters=(
                                dlstbx.mimas.MimasISPyBParameter(
                                    key="spacegroup", value=spacegroup
                                ),
                            ),
                        )
                    )
                    # xia2-dials
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "xia2/DIALS",
                            recipe="autoprocessing-xia2-dials-eiger",
                            source="automatic",
                            parameters=parameters,
                        )
                    )
                    # xia2-3dii
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "xia2/XDS",
                            recipe="autoprocessing-xia2-3dii-eiger",
                            source="automatic",
                            parameters=parameters,
                        )
                    )
                if multi_xia2:
                    # xia2-dials
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=False,
                            recipe="autoprocessing-multi-xia2-dials-eiger",
                            source="automatic",
                            parameters=parameters,
                            sweeps=tuple(scenario.getsweepslistfromsamedcg),
                        )
                    )
                    # xia2-3dii
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=False,
                            recipe="autoprocessing-multi-xia2-3dii-eiger",
                            source="automatic",
                            parameters=parameters,
                            sweeps=tuple(scenario.getsweepslistfromsamedcg),
                        )
                    )
                if scenario.dcclass is dlstbx.mimas.MimasDCClass.ROTATION:
                    if scenario.unitcell:
                        parameters = (
                            dlstbx.mimas.MimasISPyBParameter(
                                key="spacegroup", value=spacegroup
                            ),
                            dlstbx.mimas.MimasISPyBParameter(
                                key="unit_cell", value=scenario.unitcell.string
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
                            autostart=scenario.preferred_processing == "autoPROC",
                            recipe="autoprocessing-autoPROC-eiger",
                            source="automatic",
                            parameters=parameters,
                        )
                    )
            else:
                # Space group is not set, only run fast_dp
                if scenario.dcclass is dlstbx.mimas.MimasDCClass.ROTATION:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=True,
                            recipe="autoprocessing-fast-dp-eiger",
                            source="automatic",
                        )
                    )

    return tasks
