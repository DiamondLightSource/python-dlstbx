from typing import List, Union

import dlstbx.mimas


SWMR_BEAMLINES = {"i03", "i24"}


def run(
    scenario: dlstbx.mimas.MimasScenario,
) -> List[
    Union[dlstbx.mimas.MimasRecipeInvocation, dlstbx.mimas.MimasISPyBJobInvocation]
]:
    tasks = []

    multi_xia2: bool = False
    if (
        scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION
        and scenario.getsweepslistfromsamedcg
        and any(
            sweep.DCID != scenario.DCID for sweep in scenario.getsweepslistfromsamedcg
        )
    ):
        multi_xia2 = True

    if scenario.event == dlstbx.mimas.MimasEvent.START:
        if scenario.beamline in ("i19-1", "i19-2"):
            # i19 is a special case
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

        elif scenario.beamline == "i02-2":
            # VMXi is also a special case
            pass  # nothing defined

        elif scenario.detectorclass == dlstbx.mimas.MimasDetectorClass.PILATUS:
            if not scenario.isitagridscan:
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
                if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "autoPROC",
                            comment="",
                            displayname="",
                            parameters=(),
                            recipe="autoprocessing-autoPROC",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if scenario.spacegroup:
                    # Space group is set, run fast_dp, xia2 and autoPROC with space group
                    spacegroup = scenario.spacegroup.string
                    if spacegroup == "P1211":
                        spacegroup = "P21"  # I04-1 hothothotfix for 20190508 only
                    if spacegroup == "C1211":
                        spacegroup = "C2"  # I04-1 hothothotfix for 20190510 only
                    if spacegroup == "C121":
                        spacegroup = "C2"  # I03 hothothotfix for 20190510 only
                    # Space group is set, run fast_dp, xia2 and autoPROC with space group
                    if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
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
                    if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
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
                    if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
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
            else:
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

        if scenario.detectorclass.name == "EIGER":
            # For SWMR beamlines trigger SWMR gridscan and rotation recipes
            # For non-SWMR beamlines trigger streamdump gridscan recipe
            # (non-SWMR beamline rotation scans will be handled elsewhere)
            if scenario.beamline in SWMR_BEAMLINES:
                # use swmr PIA
                if scenario.isitagridscan:
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
            elif scenario.isitagridscan:
                # use legacy streamdump PIA
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="per-image-analysis-eiger-streamdump"
                    )
                )

    if scenario.event == dlstbx.mimas.MimasEvent.END:

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

        elif scenario.beamline == "i02-2":
            # VMXi is also a special case
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
            if scenario.dcclass == dlstbx.mimas.MimasDCClass.GRIDSCAN:
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
                if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
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

                if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "xia2/DIALS",
                            comment="",
                            displayname="",
                            parameters=(
                                dlstbx.mimas.MimasISPyBParameter(
                                    key="remove_blanks", value="true"
                                ),
                            ),
                            recipe="autoprocessing-xia2-dials-eiger",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "xia2/XDS",
                            comment="",
                            displayname="",
                            parameters=(),
                            recipe="autoprocessing-xia2-3dii-eiger",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "autoPROC",
                            comment="",
                            displayname="",
                            parameters=(),
                            recipe="autoprocessing-autoPROC-eiger",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )

        elif scenario.detectorclass == dlstbx.mimas.MimasDetectorClass.PILATUS:
            if scenario.dcclass == dlstbx.mimas.MimasDCClass.SCREENING:
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
            if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="processing-rlv"
                    )
                )

            # Always run xia2 and autoPROC without space group set
            if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=scenario.preferred_processing == "xia2/DIALS",
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
            if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=scenario.preferred_processing == "xia2/XDS",
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
            if multi_xia2:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,
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
            if multi_xia2:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,
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
            if scenario.spacegroup:
                # Space group is set, run xia2 and autoPROC with space group
                spacegroup = scenario.spacegroup.string
                if spacegroup == "P1211":
                    spacegroup = "P21"  # I04-1 hothothotfix for 20190508 only
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

                if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "xia2/DIALS",
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-xia2-dials",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "xia2/XDS",
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-xia2-3dii",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if multi_xia2:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=False,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-multi-xia2-dials",
                            source="automatic",
                            sweeps=tuple(scenario.getsweepslistfromsamedcg),
                            triggervariables=(),
                        )
                    )
                if multi_xia2:
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

            if scenario.beamline not in SWMR_BEAMLINES:
                # Only trigger rotation PIA at end of data collection for
                # non-SWMR EIGER beamlines
                tasks.append(
                    dlstbx.mimas.MimasRecipeInvocation(
                        DCID=scenario.DCID, recipe="per-image-analysis-eiger-rotation"
                    )
                )

            if scenario.dcclass == dlstbx.mimas.MimasDCClass.SCREENING:
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

            if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=scenario.preferred_processing == "xia2/DIALS",
                        comment="",
                        displayname="",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                        recipe="autoprocessing-xia2-dials-eiger",
                        source="automatic",
                        sweeps=(),
                        triggervariables=(),
                    )
                )
            if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=scenario.preferred_processing == "xia2/XDS",
                        comment="",
                        displayname="",
                        parameters=(
                            dlstbx.mimas.MimasISPyBParameter(
                                key="resolution.cc_half_significance_level", value="0.1"
                            ),
                        ),
                        recipe="autoprocessing-xia2-3dii-eiger",
                        source="automatic",
                        sweeps=(),
                        triggervariables=(),
                    )
                )
            if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=scenario.preferred_processing == "autoPROC",
                        comment="",
                        displayname="",
                        parameters=(),
                        recipe="autoprocessing-autoPROC-eiger",
                        source="automatic",
                        sweeps=(),
                        triggervariables=(),
                    )
                )
            if multi_xia2:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,
                        comment="",
                        displayname="",
                        parameters=(),
                        recipe="autoprocessing-multi-xia2-dials-eiger",
                        source="automatic",
                        sweeps=tuple(scenario.getsweepslistfromsamedcg),
                        triggervariables=(),
                    )
                )
            if multi_xia2:
                tasks.append(
                    dlstbx.mimas.MimasISPyBJobInvocation(
                        DCID=scenario.DCID,
                        autostart=False,
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

                if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
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
                if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "xia2/DIALS",
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-xia2-dials-eiger",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=scenario.preferred_processing == "xia2/XDS",
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-xia2-3dii-eiger",
                            source="automatic",
                            sweeps=(),
                            triggervariables=(),
                        )
                    )
                if multi_xia2:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=False,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-multi-xia2-dials-eiger",
                            source="automatic",
                            sweeps=tuple(scenario.getsweepslistfromsamedcg),
                            triggervariables=(),
                        )
                    )
                if multi_xia2:
                    tasks.append(
                        dlstbx.mimas.MimasISPyBJobInvocation(
                            DCID=scenario.DCID,
                            autostart=False,
                            comment="",
                            displayname="",
                            parameters=parameters,
                            recipe="autoprocessing-multi-xia2-3dii-eiger",
                            source="automatic",
                            sweeps=tuple(scenario.getsweepslistfromsamedcg),
                            triggervariables=(),
                        )
                    )
                if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
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
                if scenario.dcclass == dlstbx.mimas.MimasDCClass.ROTATION:
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

    return tasks
