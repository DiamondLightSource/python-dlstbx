from __future__ import annotations

import logging
from collections import defaultdict
from operator import attrgetter

import ispyb
import ispyb.sqlalchemy
import sqlalchemy
from ispyb.sqlalchemy import (
    CTF,
    AutoProcIntegration,
    AutoProcProgram,
    DataCollection,
    ImageQualityIndicators,
    MotionCorrection,
    ParticleClassification,
    ParticleClassificationGroup,
    ParticlePicker,
    ProcessingJob,
    RelativeIceThickness,
    Screening,
    ScreeningOutput,
    ScreeningOutputLattice,
)
from sqlalchemy.orm import Load

import dlstbx.dc_sim.definitions as df

logger = logging.getLogger("dlxtbx.dc_sim.check")


def check_test_outcome(test, db_session=None):
    expected_outcome = df.tests.get(test["scenario"], {}).get("results", {})
    if expected_outcome == {}:
        print(f"Scenario {test['scenario']} is happy with any outcome.")
        test["success"] = True
        return

    check_functions = {
        "mx": _check_mx_outcome,
        "em-spa": _check_relion_outcome,
    }
    if (
        scenario_type := df.tests.get(test["scenario"], {}).get("type", "mx")
    ) not in check_functions:
        print(f"Skipping unknown test scenario type {scenario_type}")
        return

    check_function = check_functions[scenario_type]

    try:
        overall, failed_tests = check_function(test, expected_outcome, db_session)
    except Exception as e:
        print(f"Test validation failed with {e}, leaving test alone")
        logger.error(
            f"dc_sim validation for {scenario_type} failed with {e}", exc_info=True
        )
        return test

    if failed_tests:
        test["success"] = False
        test["reason"] = "\n".join(failed_tests)

    if overall and all(overall.values()):
        test["success"] = True

    print(test)
    return test


def _retrieve_mx_processing(dcid, session):
    image_count = (
        session.query(sqlalchemy.func.count(1))
        .filter(
            ImageQualityIndicators.dataCollectionId == DataCollection.dataCollectionId
        )
        .group_by(ImageQualityIndicators.dataCollectionId)
        .label("image_count")
    )

    query = (
        session.query(
            DataCollection,
            AutoProcProgram,
            AutoProcIntegration,
            ScreeningOutput,
            ScreeningOutputLattice,
            image_count,
        )
        .outerjoin(
            ProcessingJob,
            ProcessingJob.dataCollectionId == DataCollection.dataCollectionId,
        )
        .outerjoin(
            AutoProcProgram,
            AutoProcProgram.processingJobId == ProcessingJob.processingJobId,
        )
        .outerjoin(
            AutoProcIntegration,
            AutoProcIntegration.autoProcProgramId == AutoProcProgram.autoProcProgramId,
        )
        .outerjoin(
            Screening, DataCollection.dataCollectionId == Screening.dataCollectionId
        )
        .outerjoin(
            ScreeningOutput, ScreeningOutput.screeningId == Screening.screeningId
        )
        .outerjoin(
            ScreeningOutputLattice,
            ScreeningOutputLattice.screeningOutputId
            == ScreeningOutput.screeningOutputId,
        )
        .filter(DataCollection.dataCollectionId == dcid)
        .options(
            Load(DataCollection).load_only(
                "dataCollectionId",
                "numberOfImages",
            ),
            Load(AutoProcProgram).load_only("processingPrograms"),
            Load(AutoProcIntegration).load_only(
                "autoProcIntegrationId",
                "cell_a",
                "cell_b",
                "cell_c",
                "cell_alpha",
                "cell_beta",
                "cell_gamma",
            ),
            Load(ScreeningOutput).load_only("screeningId", "program"),
            Load(ScreeningOutputLattice).load_only(
                "spaceGroup",
                "unitCell_a",
                "unitCell_b",
                "unitCell_c",
                "unitCell_alpha",
                "unitCell_beta",
                "unitCell_gamma",
            ),
        )
    )
    query_results = query.all()

    return list(query_results)


def _check_mx_outcome(test, expected_outcome, session):
    failed_tests = []
    outcomes = {}
    overall = {}
    for dcid in test.get("DCIDs", []):
        rows = _retrieve_mx_processing(dcid, session)
        for (
            data_collection,
            autoprocprogram,
            integration,
            screening_output,
            screening_output_lattice,
            iqi_count,
        ) in rows:
            if getattr(screening_output, "screeningId", None):
                check_screening_outcomes(
                    dcid,
                    screening_output.program,
                    screening_output_lattice,
                    outcomes,
                    expected_outcome,
                )
            elif getattr(integration, "autoProcIntegrationId", None):
                check_integration_outcomes(
                    dcid,
                    autoprocprogram.processingPrograms,
                    integration,
                    outcomes,
                    expected_outcome,
                )

            if getattr(data_collection, "numberOfImages", None):
                outcomes.update(
                    check_pia_outcomes(
                        dcid,
                        data_collection.numberOfImages,
                        iqi_count,
                        outcomes,
                        expected_outcome,
                    )
                )

        for program in expected_outcome.get("required", []):
            if program not in outcomes or outcomes[program]["success"] is None:
                outcomes[program] = {
                    "success": False,
                    "reason": [
                        "Expected result not present for program {program}, DCID:{dcid}".format(
                            program=program, dcid=dcid
                        )
                    ],
                }

        for program in outcomes:
            overall.setdefault(program, True)
            if outcomes[program]["success"] is False:
                overall[program] = False
                failed_tests.extend(outcomes[program]["reason"])
            elif outcomes[program]["success"] is None and overall[program] is not False:
                overall[program] = None
    return overall, failed_tests


def _check_relion_outcome(test, expected_outcome, db):
    failed_tests = []
    overall = {}
    for jobid in test.get("JobIDs", []):
        program = "relion"
        motioncorr_data, autoprocpid = _retrieve_motioncorr(db, jobid)
        job_results = {
            "motion_correction": motioncorr_data,
            "ctf": _retrieve_ctf(db, autoprocpid),
            "relative_ice_thickness": _retrieve_relative_ice_thickness(db, autoprocpid),
            "particle_picker": _retrieve_particle_picker(db, autoprocpid),
            "particle_classification": _retrieve_particle_classification(
                db, autoprocpid
            ),
        }
        # print(job_results)
        if (
            len(job_results["motion_correction"]) == 0
            or len(job_results["ctf"]) == 0
            or len(job_results["relative_ice_thickness"]) == 0
            or len(job_results["particle_picker"]) == 0
            or len(job_results["particle_classification"]) != 50
        ):
            overall[program] = None
            continue
        outcomes = check_relion_outcomes(job_results, expected_outcome, jobid)

        overall.setdefault(program, True)
        if outcomes[program]["success"] is False:
            overall[program] = False
            failed_tests.extend(outcomes[program]["reason"])
        elif outcomes[program]["success"] is None and overall[program] is not False:
            overall[program] = None
    return overall, failed_tests


def check_screening_outcomes(
    dcid, program, screening_output_lattice, outcomes, expected_outcome
):
    all_screening_programs = [
        "mosflm",
        "Stepped transmission 1",
        "XOalign",
        "dials.align_crystal",
        "EDNA MXv1",
    ]
    if program not in all_screening_programs:
        return
    outcomes.update(
        {
            program: {"success": None}
            for program in all_screening_programs
            if program not in outcomes
        }
    )
    error_explanation = (
        "{variable}: {value} outside range {expected}, program: {program}, DCID:{dcid}"
    )
    if not outcomes[program]["success"]:
        failure_reasons = []
        results = {"spacegroup": screening_output_lattice.spaceGroup}
        results.update(
            {
                v: getattr(screening_output_lattice, "unitCell_" + v)
                for v in ("a", "b", "c", "alpha", "beta", "gamma")
            }
        )
        for variable, outcome in results.items():
            if outcome is None or expected_outcome[variable] != outcome:
                failure_reasons.append(
                    error_explanation.format(
                        variable=variable,
                        value=outcome,
                        expected=expected_outcome[variable],
                        program=program,
                        dcid=dcid,
                    )
                )
        if failure_reasons:
            outcomes[program]["success"] = False
            outcomes[program]["reason"] = failure_reasons
        else:
            outcomes[program]["success"] = True
            outcomes[program]["reason"] = []


def check_integration_outcomes(dcid, program, integration, outcomes, expected_outcome):
    all_integration_programs = {
        "fast_dp",
        "xia2 3dii",
        "xia2 dials",
        "autoPROC",
        "autoPROC+STARANISO",
    }
    if program not in all_integration_programs:
        return
    outcomes.update(
        {
            program: {"success": None}
            for program in all_integration_programs
            if program not in outcomes
        }
    )
    error_explanation = (
        "{variable}: {value} outside range {expected}, program: {program}, DCID:{dcid}"
    )

    if not outcomes[program]["success"]:
        failure_reasons = []
        # No result registered, may still be running
        if integration.cell_a and expected_outcome:
            for variable in ("a", "b", "c", "alpha", "beta", "gamma"):
                outcome = getattr(integration, "cell_" + variable)
                if outcome is None or expected_outcome[variable] != outcome:
                    failure_reasons.append(
                        error_explanation.format(
                            variable=variable,
                            value=outcome,
                            expected=expected_outcome[variable],
                            program=program,
                            dcid=dcid,
                        )
                    )
        outcomes[program]["success"] = not failure_reasons
        outcomes[program]["reason"] = failure_reasons


def check_pia_outcomes(dcid, image_count, iqi_count, outcomes, expected_outcome):
    error_explanation = "Expected PIA result count for {dcid}: {expected}, got {actual}"
    expected_pia_count = expected_outcome.get("pia", min(image_count, 250))
    if iqi_count == expected_pia_count:
        outcomes["pia"] = {"success": True}
    else:
        outcomes["pia"] = {
            "success": None,
            "reason": [
                error_explanation.format(
                    dcid=dcid,
                    expected=expected_pia_count,
                    actual=iqi_count,
                )
            ],
        }
    return outcomes


def _retrieve_motioncorr(db_session, jobid):
    autoproc_query = (
        db_session.query(AutoProcProgram)
        .options(
            Load(AutoProcProgram).load_only("autoProcProgramId", "processingJobId"),
        )
        .filter(AutoProcProgram.processingJobId == jobid)
    )

    autoproc_query_result = autoproc_query.order_by(
        AutoProcProgram.autoProcProgramId.desc()
    ).first()
    autoprocpid = autoproc_query_result.autoProcProgramId

    query = db_session.query(MotionCorrection).filter(
        MotionCorrection.autoProcProgramId == autoprocpid
    )

    query_results = query.all()

    return list(query_results), autoprocpid


def _retrieve_ctf(db_session, autoprocid):
    query = (
        db_session.query(CTF, MotionCorrection)
        .join(
            MotionCorrection,
            MotionCorrection.motionCorrectionId == CTF.motionCorrectionId,
        )
        .filter(MotionCorrection.autoProcProgramId == autoprocid)
    )
    query_results = query.all()

    return [q[0] for q in query_results]


def _retrieve_relative_ice_thickness(db_session, autoprocid):
    query = (
        db_session.query(RelativeIceThickness, MotionCorrection)
        .join(
            MotionCorrection,
            MotionCorrection.motionCorrectionId
            == RelativeIceThickness.motionCorrectionId,
        )
        .filter(MotionCorrection.autoProcProgramId == autoprocid)
    )
    query_results = query.all()

    return [q[0] for q in query_results]


def _retrieve_particle_picker(db_session, autoprocid):
    query = (
        db_session.query(ParticlePicker, MotionCorrection)
        .join(
            MotionCorrection,
            MotionCorrection.motionCorrectionId
            == ParticlePicker.firstMotionCorrectionId,
        )
        .filter(MotionCorrection.autoProcProgramId == autoprocid)
    )
    query_results = query.all()

    return [q[0] for q in query_results]


def _retrieve_particle_classification(db_session, autoprocid):
    query = (
        db_session.query(ParticleClassification, ParticleClassificationGroup)
        .join(
            ParticleClassificationGroup,
            ParticleClassificationGroup.particleClassificationGroupId
            == ParticleClassification.particleClassificationGroupId,
        )
        .filter(ParticleClassificationGroup.programId == autoprocid)
    )

    query_results = query.all()

    return [q[0] for q in query_results]


def check_relion_outcomes(job_results, expected_outcome, jobid):
    all_programs = [
        "relion",
    ]
    outcomes = {program: {"success": None} for program in all_programs}

    failure_reasons = []
    count_based_failure_reasons = []
    for record_type, readable_name, get_micrograph_path in (
        ("motion_correction", "motion correction", attrgetter("micrographFullPath")),
        ("ctf", "CTF", attrgetter("MotionCorrection.micrographFullPath")),
        (
            "relative_ice_thickness",
            "ice thickness",
            attrgetter("MotionCorrection.micrographFullPath"),
        ),
        (
            "particle_picker",
            "Particle Picker",
            attrgetter("MotionCorrection.micrographFullPath"),
        ),
    ):
        seen_records = defaultdict(int)
        for result in job_results[record_type]:
            micrograph = get_micrograph_path(result)
            if not micrograph:
                failure_reasons.append(f"Unexpected {readable_name} result: {result!r}")
                continue
            expected_record = expected_outcome[record_type].get(micrograph)
            if not expected_record:
                failure_reasons.append(
                    f"Unexpected {readable_name} result for micrograph {micrograph}"
                )
                continue
            seen_records[micrograph] += 1
            for variable in expected_record:
                outcome = getattr(result, variable, None)
                if outcome is None or expected_record[variable] != outcome:
                    failure_reasons.append(
                        f"{readable_name} for {micrograph} {variable}: {outcome} "
                        f"outside range {expected_record[variable]} in JobID:{jobid}"
                    )
        if len(seen_records) != len(expected_outcome[record_type]):
            # if the lengths don't match the test is inconclusive so will need to set success to None
            count_based_failure_reasons.append(
                f"Out of {len(expected_outcome[record_type])} expected micrographs "
                f"only {len(seen_records)} were seen"
            )
        if any(count > 1 for count in seen_records.values()):
            failure_reasons.append(
                f"{readable_name} micrographs were seen more than once: %r"
                % {
                    micrograph
                    for micrograph, count in seen_records.items()
                    if count > 1
                }
            )

    outcomes["relion"]["reason"] = failure_reasons + count_based_failure_reasons
    outcomes["relion"]["success"] = not failure_reasons
    if outcomes["relion"]["success"] and count_based_failure_reasons:
        outcomes["relion"]["success"] = None
    return outcomes


if __name__ == "__main__":
    db = ispyb.open("/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg")

    check_test_outcome(
        {
            "time_start": 1539264122.187212,
            "scenario": "native",
            "success": None,
            "time_end": 1539264176.104456,
            "DCIDs": [3029691],
            "beamline": "i03",
        },
        db,
    )
