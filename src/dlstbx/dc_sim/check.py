import logging
from collections import defaultdict

import ispyb
import ispyb.model.__future__
import ispyb.sqlalchemy
import sqlalchemy
from ispyb.sqlalchemy import CTF, AutoProcProgram, MotionCorrection
from sqlalchemy.orm import Load

import dlstbx.dc_sim.definitions as df

logger = logging.getLogger("dlxtbx.dc_sim.check")


def check_test_outcome(test, db_classic, db_session=None):
    expected_outcome = df.tests.get(test["scenario"], {}).get("results")

    url = ispyb.sqlalchemy.url()
    engine = sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    db_session = sqlalchemy.orm.sessionmaker(bind=engine)

    if expected_outcome == {}:
        print(f"Scenario {test['scenario']} is happy with any outcome.")
        test["success"] = True
        return

    if not expected_outcome:
        print(f"Skipping unknown test scenario {test['scenario']}")
        return

    check_functions = {
        "mx": _check_mx_outcome,
        "em-spa": _check_relion_outcome,
    }
    scenario_type = df.tests[test["scenario"]]["type"]
    if scenario_type not in check_functions:
        print(f"Skipping unknown test scenario type {scenario_type}")
        return

    check_function = check_functions[scenario_type]

    with db_session() as dbs:
        try:
            overall, failed_tests = check_function(
                test, expected_outcome, db_classic, dbs
            )
        except Exception as e:
            print(f"Test validation failed with {e}, leaving test alone")
            logger.error(
                f"dc_sim valication for {scenario_type} failed with {e}", exc_info=True
            )
            return test

    if failed_tests:
        test["success"] = False
        test["reason"] = "\n".join(failed_tests)

    if all(overall.values()):
        test["success"] = True

    print(test)
    return test


def _check_mx_outcome(test, expected_outcome, db, db_session_unused):
    failed_tests = []
    overall = {}
    for dcid in test.get("DCIDs", []):
        data_collection = db.get_data_collection(dcid)
        if getattr(data_collection, "screenings", None):
            outcomes = check_screening_outcomes(data_collection, expected_outcome)
        else:
            outcomes = check_integration_outcomes(data_collection, expected_outcome)

        if getattr(data_collection, "image_count", None):
            outcomes.update(check_pia_outcomes(data_collection, expected_outcome))

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


def _check_relion_outcome(test, expected_outcome, db_unused, db):
    failed_tests = []
    overall = {}
    for jobid in test.get("JobIDs", []):
        program = "relion"
        motioncorr_data, autoprocpid = _retrieve_motioncorr(db, jobid)
        job_results = {
            "motion_correction": motioncorr_data,
            "ctf": _retrieve_ctf(db, autoprocpid),
        }
        if len(job_results["motion_correction"]) == 0 or len(job_results["ctf"]) == 0:
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


def check_screening_outcomes(data_collection, expected_outcome):
    all_programs = [
        "mosflm",
        "Stepped transmission 1",
        "XOalign",
        "dials.align_crystal",
        "EDNA MXv1",
    ]
    error_explanation = (
        "{variable}: {value} outside range {expected}, program: {program}, DCID:{dcid}"
    )
    outcomes = {program: {"success": None} for program in all_programs}
    for screening in data_collection.screenings:
        if screening.program not in outcomes:
            continue
        if outcomes[screening.program]["success"]:
            continue

        failure_reasons = []

        for screening_output in screening.outputs:
            for lattice in screening_output.lattices:
                results = {"spacegroup": lattice.spacegroup}
                print(lattice.unit_cell)
                for v in ("a", "b", "c", "alpha", "beta", "gamma"):
                    results[v] = getattr(lattice.unit_cell, v)
                for variable, outcome in results.items():
                    if outcome is None or expected_outcome[variable] != outcome:
                        failure_reasons.append(
                            error_explanation.format(
                                variable=variable,
                                value=outcome,
                                expected=expected_outcome[variable],
                                program=screening.program,
                                dcid=data_collection.dcid,
                            )
                        )

        if failure_reasons:
            outcomes[screening.program]["success"] = False
            outcomes[screening.program]["reason"] = failure_reasons
        else:
            outcomes[screening.program]["success"] = True
            outcomes[screening.program]["reason"] = []

    return outcomes


def check_integration_outcomes(data_collection, expected_outcome):
    all_programs = [
        "fast_dp",
        "xia2 3dii",
        "xia2 dials",
        "autoPROC",
        "autoPROC+STARANISO",
    ]
    error_explanation = (
        "{variable}: {value} outside range {expected}, program: {program}, DCID:{dcid}"
    )
    outcomes = {program: {"success": None} for program in all_programs}

    for integration in data_collection.integrations:
        if integration.program.name not in outcomes:
            continue
        if outcomes[integration.program.name]["success"] is True:
            continue

        failure_reasons = []

        if integration.unit_cell.a is None:
            # No result registered, may still be running
            continue

        for variable in ("a", "b", "c", "alpha", "beta", "gamma"):
            outcome = getattr(integration.unit_cell, variable)
            if outcome is None or expected_outcome[variable] != outcome:
                failure_reasons.append(
                    error_explanation.format(
                        variable=variable,
                        value=outcome,
                        expected=expected_outcome[variable],
                        program=integration.program.name,
                        dcid=data_collection.dcid,
                    )
                )

        outcomes[integration.program.name]["success"] = not failure_reasons
        outcomes[integration.program.name]["reason"] = failure_reasons

    return outcomes


def check_pia_outcomes(data_collection, expected_outcome):
    error_explanation = "Expected PIA result count for {dcid}: {expected}, got {actual}"
    expected_pia_count = expected_outcome.get(
        "pia", min(data_collection.image_count, 250)
    )
    if not hasattr(data_collection, "image_quality"):
        outcomes = {}
    elif len(data_collection.image_quality) == expected_pia_count:
        outcomes = {"pia": {"success": True}}
    else:
        outcomes = {
            "pia": {
                "success": None,
                "reason": [
                    error_explanation.format(
                        dcid=data_collection.dcid,
                        expected=expected_pia_count,
                        actual=len(data_collection.image_quality),
                    )
                ],
            }
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


def check_relion_outcomes(job_results, expected_outcome, jobid):
    all_programs = [
        "relion",
    ]
    outcomes = {program: {"success": None} for program in all_programs}

    failure_reasons = []

    seen_micrographs = defaultdict(int)
    for result in job_results["motion_correction"]:
        micrograph = result.micrographFullPath
        if not micrograph:
            failure_reasons.append(f"Unexpected motion correction result: {result!r}")
            continue
        expected_mc = expected_outcome["motion_correction"].get(micrograph)
        if not expected_mc:
            failure_reasons.append(
                f"Unexpected motion correction result for micrograph {micrograph}"
            )
            continue
        seen_micrographs[micrograph] += 1
        for variable in expected_mc:
            outcome = getattr(result, variable, None)
            if outcome is None or expected_mc[variable] != outcome:
                failure_reasons.append(
                    f"Motion correction for {micrograph} {variable}: {outcome} outside range {expected_mc[variable]} in JobID:{jobid}"
                )
    if len(seen_micrographs) != len(expected_outcome["motion_correction"]):
        failure_reasons.append(
            "Out of %d expected micrographs only %d were seen"
            % (len(expected_outcome["motion_correction"]), len(seen_micrographs))
        )
    if any(count > 1 for count in seen_micrographs.values()):
        failure_reasons.append(
            "Motion corrected micrographs were seen more than once: %r"
            % {
                micrograph
                for micrograph, count in seen_micrographs.items()
                if count > 1
            }
        )

    seen_ctfs = defaultdict(int)
    for result in job_results["ctf"]:
        micrograph = result.micrographFullPath
        if not micrograph:
            failure_reasons.append(f"Unexpected CTF result: {result!r}")
            continue
        expected_ctf = expected_outcome["ctf"].get(micrograph)
        if not expected_ctf:
            failure_reasons.append(f"Unexpected CTF result for micrograph {micrograph}")
            continue
        seen_ctfs[micrograph] += 1
        for variable in expected_ctf:
            outcome = getattr(result, variable, None)
            if outcome is None or expected_ctf[variable] != outcome:
                failure_reasons.append(
                    f"CTF for {micrograph} {variable}: {outcome} outside range {expected_ctf[variable]} in JobID:{jobid}"
                )

    outcomes["relion"]["success"] = not failure_reasons
    outcomes["relion"]["reason"] = failure_reasons
    return outcomes


if __name__ == "__main__":
    ispyb.model.__future__.enable("/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg")
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
