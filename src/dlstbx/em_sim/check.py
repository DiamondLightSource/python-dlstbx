import dlstbx.em_sim.definitions as df
import ispyb
import ispyb.model.__future__
import ispyb.sqlalchemy
from ispyb.sqlalchemy import MotionCorrection, CTF
import sqlalchemy
from sqlalchemy.orm import Load


def check_test_outcome(test, db):
    failed_tests = []
    overall = {}
    expected_outcome = df.tests.get(test["scenario"], {}).get("results")

    url = ispyb.sqlalchemy.url("/dls_sw/dasc/mariadb/credentials/ispyb.cfg")
    engine = sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    db_session = sqlalchemy.orm.Session(bind=engine)

    if expected_outcome == {}:
        print("Scenario %s is happy with any outcome." % test["scenario"])
        test["success"] = True
        return

    if not expected_outcome:
        print("Skipping unknown test scenario %s" % test["scenario"])
        return

    program = "relion"
    data_collection_results = {}
    for jpid in test["JobIDs"]:
        data_collection_results[jpid] = {}
        data_collection_results["motion_correction"] = retrieve_motioncorr(
            db_session, jpid
        )
        data_collection_results["ctf"] = retrieve_ctf(db_session, jpid)
        outcomes = check_relion_outcomes(
            data_collection_results, expected_outcome, jpid
        )

        overall.setdefault(program, True)
        if outcomes[program]["success"] is False:
            overall[program] = False
            failed_tests.extend(outcomes[program]["reason"])
        elif outcomes[program]["success"] is None and overall[program] is not False:
            overall[program] = None

    if failed_tests:
        test["success"] = False
        test["reason"] = "\n".join(failed_tests)

    if all(overall.values()):
        test["success"] = True

    print(test)
    return test


def retrieve_motioncorr(db_session, jpid):
    query = db_session.query(MotionCorrection).filter(
        MotionCorrection.dataCollectionId == dcid
    )
    query_results = query.all()
    required_records = [
        "micrographFullPath",
        "totalMotion",
        "averageMotionPerFrame",
    ]
    required_lines = []
    for qr in query_results:
        required_lines.append([q.getattr(r) for r in required_records])
    return [dict(zip(required_records, line)) for line in required_lines]


def retrieve_ctf(db_session, jpid):
    query = (
        db_session.query(CTF, MotionCorrection)
        .join(
            MotionCorrection,
            MotionCorrection.motionCorrectionId == CTF.motionCorrectionId,
        )
        .filter(MotionCorrection.autoProcProgramId == autoprocid)
    )
    query_results = query.all()
    required_records = [
        "astimagtism",
        "astigmatismAngle",
        "maxResolution",
        "estimatedDefocus",
        "ccValue",
    ]
    required_lines = []
    for qr in query_results:
        required_lines.append([q.getattr(r) for r in required_records])
    return [dict(zip(required_records, line)) for line in required_lines]


def check_relion_outcomes(data_collection_results, expected_outcome, jpid):
    all_programs = [
        "relion",
    ]
    error_explanation = (
        "{variable}: {value} outside range {expected}, program: {program}, JobID:{jpid}"
    )
    outcomes = {program: {"success": None} for program in all_programs}

    failure_reasons = []

    tabvars = {
        "motion_corr": [
            "micrographFulPath",
            "totalMotion",
            "averageMotionPerFrame",
        ],
        "ctf": [
            "astigmatism",
            "astigmatismAngle",
            "maxEstimatedResolution",
            "estiamtedDefocus",
            "ccValue",
        ],
    }

    for table in ("motion_corr", "ctf"):
        for variable in tabvars[table]:
            outcome = data_collection_results.get(variable)
            if outcome is None or expected_outcome[variable] != outcome:
                failure_reasons.append(
                    error_explanation.format(
                        variable=variable,
                        value=outcome,
                        expected=expected_outcome[variable],
                        jpid=jpid,
                    )
                )

    if failure_reasons:
        outcomes["relion"]["success"] = False
        outcomes["relion"]["reason"] = failure_reasons
    else:
        outcomes["relion"]["success"] = True
        outcomes["relion"]["reason"] = []

    return outcomes
