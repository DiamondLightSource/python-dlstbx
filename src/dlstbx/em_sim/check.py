import dlstbx.em_sim.definitions as df
import ispyb
import ispyb.model.__future__
import ispyb.sqlalchemy
from ispyb.sqlalchemy import MotionCorrection, CTF, AutoProcProgram
import sqlalchemy
from sqlalchemy.orm import Load


def check_test_outcome(test):
    failed_tests = []
    overall = {}
    expected_outcome = df.tests.get(test["scenario"], {}).get("results")

    url = ispyb.sqlalchemy.url()
    engine = sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    db_session = sqlalchemy.orm.sessionmaker(bind=engine)

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
        with db_session() as dbs:
            motioncorr_data, autoprocpid = retrieve_motioncorr(dbs, jpid)
            data_collection_results[jpid]["motion_correction"] = motioncorr_data
            data_collection_results[jpid]["ctf"] = retrieve_ctf(dbs, autoprocpid)
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
    autoproc_query = (
        db_session.query(AutoProcProgram)
        .options(
            Load(AutoProcProgram).load_only("autoProcProgramId", "processingJobId"),
        )
        .filter(AutoProcProgram.processingJobId == jpid)
    )

    autoproc_query_result = autoproc_query.order_by(
        AutoProcProgram.autoProcProgramId.desc()
    ).first()
    autoprocpid = autoproc_query_result.autoProcProgramId

    query = db_session.query(MotionCorrection).filter(
        MotionCorrection.autoProcProgramId == autoprocpid
    )

    query_results = query.all()

    return [q[0] for q in query_results], autoprocpid


def retrieve_ctf(db_session, autoprocid):
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


def check_relion_outcomes(data_collection_results, expected_outcome, jpid):
    all_programs = [
        "relion",
    ]
    # error_explanation = (
    #    "{variable}: {value} outside range {expected}, program: {program}, JobID:{jpid}"
    # )
    outcomes = {program: {"success": None} for program in all_programs}

    failure_reasons = []

    tabvars = {
        "motion_correction": [
            "micrographFullPath",
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

    for table in ("motion_correction", "ctf"):
        for variable in tabvars[table]:
            for i, expoutcome in enumerate(expected_outcome[table]):
                outcome = getattr(
                    data_collection_results[jpid][table][i], variable, None
                )
                if outcome is None or expoutcome[variable] != outcome:
                    failure_reasons.append(
                        f"{variable}: {outcome} outside range {expoutcome[variable]}, program: relion, JobID:{jpid}"
                    )

    if failure_reasons:
        outcomes["relion"]["success"] = False
        outcomes["relion"]["reason"] = failure_reasons
    else:
        outcomes["relion"]["success"] = True
        outcomes["relion"]["reason"] = []

    return outcomes
