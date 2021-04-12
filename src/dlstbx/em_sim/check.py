import dlstbx.em_sim.definitions as df
import ispyb
import ispyb.model.__future__


def collect_ctf_results(db, dcid):
    db.cursosr.execute(
        "SELECT astigmatism, astigmatism_angle, max_estimated_resolution, estiamted_defocus, cc_value"
        f"FROM CTF WHERE datacollectionid={dcid}"
    )
    desc = [d[0] for d in db.cursor.description]
    result = [dict(zip(desc, line)) for line in db.cursor]
    return result


def check_test_outcome(test, db):
    failed_tests = []
    overall = {}
    expected_outcome = df.tests.get(test["scenario"], {}).get("results")

    if expected_outcome == {}:
        print("Scenario %s is happy with any outcome." % test["scenario"])
        test["success"] = True
        return

    if not expected_outcome:
        print("Skipping unknown test scenario %s" % test["scenario"])
        return

    for dcid in test["DCIDs"]:
        data_collection = db.get_data_collection(dcid)
        outcomes = check_relion_outcomes(data_collection, expected_outcome)

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

    if failed_tests:
        test["success"] = False
        test["reason"] = "\n".join(failed_tests)

    if all(overall.values()):
        test["success"] = True

    print(test)


def check_relion_outcomes(data_collection, expected_outcome):
    all_programs = [
        "relion",
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

        tabvars = {
            "motion_corr": [
                "micrograph_name",
                "total_motion",
                "early_motion",
                "late_motion",
                "average_motion_per_frame",
            ],
            "ctf": [
                "astigmatism",
                "astigmatism_angle",
                "max_estimated_resolution",
                "estiamted_defocus",
                "cc_value",
            ],
        }

        for table in ("motion_corr", "ctf"):
            for variable in tabvars[table]:
                # need to actually so some db stuff here
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

        if failure_reasons:
            outcomes[integration.program.name]["success"] = False
            outcomes[integration.program.name]["reason"] = failure_reasons
        else:
            outcomes[integration.program.name]["success"] = True
            outcomes[integration.program.name]["reason"] = []

    return outcomes
