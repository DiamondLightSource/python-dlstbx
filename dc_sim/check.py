from __future__ import absolute_import, division, print_function

import dlstbx.dc_sim.definitions as df
import ispyb
import ispyb.model.__future__


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

    if failed_tests:
        test["success"] = False
        test["reason"] = "\n".join(failed_tests)

    if all(overall.values()):
        test["success"] = True

    print(test)


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

        if failure_reasons:
            outcomes[integration.program.name]["success"] = False
            outcomes[integration.program.name]["reason"] = failure_reasons
        else:
            outcomes[integration.program.name]["success"] = True
            outcomes[integration.program.name]["reason"] = []

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
