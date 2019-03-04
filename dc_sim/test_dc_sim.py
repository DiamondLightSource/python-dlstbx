from __future__ import absolute_import, division, print_function

import mock
import pytest
import random
import time

import dlstbx.dc_sim.check
import dlstbx.dc_sim.definitions

all_programs = ["fast_dp", "xia2 3dii", "xia2 dials", "autoPROC", "autoPROC+STARANISO"]


def make_dummy_db_and_test_dictionary(beamline, scenario, results):
    """
  Helper function to create a dummy database interface and a dictionary to pass
  to the check function.

  :param beamline: lower case string
  :param scenario: scenario name as given in definitions
  :param results: dictionary of DCID to list of results. Results are
                  (string, boolean) tuples of program names
                  and whether or not this test should be successful.
  """

    test_dictionary = {
        "beamline": beamline,
        "scenario": scenario,
        "DCIDs": [int(i) for i in results],
        "success": None,
        "time_start": time.time() - 900,
        "time_end": time.time() - 1,
    }

    db = mock.Mock()
    db.get_data_collection.return_value.integrations = []

    result_mocks = {}
    for dcid, dcid_results in results.items():
        result_mocks[dcid] = mock.Mock()
        result_mocks[dcid].integrations = []
        for program, success in dcid_results:
            m = mock.Mock()
            m.program.name = program
            m.unit_cell.a = mock.ANY
            m.unit_cell.b = mock.ANY
            m.unit_cell.c = mock.ANY
            m.unit_cell.alpha = mock.ANY
            m.unit_cell.beta = mock.ANY
            m.unit_cell.gamma = mock.ANY
            if not success:
                m.unit_cell.beta = -2
            result_mocks[dcid].integrations.append(m)
    db.get_data_collection = lambda x: result_mocks[x]
    return db, test_dictionary


def test_check_should_return_NONE_if_no_processing_results_have_arrived_yet():
    db, result = make_dummy_db_and_test_dictionary("i03", "native", {2960726: []})

    dlstbx.dc_sim.check.check_test_outcome(result, db)

    assert result["success"] is None


def test_check_should_return_NONE_if_some_successful_processing_results_are_available():
    db, result = make_dummy_db_and_test_dictionary(
        "i03", "native", {2960726: [(p, True) for p in all_programs[:2]]}
    )

    dlstbx.dc_sim.check.check_test_outcome(result, db)

    assert result["success"] is None


def test_check_should_return_PASS_if_all_processing_results_are_good():
    db, result = make_dummy_db_and_test_dictionary(
        "i03", "native", {2960726: [(p, True) for p in all_programs]}
    )

    dlstbx.dc_sim.check.check_test_outcome(result, db)

    assert result["success"] is True


def test_check_should_return_PASS_if_all_processing_results_are_good_no_matter_what_other_programs_say():
    db, result = make_dummy_db_and_test_dictionary(
        "i03",
        "native",
        {2960726: [(p, True) for p in all_programs] + [("random program", False)]},
    )

    dlstbx.dc_sim.check.check_test_outcome(result, db)

    assert result["success"] is True


# @pytest.mark.parametrize('program', all_programs)
def test_check_should_ignore_invalid_results_if_another_valid_result_exists_from_the_same_program():
    program = all_programs[3]

    # case where only a single program returned results: outcome should be undecided
    db, result = make_dummy_db_and_test_dictionary(
        "i03", "native", {2960726: [(program, True), (program, False)]}
    )

    dlstbx.dc_sim.check.check_test_outcome(result, db)

    assert result["success"] is None

    # case where all programs passed: outcome should be PASS
    db, result = make_dummy_db_and_test_dictionary(
        "i03",
        "native",
        {2960726: [(program, False)] + [(p, True) for p in all_programs]},
    )

    dlstbx.dc_sim.check.check_test_outcome(result, db)

    assert result["success"] is True


def test_check_should_return_FAIL_if_all_processing_results_are_bad():
    db, result = make_dummy_db_and_test_dictionary(
        "i03", "native", {2960726: [(p, False) for p in all_programs]}
    )

    dlstbx.dc_sim.check.check_test_outcome(result, db)

    assert result["success"] is False
    # and a reason should be given
    assert result["reason"]


@pytest.mark.parametrize("broken_program", all_programs)
def test_check_should_return_FAIL_if_one_processing_result_is_bad(broken_program):
    broken_results = [(p, p != broken_program) for p in all_programs]
    random.shuffle(broken_results)

    db, result = make_dummy_db_and_test_dictionary(
        "i03", "native", {2960726: broken_results}
    )

    dlstbx.dc_sim.check.check_test_outcome(result, db)

    assert result["success"] is False
    # and a reason should be given
    assert result["reason"]
    # and the broken program should be mentioned
    assert broken_program in result["reason"]


@pytest.mark.parametrize("broken_program", all_programs)
def test_check_should_return_FAIL_if_a_single_processing_result_is_bad(broken_program):
    db, result = make_dummy_db_and_test_dictionary(
        "i03", "native", {2960726: [(broken_program, False)]}
    )

    dlstbx.dc_sim.check.check_test_outcome(result, db)

    assert result["success"] is False
    # and a reason should be given
    assert result["reason"]
    # and the broken program should be mentioned
    assert broken_program in result["reason"]


@pytest.mark.parametrize("missing_program", all_programs)
@pytest.mark.parametrize("added_program", all_programs + ["random program"])
def test_check_should_not_be_confused_by_other_programs_appearing_instead_of_known_programs(
    missing_program, added_program
):
    if missing_program == added_program:
        return

    program_list = set(all_programs)
    program_list.remove(missing_program)
    program_list.add(added_program)
    db, result = make_dummy_db_and_test_dictionary(
        "i03", "native", {2960726: [(p, True) for p in program_list]}
    )

    dlstbx.dc_sim.check.check_test_outcome(result, db)

    assert result["success"] is None
