from __future__ import annotations

import random
import time
from dataclasses import dataclass
from unittest import mock

import pytest

import dlstbx.dc_sim.check
import dlstbx.dc_sim.definitions

all_programs = ["fast_dp", "xia2 3dii", "xia2 dials", "autoPROC", "autoPROC+STARANISO"]


@dataclass(frozen=True)
class DataCollectionResult:
    dataCollectionId: int = 12345
    numberOfImages: int = 900


@dataclass(frozen=True)
class AutoProcProgramResult:
    processingPrograms: str


@dataclass(frozen=True)
class AutoProcIntegrationResult:
    cell_a: float
    cell_b: float
    cell_c: float
    cell_alpha: float
    cell_beta: float
    cell_gamma: float
    autoProcIntegrationId: int = 12345


@dataclass(frozen=True)
class ScreeningOutputResult:
    program: str
    screeningId: int = 12345


@dataclass(frozen=True)
class ScreeningOutputLatticeResult:
    SpaceGroup: str
    unitCell_a: float = 100.0
    unitCell_b: float = 100.0
    unitCell_c: float = 100.0
    unitCell_alpha: float = 100.0
    unitCell_beta: float = 100.0
    unitCell_gamma: float = 100.0


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

    result_mocks = []
    for dcid, dcid_results in results.items():
        for program, success in dcid_results:
            data_collection = DataCollectionResult(dataCollectionId=dcid)
            autoprocprogram = AutoProcProgramResult(processingPrograms=program)
            if success:
                integration = AutoProcIntegrationResult(
                    cell_a=mock.ANY,
                    cell_b=mock.ANY,
                    cell_c=mock.ANY,
                    cell_alpha=mock.ANY,
                    cell_beta=mock.ANY,
                    cell_gamma=mock.ANY,
                )
            else:
                integration = AutoProcIntegrationResult(
                    cell_a=mock.ANY,
                    cell_b=mock.ANY,
                    cell_c=mock.ANY,
                    cell_alpha=mock.ANY,
                    cell_beta=-2,
                    cell_gamma=mock.ANY,
                )
            screening_output = None
            screening_output_lattice = None
            iqi_count = 250
            result_mocks.append(
                (
                    data_collection,
                    autoprocprogram,
                    integration,
                    screening_output,
                    screening_output_lattice,
                    iqi_count,
                )
            )
    return result_mocks, test_dictionary


@mock.patch("dlstbx.dc_sim.check._retrieve_mx_processing")
def test_check_should_return_NONE_if_no_processing_results_have_arrived_yet(
    mock_retrieve_mx_processing,
):
    result_mocks, result = make_dummy_db_and_test_dictionary(
        "i03", "i03-native", {2960726: []}
    )
    mock_retrieve_mx_processing.return_value = result_mocks

    dlstbx.dc_sim.check.check_test_outcome(result, mock.Mock())

    assert result["success"] is None


@mock.patch("dlstbx.dc_sim.check._retrieve_mx_processing")
def test_check_should_return_NONE_if_some_successful_processing_results_are_available(
    mock_retrieve_mx_processing,
):
    result_mocks, result = make_dummy_db_and_test_dictionary(
        "i03", "i03-native", {2960726: [(p, True) for p in all_programs[:2]]}
    )
    mock_retrieve_mx_processing.return_value = result_mocks

    dlstbx.dc_sim.check.check_test_outcome(result, mock.Mock())

    assert result["success"] is None


@mock.patch("dlstbx.dc_sim.check._retrieve_mx_processing")
def test_check_should_return_PASS_if_all_processing_results_are_good(
    mock_retrieve_mx_processing,
):
    result_mocks, result = make_dummy_db_and_test_dictionary(
        "i03", "i03-native", {2960726: [(p, True) for p in all_programs]}
    )
    mock_retrieve_mx_processing.return_value = result_mocks

    dlstbx.dc_sim.check.check_test_outcome(result, mock.Mock())

    assert result["success"] is True


@mock.patch("dlstbx.dc_sim.check._retrieve_mx_processing")
def test_check_should_return_PASS_if_all_processing_results_are_good_no_matter_what_other_programs_say(
    mock_retrieve_mx_processing,
):
    result_mocks, result = make_dummy_db_and_test_dictionary(
        "i03",
        "i03-native",
        {2960726: [(p, True) for p in all_programs] + [("random program", False)]},
    )
    mock_retrieve_mx_processing.return_value = result_mocks

    dlstbx.dc_sim.check.check_test_outcome(result, mock.Mock())

    assert result["success"] is True


# @pytest.mark.parametrize('program', all_programs)
@mock.patch("dlstbx.dc_sim.check._retrieve_mx_processing")
def test_check_should_ignore_invalid_results_if_another_valid_result_exists_from_the_same_program(
    mock_retrieve_mx_processing,
):
    program = all_programs[3]

    # case where only a single program returned results: outcome should be undecided
    result_mocks, result = make_dummy_db_and_test_dictionary(
        "i03", "i03-native", {2960726: [(program, True), (program, False)]}
    )
    mock_retrieve_mx_processing.return_value = result_mocks

    dlstbx.dc_sim.check.check_test_outcome(result, mock.Mock())

    assert result["success"] is None

    # case where all programs passed: outcome should be PASS
    result_mocks, result = make_dummy_db_and_test_dictionary(
        "i03",
        "i03-native",
        {2960726: [(program, False)] + [(p, True) for p in all_programs]},
    )
    mock_retrieve_mx_processing.return_value = result_mocks

    dlstbx.dc_sim.check.check_test_outcome(result, mock.Mock())

    assert result["success"] is True


@mock.patch("dlstbx.dc_sim.check._retrieve_mx_processing")
def test_check_should_return_FAIL_if_all_processing_results_are_bad(
    mock_retrieve_mx_processing,
):
    result_mocks, result = make_dummy_db_and_test_dictionary(
        "i03", "i03-native", {2960726: [(p, False) for p in all_programs]}
    )
    mock_retrieve_mx_processing.return_value = result_mocks

    dlstbx.dc_sim.check.check_test_outcome(result, mock.Mock())

    assert result["success"] is False
    # and a reason should be given
    assert result["reason"]


@mock.patch("dlstbx.dc_sim.check._retrieve_mx_processing")
@pytest.mark.parametrize("broken_program", all_programs)
def test_check_should_return_FAIL_if_one_processing_result_is_bad(
    mock_retrieve_mx_processing, broken_program
):
    broken_results = [(p, p != broken_program) for p in all_programs]
    random.shuffle(broken_results)

    result_mocks, result = make_dummy_db_and_test_dictionary(
        "i03", "i03-native", {2960726: broken_results}
    )
    mock_retrieve_mx_processing.return_value = result_mocks

    dlstbx.dc_sim.check.check_test_outcome(result, mock.Mock())

    assert result["success"] is False
    # and a reason should be given
    assert result["reason"]
    # and the broken program should be mentioned
    assert broken_program in result["reason"]


@mock.patch("dlstbx.dc_sim.check._retrieve_mx_processing")
@pytest.mark.parametrize("broken_program", all_programs)
def test_check_should_return_FAIL_if_a_single_processing_result_is_bad(
    mock_retrieve_mx_processing, broken_program
):
    result_mocks, result = make_dummy_db_and_test_dictionary(
        "i03", "i03-native", {2960726: [(broken_program, False)]}
    )
    mock_retrieve_mx_processing.return_value = result_mocks

    dlstbx.dc_sim.check.check_test_outcome(result, mock.Mock())

    assert result["success"] is False
    # and a reason should be given
    assert result["reason"]
    # and the broken program should be mentioned
    assert broken_program in result["reason"]


@mock.patch("dlstbx.dc_sim.check._retrieve_mx_processing")
@pytest.mark.parametrize("missing_program", all_programs)
@pytest.mark.parametrize("added_program", all_programs + ["random program"])
def test_check_should_not_be_confused_by_other_programs_appearing_instead_of_known_programs(
    mock_retrieve_mx_processing, missing_program, added_program
):
    if missing_program == added_program:
        return

    program_list = set(all_programs)
    program_list.remove(missing_program)
    program_list.add(added_program)
    result_mocks, result = make_dummy_db_and_test_dictionary(
        "i03", "i03-native", {2960726: [(p, True) for p in program_list]}
    )
    mock_retrieve_mx_processing.return_value = result_mocks

    dlstbx.dc_sim.check.check_test_outcome(result, mock.Mock())

    assert result["success"] is None
