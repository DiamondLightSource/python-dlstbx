from __future__ import annotations

import subprocess
from typing import Optional, Tuple

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status

_success = "✓"
_failure = "✘"
_history_length = 120

# For how long should failures be remembered?
_threshold_error = 5
_threshold_warning = 30
_threshold_notice = 60


def _run_fs_check(filesystem: str, location: str) -> Tuple[bool, str]:
    result = subprocess.run(
        ("/home/dlshudson/dials_check_dls_it/ministresstest", location),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=15,
    )
    output = result.stdout.decode("latin-1")
    return (result.returncode == 0, output)


def _get_test_failure_history(status: Optional[Status]) -> list[bool]:
    if not status or not status.MessageBody:
        return []
    last_line = status.MessageBody.split("\n")[-1]
    test_history = [
        char == _failure for char in last_line if char in {_success, _failure}
    ]
    return test_history[-(_history_length - 1) :]


def _store_test_history(history: list[bool], success: bool) -> str:
    history.append(not success)
    return "Test history timeline (from oldest to most recent):\n" + "".join(
        _failure if result else _success for result in history
    )


def check_filesystems(cfc: CheckFunctionInterface):
    systems = (
        ("/dls/i03", "/dls/i03/data/2017/cm16791-3/tmp"),
        ("/dls/i04", "/dls/i04/data/2017/cm16781-3/tmp"),
        ("/dls/i04-1", "/dls/i04-1/data/2017/cm16749-3/tmp"),
        ("/dls/i18", "/dls/i18/data/2016/cm14473-1/tmp"),
        ("/dls/i19-1", "/dls/i19-1/data/2016/cm14476-1/tmp"),
        ("/dls/i23", "/dls/i23/data/2017/cm16790-3/tmp"),
        ("/dls/i24", "/dls/i24/data/test"),
        ("/dls/mx/data", "/dls/mx/data/nt18231/nt18231-23/tmp"),
        ("/dls/science", "/dls/science/users/wra62962/jenkins"),
        ("/dls/tmp", "/dls/tmp"),
        ("/dls_sw", "/dls_sw/apps/dials"),
    )
    results: list[Status] = []
    for filesystem, location in systems:
        fs_test_name = cfc.name + filesystem.replace("/", ".")
        fs_test_history = _get_test_failure_history(
            cfc.current_status.get(fs_test_name)
        )
        minimum_result_level = REPORT.PASS
        if sum(fs_test_history[-_threshold_error:]):
            minimum_result_level = REPORT.ERROR
        elif sum(fs_test_history[-_threshold_warning:]):
            minimum_result_level = REPORT.WARNING
        elif sum(fs_test_history[-_threshold_notice:]):
            minimum_result_level = REPORT.NOTICE
        if sum(fs_test_history):
            history_summary = f", {sum(fs_test_history)} out of previous {len(fs_test_history)} test runs failed"
        else:
            history_summary = ""
        try:
            success, output = _run_fs_check(filesystem, location)
            if success:
                outcome = Status(
                    Source=fs_test_name,
                    Level=max(REPORT.PASS, minimum_result_level),
                    Message=f"Filesystem check succeeded{history_summary}",
                    MessageBody=_store_test_history(fs_test_history, success=True),
                    URL="https://confluence.diamond.ac.uk/display/SCI/Stress-testing+the+filesystem",
                )
            else:
                outcome = Status(
                    Source=fs_test_name,
                    Level=REPORT.ERROR,
                    Message=f"Filesystem check failed{history_summary}",
                    MessageBody=output
                    + "\n"
                    + _store_test_history(fs_test_history, success=False),
                    URL="https://confluence.diamond.ac.uk/display/SCI/Stress-testing+the+filesystem",
                )
        except subprocess.TimeoutExpired:
            outcome = Status(
                Source=fs_test_name,
                Level=REPORT.ERROR,
                Message=f"Filesystem check failed with timeout{history_summary}",
                MessageBody="Test exceeded 15 seconds\n"
                + _store_test_history(fs_test_history, success=False),
                URL="https://confluence.diamond.ac.uk/display/SCI/Stress-testing+the+filesystem",
            )
        except Exception as e:
            outcome = Status(
                Source=fs_test_name,
                Level=REPORT.ERROR,
                Message=f"Filesystem check failed{history_summary}",
                MessageBody=repr(e)
                + "\n"
                + _store_test_history(fs_test_history, success=False),
                URL="https://confluence.diamond.ac.uk/display/SCI/Stress-testing+the+filesystem",
            )
        results.append(outcome)
    return results
