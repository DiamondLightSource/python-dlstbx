from __future__ import annotations

import itertools
import os
import socket
import subprocess
from typing import Optional, Tuple

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status

_success = "✓"
_failure = "✘"
_history_length = 120

# For how long should the error condition persist? (runs)
_threshold_error = 15
_threshold_warning = 45


def _get_test_failure_history(status: Optional[Status]) -> list[bool]:
    if not status or not status.MessageBody:
        return []
    test_history = [
        char == _failure for char in status.MessageBody if char in {_success, _failure}
    ]
    return test_history[-(_history_length - 1) :]


def _grouper(iterable, n, fillvalue=None):
    "Collect data into non-overlapping fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    # taken from https://docs.python.org/3/library/itertools.html
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def _run_fs_check(filesystem: str, location: str) -> Tuple[bool, str]:
    result = subprocess.run(
        ("/home/dlshudson/dials_check_dls_it/ministresstest", location),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=15,
    )
    output = result.stdout.decode("latin-1")
    return (result.returncode == 0, output)


def _print_history(history: list[bool], success: bool, message: str) -> str:
    history.append(not success)
    historychars = (_failure if result else _success for result in history)
    if message:
        message += "\n"
    return (
        f"{message}Test history timeline (from oldest to most recent):\n"
        + "\n".join("".join(char) for char in _grouper(historychars, 60, fillvalue=""))
    )


def check_filesystems(cfc: CheckFunctionInterface):
    systems = (
        ("/dls/i03", "/dls/i03/data/2023/cm33866-1/tmp/"),
        ("/dls/i04", "/dls/i04/data/2023/cm33903-1/tmp/"),
        ("/dls/i04-1", "/dls/i04-1/data/2023/cm33904-1/tmp"),
        ("/dls/i18", "/dls/i18/data/2023/cm33872-1/tmp"),
        ("/dls/i19-1", "/dls/i19-1/data/2023/cm33867-1/tmp"),
        ("/dls/i19-2", "/dls/i19-2/data/2023/cm33868-1/tmp/"),
        ("/dls/i23", "/dls/i23/data/2023/cm33851-1/tmp"),
        ("/dls/i24", "/dls/i24/data/test"),
        ("/dls/m12", "/dls/m12/data/2023/cm33870-3/tmp"),
        ("/dls/mx/data", "/dls/mx/data/nt33918/nt33918-9/tmp/"),
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
        elif sum(fs_test_history):
            minimum_result_level = REPORT.NOTICE
        try:
            outcome_success, output = _run_fs_check(filesystem, location)
            if outcome_success:
                outcome_message, outcome_body = "OK", ""
            else:
                outcome_message, outcome_body = "Filesystem check failed", output
        except subprocess.TimeoutExpired:
            outcome_success = False
            outcome_message, outcome_body = "Filesystem check failed with timeout", ""
        except Exception as e:
            outcome_success = False
            outcome_message, outcome_body = "Filesystem check failed", repr(e)

        if outcome_success:
            outcome_level = max(REPORT.PASS, minimum_result_level)
        else:
            outcome_level = REPORT.ERROR
        if outcome_level == REPORT.PASS:
            outcome_url = ""
        else:
            outcome_url = "https://confluence.diamond.ac.uk/display/SCI/Stress-testing+the+filesystem"
        outcome_body = _print_history(
            fs_test_history, success=outcome_success, message=outcome_body
        )
        if sum(fs_test_history):
            outcome_message += f", {sum(fs_test_history)} out of {len(fs_test_history)} test runs failed"
            if outcome_success:
                outcome_message += f", last failure {list(reversed(fs_test_history)).index(True)} run(s) ago"

        results.append(
            Status(
                Source=fs_test_name,
                Level=outcome_level,
                Message=outcome_message,
                MessageBody=outcome_body,
                URL=outcome_url,
            )
        )
    return results


def _parse_df_output(source: str, percent_str: str, directory: str) -> Status:
    percent = int(percent_str.rstrip("%"))
    message = f"{percent}% used on {directory}"
    if percent > 95:
        return Status(
            Source=source, Level=REPORT.ERROR, Message=f"{message} (should be <= 95%)"
        )
    if percent > 93:
        return Status(Source=source, Level=REPORT.WARNING, Message=message)
    if percent > 90:
        return Status(Source=source, Level=REPORT.NOTICE, Message=message)
    return Status(Source=source, Level=REPORT.PASS, Message=message)


def _run_df_in(
    directory: str, *, test_name: str, source_prefix: Optional[str] = None
) -> Status:
    readable_directory = directory.replace("/", ".")
    if readable_directory == ".":
        readable_directory = ".root"
    source = test_name + (source_prefix or "") + readable_directory

    try:
        result = subprocess.run(
            ("df", directory),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
        )
        output = result.stdout.decode("latin-1")
        if result.returncode:
            return Status(
                Source=source,
                Level=REPORT.WARNING,
                Message=f"Could not determine free space on {directory}",
                MessageBody=output,
            )
    except Exception as e:
        return Status(
            Source=source,
            Level=REPORT.WARNING,
            Message=f"Could not determine free space on {directory}",
            MessageBody=repr(e),
        )

    df_output = output.split("\n")
    df_output.pop(0)
    parseable_output = " ".join(df_output).split()
    device, size, used, available, percent_str, mountpoint = parseable_output
    return _parse_df_output(source, percent_str, directory)


def _run_df_locally(test_name: str, machine_name: str) -> list[Status]:
    try:
        result = subprocess.run(
            ("df", "-l", "-x", "tmpfs", "-x", "devtmpfs"),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
        )
        output = result.stdout.decode("latin-1").strip()
        if result.returncode:
            return Status(
                Source=f"{test_name}.{machine_name}.root",
                Level=REPORT.WARNING,
                Message="Could not determine free space on local drives",
                MessageBody=output,
            )
    except Exception as e:
        return Status(
            Source=f"{test_name}.{machine_name}.root",
            Level=REPORT.WARNING,
            Message="Could not determine free space on local drives",
            MessageBody=repr(e),
        )

    df_output = output.split("\n")
    df_output.pop(0)

    results: list[Status] = []
    for line in df_output:
        parseable_output = line.split()
        device, size, used, available, percent_str, mountpoint = parseable_output

        readable_directory = mountpoint.replace("/", ".")
        if readable_directory == ".":
            readable_directory = ".root"
        source = f"{test_name}.{machine_name}{readable_directory}"
        results.append(_parse_df_output(source, percent_str, mountpoint))
    return results


def check_free_space(cfc: CheckFunctionInterface) -> list[Status]:
    locations = {
        "/dls/science",
        "/dls/tmp",
        "/dls_sw/apps",
    }
    results: list[Status] = [
        _run_df_in(directory, test_name=cfc.name) for directory in locations
    ]
    hostname = socket.gethostname()
    if hostname.endswith(".diamond.ac.uk"):
        hostname = hostname[:-14]
        results.extend(_run_df_locally(test_name=cfc.name, machine_name=hostname))
    return results


def check_vmxi_holding_area(cfc: CheckFunctionInterface) -> Status:
    count = len(os.listdir("/dls/science/vmxi/formulatrix/"))
    message = f"{count} image(s) waiting in VMXi holding area /dls/science/vmxi/formulatrix/ (managed by Karl Levik)"
    if count <= 3000:
        return Status(Source=cfc.name, Level=REPORT.PASS, Message=message)
    if count <= 5000:
        return Status(Source=cfc.name, Level=REPORT.WARNING, Message=message)
    return Status(Source=cfc.name, Level=REPORT.ERROR, Message=message)
