from __future__ import annotations

import procrunner

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status, limit_level


@limit_level(max_increase=10)
def get_diamond_ring_status(cfc: CheckFunctionInterface):
    command = ("caget", "-St", "CS-CS-MSTAT-01:MODE", "CS-CS-MSTAT-01:MESS01")
    try:
        result = procrunner.run(
            command,
            print_stdout=False,
            print_stderr=False,
            timeout=10,
            raise_timeout_exception=True,
        )
    except Exception as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"EPICS failure: {e}",
            MessageBody=f"Encountered {e} running external process",
        )

    if result.returncode or result.stderr:
        error = (
            result.stderr.decode("latin-1")
            or f"Process terminated with returncode {result.returncode}"
        )
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message="EPICS failure",
            MessageBody=error,
        )

    epics_result = result.stdout.decode("latin-1").split("\n")
    if len(epics_result) < 3:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message="EPICS failure",
            MessageBody=f"Received invalid EPICS response: {epics_result}",
        )

    if not epics_result[0]:
        return Status(
            Source=cfc.name,
            Level=REPORT.WARNING,
            Message="Could not determine Diamond run mode",
            MessageBody=f"{epics_result[0]}\n{epics_result[1]}",
        )

    level = REPORT.PASS
    if epics_result[0] == "No Beam":
        level = REPORT.WARNING
    elif epics_result[0] not in {
        "User",
        "Special",
        "Shutdown",
    }:
        # ie. "Injection", "BL Startup", "Mach. Dev."
        level += 1

    return Status(
        Source=cfc.name,
        Level=level,
        Message=f"Diamond mode: {epics_result[0]}",
        MessageBody=epics_result[1],
    )
