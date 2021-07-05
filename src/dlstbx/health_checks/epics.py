import procrunner

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status


def _get_epics_status(name):
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
            Source=name,
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
            Source=name, Level=REPORT.ERROR, Message="EPICS failure", MessageBody=error
        )

    epics_result = result.stdout.decode("latin-1").split("\n")
    if len(epics_result) < 3:
        return Status(
            Source=name,
            Level=REPORT.ERROR,
            Message="EPICS failure",
            MessageBody=f"Received invalid EPICS response: {epics_result}",
        )

    if not epics_result[0]:
        return Status(
            Source=name,
            Level=REPORT.WARNING,
            Message="Could not determine Diamond run mode",
            MessageBody=f"{epics_result[0]}\n{epics_result[1]}",
        )

    return Status(
        Source=name,
        Level=REPORT.PASS,
        Message=f"Diamond mode: {epics_result[0]}",
        MessageBody=epics_result[1],
    )


def get_diamond_ring_status(cfc: CheckFunctionInterface):
    current_status = cfc.current_status.get(cfc.name)
    new_status = _get_epics_status(cfc.name)

    if not current_status:
        return new_status  # unknown previous status
    if new_status.Level <= current_status.Level:
        return new_status  # things stayed the same or have improved
    # smooth out transient errors by limiting how fast errors are escalated
    new_status.Level = min(current_status.Level + 10, new_status.Level)
    return new_status
