import os

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status


def check_zocalo_stash(cfc: CheckFunctionInterface):
    count = len(os.listdir("/dls_sw/apps/zocalo/dropfiles/"))

    if count:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"{count} message{'' if count == 1 else 's'} waiting for zocalo",
            MessageBody="Address underlying issue, then run 'dlstbx.pickup' as user gda2 to clear",
        )
    return Status(
        Source=cfc.name,
        Level=REPORT.PASS,
        Message=f"No unprocessed Zocalo messages waiting",
    )
