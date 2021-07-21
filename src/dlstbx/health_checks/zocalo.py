import os

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status


def check_zocalo_stash(cfc: CheckFunctionInterface):
    try:
        count = len(os.listdir("/dls_sw/apps/zocalo/dropfiles/"))
    except PermissionError:
        return Status(
            Source=cfc.name,
            Level=REPORT.WARNING,
            Message="PermissionError on attempting to read dropfile location",
            MessageBody="Please ensure the check is run as user dlshudson or gda2",
        )

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
        Message="No unprocessed Zocalo messages waiting",
    )
