from __future__ import annotations

import pathlib
from datetime import datetime

import jose.exceptions
import jose.jwt

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status


def check_slurm_gda2_jwt(cfc: CheckFunctionInterface) -> Status:
    token_file = pathlib.Path("/dls_sw/apps/zocalo/secrets/slurmrest-gda2.tkn")
    token = token_file.read_text().strip()
    try:
        data = jose.jwt.decode(token, key=None, options={"verify_signature": False})
    except jose.exceptions.ExpiredSignatureError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message="Slurm user token has expired",
            MessageBody=repr(e),
        )
    except jose.exceptions.JWTError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message="Error verifying slurm user token",
            MessageBody=repr(e),
        )
    try:
        expiry = datetime.fromtimestamp(data["exp"])
    except KeyError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message="Slurm user token doesn't contain an expiry",
            MessageBody=repr(e),
        )
    except TypeError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message="Invalid timestamp for 'exp' in slurm user token",
            MessageBody=repr(e),
        )
    time_to_expiry = expiry - datetime.now()
    days_to_level = {
        1: REPORT.ERROR,
        3: REPORT.WARNING,
        5: REPORT.NOTICE,
    }
    for days, level in sorted(days_to_level.items()):
        if time_to_expiry.days < days:
            return Status(
                Source=cfc.name,
                Level=level,
                Message=f"Slurm user token {token_file} expires in {time_to_expiry}",
            )
    try:
        sun = data["sun"]
    except KeyError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message="Slurm user token payload doesn't contain 'sun' keyword",
            MessageBody=repr(e),
        )
    if sun != "gda2":
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Slurm user token {sun=} (expected gda2)",
        )
    return Status(
        Source=cfc.name,
        Level=REPORT.PASS,
        Message="Slurm gda2 user token is valid",
    )
