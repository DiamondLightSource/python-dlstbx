from __future__ import annotations

import functools
import re

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status


@functools.lru_cache(maxsize=None)
def _quotafile() -> list[str]:
    with open("/dls/tmp/quota.netapp") as fh:
        return fh.readlines()


def _parse_quota_string(q: str) -> float:
    factor = 1
    if "KB" in q:
        factor = 1024
    if "MB" in q:
        factor = pow(1024, 2)
    if "GB" in q:
        factor = pow(1024, 3)
    if "TB" in q:
        factor = pow(1024, 4)
    match = re.match("[0-9.]*", q)
    if match:
        number = float(match.group())
    else:
        number = 0
    return number * factor


def _check_quota_for_user(
    check_prefix: str, username: str, min_entries: int = 2
) -> Status:
    entries = [entry for entry in _quotafile() if username in entry]
    limits = {"notice": 0.925, "warning": 0.95, "error": 0.975}

    def report_level(value: float, limit: float) -> REPORT:
        if value >= limits["error"] * limit:
            return REPORT.ERROR
        if value >= limits["warning"] * limit:
            return REPORT.WARNING
        if value >= limits["notice"] * limit:
            return REPORT.NOTICE
        return REPORT.PASS

    if len(entries) < min_entries:
        # There should generally be at least 2 entries, staff-home and science/groups
        return Status(
            Source=f"{check_prefix}.{username}",
            Level=REPORT.ERROR,
            Message=f"Only {len(entries)} entries found in quota file for {username}",
        )
    notices: list[tuple[REPORT, str]] = []

    for entry in entries:
        tokens = entry.split()
        if len(tokens) == 9:
            (
                volume,
                subvolume,
                qclass,
                qid,
                space_used,
                space_allowed,
                inodes_used,
                inodes_allowed,
                _,
            ) = tokens
        elif len(tokens) == 8:
            (
                volume,
                qclass,
                qid,
                space_used,
                space_allowed,
                inodes_used,
                inodes_allowed,
                _,
            ) = tokens
            subvolume = "-"
        else:
            return Status(
                Source=f"{check_prefix}.{username}",
                Level=REPORT.ERROR,
                Message=f"Encountered invalid line with {len(tokens)} in quota file for {username}",
            )
        if inodes_allowed != "-":
            inodes_used_int = int(inodes_used)
            inodes_allowed_int = int(inodes_allowed)
            notices.append(
                (
                    report_level(inodes_used_int, inodes_allowed_int),
                    f"{inodes_used} of {inodes_allowed} inodes used on {volume}/{subvolume} ({100*inodes_used_int/inodes_allowed_int:.1f}% of quota)",
                )
            )
        if space_allowed != "-":
            space_used_num = _parse_quota_string(space_used)
            space_allowed_num = _parse_quota_string(space_allowed)
            notices.append(
                (
                    report_level(space_used_num, space_allowed_num),
                    f"{space_used} of {space_allowed} used on {volume}/{subvolume} ({100*space_used_num/space_allowed_num:.1f}% of quota)",
                )
            )
    max_notice_level = max(n[0] for n in notices)
    status_message = {
        REPORT.PASS: f"User {username} within quotas",
        REPORT.NOTICE: f"User {username} is nearing warning levels",
        REPORT.WARNING: f"User {username} is getting close to quota limits",
        REPORT.ERROR: f"User {username} is at quota limits",
    }[max_notice_level]
    message_body = "\n".join(n[1] for n in notices if n[0] is max_notice_level)
    return Status(
        Source=f"{check_prefix}.{username}",
        Level=max_notice_level,
        Message=status_message,
        MessageBody=message_body,
    )


def check_quota(cfc: CheckFunctionInterface) -> list[Status]:
    check_prefix = cfc.name
    return [
        _check_quota_for_user(check_prefix, "wra62962"),
        _check_quota_for_user(check_prefix, "dlshudson"),
        _check_quota_for_user(check_prefix, "dls_dasc", min_entries=1),
    ]
