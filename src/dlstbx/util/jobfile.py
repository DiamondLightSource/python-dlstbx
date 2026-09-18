"""Filesystem operations a service performs on behalf of a job it is submitting.

The work here is trivial - a mkdir and a write. What is not trivial, and what this
module exists to keep in one place, is deciding what to do with the *message* when
that work fails:

  - PermissionError    the job is trying to write somewhere it is not allowed to.
                       That will not fix itself, so requeuing the message just
                       replays the same failure forever. Dead-letter it instead
                       (nack with requeue=False).
  - OSError / ENOENT   the underlying filesystem went away - a GPFS mount flapping,
                       say. That may well come back, so requeue and retry later.
  - anything else      we do not know what it is, so do not pretend to have handled
                       it. Re-raise.

That triage came out of DiamondLightSource/python-dlstbx#390 ("Handle permission
errors in cluster service without crashing"). It previously lived as a private
method on DLSCluster, which meant any other service wanting the same behaviour had
to reach into DLSCluster and hand it a `self` that was not a DLSCluster at all -
relying on an undeclared assumption about which attributes that `self` carries.
These functions take what they need as named arguments instead, so the contract is
the signature and a future dependency cannot be added silently.
"""

from __future__ import annotations

import errno
import logging
import os
from typing import Any


def _reject(
    exc: OSError,
    description: str,
    log: logging.Logger,
    transport: Any,
    header: Any,
) -> None:
    """Apply the nack policy for a failed job filesystem operation.

    Returns normally once the message has been rejected; re-raises the original
    error when it is not one we have a considered answer for.
    """
    if isinstance(exc, PermissionError):
        # Permanent: retrying cannot help, so do not put it back on the queue.
        log.error(f"Could not write {description}: {exc}", exc_info=True)
        transport.nack(header, requeue=False)
        return
    if exc.errno != errno.ENOENT:
        raise exc
    # Transient: the filesystem may return, so allow a redelivery.
    log.error(
        f"Error in underlying filesystem writing {description}: {exc}",
        exc_info=True,
    )
    transport.nack(header)


def write_job_file(
    path: str,
    content: str,
    *,
    description: str,
    log: logging.Logger,
    transport: Any,
    header: Any,
) -> bool:
    """Write a file needed by a job, creating its parent directory.

    Args:
        path: Full path of the file to write.
        content: Text to write to it.
        description: Human-readable name for the file, used in log messages
            ("recipe wrapper", "job script", ...).
        log: The calling service's logger.
        transport: The calling service's transport, used to nack on failure.
        header: The message header, so the message can be rejected.

    Returns:
        True on success. False if the message has been rejected and the caller
        should stop processing it - the nack has already been sent.
    """
    log.debug("Writing %s to %s", description, path)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write(content)
    except OSError as e:
        # PermissionError is a subclass of OSError, so one except clause covers
        # both cases and _reject separates them.
        _reject(e, description, log, transport, header)
        return False
    return True


def make_job_dir(
    path: str | os.PathLike,
    *,
    description: str,
    log: logging.Logger,
    transport: Any,
    header: Any,
) -> bool:
    """Create a directory a job needs, with the same failure policy as
    write_job_file. Existing directories are fine.

    Returns:
        True on success. False if the message has been rejected and the caller
        should stop processing it.
    """
    log.debug("Creating %s at %s", description, path)
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as e:
        _reject(e, description, log, transport, header)
        return False
    return True
