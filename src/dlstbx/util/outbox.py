"""The outbox: a filesystem stand-in for the message broker.

Wrappers running on a workflows-cluster pod have /dls access but no broker
access, so they cannot send their recipe messages the normal way. Instead they
run with OutboxTransport, which writes each outgoing message to a file in an
outbox directory on /dls rather than sending it. Once the pod's job finishes,
dlstbx.services.workflows_cluster.DLSWorkflowsCluster replays those files onto
the real broker with replay_outbox().

Both halves live here on purpose. The JSON envelope below is the contract
between them, and splitting the writer from the reader is the easiest way for
the two to drift apart:

    {
      "time":        <float, when the pod wrote it>,
      "destination": <queue name the message was bound for>,
      "headers":     {...},
      "message":     <the already-mangled message body>,
      "delay":       <float | null, applied at replay time>,
      "expiration":  <int | null, applied at replay time>
    }
"""

from __future__ import annotations

import argparse
import contextlib
import itertools
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any

import workflows
from workflows.transport.offline_transport import OfflineTransport

# Orders outbox filenames deterministically within a single wrapper run. A wrapper
# process runs once per job, so a fresh, process-local counter is enough to make
# replay order match send order - wall-clock time (e.g. time.time_ns()) can step
# backwards across an NTP correction and silently reorder messages that depend on
# each other (e.g. an ISPyB register before the trigger that references it).
_sequence = itertools.count()


class OutboxTransport(OfflineTransport):
    """Taking advantage of the blank slate of OfflineTransport (no RMQ interaction),
    we override the inherited _send method to deliver messages to the /dls
    filesystem. This is useful when we have /dls filesystem access, but no
    message bus access - i.e wrappers running on workflows pods

    Delay and expiration configurations are for during replay stages.

    The outbox directory is supplied on the command line, e.g.
        dlstbx.wrap -t OutboxTransport --outbox /dls/.../outbox ...
    """

    # These shadow the identically-named attributes on OfflineTransport, and are
    # load-bearing: add_command_line_options() below writes the parsed --outbox
    # value into cls.config, and workflows.transport.add_command_line_options()
    # calls that method on *every* registered transport on every run. Without our
    # own dicts here we would be mutating OfflineTransport's, which every sibling
    # transport shares.
    defaults: dict[Any, Any] = {}
    config: dict[Any, Any] = {}

    @classmethod
    def add_command_line_options(cls, parser: argparse.ArgumentParser) -> None:
        """Add the --outbox option, storing its value on the class config."""

        class SetParameter(argparse.Action):
            def __call__(self, parser, namespace, value, option_string=None):
                cls.config[option_string] = value

        parser.add_argument(
            "--outbox",
            metavar="DIR",
            help="Directory on /dls where outbox messages are written",
            action=SetParameter,
        )

    def _send(
        self,
        destination: str,
        message: Any,
        headers: dict[str, Any] | None = None,
        delay: float | None = None,
        expiration: int | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Send a message to the outbox on /dls. This message will be picked up by a zocalo service
        configured to find and read the message to continue the execution of a recipe.

        The message is written as a JSON envelope holding the recipe wrapper headers, the
        target queue, and the (already mangled) message body, so that the replaying service
        can reconstruct the original send. Writes are staged to a dotfile and atomically
        renamed into place, so a service polling the outbox never sees a partial message,
        as the polling service doesn't look for dotfiles.

        Args:
            destination: Queue name the message is bound for. Recorded in the envelope
                so the replay service knows where to re-send it.
            message: A string or bytes to be sent
            headers: Further arbitrary headers to pass on
            delay: DURING REPLAY: Delay transport of message by this many seconds
            expiration: DURING REPLAY: Optional TTL expiration time, relative to sending time.

        Raises:
            workflows.Error: If no outbox is configured, or it is missing or cannot be
                written to. The filesystem is the only delivery path available here, so
                an undeliverable message must fail the wrapper rather than be dropped.
        """
        if not headers:
            headers = {}

        outbox_dir = self.config.get("--outbox", self.defaults.get("--outbox"))
        if not outbox_dir:
            raise workflows.Error(
                "Cannot write message to outbox: no --outbox directory configured"
            )
        outbox = Path(outbox_dir)
        if not outbox.is_dir():
            raise workflows.Error(
                f"Cannot write {len(message)} byte message to outbox: "
                f"{outbox} does not exist or is not a directory"
            )

        envelope = {
            "time": time.time(),
            "destination": destination,
            "headers": headers,
            "message": message,
            "delay": delay,
            "expiration": expiration,
        }

        filename = f"{next(_sequence):012d}-{uuid.uuid4()}.json"
        staging = outbox / f".{filename}.tmp"
        try:
            staging.write_text(json.dumps(envelope))
            os.replace(staging, outbox / filename)
        except OSError as e:
            # A failed write may leave a partial dotfile behind, but clearing it up is
            # best-effort - the original error is the one worth propagating.
            with contextlib.suppress(OSError):
                staging.unlink(missing_ok=True)
            raise workflows.Error(
                f"Could not write {len(message)} byte message to outbox: {outbox} ({e})"
            ) from e

        self._output(
            f"Writing {len(message)} bytes to outbox for {destination}", message
        )


def replay_outbox(
    transport: Any, outbox_dir: os.PathLike | str, logger: logging.Logger | None = None
) -> int:
    """
    Replay every message file in outbox_dir onto a live, connected transport, in
    filename order, deleting each file once it has been sent.

    Messages were already mangled once by OutboxTransport (the pod's send_to/send
    calls ran through the normal mangle-then-_send path), so they are replayed with
    raw_send() here rather than send(), to avoid mangling them a second time.

    Stops and re-raises on the first failure, leaving that file - and any after it -
    in place. That way a retry resumes from the message that actually failed, instead
    of skipping it or replaying earlier messages a second time.

    Args:
        transport: A connected transport (e.g. the calling service's self._transport
            or rw.transport) to replay messages onto.
        outbox_dir: Directory containing envelope files written by OutboxTransport.
        logger: Optional logger for per-message debug output.

    Returns:
        The number of messages successfully replayed.
    """
    log = logger or logging.getLogger("dlstbx.util.outbox")
    outbox = Path(outbox_dir)

    # Dotfiles are in-progress writes being staged by OutboxTransport (write to
    # .name.json.tmp, then os.replace to name.json) - never replay one of those.
    files = sorted(
        f for f in outbox.iterdir() if f.is_file() and not f.name.startswith(".")
    )

    replayed = 0
    for f in files:
        with f.open() as fh:
            envelope = json.load(fh)

        transport.raw_send(
            envelope["destination"],
            envelope["message"],
            headers=envelope.get("headers") or {},
            delay=envelope.get("delay"),
            expiration=envelope.get("expiration"),
        )
        os.remove(f)
        replayed += 1
        log.debug(
            "Replayed outbox message %s bound for %s", f.name, envelope["destination"]
        )

    return replayed
