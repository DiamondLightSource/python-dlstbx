"""
Processes a ``dlstbx.go`` backlog.

If ``dlstbx.go`` cannot make a connection to a message queue server, the fallback is to
save the request to disk at a location specified in the ``zocalo.go.fallback_location``
field of the Zocalo configuration.  Once a connection has been re-established,
this utility can be used to feed the backlog into the message queue.  Messages will
be injected from the backlog gradually, to avoid swamping new messages with too many
old messages reinjected at once.
"""

import json
import os
import sys
import time
from argparse import ArgumentParser
from typing import List, Optional

import workflows.transport
import zocalo.configuration

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "-d",
    "--delay",
    type=int,
    default=2,
    help="Number of seconds to wait between message dispatches.",
)
parser.add_argument(
    "-w",
    "--wait",
    type=int,
    default=60,
    help="Number of seconds to wait initially.",
)
parser.add_argument(
    "-v",
    "--verbose",
    action="store_true",
    default=False,
    help="Show raw message before sending.",
)
workflows.transport.add_command_line_options(parser, transport_argument=True)


def run(parser: ArgumentParser = parser, args: Optional[List[str]] = None):
    zc = zocalo.configuration.from_file()
    zc.activate()

    zc.add_command_line_options(parser)

    try:
        dropdir = zc.storage["zocalo.go.fallback_location"]
    except KeyError:
        sys.exit(
            "The Zocalo configuration contains no specified location for drop files.  "
            "No drop files could be retrieved."
        )

    args = parser.parse_args(args)

    transport = workflows.transport.lookup(args.transport)()

    try:
        files = os.listdir(dropdir)
    except OSError:
        sys.exit("This program is only available to privileged users.")

    print(f"Found {len(files)} files.")
    if not files:
        sys.exit()

    if args.wait:
        print(f"Waiting {args.wait} seconds.")
        time.sleep(args.wait)

    print("Connecting to transport layer...")
    transport.connect()

    file_info = {f: {"filename": os.path.join(dropdir, f)} for f in files}
    hosts = {}

    recipe_priorities = {
        "per-image-analysis-dozor": -30,
        "xray-centering": -10,
        "per-image-analysis-gridscan": -10,
        "per-image-analysis-rotation": -5,
        "archive-cbf": 3,
        "most-recent-for-host": 30,
    }

    for f in file_info.values():
        with open(f["filename"]) as fh:
            data = json.load(fh)
            f["message"] = data["message"]
            f["headers"] = data["headers"]
        f["originating-host"] = f["headers"].get("dlstbx.go.host")
        f["recipes"] = ",".join(f["message"].get("recipes", []))
        f["priority"] = sum(
            recipe_priorities.get(r, 0) for r in f["message"].get("recipes", [])
        )
        f["last-touch"] = os.path.getmtime(f["filename"])
        if hosts.get(f["originating-host"], {}).get("last-touch", 0) < f["last-touch"]:
            hosts[f["originating-host"]] = f

    for f in hosts.values():
        f["priority"] += recipe_priorities["most-recent-for-host"]

    count = 0
    file_count = len(file_info)
    for f in sorted(file_info, key=lambda f: file_info[f]["priority"], reverse=True):
        print(
            f"Sending {f} from host {file_info[f]['originating-host']} with recipes "
            f"{file_info[f]['recipes']}."
        )
        assert os.path.exists(file_info[f]["filename"])
        transport.send(
            "processing_recipe",
            file_info[f]["message"],
            headers=file_info[f]["headers"],
        )
        os.remove(file_info[f]["filename"])
        count += 1
        print(f"Done ({count} of {file_count}).")
        try:
            time.sleep(args.delay)
        except KeyboardInterrupt:
            print("CTRL+C — stopping.")
            time.sleep(0.5)
            sys.exit(1)
