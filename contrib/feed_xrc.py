#!/usr/bin/env python3

"""
Directly feed PIA results into X-Ray Centering and fetch the result

Wanted to have an easy way to feed the captured results from the PIA
service directly into the DLSXRayCentering service and get the results
back, without having to set uup a fixed test or inject test data into
the live Zocalo system.

Args:
    GRID_WIDTH  How wide the grid scan was taken at. This is used to
                derive the other dimension for each scan.
    PIA_FILES   JSON-lines files, with one entry on each line of a
                message with the following structure:

                    {"file-number": N, "file-seen-at": 123456, "n_spots_total": 43}
"""

from __future__ import annotations

import json
import logging
import types
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path
from unittest import mock

from workflows.recipe.wrapper import RecipeWrapper

import dlstbx.services.xray_centering

try:
    from rich import print
except ModuleNotFoundError:
    pass


class FakeTransport:
    messages = []

    def subscribe(self, *args, **kwargs):
        pass

    def connect(self):
        return True

    def disconnect(self):
        pass

    def subscription_callback_set_intercept(self, *args):
        pass

    def transaction_begin(self, *args, **kwargs):
        return mock.sentinel.transaction

    def ack(self, *args, **kwargs):
        pass

    def transaction_commit(self, *args, **kwargs):
        return mock.sentinel.transaction


def generate_recipe_message(parameters, gridinfo):
    """Helper function taken from XRC tests."""
    message = {
        "recipe": {
            "1": {
                "service": "DLS X-Ray Centering",
                "queue": "reduce.xray_centering",
                "parameters": parameters,
                "gridinfo": gridinfo,
            },
            "start": [(1, [])],
        },
        "recipe-pointer": 1,
        "recipe-path": [],
        "environment": {
            "ID": mock.sentinel.GUID,
            "source": mock.sentinel.source,
            "timestamp": mock.sentinel.timestamp,
        },
        "payload": mock.sentinel.payload,
    }
    return message


class Collection:
    def __init__(self, dcid: int, pia_file: Path):
        self.dcid = dcid
        self.pia_file = pia_file
        self.pia = read_pia_file(pia_file)


def read_pia_file(file: Path) -> list[dict]:
    pia = []
    for line in file.read_text().splitlines():
        if not line:
            continue
        data = json.loads(line)
        # Integerize this to avoid potential pydantic problem
        if "file-seen-at" in data:
            data["file-seen-at"] = int(data["file-seen-at"])
        pia.append(data)

    return pia


parser = ArgumentParser(epilog=__doc__, formatter_class=RawDescriptionHelpFormatter)
parser.add_argument(
    "grid_width",
    type=int,
    help="Width of image grid. Used for constructing the grid layout.",
    metavar="GRID_WIDTH",
)
parser.add_argument(
    "pia_files",
    nargs="+",
    type=Path,
    metavar="PIA_FILES",
    help="JSONLines data files containing PIA results, one file per DCID",
)
parser.add_argument(
    "--no-snaked",
    action="store_false",
    dest="snaked",
    help="The supplied data file isn't snaked",
)
args = parser.parse_args()

logging.basicConfig(level=logging.DEBUG, format="%(message)s")

xrc = dlstbx.services.xray_centering.DLSXRayCentering()
xrc.transport = FakeTransport()
xrc.start()
xrc.log = logging.getLogger("xrc")
# Initializing this screw up logging.. reset it here
logging.getLogger().setLevel(logging.DEBUG)
for h in logging.getLogger().handlers:
    h.setLevel(logging.DEBUG)

dcs = [Collection(i + 1, p) for i, p in enumerate(args.pia_files)]

messages_out = {"success": []}


def _rw_send_result(self, channel, message, **kwargs):
    messages_out[channel].append(message)


for i, dc in enumerate(dcs):
    print(f"Handling {dc.dcid} ({dc.pia_file})")

    assert len(dc.pia) % args.grid_width == 0
    gridinfo = {
        "dx_mm": 0.02,
        "dy_mm": 0.02,
        "gridInfoId": 1000 + i,
        "orientation": "horizontal",
        "micronsPerPixelX": 0.72,
        "micronsPerPixelY": 0.72,
        "snaked": 1 if args.snaked else 0,
        "snapshot_offsetXPixel": 0,
        "snapshot_offsetYPixel": 0,
        "steps_x": args.grid_width,
        "steps_y": len(dc.pia) // args.grid_width,
    }
    parameters = {
        "dcid": f"{dc.dcid}",
        "dcg_dcids": [x.dcid for x in dcs[:i]],
        "experiment_type": "Mesh3D",
        "beamline": "i03",
        "threshold": 0.05,
        "threshold_absolute": 5
    }

    rw = RecipeWrapper(
        message=generate_recipe_message(parameters, gridinfo), transport=xrc.transport
    )
    rw.send_to = types.MethodType(_rw_send_result, rw)
    for pia in dc.pia:
        header = {
            "message-id": mock.sentinel.message_id,
            "subscription": mock.sentinel.subscription,
        }
        xrc.add_pia_result(rw, header, pia)

print(messages_out)
