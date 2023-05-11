from __future__ import annotations

import pathlib
from io import BytesIO

import PIL.Image
import requests

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status

_dials_rest_url = "https://dials-rest.diamond.ac.uk/export_bitmap/"
_dials_rest_access_token = (
    pathlib.Path("/dls_sw/apps/zocalo/secrets/dials-rest.tkn").read_text().strip()
)


def check_dials_rest(cfc: CheckFunctionInterface) -> Status:
    try:
        response = requests.post(
            _dials_rest_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {_dials_rest_access_token}",
            },
            json={
                "filename": "/dls/i03/data/2023/cm33866-2/TestInsulin/ins_16/ins_16_4_master.h5",
                "image_index": 1,
                "format": "png",
                "binning": 4,
                "display": "image",
                "colour_scheme": "greyscale",
                "brightness": 10,
            },
            timeout=10,
        )
        response.raise_for_status()
    except requests.HTTPError as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"HTTPError connecting to {_dials_rest_url}",
            MessageBody=repr(e),
            URL=_dials_rest_url,
        )
    except requests.Timeout as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Timeout connecting to {_dials_rest_url}",
            MessageBody=repr(e),
            URL=_dials_rest_url,
        )

    # verify that it returned an understandable image
    try:
        PIL.Image.open(BytesIO(response.content))
    except Exception as e:
        return Status(
            Source=cfc.name,
            Level=REPORT.ERROR,
            Message=f"Invalid image returned by {_dials_rest_url}",
            MessageBody=repr(e),
            URL=_dials_rest_url,
        )

    return Status(
        Source=cfc.name,
        Level=REPORT.PASS,
        Message="DIALS REST service alive",
        URL=_dials_rest_url,
    )
