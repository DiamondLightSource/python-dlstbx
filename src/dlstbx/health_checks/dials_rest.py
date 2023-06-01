from __future__ import annotations

import pathlib
from datetime import date
from io import BytesIO

import PIL.Image
import requests
from dateutil.relativedelta import relativedelta

from dlstbx.health_checks import REPORT, CheckFunctionInterface, Status

_dials_rest_url = "https://dials-rest.diamond.ac.uk/export_bitmap/"
_dials_rest_access_token = (
    pathlib.Path("/dls_sw/apps/zocalo/secrets/dials-rest.tkn").read_text().strip()
)


def check_dials_rest(cfc: CheckFunctionInterface) -> Status:
    filenames = {
        "/dls/i03/data/2023/cm33866-2/TestInsulin/ins_16/ins_16_4_master.h5",
        "/dls/i23/data/2023/cm33851-2/Germ/Germ_11May/data_1_#####.cbf",
    }
    for filename in filenames:
        try:
            response = requests.post(
                _dials_rest_url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {_dials_rest_access_token}",
                },
                json={
                    "filename": filename,
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
        except requests.ConnectionError as e:
            return Status(
                Source=cfc.name,
                Level=REPORT.ERROR,
                Message=f"Error connecting to {_dials_rest_url}",
                MessageBody=repr(e),
                URL=_dials_rest_url,
            )
        except requests.HTTPError as e:
            message_body = repr(e)
            if response.status_code == 403:
                today = date.today()
                today_plus_one_month = today + relativedelta(months=1)
                message_body = (
                    "Please update the access token with (e.g.):\n"
                    "    kubectl exec -n dials-rest --stdin -it deployment/dials-rest -- /env/bin/create-access-token "
                    f"--expiry {today_plus_one_month} > /dls_sw/apps/zocalo/secrets/dials-rest.tkn"
                )
            return Status(
                Source=cfc.name,
                Level=REPORT.ERROR,
                Message=f"HTTPError {e}",
                MessageBody=message_body,
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
                Message=f"Invalid image returned by {_dials_rest_url} for {filename}",
                MessageBody=repr(e),
                URL=_dials_rest_url,
            )

    return Status(
        Source=cfc.name,
        Level=REPORT.PASS,
        Message="DIALS REST service alive",
        MessageBody=f"Valid response received from {_dials_rest_url}",
        URL=_dials_rest_url,
    )
