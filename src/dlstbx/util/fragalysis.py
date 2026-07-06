from __future__ import annotations

import configparser
from pathlib import Path

import requests

"""Push an XChemAlign upload tarball to Fragalysis.

Repurposed from the standalone ``upload.py`` / ``oidc_session.py`` scripts so it
can run as the final step of the xchem_collate wrapper. The service-account
client credentials live in a configparser cfg under /dls_sw/apps/zocalo/secrets
rather than being hardcoded.
"""

CREDENTIALS = "/dls_sw/apps/zocalo/secrets/credentials-fragalysis.cfg"

# Both production & staging Fragalysis deployments authenticate against this realm.
DEFAULT_OIDC_REALM = "https://identity.diamond.ac.uk/realms/dls"

PRODUCTION_URL = "https://fragalysis.diamond.ac.uk/api/upload_target_experiments/"
STAGING_URL = "https://fragalysis.xchem.diamond.ac.uk/api/upload_target_experiments/"
UPLOAD_URL = STAGING_URL

TOKEN_TIMEOUT = 10.0
# Generous read timeout: the gzip can be multiple GB, so the connection may sit
# busy uploading for a long time before the server responds.
UPLOAD_TIMEOUT = 1800.0


def _get_access_token(credentials: str = CREDENTIALS) -> str:
    """Get a service-account bearer token via the OIDC client_credentials grant."""
    config = configparser.ConfigParser()
    if not config.read(credentials):
        raise FileNotFoundError(f"No Fragalysis credentials found at {credentials}")
    section = config["fragalysis"]
    client_id = section["client_id"]
    client_secret = section["client_secret"]
    realm = section.get("oidc_realm", DEFAULT_OIDC_REALM)

    resp = requests.post(
        f"{realm}/protocol/openid-connect/token",
        data={"grant_type": "client_credentials", "scope": "openid"},
        auth=(client_id, client_secret),
        timeout=TOKEN_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _upload_target_experiment(
    tgz_path: Path,
    target_access_string: str,
    token: str,
    *,
    url: str = UPLOAD_URL,
) -> dict:
    """POST a target-experiment gzip to Fragalysis."""
    headers = {"Authorization": f"Bearer {token}"}
    with open(tgz_path, "rb") as f:
        files = {"file": (tgz_path.name, f, "application/gzip")}
        data = {"target_access_string": target_access_string}
        resp = requests.post(
            url, headers=headers, files=files, data=data, timeout=UPLOAD_TIMEOUT
        )
    resp.raise_for_status()
    return resp.json()


def upload_to_fragalysis(
    tgz_path: Path,
    target_access_string: str,
    logger,
    *,
    credentials: str = CREDENTIALS,
    url: str = UPLOAD_URL,
) -> None:
    """Upload an XChemAlign upload tarball to Fragalysis.

    ``tgz_path`` is the gzip the wrapper tarred from the aligner's upload
    directory.
    """
    tgz_path = Path(tgz_path)
    if not tgz_path.is_file():
        logger.error(f"No tarball at {tgz_path}, skipping Fragalysis upload")
        return

    logger.info(
        f"Uploading {tgz_path} to Fragalysis (access string '{target_access_string}')"
    )
    token = _get_access_token(credentials)
    result = _upload_target_experiment(tgz_path, target_access_string, token, url=url)
    logger.info(f"Fragalysis upload response: {result}")


if __name__ == "__main__":
    import logging
    import sys

    logging.basicConfig(level=logging.INFO)
    upload_to_fragalysis(
        Path(sys.argv[1]), target_access_string=sys.argv[2], logger=logging.getLogger()
    )
