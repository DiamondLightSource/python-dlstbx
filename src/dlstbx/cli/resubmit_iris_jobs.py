from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path
from pprint import pprint

import minio

from dlstbx.util.iris import get_minio_client


def read_dcid_info(minio_client, bucket_name, dcid):
    info_name = f"{dcid}_info"
    response = minio_client.get_object(bucket_name, info_name)
    try:
        response_info = json.loads(response.data.decode())
    finally:
        response.close()
        response.release_conn()
    rpid_set = set(response_info["pid"])
    return rpid_set


def get_rpid_values_from_s3echo(minio_client: minio.Minio):
    rpids: dict[int | str, set[int]] = {}
    for bucket in minio_client.list_buckets():
        print(f"Reading bucket {bucket.name}")
        if bucket.name in (
            "autoproc",
            "big-ep",
            "fast-ep",
            "mrbump",
            "multi-xia2-3dii",
            "multi-xia2-dials",
            "xia2-3dii",
            "xia2-dials",
        ):
            rpids_downstream: set[int] = set()
            for obj in minio_client.list_objects(bucket.name):
                try:
                    rpid = int(obj.object_name.split("_")[0])
                    rpids_downstream.add(rpid)
                except (TypeError, ValueError):
                    continue
            rpids.setdefault("rpids", set()).update(rpids_downstream)
            pprint(f"Adding downstream reprocessing rpid values: {rpids_downstream}")
        else:
            for obj in minio_client.list_objects(bucket.name):
                dcid_match = re.search(r"^(\d+)_info$", obj.object_name)
                if dcid_match:
                    dcid = int(dcid_match.group(1))
                    print(f"Found {dcid}_info object")
                    rpid_vals = read_dcid_info(minio_client, bucket.name, dcid)
                    pprint(f"Adding rpid values: {rpid_vals}")
                    rpids[dcid] = rpid_vals
    return rpids


def get_rpid_values_from_dlq(dlq_dir, pattern):
    # Get rpid values from DLQ messages
    rpids = {}
    for dlq_file in dlq_dir.glob(pattern):
        with dlq_file.open("r", encoding="utf-8") as fp:
            dcid = None
            rpid = None
            for line in fp:
                dcid_match = re.search(r'"dcid":\s*"(\d+)"', line)
                if dcid_match:
                    dcid = int(dcid_match.group(1))
                rpid_match = re.search(r'"rpid":\s*"(\d+)"', line)
                if rpid_match:
                    rpid = int(rpid_match.group(1))
                if dcid and rpid:
                    rpids.setdefault(dcid, set()).add(rpid)
                    break
    return rpids


def resubmit_rpid(rpid, dry_run):
    command = ["zocalo.go", "-p", str(rpid)]
    print(" ".join(command))
    if dry_run:
        return True
    result = subprocess.run(command)
    if result.returncode:
        print(f"ERROR: {' '.join(command)} failed with exit code {result.returncode}")
        return False
    return True


def run_rpid_jobs(minio_client: minio.Minio, rpids, dry_run):
    failed = []
    if "rpids" in rpids:
        for rpid in sorted(rpids["rpids"]):
            if not resubmit_rpid(rpid, dry_run):
                failed.append(rpid)
        del rpids["rpids"]
    for bucket in minio_client.list_buckets():
        for obj in minio_client.list_objects(bucket.name):
            for dcid in rpids:
                info_name = f"{dcid}_info"
                if obj.object_name != info_name:
                    continue
                rpid_set = read_dcid_info(minio_client, bucket.name, dcid)
                if rpid_set != rpids[dcid]:
                    print(f"ERROR: Some jobs are still running for {dcid}: {rpid_set}")
                    continue
                print(f"Removing info file {info_name} from {bucket.name}")
                if not dry_run:
                    minio_client.remove_object(bucket.name, info_name)
                for rpid in sorted(rpids[dcid]):
                    if not resubmit_rpid(rpid, dry_run):
                        failed.append(rpid)
    return failed


def run():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="dlstbx.resubmit_iris_jobs - Resubmit IRIS processing jobs from s3echo.watch DLQ messages "
        "or S3Echo object store contents",
        epilog="Processing IDs are read from DLQ messages (--dlq_path, --pattern) or, with --rerun_s3echo, "
        "from info files and downstream buckets in the S3Echo object store. For each DCID whose processing "
        "jobs have all completed, remove the S3Echo info file and rerun the jobs with zocalo.go.",
    )
    parser.add_argument(
        "s3echo_key", help="File containing S3Echo object store credentials", type=Path
    )
    parser.add_argument(
        "--dlq_path", help="Directory containing s3echo.watch DLQ messages", type=Path
    )
    parser.add_argument(
        "--pattern",
        help="Pattern for DLQ message file name matching",
        type=str,
    )
    parser.add_argument(
        "--rerun_s3echo",
        help="Rerun jobs that haven't been cleared from S3Echo visit buckets",
        action="store_true",
    )
    parser.add_argument(
        "--dry_run",
        help="Print the zocalo.go commands without running them or removing info files",
        action="store_true",
    )

    args = parser.parse_args()
    minio_client = get_minio_client(args.s3echo_key)
    if args.rerun_s3echo:
        rpid_dict = get_rpid_values_from_s3echo(minio_client)
    else:
        rpid_dict = get_rpid_values_from_dlq(args.dlq_path, args.pattern)
    pprint(rpid_dict)
    failed = run_rpid_jobs(minio_client, rpid_dict, args.dry_run)
    if failed:
        raise SystemExit(f"Failed to resubmit processing jobs: {failed}")
