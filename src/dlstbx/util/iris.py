from __future__ import annotations

import configparser
import getpass
import glob
import os
import shutil
import subprocess
import time
import urllib.parse
from datetime import timedelta
from pathlib import Path

import certifi
import minio
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL_EXPIRE = timedelta(days=7)

# http.client.HTTPConnection.debuglevel = 1


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None and hasattr(self, "timeout"):
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)

    # def init_poolmanager(self, connections, maxsize, block=False):
    #    self.poolmanager = PoolManager(num_pools=connections,
    #                                   maxsize=maxsize,
    #                                   block=block,
    #                                   ssl_version=ssl.PROTOCOL_TLSv1)


def get_minio_client(configuration, user):
    config = configparser.ConfigParser()
    config.read(configuration)
    host = urllib.parse.urlparse(config[user]["endpoint"])
    minio_client = minio.Minio(
        host.netloc,
        access_key=config[user]["access_key_id"],
        secret_key=config[user]["secret_access_key"],
        secure=True,
    )
    return minio_client


def get_objects_from_s3(working_directory, s3_urls, logger):
    retries = 8
    backoff_factor = 2
    status_forcelist = [429, 500, 502, 503, 504]

    session = requests.Session()
    retries = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    for filename, vals in s3_urls.items():
        logger.info(f"Downloading {filename} from object store using {vals['url']}")
        timestamp = time.perf_counter()
        file_data = session.get(vals["url"])
        timestamp = time.perf_counter() - timestamp
        logger.info(
            f"Download of {filename} from object store completed in {timestamp:.3f} seconds."
        )
        filepath = working_directory / filename.split("_", 1)[-1]
        # retrieve_file_with_url(filepath, vals["url"], logger)
        with open(filepath, "wb") as fp:
            fp.write(file_data.content)

        file_size = filepath.stat().st_size
        if file_size != vals["size"]:
            raise ValueError(
                f"Invalid size for downloaded {filepath.name} file: Expected {vals['size']}, got {file_size}"
            )
        logger.info(
            f"Data transfer rate for {filename} object: {8e-9 * file_size / timestamp:.3f}Gb/s"
        )


def remove_objects_from_s3(minio_clinet, bucket_name, s3_urls):
    for filename in s3_urls.keys():
        minio_clinet.remove_object(bucket_name, filename)


def get_image_files(working_directory, images, logger):
    file_list = {}
    h5_paths = {Path(s.split(":")[0]) for s in images.split(",")}
    for h5_file in h5_paths:
        image_pattern = str(h5_file).split("master")[0] + "*"
        for filepath in glob.glob(image_pattern):
            filename = Path(filepath).name
            logger.info(f"Found image file {filepath}")
            file_list[filename] = filepath
            if working_directory:
                shutil.copy(filepath, working_directory / filename)
    return file_list


def compress_results_directories(working_directory, dirs, logger):
    filelist = []
    for tmp_dir in dirs:
        start_time = time.perf_counter()
        filename = f"{tmp_dir}.tar.gz"
        result = subprocess.run(
            [
                "tar",
                "-zcvf",
                filename,
                f"{tmp_dir}",
                "--owner=nobody",
                "--group=nobody",
            ],
            cwd=working_directory,
        )
        runtime = time.perf_counter() - start_time
        if not result.returncode:
            filelist.append(filename)
            logger.info(f"Compressing {tmp_dir} took {runtime} seconds")
        else:
            logger.info(
                f"Compressing {tmp_dir} failed with exitcode {result.returncode}"
            )
            logger.debug(result.stdout)
            logger.debug(result.stderr)
    return filelist


def decompress_results_file(working_directory, filename, logger):
    start_time = time.perf_counter()
    result = subprocess.run(
        ["tar", "-xvzf", filename, "--no-same-owner", "--no-same-permissions"],
        cwd=working_directory,
    )
    runtime = time.perf_counter() - start_time
    if not result.returncode:
        logger.info(f"Uncompressing {filename} took {runtime} seconds")
    else:
        logger.info(
            f"Uncompressing {filename} failed with exitcode {result.returncode}"
        )
        logger.debug(result.stdout)
        logger.debug(result.stderr)


def get_presigned_urls(minio_client, bucket_name, pid, files, logger):

    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    else:
        logger.info(f"Object store bucket {bucket_name} already exists.")

    s3_urls = {}
    store_objects = [obj.object_name for obj in minio_client.list_objects(bucket_name)]
    for filepath in files:
        filename = "_".join([pid, Path(filepath).name])
        file_size = Path(filepath).stat().st_size
        upload_file = True
        if filename in store_objects:
            upload_file = False
            logger.info(
                f"File {filename} already exists in object store bucket {bucket_name}."
            )
            result = minio_client.stat_object(bucket_name, filename)
            if file_size != result.size:
                logger.info(
                    f"Reuploading {filename} because of mismatch in file size: Expected {file_size}, got {result.size}"
                )
                upload_file = True
        if upload_file:
            logger.info(f"Uploading file {filename} into object store.")
            timestamp = time.perf_counter()
            minio_client.fput_object(
                bucket_name,
                filename,
                filepath,
                part_size=100 * 1024 * 1024,
                num_parallel_uploads=5,
            )
            timestamp = time.perf_counter() - timestamp
            logger.info(
                f"Upload of {filename} into object store completed in {timestamp:.3f} seconds."
            )
            result = minio_client.stat_object(bucket_name, filename)
            if file_size != result.size:
                raise ValueError(
                    f"Invalid size for uploaded {filename} file: Expected {file_size}, got {result.size}"
                )
            logger.info(
                f"Data transfer rate for {filename} object: {8e-9 * file_size / timestamp:.3f}Gb/s"
            )
        s3_urls[filename] = {
            "url": minio_client.presigned_get_object(
                bucket_name, filename, expires=URL_EXPIRE
            ),
            "size": file_size,
        }
    logger.info(f"File URLs: {s3_urls}")
    return s3_urls


def get_presigned_urls_images(minio_client, bucket_name, pid, images, logger):
    image_files = get_image_files(None, images, logger)
    s3_urls = get_presigned_urls(
        minio_client, bucket_name, pid, image_files.values(), logger
    )
    return s3_urls


def store_results_in_s3(minio_client, bucket_name, pfx, output_directory, logger):
    compressed_results_files = compress_results_directories(
        output_directory.parent,
        [
            output_directory.name,
        ],
        logger,
    )
    get_presigned_urls(
        minio_client,
        bucket_name,
        pfx,
        compressed_results_files,
        logger,
    )
    for filename in compressed_results_files:
        os.remove(filename)


def retrieve_results_from_s3(
    minio_client, bucket_name, working_directory, pfx, results_filename, logger
):
    s3echo_filename = f"{pfx}_{results_filename}.tar.gz"
    minio_client.fget_object(
        bucket_name, s3echo_filename, working_directory / s3echo_filename
    )
    decompress_results_file(working_directory, s3echo_filename, logger)

    # Fix ACL mask for files extracted from .tar archive
    # Using m:rwX resets mask for files as well, unclear why.
    # Hence, running find to apply mask to files and directories separately
    for (ft, msk) in (("d", "m:rwx"), ("f", "m:rw")):
        setfacl_command = r"find %s -type %s -exec setfacl -m %s '{}' ';'" % (
            results_filename,
            ft,
            msk,
        )
        logger.info(f"Running command to fix ACLs: {setfacl_command}")
        result = subprocess.run(
            [
                setfacl_command,
            ],
            cwd=working_directory,
            shell=True,
        )
        if not result.returncode:
            logger.info(f"Resetting ALC mask to {msk} in {results_filename}")
        else:
            logger.info(f"Failed to reset ALC mask to {msk} in {results_filename}")

    minio_client.remove_object(bucket_name, s3echo_filename)
    os.remove(working_directory / s3echo_filename)


def retrieve_file_with_url(filename, url, logger):

    import pycurl

    logger.info(f"Retrieving data from {url}")
    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.CAINFO, certifi.where())
    retries_left = 3
    logger.info(f"Attempting download {filename}")
    while retries_left > 0:
        logger.info(f"{retries_left} attempts remaining")
        try:
            if filename.is_file():
                f = open(filename, "ab")
                c.setopt(pycurl.RESUME_FROM, filename.stat().st_size)
            else:
                f = open(filename, "wb")
            c.setopt(pycurl.WRITEDATA, f)
            c.perform()
            logger.info("Download successful")
            f.close()
            break
        except BaseException:
            logger.exception("Download failed")
            retries_left -= 1
            time.sleep(5)
    c.close()


def write_singularity_script(
    working_directory: Path, singularity_image: str, tmp_mount: str | None = None
):
    singularity_script = working_directory / "run_singularity.sh"
    add_tmp_mount = f"--bind ${{PWD}}/{tmp_mount}:/opt/xia2/tmp" if tmp_mount else ""
    commands = [
        "#!/bin/bash",
        f"/usr/bin/singularity exec --home ${{PWD}} {add_tmp_mount} {singularity_image} $@",
    ]
    with open(singularity_script, "w") as fp:
        fp.write("\n".join(commands))


def write_mrbump_singularity_script(
    working_directory: Path, singularity_image: str, tmp_mount: str, pdblocal: str
):
    singularity_script = working_directory / "run_singularity.sh"

    tmp_pdb_mount = (
        f"--bind ${{PWD}}/{tmp_mount}:/opt/xia2/tmp --bind {pdblocal}:{pdblocal}"
        if tmp_mount
        else ""
    )
    commands = [
        "#!/bin/bash",
        f"export USER={getpass.getuser()}",
        "export HOME=${PWD}/auto_mrbump",
        f"/usr/bin/singularity exec --home ${{PWD}} {tmp_pdb_mount} {singularity_image} $@",
    ]
    singularity_script.write_text("\n".join(commands))
