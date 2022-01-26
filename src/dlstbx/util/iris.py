from __future__ import annotations

import configparser
import glob
import shutil
import time
import urllib
from datetime import timedelta
from pathlib import Path

import certifi
import minio
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

S3_CONFIG = "/dls_sw/apps/zocalo/secrets/credentials-echo-mx.cfg"
S3_NAME = "echo-mx"
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


def get_minio_client():
    config = configparser.ConfigParser()
    config.read(S3_CONFIG)
    host = urllib.parse.urlparse(config[S3_NAME]["endpoint"])
    minio_client = minio.Minio(
        host.netloc,
        access_key=config[S3_NAME]["access_key_id"],
        secret_key=config[S3_NAME]["secret_access_key"],
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
            f"Data transfer rate for {filename} object: {1e-9 * file_size / timestamp:.3f}Gb/s"
        )


def remove_objects_from_s3(bucket_name, s3_urls):
    minio_clinet = get_minio_client()
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
            shutil.copy(filepath, working_directory / filename)
    return file_list


def get_presigned_urls_images(bucket_name, pid, images, logger):
    minio_client = get_minio_client()

    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    else:
        logger.info(f"Object store bucket {bucket_name} already exists.")

    s3_urls = {}
    store_objects = [obj.object_name for obj in minio_client.list_objects(bucket_name)]
    h5_paths = {Path(s.split(":")[0]) for s in images.split(",")}
    for h5_file in h5_paths:
        image_pattern = str(h5_file).split("master")[0] + "*"
        for filepath in glob.glob(image_pattern):
            filename = "_".join([pid, Path(filepath).name])
            if filename in store_objects:
                logger.info(
                    f"File {filename} already exists in object store bucket {bucket_name}."
                )
            else:
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
                file_size = Path(filepath).stat().st_size
                if file_size != result.size:
                    raise ValueError(
                        f"Invalid size for uploaded {filepath.name} file: Expected {file_size}, got {result.size}"
                    )
                logger.info(
                    f"Data transfer rate for {filename} object: {1e-9 * file_size / timestamp:.3f}Gb/s"
                )
            s3_urls[filename] = {
                "url": minio_client.presigned_get_object(
                    bucket_name, filename, expires=URL_EXPIRE
                ),
                "size": file_size,
            }
    logger.info(f"Image file URLs: {s3_urls}")
    return s3_urls


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


def write_singularity_script(working_directory, singularity_image, tmp_mount=False):
    singularity_script = working_directory / "run_singularity.sh"
    add_tmp_mount = f"--bind ${{PWD}}/{tmp_mount}:/opt/xia2/tmp" if tmp_mount else ""
    commands = [
        "#!/bin/bash",
        f"/usr/bin/singularity exec --home ${{PWD}} {add_tmp_mount} {singularity_image} $@",
    ]
    with open(singularity_script, "w") as fp:
        fp.write("\n".join(commands))
