import configparser
import glob
import urllib
from datetime import timedelta
from pathlib import Path

import minio
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

S3_CONFIG = "/dls_sw/apps/zocalo/secrets/credentials-echo-mx.cfg"
S3_NAME = "echo-mx"
URL_EXPIRE = timedelta(days=7)


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


def get_objects_from_s3(working_directory, s3_urls):
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
    session.mount("http://", HTTPAdapter(max_retries=retries))
    for filename, s3_url in s3_urls.items():
        file_data = session.get(s3_url)
        filepath = working_directory / filename.split("_", 1)[-1]
        with open(filepath, "wb") as fp:
            fp.write(file_data.content)


def remove_objects_from_s3(bucket_name, s3_urls):
    minio_clinet = get_minio_client()
    for filename in s3_urls.keys():
        minio_clinet.remove_object(bucket_name, filename)


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
                logger.info(f"Writing file {filename} into object store.")
                minio_client.fput_object(bucket_name, filename, filepath)
            s3_urls[filename] = minio_client.presigned_get_object(
                bucket_name, filename, expires=URL_EXPIRE
            )
    logger.info(f"Image file URLs: {s3_urls}")
    return s3_urls


def write_singularity_script(working_directory, singularity_image, tmp_mount=False):
    singularity_script = working_directory / "run_singularity.sh"
    add_tmp_mount = f"--bind ${{PWD}}/{tmp_mount}:/opt/xia2/tmp" if tmp_mount else ""
    commands = [
        "#!/bin/bash",
        f"/usr/bin/singularity exec --home ${{PWD}} {add_tmp_mount} {singularity_image} $@",
    ]
    with open(singularity_script, "w") as fp:
        fp.write("\n".join(commands))
