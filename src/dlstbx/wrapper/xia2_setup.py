import configparser
import glob
import logging
import urllib
from datetime import timedelta
from pathlib import Path

import minio
import zocalo.wrapper

import dlstbx.util.symlink

logger = logging.getLogger("dlstbx.wrap.xia2_setup")


def write_singularity_script(working_directory, singularity_image):
    singularity_script = working_directory / "run_singularity.sh"
    commands = [
        "#!/bin/bash",
        f"/usr/bin/singularity exec --home ${{PWD}} --bind ${{PWD}}/TMP:/opt/xia2/tmp {singularity_image} $@",
    ]
    with open(singularity_script, "w") as fp:
        fp.write("\n".join(commands))


def s3_get_presigned_urls(params):
    s3_config = Path(params["s3_config"])
    s3_name = params["s3_name"]

    config = configparser.ConfigParser()
    config.read(str(s3_config))

    host = urllib.parse.urlparse(config[s3_name]["endpoint"])
    minio_client = minio.Minio(
        host.netloc,
        access_key=config[s3_name]["access_key_id"],
        secret_key=config[s3_name]["secret_access_key"],
        secure=True,
    )

    bucket_name = params["dcid"]
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    else:
        logger.info("Object store bucket {bucket_name} already exists.")
    s3_urls = {}
    store_objects = [obj.object_name for obj in minio_client.list_objects(bucket_name)]
    for filepath in glob.glob(params["image_pattern"]):
        filename = Path(filepath).name
        if filename in store_objects:
            logger.info(
                f"File {filename} already exists in object store bucket {bucket_name}."
            )
        else:
            logger.info(f"Writing file {filename} into object store.")
            minio_client.fput_object(bucket_name, filename, filepath)
        s3_urls[filename] = minio_client.presigned_get_object(
            bucket_name, filename, expires=timedelta(hours=2)
        )
    logger.info(f"Image file URLs: {s3_urls}")
    return s3_urls


class Xia2SetupWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        # Adjust all paths if a spacegroup is set in ISPyB
        if params.get("ispyb_parameters"):
            if (
                params["ispyb_parameters"].get("spacegroup")
                and "/" not in params["ispyb_parameters"]["spacegroup"]
            ):
                if "create_symlink" in params:
                    params["create_symlink"] += (
                        "-" + params["ispyb_parameters"]["spacegroup"]
                    )

        working_directory = Path(params["working_directory"])

        # Create working directory with symbolic link
        working_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                str(working_directory), params["create_symlink"]
            )

        tmp_path = working_directory / "TMP"
        tmp_path.mkdir(parents=True, exist_ok=True)

        singularity_image = params.get("singularity_image")
        if singularity_image:
            try:
                write_singularity_script(working_directory, singularity_image)
            except Exception:
                logger.exception("Error writing singularity script")
                return False

        if params.get("s3_config"):
            s3_urls = s3_get_presigned_urls(params)
            self.recwrap.send_to("cloud", {"s3_urls": s3_urls})

        return True
