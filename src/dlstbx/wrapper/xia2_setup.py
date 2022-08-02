from __future__ import annotations

import logging
from pathlib import Path

import dlstbx.util.symlink
from dlstbx.util.iris import (
    get_image_files,
    get_presigned_urls_images,
    write_singularity_script,
)
from dlstbx.wrapper import Wrapper


class Xia2SetupWrapper(Wrapper):

    _logger_name = "zocalo.wrap.xia2_setup"

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
                str(working_directory), params["create_symlink"], levels=1
            )

        singularity_image = params.get("singularity_image")
        if singularity_image:
            try:
                tmp_path = working_directory / "TMP"
                tmp_path.mkdir(parents=True, exist_ok=True)
                # shutil.copy(singularity_image, str(working_directory))
                # image_name = Path(singularity_image).name
                write_singularity_script(
                    working_directory, singularity_image, tmp_path.name
                )
                self.recwrap.environment.update(
                    {"singularity_image": singularity_image}
                )
            except Exception:
                self.log.exception("Error writing singularity script")
                return False

            if params.get("s3_urls"):
                formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
                handler = logging.StreamHandler()
                handler.setFormatter(formatter)
                self.log.addHandler(handler)
                self.log.setLevel(logging.DEBUG)
                s3_urls = get_presigned_urls_images(
                    params.get("create_symlink").lower(),
                    params["rpid"],
                    params["images"],
                    self.log,
                )
                self.recwrap.environment.update({"s3_urls": s3_urls})
            else:
                image_files = get_image_files(
                    working_directory, params["images"], self.log
                )
                self.recwrap.environment.update(
                    {"htcondor_upload_images": ",".join(image_files.keys())}
                )

        return True
