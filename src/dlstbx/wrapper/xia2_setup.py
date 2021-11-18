import logging
from pathlib import Path

import zocalo.wrapper

import dlstbx.util.symlink
from dlstbx.util.iris import get_presigned_urls_images, write_singularity_script

logger = logging.getLogger("zocalo.wrap.xia2_setup")


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
                str(working_directory), params["create_symlink"], levels=1
            )

        singularity_image = params.get("singularity_image")
        if singularity_image:
            try:
                tmp_path = working_directory / "TMP"
                tmp_path.mkdir(parents=True, exist_ok=True)
                write_singularity_script(
                    working_directory, singularity_image, tmp_path.name
                )
            except Exception:
                logger.exception("Error writing singularity script")
                return False

            s3_urls = get_presigned_urls_images(
                params["rpid"], params["images"], logger
            )
            self.recwrap.send_to("cloud", {"s3_urls": s3_urls})

        return True
