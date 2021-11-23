import logging
from pathlib import Path

import zocalo.wrapper

import dlstbx.util.symlink
from dlstbx.util.iris import get_presigned_urls_images, write_singularity_script

logger = logging.getLogger("zocalo.wrap.autoPROC_setup")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
    "FONTCONFIG_PATH": "",
    "FONTCONFIG_FILE": "",
}


class autoPROCSetupWrapper(zocalo.wrapper.BaseWrapper):
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
                write_singularity_script(working_directory, singularity_image)
            except Exception:
                logger.exception("Error writing singularity script")
                return False

            s3_urls = get_presigned_urls_images(
                params.get("create_symlink").lower(),
                params["rpid"],
                params["images"],
                logger,
            )
            self.recwrap.environment.update({"s3_urls": s3_urls})

        return True
