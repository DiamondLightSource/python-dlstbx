from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

import dlstbx.util.symlink
from dlstbx.util.iris import write_mrbump_singularity_script
from dlstbx.wrapper import Wrapper

logger = logging.getLogger("dlstbx.wrap.mrbump_setup")


class MrBUMPSetupWrapper(Wrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        # Create working directory with symbolic link
        working_directory = Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                str(working_directory), params["create_symlink"], levels=1
            )

        singularity_image = params.get("singularity_image")
        if singularity_image:
            # Copy files into mrbump_data directory for HTCondor transfer
            data_directory = working_directory / "mrbump_data"
            data_directory.mkdir(parents=True, exist_ok=True)
            # Extend stdin with those provided in ispyb_parameters
            for key, val in params.get("ispyb_parameters", {}).items():
                if key == "hklin":
                    # This is provided as command line keyword and handled elsewhere
                    hkl_paths = []
                    for pth in val:
                        shutil.copy(pth, data_directory)
                        hkl_paths.append(
                            os.sep.join([data_directory.name, os.path.basename(pth)])
                        )
                    self.recwrap.environment.update({"hklin": hkl_paths})
                elif key == "localfile":
                    localfile_paths = []
                    for localfile in val:
                        shutil.copy(localfile, data_directory)
                        localfile_paths.append(
                            os.sep.join(
                                [data_directory.name, os.path.basename(localfile)]
                            )
                        )
                    self.recwrap.environment.update({"localfile": localfile_paths})

            try:
                tmp_path = working_directory / "TMP"
                tmp_path.mkdir(parents=True, exist_ok=True)
                pdbmount = Path(params["mrbump"]["pdbmount"])
                pdblocal = Path(params["mrbump"]["pdblocal"])
                # shutil.copy(singularity_image, str(working_directory))
                # image_name = Path(singularity_image).name
                write_mrbump_singularity_script(
                    working_directory,
                    singularity_image,
                    tmp_path.name,
                    str(pdbmount),
                    str(pdblocal),
                )
                self.recwrap.environment.update(
                    {"singularity_image": singularity_image}
                )
            except Exception:
                logger.exception("Error writing singularity script")
                return False

        return True
