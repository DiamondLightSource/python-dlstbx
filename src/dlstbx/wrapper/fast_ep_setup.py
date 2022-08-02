from __future__ import annotations

import logging
import os
from pathlib import Path

import dlstbx.util.symlink
from dlstbx.util.iris import write_singularity_script
from dlstbx.wrapper import Wrapper

logger = logging.getLogger("zocalo.wrap.fast_ep_setup")


class FastEPSetupWrapper(Wrapper):
    def stop_fast_ep(self, params):
        """Decide whether to run fast_ep or not based on the completeness, dI/s(dI) and
        resolution of actual data."""

        from iotbx.reflection_file_reader import any_reflection_file

        if "go_fast_ep" not in params:
            logger.info("go_fast_ep settings not available")
            return False

        thres_d_min = params["go_fast_ep"].get("d_min", -1)

        def check_thresholds(data, threshold):
            thres_completeness = threshold.get("completeness", -1)
            thres_dIsigdI = threshold.get("dI/sigdI", -1)

            differences = data.anomalous_differences()
            dIsigdI = sum(abs(differences.data())) / sum(differences.sigmas())
            completeness = data.completeness()
            if completeness < thres_completeness:
                logger.info(
                    "Data completeness %.2f below threshold value %.2f. Aborting.",
                    completeness,
                    thres_completeness,
                )
                return True
            if dIsigdI < thres_dIsigdI:
                logger.info(
                    "Data dI/s(dI) %.2f below threshold value %.2f. Aborting.",
                    dIsigdI,
                    thres_dIsigdI,
                )
                return True
            logger.info(
                "Data completeness: %.2f  threshold: %.2f",
                completeness,
                thres_completeness,
            )
            logger.info(f"Data dI/s(dI): {dIsigdI:.2f}  threshold: {thres_dIsigdI:.2f}")
            return False

        hkl_file = any_reflection_file(params["data"])
        mas = hkl_file.as_miller_arrays()
        try:
            all_data = next(m for m in mas if m.anomalous_flag())
        except StopIteration:
            logger.exception("No anomalous data found in %s", params["data"])
            return True
        if all_data.d_min() > thres_d_min:
            select_data = all_data
            res = check_thresholds(select_data, params["go_fast_ep"].get("low_res", {}))
        else:
            select_data = all_data.resolution_filter(d_min=thres_d_min)
            res = check_thresholds(
                select_data, params["go_fast_ep"].get("high_res", {})
            )
        return res

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        if params.get("ispyb_parameters"):
            if params["ispyb_parameters"].get("data"):
                params["data"] = os.path.abspath(params["ispyb_parameters"]["data"])
            if int(
                params["ispyb_parameters"].get("check_go_fast_ep", False)
            ) and self.stop_fast_ep(params):
                logger.info("Skipping fast_ep (go_fast_ep == No)")
                return False

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
                # shutil.copy(singularity_image, str(working_directory))
                # image_name = Path(singularity_image).name
                write_singularity_script(working_directory, singularity_image)
                self.recwrap.environment.update(
                    {"singularity_image": singularity_image}
                )
            except Exception:
                logger.exception("Error writing singularity script")
                return False

        return True
