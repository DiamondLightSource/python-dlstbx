import ispyb


class EM_Mixin:
    def do_insert_ctf(self, parameters, **kwargs):

        # This gives some output we can read from; the motion correction ID doesn't work without the ISPyB components in place
        diff_val = {"Difference indicator (astigmatism)": parameters("astigmatism")}
        message = (
            "Putting CTF parameters."
            + str(
                self.get_motioncorrection_id(
                    parameters("datacollection_id"), parameters("micrograph_name")
                )
            )
            + str(diff_val)
        )
        self.log.info(message)
        return {"success": True, "return_value": parameters}

        try:
            result = self.ispyb.em_acquisition.insert_ctf(
                ctf_id=parameters("ctf_id"),
                motion_correction_id=self.get_motioncorrection_id(
                    parameters("datacollection_id"), parameters("micrograph_name")
                )["motioncorrection_id"],
                auto_proc_program_id=parameters("auto_proc_program_id"),
                box_size_x=parameters("box_size_x"),
                box_size_y=parameters("box_size_y"),
                min_resolution=parameters("min_resolution"),
                max_resolution=parameters("max_resolution"),
                min_defocus=parameters("min_defocus"),
                max_defocus=parameters("max_defocus"),
                astigmatism=parameters("astigmatism"),
                defocus_step_size=parameters("defocus_step_size"),
                astigmatism_angle=parameters("astigmatism_angle"),
                estimated_resolution=parameters("max_estimated_resolution"),
                estimated_defocus=parameters("estimated_defocus"),
                amplitude_contrast=parameters("amplitude_contrast"),
                cc_value=parameters("cc_value"),
                fft_theoretical_full_path=parameters("fft_theoretical_full_path"),
                comments=parameters("comments"),
            )
            return {"success": True, "return_value": result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Updating DIMPLE failure for %s caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    def get_motioncorrection_id(self, datacollectionid, micrographname):
        """Not implemented yet"""
        return {"motioncorrection_id": 1234}

    def do_insert_motion_correction(self, parameters, **kwargs):
        # This gives some output we can read from; the motion correction ID doesn't work without the ISPyB components in place
        diff_val = {"Difference indicator (total_motion)": parameters("total_motion")}
        message = (
            "Putting Motion Correction parameters."
            + str(
                self.get_motioncorrection_id(
                    parameters("datacollection_id"), parameters("micrograph_name")
                )
            )
            + str(diff_val)
        )
        self.log.info(message)
        return {"success": True, "return_value": parameters}

        # insert_motion_correction still needs to be implemented in the ISPyB API

        try:
            result = self.ispyb.em_acquisition.insert_motion_correction(
                motion_correction_id=self.get_motioncorrection_id(
                    parameters("datacollection_id"), parameters("micrograph_name")
                )["motioncorrection_id"],
                movie_id=parameters("movie_id"),
                auto_proc_program_id=parameters("auto_proc_program_id"),
                image_number=parameters("image_number"),
                first_frame=parameters("first_frame"),
                last_frame=parameters("last_frame"),
                dose_per_frame=parameters("dose_per_frame"),
                total_motion=parameters("total_motion"),
                average_motion_per_frame=parameters("average_motion_per_frame"),
                drift_plot_full_path=parameters("drift_plot_full_path"),
                micrograph_full_path=parameters("micrograph_full_path"),
                micrograph_snapshot_full_path=parameters(
                    "micrograph_snapshot_full_path"
                ),
                fft_full_path=parameters("fft_full_path"),
                fft_corrected_full_path=parameters("fft_corrected_full_path"),
                patches_used_x=parameters("patches_used_x"),
                patches_used_y=parameters("patches_used_y"),
                comments=parameters("comments"),
            )

            return {"success": True, "return_value": result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Updating DIMPLE failure for %s caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False
