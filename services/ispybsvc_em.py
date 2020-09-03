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
                estimated_resolution=parameters("estimated_resolution"),
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
