import ispyb
from ispyb.sqlalchemy import MotionCorrection


class EM_Mixin:
    def do_insert_ctf(self, *, parameters, session, **kwargs):
        dcid = parameters("dcid")
        micrographname = parameters("micrograph_name")
        appid = parameters("program_id")
        mcid = self._get_motioncorrection_id(
            micrographname,
            appid,
            session,
        )
        if mcid is None:
            self.log.error(
                f"No Motion Correction ID found. MG: {micrographname}, APPID: {appid}"
            )
            return False
        self.log.info(f"Inserting CTF parameters. DCID: {dcid}")
        try:
            result = self.ispyb.em_acquisition.insert_ctf(
                ctf_id=parameters("ctf_id"),
                motion_correction_id=mcid,
                auto_proc_program_id=parameters("program_id"),
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
            self.log.info(f"Created CTF record {result} for DCID {dcid}")
            return {"success": True, "return_value": result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting CTF entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    def _get_motioncorrection_id(
        self,
        micrographname,
        autoproc_program_id,
        db_session,
    ):
        self.log.info(
            f"Looking for Motion Correction ID."
            f"Micrograph name: {micrographname} APPID: {autoproc_program_id}"
        )
        mc_query = db_session.query(MotionCorrection).filter(
            MotionCorrection.micrographFullPath == micrographname,
            MotionCorrection.autoProcProgramId == autoproc_program_id,
        )
        results = mc_query.all()
        if results:
            mcid = results[0].motionCorrectionId
            self.log.info(f"Found Motion Correction ID: {mcid}")
            return mcid
        else:
            return None

    def do_insert_motion_correction(self, parameters, **kwargs):
        self.log.info(f"Inserting Motion Correction parameters.")
        try:
            result = self.ispyb.em_acquisition.insert_motion_correction(
                movie_id=parameters("movie_id"),
                auto_proc_program_id=parameters("program_id"),
                image_number=parameters("image_number"),
                first_frame=parameters("first_frame"),
                last_frame=parameters("last_frame"),
                dose_per_frame=parameters("dose_per_frame"),
                total_motion=parameters("total_motion"),
                average_motion_per_frame=parameters("average_motion_per_frame"),
                drift_plot_full_path=parameters("drift_plot_full_path"),
                micrograph_full_path=parameters("micrograph_name"),
                micrograph_snapshot_full_path=parameters(
                    "micrograph_snapshot_full_path"
                ),
                fft_full_path=parameters("fft_full_path"),
                fft_corrected_full_path=parameters("fft_corrected_full_path"),
                patches_used_x=parameters("patches_used_x"),
                patches_used_y=parameters("patches_used_y"),
                comments=parameters("comments"),
            )
            self.log.info(f"Created MotionCorrection record {result}")

            return {"success": True, "return_value": result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting motion correction entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    def do_insert_class2d(self, parameters, **kwargs):
        # This gives some output we can read from; ISPyB doesn't have fields for Class 2D yet

        dcid = parameters("datacollection_id")
        ref_image = parameters("reference_image")
        self.log.info(f"Would insert Class 2D parameters. DCID: {dcid} {ref_image}")
        return {"success": True, "return_value": None}

    def do_insert_class3d(self, parameters, **kwargs):
        # This gives some output we can read from; ISPyB doesn't have fields for Class 3D yet

        dcid = parameters("datacollection_id")
        ref_image = parameters("reference_image")

        self.log.info(f"Would insert Class 3D parameters. DCID: {dcid} {ref_image}")
        return {"success": True, "return_value": None}
