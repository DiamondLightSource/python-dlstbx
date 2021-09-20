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

    def do_insert_ctf_buffer(self, parameters, message=None, **kwargs):
        if message is None:
            message = {}
        dcid = parameters("dcid")
        self.log.info(f"Inserting CTF parameters. DCID: {dcid}")

        def full_parameters(param):
            return message.get(param) or parameters(param)

        try:
            result = self.ispyb.em_acquisition.insert_ctf(
                ctf_id=full_parameters("ctf_id"),
                motion_correction_id=full_parameters("motion_correction_id"),
                auto_proc_program_id=full_parameters("program_id"),
                box_size_x=full_parameters("box_size_x"),
                box_size_y=full_parameters("box_size_y"),
                min_resolution=full_parameters("min_resolution"),
                max_resolution=full_parameters("max_resolution"),
                min_defocus=full_parameters("min_defocus"),
                max_defocus=full_parameters("max_defocus"),
                astigmatism=full_parameters("astigmatism"),
                defocus_step_size=full_parameters("defocus_step_size"),
                astigmatism_angle=full_parameters("astigmatism_angle"),
                estimated_resolution=full_parameters("estimated_resolution"),
                estimated_defocus=full_parameters("estimated_defocus"),
                amplitude_contrast=full_parameters("amplitude_contrast"),
                cc_value=full_parameters("cc_value"),
                fft_theoretical_full_path=full_parameters("fft_theoretical_full_path"),
                comments=full_parameters("comments"),
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
            f"Looking for Motion Correction ID. Micrograph name: {micrographname} APPID: {autoproc_program_id}"
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
        self.log.info("Inserting Motion Correction parameters.")
        try:
            movieid = None
            if parameters("movie_id") is None:
                movie_params = self.ispyb.em_acquisition.get_movie_params()
                movie_params["dataCollectionId"] = parameters("dcid")
                movie_params["movieNumber"] = parameters("image_number")
                movie_params["movieFullPath"] = parameters("micrograph_name")
                movieid = self.ispyb.em_acquisition.insert_movie(
                    list(movie_params.values())
                )
                self.log.info(f"Created Movie record {movieid}")
            result = self.ispyb.em_acquisition.insert_motion_correction(
                movie_id=parameters("movie_id") or movieid,
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
            driftparams = self.ispyb.em_acquisition.get_motion_correction_drift_params()
            driftparams["motionCorrectionId"] = result
            if parameters("drift_frames") is not None:
                for frame, x, y in parameters("drift_frames"):
                    driftparams["frameNumber"] = frame
                    driftparams["deltaX"] = x
                    driftparams["deltaY"] = y
                    driftid = self.ispyb.em_acquisition.insert_motion_correction_drift(
                        list(driftparams.values())
                    )
                    self.log.info(f"Created MotionCorrectionDrift record {driftid}")
            return {"success": True, "return_value": result}

        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting motion correction entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    def do_insert_motion_correction_buffer(self, parameters, message=None, **kwargs):
        if message is None:
            message = {}
        self.log.info("Inserting Motion Correction parameters.")

        def full_parameters(param):
            return message.get(param) or parameters(param)

        try:
            movieid = None
            if full_parameters("movie_id") is None:
                movie_params = self.ispyb.em_acquisition.get_movie_params()
                movie_params["dataCollectionId"] = full_parameters("dcid")
                movie_params["movieNumber"] = full_parameters("image_number")
                movie_params["movieFullPath"] = full_parameters("micrograph_full_path")
                movieid = self.ispyb.em_acquisition.insert_movie(
                    list(movie_params.values())
                )
                self.log.info(f"Created Movie record {movieid}")
            result = self.ispyb.em_acquisition.insert_motion_correction(
                movie_id=full_parameters("movie_id") or movieid,
                auto_proc_program_id=full_parameters("program_id"),
                image_number=full_parameters("image_number"),
                first_frame=full_parameters("first_frame"),
                last_frame=full_parameters("last_frame"),
                dose_per_frame=full_parameters("dose_per_frame"),
                total_motion=full_parameters("total_motion"),
                average_motion_per_frame=full_parameters("average_motion_per_frame"),
                drift_plot_full_path=full_parameters("drift_plot_full_path"),
                micrograph_full_path=full_parameters("micrograph_full_path"),
                micrograph_snapshot_full_path=full_parameters(
                    "micrograph_snapshot_full_path"
                ),
                fft_full_path=full_parameters("fft_full_path"),
                fft_corrected_full_path=full_parameters("fft_corrected_full_path"),
                patches_used_x=full_parameters("patches_used_x"),
                patches_used_y=full_parameters("patches_used_y"),
                comments=full_parameters("comments"),
            )
            self.log.info(f"Created MotionCorrection record {result}")
            driftparams = self.ispyb.em_acquisition.get_motion_correction_drift_params()
            driftparams["motionCorrectionId"] = result
            if full_parameters("drift_frames") is not None:
                for frame, x, y in full_parameters("drift_frames"):
                    driftparams["frameNumber"] = frame
                    driftparams["deltaX"] = x
                    driftparams["deltaY"] = y
                    driftid = self.ispyb.em_acquisition.insert_motion_correction_drift(
                        list(driftparams.values())
                    )
                    self.log.info(f"Created MotionCorrectionDrift record {driftid}")
            return {"success": True, "return_value": result}

        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting motion correction entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    def do_insert_particle_picker(self, parameters, message=None, **kwargs):
        if message is None:
            message = {}
        dcid = parameters("dcid")
        self.log.info(f"Inserting Particle Picker parameters. DCID: {dcid}")

        def full_parameters(param):
            return message.get(param) or parameters(param)

        try:
            result = self.ispyb.em_acquisition.insert_particle_picker(
                particle_picker_id=full_parameters("particle_picker_id"),
                first_motion_correction_id=full_parameters("motion_correction_id"),
                auto_proc_program_id=full_parameters("program_id"),
                particle_picking_template=full_parameters("particle_picking_template"),
                particle_diameter=full_parameters("particle_diameter"),
                number_of_particles=full_parameters("number_of_particles"),
                summary_image_full_path=full_parameters("summary_image_full_path"),
            )
            self.log.info(f"Created ParticlePicker record {result} for DCID {dcid}")
            return {"success": True, "return_value": result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting Particle Picker entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    do_insert_particle_picker_buffer = do_insert_particle_picker
    # Deprecated 2021-09-20

    def do_insert_class2d(self, parameters, **kwargs):
        # This gives some output we can read from; ISPyB doesn't have fields for Class 2D yet

        appid = parameters("program_id")
        dcid = parameters("dcid")
        self.log.info(
            f"Would insert Class 2D parameters. AutoProcProgramID: {appid}, DCID: {dcid}"
        )
        return {"success": True, "return_value": None}

    def do_insert_class3d(self, parameters, **kwargs):
        # This gives some output we can read from; ISPyB doesn't have fields for Class 3D yet

        appid = parameters("program_id")
        dcid = parameters("dcid")
        self.log.info(
            f"Would insert Class 3D parameters. AutoProcProgramID: {appid}, DCID: {dcid}"
        )
        return {"success": True, "return_value": None}

    def do_insert_particle_classification(self, parameters, message=None, **kwargs):
        if message is None:
            message = {}
        dcid = parameters("dcid")

        def full_parameters(param):
            return message.get(param) or parameters(param)

        try:
            class_group_result = (
                self.ispyb.em_acquisition.insert_particle_classification(
                    particle_classification_id=full_parameters(
                        "particle_classification_id"
                    ),
                    particle_classification_group_id=full_parameters(
                        "particle_classification_group_id"
                    ),
                    class_number=full_parameters("class_number"),
                    class_image_full_path=full_parameters("class_image_full_path"),
                    particles_per_class=full_parameters("particles_per_class"),
                    class_distribution=full_parameters("class_distribution"),
                    rotation_accuracy=full_parameters("rotation_accuracy"),
                    translation_accuracy=full_parameters("translation_accuracy"),
                    estimated_resolution=full_parameters("estimated_resolution"),
                    overall_fourier_completeness=full_parameters(
                        "overall_fourier_completeness"
                    ),
                )
            )
            self.log.info(
                f"Created particle classification record {class_group_result} for DCID {dcid}"
            )
            return {"success": True, "return_value": class_group_result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting particle classification entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    def do_insert_particle_classification_group(
        self, parameters, message=None, **kwargs
    ):
        if message is None:
            message = {}
        dcid = parameters("dcid")

        def full_parameters(param):
            return message.get(param) or parameters(param)

        self.log.info(f"Inserting particle classification parameters. DCID: {dcid}")
        try:
            class_result = (
                self.ispyb.em_acquisition.insert_particle_classification_group(
                    particle_classification_group_id=full_parameters(
                        "particle_classification_group_id"
                    ),
                    particle_picker_id=full_parameters("particle_picker_id"),
                    auto_proc_program_id=full_parameters("program_id"),
                    type=full_parameters("type"),
                    batch_number=full_parameters("batch_number"),
                    number_of_particles_per_batch=full_parameters(
                        "number_of_particles_per_batch"
                    ),
                    number_of_classes_per_batch=full_parameters(
                        "number_of_classes_per_batch"
                    ),
                    symmetry=full_parameters("symmetry"),
                )
            )
            self.log.info(
                f"Created particle classification group record {class_result} for DCID {dcid}"
            )
            return {"success": True, "return_value": class_result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting particle classification group entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    # def do_insert_cryoem_initial_model(self, parameters, message, **kwargs):
    #    return {"success": True, "return_value": None}

    def do_insert_cryoem_initial_model(self, parameters, message, **kwargs):
        if message is None:
            message = {}
        dcid = parameters("dcid")

        def full_parameters(param):
            return message.get(param) or parameters(param)

        self.log.info(f"Inserting CryoEM Initial Model parameters. DCID: {dcid}")
        try:
            initial_model_result = (
                self.ispyb.em_acquisition.insert_cryoem_initial_model(
                    cryoem_initial_model_id=full_parameters("cryoem_inital_model_id"),
                    particle_classification_id=full_parameters(
                        "particle_classification_id"
                    ),
                    resolution=full_parameters("resolution"),
                    number_of_particles=full_parameters("number_of_particles"),
                )
            )
            self.log.info(
                f"Created CryoEM Initial Model record {initial_model_result} for DCID {dcid}"
            )
            return {"success": True, "return_value": initial_model_result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting CryoEM Initial Model entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False
