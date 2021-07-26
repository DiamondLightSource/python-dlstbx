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

    def do_insert_ctf_buffer(self, *, parameters, session, **kwargs):
        dcid = parameters("dcid")
        self.log.info(f"Inserting CTF parameters. DCID: {dcid}")
        try:
            result = self.ispyb.em_acquisition.insert_ctf(
                ctf_id=parameters("ctf_id"),
                motion_correction_id=parameters("motion_correction_id"),
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

    def do_insert_motion_correction_buffer(self, parameters, **kwargs):
        self.log.info("Inserting Motion Correction parameters.")
        try:
            movieid = None
            if parameters("movie_id") is None:
                movie_params = self.ispyb.em_acquisition.get_movie_params()
                movie_params["dataCollectionId"] = parameters("dcid")
                movie_params["movieNumber"] = parameters("image_number")
                movie_params["movieFullPath"] = parameters("micrograph_full_path")
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

    def do_insert_particle_picker(self, *, parameters, session, **kwargs):
        # We don't yet have a way of inserting information from this message

        appid = parameters("program_id")
        dcid = parameters("dcid")
        self.log.info(
            f"Would insert particle picker parameters. AutoProcProgramID: {appid}, DCID: {dcid}"
        )
        return {"success": True, "return_value": None}

    def do_insert_particle_picker_buffer(self, *, parameters, session, **kwargs):

        dcid = parameters("dcid")
        self.log.info(f"Inserting Particle Picker parameters. DCID: {dcid}")
        try:
            result = self.ispyb.em_acquisition.insert_particle_picker(
                particle_picker_id=parameters("particle_picker_id"),
                first_motion_correction_id=parameters("motion_correction_id"),
                auto_proc_program_id=parameters("program_id"),
                particle_picking_template=parameters("particle_picking_template"),
                particle_diameter=parameters("particle_diameter"),
                number_of_particles=parameters("number_of_particles"),
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

    def do_insert_class2d(self, parameters, **kwargs):

        dcid = parameters("dcid")
        self.log.info(f"Inserting 2D Classification parameters. DCID: {dcid}")
        try:
            class2d_result = (
                self.ispyb.em_acquisition.insert_particle_classification_group(
                    particle_classification_group_id=parameters(
                        "particle_classification_group_id"
                    ),
                    particle_picker_id=parameters("particle_picker_id"),
                    auto_proc_program_id=parameters("program_id"),
                    type="2D",
                    batch_number=parameters("batch_number"),
                    number_of_particles_per_batch=parameters(
                        "number_of_particles_per_batch"
                    ),
                    number_of_classes_per_batch=parameters(
                        "number_of_classes_per_batch"
                    ),
                    symmetry=parameters("symmetry"),
                )
            )
            self.log.info(
                f"Created 2D Classification Group record {class2d_result} for DCID {dcid}"
            )
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting 2D Classification Group entry caused exception '%s'.",
                e,
                exc_info=True,
            )
        try:
            class2d_group_result = (
                self.ispyb.em_acquisition.insert_particle_classification(
                    particle_classification_id=parameters("particle_classification_id"),
                    particle_classification_group_id=parameters(
                        "particle_classification_group_id"
                    ),
                    class_number=parameters("class_number"),
                    class_image_full_path=parameters("class_image_full_path"),
                    particles_per_class=parameters("partices_per_class"),
                    rotation_accuracy=parameters("rotation_accuracy"),
                    translation_accuracy=parameters("translation_accuracy"),
                    estimated_resolution=parameters("estimated_resolution"),
                    overall_fourier_completeness=parameters(
                        "overall_fourier_completeness"
                    ),
                )
            )
            self.log.info(
                f"Created 2D Classification record {class2d_group_result} for DCID {dcid}"
            )
            return {"success": True, "return_value": class2d_group_result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting 2D Classification entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    def do_insert_class3d(self, parameters, **kwargs):

        dcid = parameters("dcid")
        class3d_result = None

        self.log.info(f"Inserting 3D Classification parameters. DCID: {dcid}")
        try:
            class3d_result = (
                self.ispyb.em_acquisition.insert_particle_classification_group(
                    particle_classification_group_id=parameters(
                        "particle_classification_group_id"
                    ),
                    particle_picker_id=parameters("particle_picker_id"),
                    auto_proc_program_id=parameters("program_id"),
                    type="3D",
                    batch_number=parameters("batch_number"),
                    number_of_particles_per_batch=parameters(
                        "number_of_particles_per_batch"
                    ),
                    number_of_classes_per_batch=parameters(
                        "number_of_classes_per_batch"
                    ),
                    symmetry=parameters("symmetry"),
                )
            )
            self.log.info(
                f"Created 3D Classification Group record {class3d_result} for DCID {dcid}"
            )
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting 3D Classification Group entry caused exception '%s'.",
                e,
                exc_info=True,
            )
        try:
            class3d_group_result = (
                self.ispyb.em_acquisition.insert_particle_classification(
                    particle_classification_id=parameters("particle_classification_id"),
                    particle_classification_group_id=parameters(
                        "particle_classification_group_id"
                    ),
                    class_number=parameters("class_number"),
                    class_image_full_path=parameters("class_image_full_path"),
                    particles_per_class=parameters("partices_per_class"),
                    rotation_accuracy=parameters("rotation_accuracy"),
                    translation_accuracy=parameters("translation_accuracy"),
                    estimated_resolution=parameters("estimated_resolution"),
                    overall_fourier_completeness=parameters(
                        "overall_fourier_completeness"
                    ),
                )
            )
            self.log.info(
                f"Created 3D Classification record {class3d_group_result} for DCID {dcid}"
            )
            return {"success": True, "return_value": class3d_group_result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting 3D Classification entry caused exception '%s'.",
                e,
                exc_info=True,
            )
        self.log.info(f"Inserting CryoEM Initial Model parameters. DCID: {dcid}")
        try:
            initial_model_result = (
                self.ispyb.em_acquisition.insert_cryoem_initial_model(
                    cryoem_initial_model_id=parameters("cryoem_inital_model_id"),
                    particle_classification_id=class3d_result,
                    resolution=parameters("init_model_resolution"),
                    number_of_particles=parameters("init_model_number_of_particles"),
                )
            )
            self.log.info(
                f"Created CryoEM Initial Model record {initial_model_result} for DCID {dcid}"
            )
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting CryoEM Initial Model entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    def do_insert_particle_classification_group(self, parameters, **kwargs):

        dcid = parameters("dcid")

        try:
            class_group_result = (
                self.ispyb.em_acquisition.insert_particle_classification(
                    particle_classification_id=parameters("particle_classification_id"),
                    particle_classification_group_id=parameters(
                        "particle_classification_group_id"
                    ),
                    class_number=parameters("class_number"),
                    class_image_full_path=parameters("class_image_full_path"),
                    particles_per_class=parameters("partices_per_class"),
                    rotation_accuracy=parameters("rotation_accuracy"),
                    translation_accuracy=parameters("translation_accuracy"),
                    estimated_resolution=parameters("estimated_resolution"),
                    overall_fourier_completeness=parameters(
                        "overall_fourier_completeness"
                    ),
                )
            )
            self.log.info(
                f"Created particle classification group record {class_group_result} for DCID {dcid}"
            )
            return {"success": True, "return_value": class_group_result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting particle classification group entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    def do_insert_particle_classification(self, parameters, **kwargs):

        dcid = parameters("dcid")
        self.log.info(f"Inserting particle classification parameters. DCID: {dcid}")
        try:
            class_result = (
                self.ispyb.em_acquisition.insert_particle_classification_group(
                    particle_classification_group_id=parameters(
                        "particle_classification_group_id"
                    ),
                    particle_picker_id=parameters("particle_picker_id"),
                    auto_proc_program_id=parameters("program_id"),
                    type=parameters("type"),
                    batch_number=parameters("batch_number"),
                    number_of_particles_per_batch=parameters(
                        "number_of_particles_per_batch"
                    ),
                    number_of_classes_per_batch=parameters(
                        "number_of_classes_per_batch"
                    ),
                    symmetry=parameters("symmetry"),
                )
            )
            self.log.info(
                f"Created particle classification record {class_result} for DCID {dcid}"
            )
            return {"success": True, "return_value": class_result}
        except ispyb.ISPyBException as e:
            self.log.error(
                "Inserting particle classification entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return False

    def do_insert_cryoem_initial_model(self, parameters, **kwargs):

        dcid = parameters("dcid")
        self.log.info(f"Inserting CryoEM Initial Model parameters. DCID: {dcid}")
        try:
            initial_model_result = (
                self.ispyb.em_acquisition.insert_cryoem_initial_model(
                    cryoem_initial_model_id=parameters("cryoem_inital_model_id"),
                    particle_classification_id=parameters("particle_classification_id"),
                    resolution=parameters("init_model_resolution"),
                    number_of_particles=parameters("init_model_number_of_particles"),
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
