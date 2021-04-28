import ispyb
from ispyb.sqlalchemy import MotionCorrection, Movie


class EM_Mixin:
    def do_insert_movie(self, parameters, **kwargs):
        params = self.ispyb.em_acquisition.get_movie_params()
        for k in params.keys():
            params[k] = parameters.get(k)

        self.log.info(f"Inserting movie parameters")
        try:
            movieId = self.ispyb.em_acquisition.insert_movie(list(params.values()))
        except (ispyb.ISPyBException, AssertionError) as e:
            self.log.error(
                "Inserting screening results: '%s' caused exception '%s'.",
                params,
                e,
                exc_info=True,
            )
            return {"success": True, "return_value": movieId}

    def do_insert_ctf(self, *, parameters, session, **kwargs):
        dcid = parameters("dcid")
        self.log.info(f"Inserting CTF parameters. DCID: {dcid}")

        movie_params = self.ispyb.em_acquisition.get_movie_params()
        movie_number = movie_params["movieNumber"]
        try:
            result = self.ispyb.em_acquisition.insert_ctf(
                ctf_id=parameters("ctf_id"),
                motion_correction_id=self._get_motioncorrection_id(
                    parameters("dcid"),
                    movie_number,
                    parameters("micrograph_full_path"),
                    parameters("program_id"),
                    session,
                ),
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
        datacollectionid,
        movie_number,
        micrographname,
        autoproc_program_id,
        db_session,
    ):

        movie_query = db_session.query(Movie).filter(
            Movie.dataCollectionId == datacollectionid,
            Movie.movieNumber == movie_number,
        )
        movie_id = movie_query.all()[0].movieId
        mc_query = db_session.query(MotionCorrection).filter(
            # insert_motion_correction() doesn't currently use the DCID.
            # The entries in the MotionCorrection table therefore don't have a DCID, so we can't filter by this value.
            # MotionCorrection.dataCollectionId.is_(None),
            MotionCorrection.movieId == movie_id,
            MotionCorrection.micrographFullPath == micrographname,
            MotionCorrection.autoProcProgramId == autoproc_program_id,
        )
        results = mc_query.all()
        if results:
            mcid = results[0].motionCorrectionId
            self.log.info(f"Found Motion Correction ID: {mcid}")
            return mcid
        else:
            self.log.info(
                f"No Motion Correction ID found. DCID: {datacollectionid}, MG: {micrographname}, APPID: {autoproc_program_id}"
            )
            raise Exception("No Motion Correction ID found")

    def do_insert_motion_correction(self, parameters, **kwargs):  # session,

        # Create movie record so that we can access a DCID. The Movie and Motion Correction tables are linked.
        # Currently don't have write access via SQLAlchemy for ispyb_zocalo user.
        # values = Movie(dataCollectionId=6018191)
        # session.add(values)
        # session.commit()

        # Create movie record using stored procedures for now.
        movie_params = self.ispyb.em_acquisition.get_movie_params()
        dc_params = self.ispyb.em_acquisition.get_data_collection_params()
        movie_params["dataCollectionId"] = dc_params["id"]
        movie_params["movieNumber"] = parameters("image_number")
        self.do_insert_movie(movie_params)

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
