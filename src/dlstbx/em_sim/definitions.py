from pytest import approx


def motion_corr_res_dict(image_number):
    res = {
        "micrographFullPath": f"MotionCorr/job002/Movies/Frames/20170629_000{image_number}_frameImage.mrc",
        "totalMotion": approx(250, 0.2),
        # "early_motion": approx(2.5, 0.5),
        # "late_motion": approx(15, 2),
        "averageMotionPerFrame": approx(16, 0.2),
    }
    return res


def ctf_res_dict():
    res = {
        "astigmatism": approx(247, 0.2),
        "astigmatismAngle": approx(83, 0.2),
        "maxEstimatedResolution": approx(5, 0.2),
        "estiamtedDefocus": approx(10800, 0.2),
        "ccValue": approx(0.15, 0.2),
    }
    return res


frame_numbers = (
    list(range(21, 32)) + list(range(35, 38)) + [39, 40] + list(range(42, 50))
)

tests = {
    "relion": {
        "dcid": 6258983,
        "src_dir": "/dls/m12/data/2021/cm28212-2/raw",
        "src_run_num": (2,),
        "src_prefix": ("",),
        "proc_params": {
            "acquisition_software": "SerialEM",
            "import_images": "/dls/m12/data/2021/cm28212-2/raw/Frames/*.tiff",
            "motioncor_gainreference": "/dls/m12/data/2021/cm28212-2/processing/gaim.mrc",
            "voltage": "200",
            "Cs": "2.7",
            "ctffind_do_phaseshift": "false",
            "angpix": "0.885",
            "motioncor_binning": "1",
            "motioncor_doseperframe": "1.277",
            "stop_after_ctf_estimation": "false",
        },
        "results": {
            "motion_correction": tuple(motion_corr_res_dict(i) for i in frame_numbers),
            "ctf": tuple(ctf_res_dict() for i in frame_numbers),
        },
    },
}
