from pytest import approx

tests = {
    "relion": {
        "src_dir": "/dls/m12/data/2021/cm28212-1/raw",
        "src_run_num": (1),
        "src_prefix": ("",),
        "results": {
            "motion_correction": {
                "micrograph_name": "MotionCorr/job002/Movies/Frames/*_frameImage.mrc",
                "total_motion": approx(250, 10),
                "early_motion": approx(2.5, 0.5),
                "late_motion": approx(15, 2),
                "average_motion_per_frame": approx(16, 2),
            },
            "ctf": {
                "astigmatism": approx(247, 10),
                "astigmatism_angle": approx(83, 2),
                "max_estimated_resolution": approx(5, 1),
                "estiamted_defocus": approx(10800, 1000),
                "cc_value": approx(0.15, 0.05),
            },
        },
    },
}
