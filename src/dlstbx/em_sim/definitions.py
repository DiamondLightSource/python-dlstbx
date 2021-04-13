from pytest import approx


def motion_corr_res_dict(image_number):
    res = {
        "micrographFullPath": f"MotionCorr/job002/Movies/Frames/20170629_000{image_number}_frameImage.mrc",
        "totalMotion": approx(250, 10),
        # "early_motion": approx(2.5, 0.5),
        # "late_motion": approx(15, 2),
        "averageMotionPerFrame": approx(16, 2),
    }
    return res


def ctf_res_dict():
    res = {
        "astigmatism": approx(247, 10),
        "astigmatismAngle": approx(83, 2),
        "maxResolution": approx(5, 1),
        "estiamtedDefocus": approx(10800, 1000),
        "ccValue": approx(0.15, 0.05),
    }
    return res


tests = {
    "relion": {
        "src_dir": "/dls/m12/data/2021/cm28212-1/raw",
        "src_run_num": (1),
        "src_prefix": ("",),
        "results": {
            "motion_correction": {[motion_corr_res_dict(i) for i in range(21, 50)]},
            "ctf": {[ctf_res_dict() for i in range(21, 50)]},
        },
    },
}
