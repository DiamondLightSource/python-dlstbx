from __future__ import absolute_import, division, print_function

from pyparsing import Literal, Group, OneOrMore, SkipTo, nums, Regex, Word
import logging
from copy import deepcopy
import numpy as np
from pprint import pformat
from scipy.stats.stats import ttest_1samp

logger = logging.getLogger("dlstbx.util.shelxc")


def parse_shelxc_logs(shelxc_log):
    """Parse log files using pattern specified in the input dictionary"""

    msg = {}
    int_number = Word(nums).setParseAction(lambda x: int(x[0]))
    float_number = Regex(r"[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?").setParseAction(
        lambda x: float(x[0])
    )

    header_data = Group(
        Literal("Resl.").suppress() + Literal("Inf.") + OneOrMore(float_number)
    )
    refl_data = Literal("N(data)").suppress() + OneOrMore(int_number)
    chisq_data = Literal("Chi-sq").suppress() + OneOrMore(float_number)
    isig_data = Literal("<I/sig>").suppress() + OneOrMore(float_number)
    comp_data = Literal("%Complete").suppress() + OneOrMore(float_number)
    dsig_data = Literal('<d"/sig>').suppress() + OneOrMore(float_number)
    cc_data = Literal("CC(1/2)").suppress() + OneOrMore(float_number)

    def __shelxc_log_pattern(pattern, pattern_key):

        res_pattern = SkipTo(pattern).suppress() + pattern(pattern_key)
        return res_pattern

    try:
        reso_dict = (
            __shelxc_log_pattern(header_data, "reso_data")
            .parseString(shelxc_log)
            .asDict()
        )
        nshells = len(reso_dict["reso_data"]) - 1
        msg.update(reso_dict)
    except Exception:
        logger.debug("Could not parse resolution values from SHELXC log.")
        return None
    for metric_pattern, metric_key in (
        (refl_data, "refl_data"),
        (chisq_data, "chisq_data"),
        (isig_data, "isig_data"),
        (comp_data, "comp_data"),
        (dsig_data, "dsig_data"),
        (cc_data, "cc_data"),
    ):
        try:
            parse_dict = (
                __shelxc_log_pattern(metric_pattern, metric_key)
                .parseString(shelxc_log)
                .asDict()
            )
            if len(parse_dict[metric_key]) != nshells:
                logger.debug(
                    "Missing or invalid values for {} found in some resolution shells.".format(
                        metric_key
                    )
                )
                continue
            msg.update(parse_dict)
        except Exception:
            logger.debug(
                "Could not parse {} values from SHELXC log.".format(metric_key)
            )

    return msg


def reduce_shelxc_results(msg, params):

    RESOL_CUTOFF = params["resol_cutoff"]
    DSIG_CUTOFF = params["dsig_cutoff"]
    HIGHRES_CUTOFF = params["dmin_cutoff"]
    CCAVER_CUTOFF = params["ccaver_cutoff"]
    AUTOCORR_CUTOFF = params["autocorr_cutoff"]
    try:
        FORCE = params["force"]
    except KeyError:
        FORCE = False
    chisq_data = None

    try:
        idx = max(
            next(i for (i, v) in enumerate(msg["reso_data"]) if v < RESOL_CUTOFF), 2
        )
    except StopIteration:
        idx = -1
    try:
        res = sorted(deepcopy(msg["chisq_data"]))[1:idx]
        msg["mean_chisq"] = np.mean(res)
        msg["std_chisq"] = np.std(res)
    except Exception:
        msg["mean_chisq"] = float("nan")
        msg["std_chisq"] = float("nan")
    try:
        msg["med_isig"] = np.mean(msg["isig_data"][1:idx])
    except Exception:
        msg["med_isig"] = float("nan")
    try:
        msg["med_dsig"] = np.mean(msg["dsig_data"][1:idx])
    except Exception:
        msg["med_dsig"] = float("nan")
    try:
        msg["high_res"] = min(msg["reso_data"])
    except Exception:
        msg["high_res"] = float("nan")
    try:
        msg["cc_aver"] = np.mean(msg["cc_data"][1:idx])
    except Exception:
        msg["cc_aver"] = float("nan")

    try:
        msg["cc_autocorr"] = np.corrcoef(
            [msg["cc_data"][1 : idx - 1], msg["cc_data"][2:idx]]
        )[0][1]
    except Exception:
        msg["cc_autocorr"] = float("nan")
    try:
        msg["dsig_autocorr"] = np.corrcoef(
            [msg["dsig_data"][1 : idx - 1], msg["dsig_data"][2:idx]]
        )[0][1]
    except Exception:
        msg["dsig_autocorr"] = float("nan")

    param_labels = ["reso", "dsig_data", "cc_data", "cc_autocorr", "dsig_autocorr"]
    param_status = [
        (
            msg["high_res"],
            "cut-off",
            HIGHRES_CUTOFF,
            "Pass" if msg["high_res"] < HIGHRES_CUTOFF else "Fail",
        ),
        (
            msg["dsig_data"],
            "mean",
            msg["med_dsig"],
            "cut-off",
            DSIG_CUTOFF,
            "Pass" if msg["med_dsig"] > DSIG_CUTOFF else "Fail",
        ),
        (
            msg["cc_data"],
            "mean",
            msg["cc_aver"],
            "cut-off",
            CCAVER_CUTOFF,
            "Pass" if msg["cc_aver"] > CCAVER_CUTOFF else "Fail",
        ),
        (
            msg["cc_autocorr"],
            "cut-off",
            AUTOCORR_CUTOFF,
            "Pass" if msg["cc_autocorr"] > AUTOCORR_CUTOFF else "Fail",
        ),
        (
            msg["dsig_autocorr"],
            "cut-off",
            AUTOCORR_CUTOFF,
            "Pass" if msg["dsig_autocorr"] > AUTOCORR_CUTOFF else "Fail",
        ),
    ]
    logger.info(pformat(dict(zip(param_labels, param_status))))

    if (
        msg["med_dsig"] > DSIG_CUTOFF
        and msg["high_res"] < HIGHRES_CUTOFF
        and msg["cc_aver"] > CCAVER_CUTOFF
        and (
            msg["cc_autocorr"] > AUTOCORR_CUTOFF
            or msg["dsig_autocorr"] > AUTOCORR_CUTOFF
        )
        or FORCE
    ):
        chisq_data = tuple(
            list(ttest_1samp(res, 1.0))
            + [msg["cc_aver"], msg["std_chisq"], msg["med_dsig"]]
        )
        msg["pvalue"] = chisq_data[1]
        logger.info(
            pformat(
                dict(
                    zip(
                        ["t-value", "p-value", "cc_aver", "std_chisq", "med_dsig"],
                        chisq_data,
                    )
                )
            )
        )

    if chisq_data:
        return msg
