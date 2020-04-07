from __future__ import absolute_import, division, print_function

import pyparsing as pp
from pyparsing import Literal, Group, OneOrMore, SkipTo, nums, Regex, Word
import logging
import numpy as np
from pprint import pformat
from scipy.stats.stats import ttest_1samp

logger = logging.getLogger("dlstbx.util.shelxc")


def parse_shelxc_logs(shelxc_log):
    """Parse log files using pattern specified in the input dictionary"""

    pp.ParserElement.setDefaultWhitespaceChars(" \t")
    msg = {}
    int_number = Word(nums).setParseAction(lambda x: int(x[0]))
    float_number = Regex(r"[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?").setParseAction(
        lambda x: float(x[0])
    )

    header_data = Group(
        Literal("Resl.").suppress() + Literal("Inf.") + OneOrMore(float_number)
    )
    refl_data = Group(Literal("N(data)").suppress() + OneOrMore(int_number))
    chisq_data = Group(Literal("Chi-sq").suppress() + OneOrMore(float_number))
    isig_data = Group(Literal("<I/sig>").suppress() + OneOrMore(float_number))
    comp_data = Group(Literal("%Complete").suppress() + OneOrMore(float_number))
    dsig_data = Group(Literal('<d"/sig>').suppress() + OneOrMore(float_number))
    cc_data = Group(Literal("CC(1/2)").suppress() + OneOrMore(float_number))

    def shelxc_log_pattern(pattern):

        res_pattern = OneOrMore(SkipTo(pattern).suppress() + pattern)
        return res_pattern

    try:
        reso_dict = {
            "reso_data": shelxc_log_pattern(header_data)
            .parseString(shelxc_log)
            .asList()
        }
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
            parse_dict = {
                metric_key: shelxc_log_pattern(metric_pattern)
                .parseString(shelxc_log)
                .asList()
            }
            if len(parse_dict[metric_key]) != nshells:
                logger.debug(
                    "Missing or invalid values for {} found in some resolution shells.".format(
                        metric_key
                    )
                )
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

    msg.update(
        {
            "mean_chisq": [],
            "std_chisq": [],
            "med_isig": [],
            "med_dsig": [],
            "high_res": [],
            "cc_aver": [],
            "cc_autocorr": [],
            "dsig_autocorr": [],
        }
    )
    idx_resol = []
    for ds in range(len(msg["reso_data"])):
        try:
            idx = max(
                next(
                    i
                    for (i, v) in enumerate(msg["reso_data"][ds][1:], 1)
                    if v < RESOL_CUTOFF
                ),
                2,
            )
        except StopIteration:
            idx = -1
        idx_resol.append(idx)
        try:
            res = sorted(msg["chisq_data"][ds][1:idx])
            msg["mean_chisq"].append(np.mean(res))
            msg["std_chisq"].append(np.std(res))
        except Exception:
            msg["mean_chisq"].append(float("nan"))
            msg["std_chisq"].append(float("nan"))
        try:
            msg["med_isig"].append(np.mean(msg["isig_data"][ds][1:idx]))
        except Exception:
            msg["med_isig"].append(float("nan"))
        try:
            msg["med_dsig"].append(np.mean(msg["dsig_data"][ds][1:idx]))
        except Exception:
            msg["med_dsig"].append(float("nan"))
        try:
            msg["high_res"].append(min(msg["reso_data"][ds][1:]))
        except Exception:
            msg["high_res"].append(float("nan"))
        try:
            msg["cc_aver"].append(np.mean(msg["cc_data"][ds][1:idx]))
        except Exception:
            msg["cc_aver"].append(float("nan"))

        try:
            msg["cc_autocorr"].append(
                np.corrcoef(
                    [msg["cc_data"][ds][1 : idx - 1], msg["cc_data"][ds][2:idx]]
                )[0][1]
            )
        except Exception:
            msg["cc_autocorr"].append(float("nan"))
        try:
            msg["dsig_autocorr"].append(
                np.corrcoef(
                    [msg["dsig_data"][ds][1 : idx - 1], msg["dsig_data"][ds][2:idx]]
                )[0][1]
            )
        except Exception:
            msg["dsig_autocorr"].append(float("nan"))

    param_labels = ["reso", "dsig_data", "cc_data", "cc_autocorr", "dsig_autocorr"]
    param_status = [
        (
            msg["high_res"],
            "cut-off",
            HIGHRES_CUTOFF,
            "Pass" if min(msg["high_res"]) < HIGHRES_CUTOFF else "Fail",
        ),
        (
            msg["dsig_data"],
            "mean",
            msg["med_dsig"],
            "cut-off",
            DSIG_CUTOFF,
            "Pass" if max(msg["med_dsig"]) > DSIG_CUTOFF else "Fail",
        ),
        (
            msg["cc_data"],
            "mean",
            msg["cc_aver"],
            "cut-off",
            CCAVER_CUTOFF,
            "Pass" if max(msg["cc_aver"]) > CCAVER_CUTOFF else "Fail",
        ),
        (
            msg["cc_autocorr"],
            "cut-off",
            AUTOCORR_CUTOFF,
            "Pass" if max(msg["cc_autocorr"]) > AUTOCORR_CUTOFF else "Fail",
        ),
        (
            msg["dsig_autocorr"],
            "cut-off",
            AUTOCORR_CUTOFF,
            "Pass" if max(msg["dsig_autocorr"]) > AUTOCORR_CUTOFF else "Fail",
        ),
    ]
    logger.info(pformat(dict(zip(param_labels, param_status))))

    if (
        max(msg["med_dsig"]) > DSIG_CUTOFF
        and min(msg["high_res"]) < HIGHRES_CUTOFF
        and max(msg["cc_aver"]) > CCAVER_CUTOFF
        and (
            max(msg["cc_autocorr"]) > AUTOCORR_CUTOFF
            or max(msg["dsig_autocorr"]) > AUTOCORR_CUTOFF
        )
        or FORCE
    ):
        chisq_data = tuple(
            [
                [
                    ttest_1samp(sorted(ds[1:idx]), 1.0).pvalue
                    for idx, ds in zip(idx_resol, msg["chisq_data"])
                ],
            ]
            + [msg["cc_aver"], msg["std_chisq"], msg["med_dsig"]]
        )
        msg["pvalue"] = chisq_data[0]
        logger.info(
            pformat(
                dict(zip(["p-value", "cc_aver", "std_chisq", "med_dsig"], chisq_data,))
            )
        )

    if chisq_data:
        return msg
