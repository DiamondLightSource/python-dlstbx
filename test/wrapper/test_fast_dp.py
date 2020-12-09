import json

from unittest import mock
import dlstbx.wrapper.fast_dp
from dlstbx.wrapper.fast_dp import FastDPWrapper


def test_FastDPWrapper(make_wrapper, tmpdir, mocker):

    image_path = "/dls/i04-1/data/2019/nt18231-19/tmp/2019-05-02/09-36-17-da63fb1b/INS2_29_2_1_0001.cbf:1:200"
    working_directory = tmpdir.join("work_dir")
    working_directory.ensure(dir=True)
    results_directory = tmpdir.join("results_dir")
    results_d = {
        "refined_beam": [206.45053836697736, 211.11095943078706],
        "commandline": "/path/to/cctbx/modules/fast_dp/fast_dp/fast_dp.py --atom=S -j 0 -J 18 -l durin-plugin.so %s --resolution-high=1.8"
        % image_path,
        "spacegroup": "I 2 3",
        "unit_cell": [77.9622, 77.9622, 77.9622, 90.0, 90.0, 90.0],
        "scaling_statistics": {
            "overall": {
                "r_merge": 0.033,
                "n_tot_unique_obs": 6240,
                "completeness": 84.1,
                "cc_half": 0.999,
                "mean_i_sig_i": 16.6,
                "r_meas_all_iplusi_minus": 0.046,
                "cc_anom": 0.127,
                "res_lim_high": 1.79,
                "anom_multiplicity": 1.1,
                "multiplicity": 2.6,
                "anom_completeness": 58.9,
                "n_tot_obs": 16315,
                "res_lim_low": 27.56,
            },
            "innerShell": {
                "r_merge": 0.027,
                "n_tot_unique_obs": 70,
                "completeness": 77.3,
                "cc_half": 0.997,
                "mean_i_sig_i": 37.5,
                "r_meas_all_iplusi_minus": 0.038,
                "cc_anom": 0.173,
                "res_lim_high": 8.02,
                "anom_multiplicity": 2.0,
                "multiplicity": 2.7,
                "anom_completeness": 53.6,
                "n_tot_obs": 189,
                "res_lim_low": 27.56,
            },
            "outerShell": {
                "r_merge": 0.246,
                "n_tot_unique_obs": 458,
                "completeness": 82.8,
                "cc_half": 0.897,
                "mean_i_sig_i": 3.6,
                "r_meas_all_iplusi_minus": 0.329,
                "cc_anom": -0.164,
                "res_lim_high": 1.79,
                "anom_multiplicity": 1.5,
                "multiplicity": 2.6,
                "anom_completeness": 61.2,
                "n_tot_obs": 1205,
                "res_lim_low": 1.84,
            },
        },
    }
    working_directory.join("fast_dp.json").write(json.dumps(results_d))

    # define the recipewrap
    recipewrap = {
        "recipe": {
            "2": {
                "job_parameters": {
                    "create_symlink": "reprocessing-1219941-fastdp",
                    "dcid": "3603387",
                    "fast_dp": {"filename": image_path},
                    "ispyb_parameters": {"d_min": "1.8"},
                    "results_directory": results_directory.strpath,
                    "timeout": None,
                    "working_directory": working_directory.strpath,
                },
                "output": {
                    "failure": 6,
                    "result-individual-file": 7,
                    "starting": 3,
                    "success": 5,
                    "updates": 4,
                },
            },
            "7": {
                "parameters": {
                    "ispyb_command": "add_program_attachment",
                    "program_id": "$ispyb_autoprocprogram_id",
                }
            },
        },
        "recipe-path": [1],
        "recipe-pointer": 2,
    }

    expected_output_files = [
        "fast_dp.log",
        "AUTOINDEX.INP",
        "XDS.INP",
        "XYCORR.LP",
        "X-CORRECTIONS.cbf",
        "Y-CORRECTIONS.cbf",
        "INIT.LP",
        "BLANK.cbf",
        "GAIN.cbf",
        "BKGINIT.cbf",
        "COLSPOT.LP",
        "IDXREF.LP",
        "autoindex.log",
        "INTEGRATE.INP",
        "DEFPIX.LP",
        "ABS.cbf",
        "BKGPIX.cbf",
        "INTEGRATE.LP",
        "INTEGRATE.HKL",
        "FRAME.cbf",
        "P1.INP",
        "CORRECT.LP",
        "DX-CORRECTIONS.cbf",
        "GX-CORRECTIONS.cbf",
        "DY-CORRECTIONS.cbf",
        "GY-CORRECTIONS.cbf",
        "DECAY.cbf",
        "MODPIX.cbf",
        "ABSORP.cbf",
        "XDS_ASCII.HKL",
        "P1.LP",
        "pointless.xml",
        "pointless.log",
        "XDS_P1.HKL",
        "CORRECT.INP",
        "xdsstat.log",
        "xds_sorted.mtz",
        "aimless.xml",
        "fast_dp.mtz",
        "fast_dp_unmerged.mtz",
        "aimless.log",
        "fast_dp.xml",
        "fast_dp.state",
        "xtriage.log",
        "fast_dp-report.html",
        "iotbx-merging-stats.json",
    ]

    # expected calls to procrunner
    expected_procrunner_calls = [
        mock.call(
            [
                "fast_dp",
                "--atom=S",
                "-j",
                "0",
                "-J",
                "18",
                "-l",
                "durin-plugin.so",
                image_path,
                "--resolution-high=1.8",
            ],
            environment_override={},
            raise_timeout_exception=True,
            timeout=None,
            working_directory=working_directory.strpath,
        ),
        mock.call(
            [
                "xia2.report",
                "log_include=%s/fast_dp.log" % working_directory.strpath,
                "prefix=fast_dp",
                "title=fast_dp",
                "fast_dp_unmerged.mtz",
            ],
            timeout=None,
            raise_timeout_exception=True,
            working_directory=working_directory.strpath,
        ),
    ]

    # non-exhaustive list of output files
    expected_individual_files = {
        "log": ["fast_dp.log", "fast_dp-report.html"],
        "result": ["INTEGRATE.HKL", "fast_dp.mtz", "fast_dp_unmerged.mtz"],
        "graph": ["iotbx-merging-stats.json"],
    }

    wrapper = make_wrapper(
        FastDPWrapper,
        recipewrap,
        expected_output_files=expected_output_files,
        expected_output_directories=None,
        expected_individual_files=expected_individual_files,
        expected_procrunner_calls=expected_procrunner_calls,
    )
    mock_get_merging_statistics = mocker.patch.object(
        dlstbx.wrapper.fast_dp, "get_merging_statistics"
    )
    mock_get_merging_statistics.return_value.as_json.return_value = ""
    wrapper.run()
    wrapper.verify()
    # test call to get_merging_statistics
    mock_get_merging_statistics.assert_called_once()
