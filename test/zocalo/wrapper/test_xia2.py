import json
import mock
import py.path
from dlstbx.wrapper.xia2 import Xia2Wrapper


def test_Xia2Wrapper(make_wrapper, tmpdir):

    image_path = "/dls/i04/data/2019/nt18231-18/tmp/2019-05-10/09-36-13-9495b73b/Therm_6_2_master.h5:1:488"
    working_directory = tmpdir.join("work_dir")
    for subdir in ("DataFiles", "LogFiles"):
        working_directory.join(subdir).ensure(dir=True)
    working_directory.ensure(dir=True)
    results_directory = tmpdir.join("results_dir")
    results_directory.ensure(dir=True)
    py.path.local(
        "/dls/i04/data/2019/nt18231-18/processed/tmp/2019-05-10/09-36-13-9495b73b/Therm_6_2_/xia2-dials/xia2.json"
    ).copy(results_directory.join("xia2.json"))
    results_d = {
        u"refined_beam": [206.45053836697736, 211.11095943078706],
        u"commandline": u"/path/to/cctbx/modules/fast_dp/fast_dp/fast_dp.py --atom=S -j 0 -J 18 -l durin-plugin.so %s --resolution-high=1.8"
        % image_path,
        u"spacegroup": u"I 2 3",
        u"unit_cell": [77.9622, 77.9622, 77.9622, 90.0, 90.0, 90.0],
        u"scaling_statistics": {
            u"overall": {
                u"r_merge": 0.033,
                u"n_tot_unique_obs": 6240,
                u"completeness": 84.1,
                u"cc_half": 0.999,
                u"mean_i_sig_i": 16.6,
                u"r_meas_all_iplusi_minus": 0.046,
                u"cc_anom": 0.127,
                u"res_lim_high": 1.79,
                u"anom_multiplicity": 1.1,
                u"multiplicity": 2.6,
                u"anom_completeness": 58.9,
                u"n_tot_obs": 16315,
                u"res_lim_low": 27.56,
            },
            u"innerShell": {
                u"r_merge": 0.027,
                u"n_tot_unique_obs": 70,
                u"completeness": 77.3,
                u"cc_half": 0.997,
                u"mean_i_sig_i": 37.5,
                u"r_meas_all_iplusi_minus": 0.038,
                u"cc_anom": 0.173,
                u"res_lim_high": 8.02,
                u"anom_multiplicity": 2.0,
                u"multiplicity": 2.7,
                u"anom_completeness": 53.6,
                u"n_tot_obs": 189,
                u"res_lim_low": 27.56,
            },
            u"outerShell": {
                u"r_merge": 0.246,
                u"n_tot_unique_obs": 458,
                u"completeness": 82.8,
                u"cc_half": 0.897,
                u"mean_i_sig_i": 3.6,
                u"r_meas_all_iplusi_minus": 0.329,
                u"cc_anom": -0.164,
                u"res_lim_high": 1.79,
                u"anom_multiplicity": 1.5,
                u"multiplicity": 2.6,
                u"anom_completeness": 61.2,
                u"n_tot_obs": 1205,
                u"res_lim_low": 1.84,
            },
        },
    }
    working_directory.join("fast_dp.json").write(json.dumps(results_d))

    # define the recipewrap
    recipewrap = {
        "environment": {
            "ID": "c9c31218-3e76-4d98-bb89-0473496bdd94",
            "ispyb_autoprocprogram_id": 76743099,
            "ispyb_integration_id": 5725755,
        },
        "payload": {"result": 5725755},
        "recipe": {
            "2": {
                "job_parameters": {
                    "create_symlink": "xia2-dials",
                    "dcid": "3675108",
                    "ispyb_parameters": {
                        "resolution.cc_half_significance_level": "0.1"
                    },
                    "results_directory": results_directory.strpath,
                    "synchweb_ticks": "/dls/i04/data/2019/nt18231-18/processed/tmp/2019-05-10/09-36-13-9495b73b/Therm_6_2_/c9c31218-3e76-4d98-bb89-0473496bdd94/../xia2/dials-run/xia2.txt",
                    "synchweb_ticks_magic": "I/sigma",
                    "timeout": None,
                    "working_directory": working_directory.strpath,
                    "xia2": {
                        "atom": "S",
                        "crystal": "DEFAULT",
                        "images": image_path,
                        "min_images": 3,
                        "pipeline": "dials",
                        "project": "AUTOMATIC",
                        "read_all_image_headers": False,
                    },
                },
                "output": {
                    "failure": 6,
                    "ispyb": 7,
                    "result-individual-file": 8,
                    "starting": 3,
                    "success": 5,
                    "updates": 4,
                },
            },
            "3": {
                "parameters": {
                    "ispyb_command": "update_processing_status",
                    "message": "starting",
                    "program_id": "$ispyb_autoprocprogram_id",
                },
                "queue": "ispyb_connector",
                "service": "DLS ISPyB connector",
            },
            "4": {
                "parameters": {
                    "ispyb_command": "update_processing_status",
                    "message": "processing",
                    "program_id": "$ispyb_autoprocprogram_id",
                },
                "queue": "ispyb_connector",
                "service": "DLS ISPyB connector",
            },
            "5": {
                "parameters": {
                    "ispyb_command": "update_processing_status",
                    "message": "processing successful",
                    "program_id": "$ispyb_autoprocprogram_id",
                    "status": "success",
                },
                "queue": "ispyb_connector",
                "service": "DLS ISPyB connector",
            },
            "6": {
                "parameters": {
                    "ispyb_command": "update_processing_status",
                    "message": "processing failure",
                    "program_id": "$ispyb_autoprocprogram_id",
                    "status": "failure",
                },
                "queue": "ispyb_connector",
                "service": "DLS ISPyB connector",
            },
            "7": {
                "output": [9, 10, 11],
                "parameters": {
                    "dcid": "3675108",
                    "integration_id": "$ispyb_integration_id",
                    "ispyb_command": "multipart_message",
                    "program_id": "$ispyb_autoprocprogram_id",
                },
                "queue": "ispyb_connector",
                "service": "DLS ISPyB connector",
            },
            "8": {
                "parameters": {
                    "ispyb_command": "add_program_attachment",
                    "program_id": "$ispyb_autoprocprogram_id",
                },
                "queue": "ispyb_connector",
                "service": "DLS ISPyB connector",
            },
            "9": {
                "parameters": {
                    "automatic": True,
                    "comment": "DIMPLE triggered by automatic xia2-dials",
                    "dcid": "3675108",
                    "mtz": "/dls/i04/data/2019/nt18231-18/processed/tmp/2019-05-10/09-36-13-9495b73b/Therm_6_2_/c9c31218-3e76-4d98-bb89-0473496bdd94/xia2-dials/DataFiles/AUTOMATIC_DEFAULT_free.mtz",
                    "scaling_id": "$ispyb_autoprocscaling_id",
                    "target": "dimple",
                },
                "queue": "trigger",
                "service": "DLS Trigger",
            },
            "10": {
                "parameters": {
                    "comment": "big_ep triggered by automatic xia2-dials",
                    "dcid": "3675108",
                    "scaling_id": "$ispyb_autoprocscaling_id",
                    "target": "big_ep",
                },
                "queue": "trigger",
                "service": "DLS Trigger",
            },
            "11": {
                "parameters": {
                    "comment": "xia2.multiplex triggered by automatic xia2-dials",
                    "dcid": "3675108",
                    "target": "multiplex",
                },
                "queue": "trigger",
                "service": "DLS Trigger",
            },
            "12": {
                "output": [2],
                "parameters": {
                    "dcid": "3675108",
                    "ispyb_command": "multipart_message",
                    "ispyb_command_list": [
                        {
                            "ispyb_command": "upsert_integration",
                            "program_id": "$ispyb_autoprocprogram_id",
                            "store_result": "ispyb_integration_id",
                        }
                    ],
                },
                "queue": "ispyb_connector",
                "service": "DLS ISPyB connector",
            },
            "start": [[12, []]],
        },
        "recipe-path": [12],
        "recipe-pointer": 2,
    }

    expected_output_files = [
        "automatic.xinfo",
        "xia2-citations.bib",
        "xia2-debug.txt",
        "xia2-diff.phil",
        "xia2-files.txt",
        "xia2.html",
        "xia2-journal.txt",
        "xia2.json",
        "xia2-report.json",
        "xia2-summary.dat",
        "xia2.txt",
        "xia2-working.phil",
    ]

    expected_output_directories = {
        "DataFiles": [
            "AUTOMATIC_DEFAULT_free.mtz",
            "AUTOMATIC_DEFAULT_SAD_SWEEP1_reflections.pickle",
            "AUTOMATIC_DEFAULT_scaled_unmerged.sca",
            "AUTOMATIC_DEFAULT_SAD_SWEEP1_experiments.json",
            "AUTOMATIC_DEFAULT_scaled.sca",
            "xia2.cif",
            "AUTOMATIC_DEFAULT_SAD_SWEEP1_INTEGRATE.mtz",
            "AUTOMATIC_DEFAULT_scaled_unmerged.mtz",
            "xia2.mmcif",
        ],
        "LogFiles": [
            "absorption_surface.png",
            "AUTOMATIC_DEFAULT_SAD_merging-statistics.json",
            "AUTOMATIC_DEFAULT_SAD_SWEEP1_INTEGRATE.html",
            "AUTOMATIC_DEFAULT_SAD_SWEEP1_REFINE.log",
            "AUTOMATIC_DEFAULT_aimless.log",
            "AUTOMATIC_DEFAULT_SAD_merging-statistics.txt",
            "AUTOMATIC_DEFAULT_SAD_SWEEP1_INTEGRATE.log",
            "AUTOMATIC_DEFAULT_SAD_truncate.log",
            "AUTOMATIC_DEFAULT_aimless.xml",
            "AUTOMATIC_DEFAULT_SAD_SWEEP1_INDEX.html",
            "AUTOMATIC_DEFAULT_SAD_SWEEP1_LATTICE.log",
            "AUTOMATIC_DEFAULT_SAD_truncate.xml",
            "AUTOMATIC_DEFAULT_pointless.log",
            "AUTOMATIC_DEFAULT_SAD_SWEEP1_INDEX.log",
            "AUTOMATIC_DEFAULT_SAD_SWEEP1_REFINE.html",
            "Xtriage.log",
        ],
    }

    # expected calls to procrunner
    expected_procrunner_calls = [
        mock.call(
            [
                "xia2",
                "pipeline=dials",
                "min_images=3",
                "project=AUTOMATIC",
                "crystal=DEFAULT",
                "read_all_image_headers=False",
                "atom=S",
                "image=%s" % image_path,
                "resolution.cc_half_significance_level=0.1",
            ],
            timeout=None,
            working_directory=working_directory.strpath,
        )
    ]

    # non-exhaustive list of output files
    expected_individual_files = {
        "log": [
            "xia2.html",
            "xia2.txt",
            "LogFiles/AUTOMATIC_DEFAULT_SAD_SWEEP1_INTEGRATE.log",
            "LogFiles/AUTOMATIC_DEFAULT_SAD_SWEEP1_INTEGRATE.html",
        ],
        "result": [
            "DataFiles/AUTOMATIC_DEFAULT_free.mtz",
            "DataFiles/AUTOMATIC_DEFAULT_scaled_unmerged.mtz",
        ],
        "graph": ["LogFiles/AUTOMATIC_DEFAULT_SAD_merging-statistics.json"],
    }

    wrapper = make_wrapper(
        Xia2Wrapper,
        recipewrap,
        expected_output_files=expected_output_files,
        expected_output_directories=expected_output_directories,
        expected_individual_files=expected_individual_files,
        expected_procrunner_calls=expected_procrunner_calls,
    )
    wrapper.run()
    wrapper.verify()

    return
