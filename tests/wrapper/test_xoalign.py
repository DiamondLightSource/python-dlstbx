from __future__ import annotations

import subprocess
from unittest import mock

import pytest
import workflows.recipe.wrapper
import workflows.transport.common_transport

from dlstbx.wrapper.xoalign import XOalignWrapper

# Tail of XOalign.py output for a thaumatin mosflm index matrix on I03
XOALIGN_SMARGON_OUTPUT = """
 Independent Solutions for DLS I03 SmarGon goniometer:

             Chi      Phi      Settings
   1     106.542   65.318   [('a*', 'b*'), ('a*', 'c*')]
   2    -106.618 -112.696   [('a*', 'b*'), ('a*', 'c*')]
   3      73.360 -115.011   [('a*', 'b*'), ('a*', 'c*')]
   4     -73.436   67.633   [('a*', 'b*'), ('a*', 'c*')]
   5    -110.645  -16.279   [('b*', 'a*'), ('b*', 'c*')]
   6     110.569  161.766   [('b*', 'a*'), ('b*', 'c*')]
   7      69.332  -18.649   [('b*', 'a*'), ('b*', 'c*')]
   8     -69.408  164.137   [('b*', 'a*'), ('b*', 'c*')]
   9     153.012  -60.910   [('c*', 'a*'), ('c*', 'b*')]
  10    -153.088  121.813   [('c*', 'a*'), ('c*', 'b*')]
  11      26.864  116.916   [('c*', 'a*'), ('c*', 'b*')]
  12     -26.941  -56.013   [('c*', 'a*'), ('c*', 'b*')]
"""


@pytest.fixture
def run_wrapper(mocker, tmp_path):
    index_mat = tmp_path / "mosflm_index.mat"
    index_mat.touch()
    mock_run = mocker.patch.object(subprocess, "run", autospec=True)
    mock_run.return_value = subprocess.CompletedProcess(
        args=[], returncode=0, stdout=XOALIGN_SMARGON_OUTPUT
    )
    mock_send_to = mocker.patch.object(
        workflows.recipe.wrapper.RecipeWrapper, "send_to"
    )

    def _run_wrapper(beamline):
        message = {
            "recipe": {
                "1": {
                    "job_parameters": {
                        "dcid": "1234",
                        "chi": "0.0",
                        "kappa": "None",
                        "omega": "0.0",
                        "phi": "0.0",
                        "beamline": beamline,
                        "working_directory": str(tmp_path / "work"),
                        "results_directory": str(tmp_path / "results"),
                    },
                    "output": {"ispyb": 2},
                },
                "start": [[1, []]],
            },
            "recipe-pointer": 1,
            "environment": {"ID": "test"},
            "payload": {"index.mat": str(index_mat)},
        }
        recwrap = workflows.recipe.wrapper.RecipeWrapper(
            message=message,
            transport=mock.create_autospec(
                workflows.transport.common_transport.CommonTransport
            ),
        )
        wrapper = XOalignWrapper()
        wrapper.set_recipe_wrapper(recwrap)
        return wrapper.run()

    return _run_wrapper, mock_run, mock_send_to, index_mat


def test_xoalign_mosflm_index_mat(run_wrapper):
    run, mock_run, mock_send_to, index_mat = run_wrapper
    assert run("i03")

    commands = mock_run.call_args.args[0]
    assert commands == ["XOalign.py", "-D 0.0,0.0,0.0", index_mat.as_posix()]
    env = mock_run.call_args.kwargs["env"]
    assert env["XOALIGN_CALIB"] == "/dls_sw/i03/etc/xoalign_config.py"

    mock_send_to.assert_called_once_with("ispyb", {"ispyb_command_list": mock.ANY})
    ispyb_commands = mock_send_to.call_args.args[1]["ispyb_command_list"]
    # Only solution 11 has 0 <= chi <= 45; each solution gets its own Screening
    assert [c["ispyb_command"] for c in ispyb_commands] == [
        "insert_screening",
        "insert_screening_output",
        "insert_screening_strategy",
        "insert_screening_strategy_wedge",
    ]
    assert ispyb_commands[0] == {
        "dcid": "1234",
        "programversion": "XOalign",
        "comments": "[(c*, a*), (c*, b*)]",
        "shortcomments": "XOalign 11",
        "ispyb_command": "insert_screening",
        "store_result": "ispyb_screening_id_11",
    }
    assert ispyb_commands[1]["screening_id"] == "$ispyb_screening_id_11"
    assert ispyb_commands[3]["chi"] == "26.86"
    assert ispyb_commands[3]["phi"] == "116.92"


def test_xoalign_mosflm_index_mat_skips_other_beamlines(run_wrapper):
    run, mock_run, mock_send_to, _ = run_wrapper
    assert run("i24")
    mock_run.assert_not_called()
    mock_send_to.assert_not_called()
