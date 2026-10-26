from __future__ import annotations

from unittest import mock

import pytest
from workflows.recipe.wrapper import RecipeWrapper

from dlstbx.services.ssx_plotter import SSXPlotter

HEADER = {"message-id": 1, "subscription": 1}


@pytest.fixture
def plotter():
    service = SSXPlotter()
    service._transport = mock.Mock()
    service.initializing()
    return service


def make_rw(parameters, transport, environment=None):
    return RecipeWrapper(
        message={
            "recipe": {
                "1": {"service": "ssx", "queue": "ssx.plot", "parameters": parameters},
                "start": [(1, [])],
            },
            "recipe-pointer": 1,
            "environment": {"ID": "abc", **(environment or {})},
            "payload": None,
        },
        transport=transport,
    )


def step_parameters(**kwargs):
    return {
        "command": "pia",
        "dcid": 1,
        "results_file": "$workdir/results.json",
        "plot_file": "$workdir/plot.png",
        "files-expected": 10,
        **kwargs,
    }


def test_waiting_for_results_checkpoints_merged_parameters(plotter, tmp_path, mocker):
    rw = make_rw(
        step_parameters(), plotter._transport, environment={"workdir": str(tmp_path)}
    )
    checkpoint = mocker.patch.object(rw, "checkpoint")
    # Message values take precedence over the recipe step parameters
    plotter.receive_msg(rw, HEADER, {"dcid": 5, "every": 2})

    checkpoint.assert_called_once()
    checkpointed = checkpoint.call_args.args[0]
    assert checkpointed["dcid"] == 5
    assert checkpointed["every"] == 2
    assert checkpointed["results_file"] == str(tmp_path / "results.json")
    assert checkpointed["files-expected"] == 10
    assert checkpointed["status"]["start_time"] > 0
    plotter._transport.nack.assert_not_called()


@pytest.mark.parametrize(
    "change",
    [
        {"files-expected": None},
        {"dcid": -1},
        {"command": None},
    ],
)
def test_invalid_parameters_are_rejected(plotter, tmp_path, change):
    parameters = {
        k: v for k, v in {**step_parameters(), **change}.items() if v is not None
    }
    rw = make_rw(parameters, plotter._transport, environment={"workdir": str(tmp_path)})
    plotter.receive_msg(rw, HEADER, {})

    plotter._transport.nack.assert_called_once_with(HEADER, requeue=False)
    # Rejected before the message is acknowledged in a transaction
    plotter._transport.transaction_begin.assert_not_called()


def test_non_dict_message_is_rejected(plotter):
    rw = make_rw(step_parameters(), plotter._transport)
    plotter.receive_msg(rw, HEADER, ["not", "a", "dict"])
    plotter._transport.nack.assert_called_once_with(HEADER, requeue=False)
