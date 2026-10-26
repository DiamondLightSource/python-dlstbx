from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest
from workflows.recipe.wrapper import RecipeWrapper

from dlstbx.services.strategy import DLSStrategy

HEADER = {"message-id": 1, "subscription": 1}


@pytest.fixture
def strategy():
    service = DLSStrategy()
    service._transport = mock.Mock()
    service.initializing()
    return service


def make_rw(parameters, transport, environment=None):
    return RecipeWrapper(
        message={
            "recipe": {
                "1": {"service": "strategy", "queue": "x", "parameters": parameters},
                "start": [(1, [])],
            },
            "recipe-pointer": 1,
            "environment": {"ID": "abc", **(environment or {})},
            "payload": None,
        },
        transport=transport,
    )


def test_strategy_parameter_precedence_and_substitution(strategy, mocker):
    rw = make_rw(
        {
            "beamline": "step",
            "wavelength": 1,
            "resolution": 2,
            "ispyb_parameters": {"beamline": "$bl", "wavelength": ["0.9", "1.1"]},
        },
        strategy._transport,
        environment={"bl": "i04"},
    )
    # Fails reading the beamline configuration, after the parameters are parsed
    mocker.patch.object(Path, "is_file", return_value=False)
    with pytest.raises(FileNotFoundError, match="/dls_sw/i03/"):
        strategy.generate_strategy(
            rw, HEADER, {"parameters": {"beamline": ["i03", "i04"]}}
        )
    with pytest.raises(FileNotFoundError, match="/dls_sw/i04/"):
        strategy.generate_strategy(rw, HEADER, {})


@pytest.mark.parametrize(
    "parameters",
    [
        {"wavelength": 1, "resolution": 2},
        {"beamline": [], "wavelength": 1, "resolution": 2},
        {"beamline": "i04", "wavelength": "x", "resolution": 2},
        {"beamline": "i04", "wavelength": 1},
        {"beamline": "i04", "wavelength": 1, "resolution": 2, "transmission": "x"},
    ],
)
def test_strategy_rejects_invalid_parameters(strategy, parameters):
    rw = make_rw(parameters, strategy._transport)
    strategy.generate_strategy(rw, HEADER, {})
    strategy._transport.nack.assert_called_once_with(HEADER, requeue=False)
    # Rejected before the message is acknowledged in a transaction
    strategy._transport.transaction_begin.assert_not_called()
    strategy._transport.ack.assert_not_called()
