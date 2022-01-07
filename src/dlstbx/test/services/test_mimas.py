from unittest import mock

import workflows.transport
from workflows.recipe.wrapper import RecipeWrapper
from workflows.transport.offline_transport import OfflineTransport

from dlstbx.services.mimas import DLSMimas


def test_mimas(with_dummy_plugins, monkeypatch, mocker):
    monkeypatch.setattr(workflows.transport, "default_transport", "OfflineTransport")
    message = {
        "recipe": {
            "1": {
                "parameters": {
                    "dcid": "123456",
                    "beamline": "i99",
                    "event": "end",
                    "dc_class": "rotation",
                    # "sweep_list"
                    "unit_cell": (10, 11, 12, 90, 90, 90),
                    "space_group": "P222",
                    "diffraction_plan_info": {
                        "anomalousScatterer": "Se",
                    },
                    "detectorclass": "eiger",
                },
            },
        },
        "recipe-pointer": 1,
    }

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }

    t = OfflineTransport()
    rw = RecipeWrapper(message, transport=t)
    spy_send = mocker.spy(rw, "send")
    spy_send_to = mocker.spy(rw, "send_to")

    mimas = DLSMimas()
    mimas.process(rw, header, message)
    assert spy_send.call_count == 2
    assert spy_send_to.call_count == 1
