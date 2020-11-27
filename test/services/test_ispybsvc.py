import mock

import workflows.transport.common_transport
from workflows.recipe.wrapper import RecipeWrapper

from dlstbx.services.ispybsvc import DLSISPyB


def test_create_ispyb_job(testdb, mocker):
    message = {
        "recipe": {
            1: {
                "queue": "ispyb_connector",
                "parameters": {
                    "ispyb_command": "create_ispyb_job",
                },
                "output": {
                    "trigger": 2,
                    "held": 3,
                },
            },
            2: {"queue": "processing_recipe"},
            3: {"queue": "mimas.held"},
        },
        "recipe-pointer": 1,
    }

    ispybsvc = DLSISPyB()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    ispybsvc.ispyb = testdb
    send_to = mocker.spy(rw, "send_to")
    for autostart in (True, False):
        message = {
            "DCID": "1066786",
            "sweeps": [],
            "parameters": [
                {"key": "spacegroup", "value": "I23"},
                {"key": "unit_cell", "value": "77.92,77.92,77.92,90.0,90.0,90.0"},
            ],
            "triggervariables": (),
            "autostart": autostart,
        }
        ispybsvc.receive_msg(rw, {"some": "header"}, message)
        if autostart:
            send_to.assert_any_call(
                "trigger", {"parameters": {"ispyb_process": mock.ANY}}
            )
        else:
            send_to.assert_any_call("held", {"parameters": {"ispyb_process": mock.ANY}})
