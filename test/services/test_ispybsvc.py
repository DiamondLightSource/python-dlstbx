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


def test_add_program_attachment(testdb, mocker, tmpdir):
    # First create the message to send to the ispybsvc
    autoproc_program_id = 56986673
    xia2_html = tmpdir.join("xia2.html")
    xia2_html.write("content")
    message = {
        "recipe": {
            1: {
                "queue": "ispyb_connector",
                "parameters": {
                    "ispyb_command": "add_program_attachment",
                    "program_id": autoproc_program_id,
                    "file_path": xia2_html.dirname,
                    "file_name": xia2_html.basename,
                    "file_type": "log",
                },
            },
        },
        "recipe-pointer": 1,
    }

    # Setup the service and point this at the testdb
    ispybsvc = DLSISPyB()
    ispybsvc.ispyb = testdb

    # Instantiate a RecipeWrapper object
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)

    # Send message to the ispybsvc and check via ispyb-api that it has been added to the DB
    ispybsvc.receive_msg(rw, {"some": "header"}, message)
    attachments = testdb.mx_processing.retrieve_program_attachments_for_program_id(
        autoproc_program_id
    )
    assert attachments[-1] == {
        "attachmentId": mock.ANY,
        "fileType": "Log",
        "filePath": xia2_html.dirname,
        "fileName": xia2_html.basename,
        "importanceRank": None,
    }

    # Explicitly set importance_rank in the message
    message["recipe"][1]["parameters"]["importance_rank"] = 1
    ispybsvc.receive_msg(rw, {"some": "header"}, message)
    attachments = testdb.mx_processing.retrieve_program_attachments_for_program_id(
        autoproc_program_id
    )
    assert attachments[-1] == {
        "attachmentId": mock.ANY,
        "fileType": "Log",
        "filePath": xia2_html.dirname,
        "fileName": xia2_html.basename,
        "importanceRank": 1,
    }
