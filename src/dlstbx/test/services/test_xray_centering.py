import json
from unittest import mock

import workflows.transport.common_transport
from workflows.recipe.wrapper import RecipeWrapper

import dlstbx.services.xray_centering


def generate_recipe_message(parameters, gridinfo):
    """Helper function for tests."""
    message = {
        "recipe": {
            "1": {
                "service": "DLS X-Ray Centering",
                "queue": "reduce.xray_centering",
                "parameters": parameters,
                "gridinfo": gridinfo,
            },
            "start": [(1, [])],
        },
        "recipe-pointer": 1,
        "recipe-path": [],
        "environment": {
            "ID": mock.sentinel.GUID,
            "source": mock.sentinel.source,
            "timestamp": mock.sentinel.timestamp,
        },
        "payload": mock.sentinel.payload,
    }
    return message


def test_xray_centering(mocker, tmp_path):
    # https://ispyb.diamond.ac.uk/dc/visit/cm28170-2/id/6153461
    parameters = {
        "dcid": "6153461",
        "output": tmp_path / "Dials5AResults.json",
        "log": tmp_path / "Dials5AResults.txt",
    }
    gridinfo = {
        "orientation": "horizontal",
        "snapshot_offsetYPixel": 57.0822,
        "gridInfoId": 1337162,
        "dx_mm": 0.04,
        "steps_y": 5.0,
        "pixelsPerMicronX": 0.438,
        "steps_x": 7.0,
        "pixelsPerMicronY": 0.438,
        "snaked": 1,
        "snapshot_offsetXPixel": 79.9863,
        "dy_mm": 0.04,
    }
    m = generate_recipe_message(parameters, gridinfo)

    mock_transport = mock.Mock()
    xc = dlstbx.services.xray_centering.DLSXRayCentering()
    setattr(xc, "_transport", mock_transport)
    xc.initializing()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=m, transport=t)
    # Spy on the rw.send_to method
    send_to = mocker.spy(rw, "send_to")
    # fmt: off
    spot_counts = [1, 0, 0, 0, 0, 1, 0, 239, 29, 3, 4, 0, 1, 0, 5, 0, 190, 249, 230, 206, 190, 202, 190, 184, 208, 107, 1, 0, 0, 0, 0, 236, 183, 193, 230]
    # fmt: on
    for i, n_spots in enumerate(spot_counts):
        message = {"n_spots_total": n_spots, "file-number": i + 1}
        xc.add_pia_result(rw, {"some": "header"}, message)
    expected_results = {
        "best_image": 18,
        "best_region": mock.ANY,
        "box_size_px": mock.ANY,
        "centre_x": 530.0841473581213,
        "centre_x_box": 4.928571428571429,
        "centre_y": 357.1474315720809,
        "centre_y_box": 3.2857142857142856,
        "message": "ok",
        "reflections_in_best_image": 249,
        "snapshot_offset": mock.ANY,
        "status": "ok",
        "steps": (7, 5),
    }
    results_json = tmp_path / "Dials5AResults.json"
    assert results_json.exists()
    results = json.loads(results_json.read_bytes())
    assert expected_results == results
    send_to.assert_called_with("success", expected_results, transaction=mock.ANY)
