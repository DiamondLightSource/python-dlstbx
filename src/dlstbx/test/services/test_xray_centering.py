from __future__ import annotations

import json
import logging
import time
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
        "experiment_type": "SAD",
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
        message = {
            "n_spots_total": n_spots,
            "file-number": i + 1,
            "file-seen-at": time.time(),
        }
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
        "steps": mock.ANY,
    }
    results_json = tmp_path / "Dials5AResults.json"
    assert results_json.exists()
    results = json.loads(results_json.read_bytes())
    assert expected_results == results
    send_to.assert_called_with("success", expected_results, transaction=mock.ANY)


def test_xray_centering_invalid_parameters(mocker, tmp_path, caplog):
    # https://ispyb.diamond.ac.uk/dc/visit/cm28170-2/id/6153461
    parameters = {
        "dcid": "6153461",
        "experiment_type": "SAD",
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

    mock_transport = mock.Mock()
    xc = dlstbx.services.xray_centering.DLSXRayCentering()
    setattr(xc, "_transport", mock_transport)
    xc.initializing()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)

    m = generate_recipe_message({**parameters, "dcid": "foo"}, gridinfo)
    rw = RecipeWrapper(message=m, transport=t)
    message = {"n_spots_total": 10, "file-number": 1, "file-seen-at": time.time()}
    with caplog.at_level(logging.ERROR):
        xc.add_pia_result(rw, {"some": "header"}, message)
    assert (
        """\
X-ray centering service called with invalid parameters: 1 validation error for RecipeStep
parameters -> dcid
  value is not a valid integer (type=type_error.integer)
"""
        in caplog.text
    )
    caplog.clear()

    m = generate_recipe_message(
        parameters, {k: v for k, v in gridinfo.items() if k != "steps_x"}
    )
    rw = RecipeWrapper(message=m, transport=t)
    message = {"n_spots_total": 10, "file-number": 1, "file-seen-at": time.time()}
    with caplog.at_level(logging.ERROR):
        xc.add_pia_result(rw, {"some": "header"}, message)
    assert (
        """\
X-ray centering service called with invalid parameters: 1 validation error for RecipeStep
gridinfo -> steps_x
  field required (type=value_error.missing)
"""
        in caplog.text
    )
    assert "steps_x\n  field required (type=value_error.missing)" in caplog.text
    caplog.clear()

    m = generate_recipe_message(parameters, gridinfo)
    rw = RecipeWrapper(message=m, transport=t)
    message = {"n_spots_total": -1, "file_number": 1, "file-seen-at": time.time()}
    with caplog.at_level(logging.ERROR):
        xc.add_pia_result(rw, {"some": "header"}, message)
    assert (
        """\
X-ray centering service called with invalid payload: 2 validation errors for Message
file-number
  field required (type=value_error.missing)
n_spots_total
  ensure this value is greater than or equal to 0 (type=value_error.number.not_ge; limit_value=0)
"""
        in caplog.text
    )


def test_xray_centering_3d(mocker, tmp_path, caplog):
    # https://ispyb.diamond.ac.uk/dc/visit/cm26458-4/id/5476360
    # https://ispyb.diamond.ac.uk/dc/visit/cm26458-4/id/5476366

    dcids = (5476360, 5476366)
    parameters = {
        "dcid": f"{dcids[0]}",
        "dcg_dcids": [],
        "experiment_type": "Mesh3D",
    }

    gridinfo = {
        "dx_mm": 0.02,
        "dy_mm": 0.02,
        "gridInfoId": 1061461,
        "orientation": "horizontal",
        "pixelsPerMicronX": 0.438,
        "pixelsPerMicronY": 0.438,
        "snaked": 1,
        "snapshot_offsetXPixel": 363.352,
        "snapshot_offsetYPixel": 274.936,
        "steps_x": 14.0,
        "steps_y": 9.0,
    }

    mock_transport = mock.Mock()
    xc = dlstbx.services.xray_centering.DLSXRayCentering()
    setattr(xc, "_transport", mock_transport)
    xc.initializing()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    # fmt: off
    spots_count_m45 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 10, 7, 0, 0, 0, 0, 0, 0, 6, 20, 29, 29, 27, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 9, 16, 16, 12, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    spot_counts_p45 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 15, 16, 11, 6, 0, 0, 0, 0, 3, 10, 11, 15, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    # fmt: on
    m = generate_recipe_message(parameters, gridinfo)
    rw = RecipeWrapper(message=m, transport=t)
    send_to = mocker.spy(rw, "send_to")
    for i, n_spots in enumerate(spots_count_m45):
        message = {
            "n_spots_total": n_spots,
            "file-number": i + 1,
            "file-seen-at": time.time(),
        }
        xc.add_pia_result(rw, {"some": "header"}, message)
    send_to.assert_not_called()

    m = generate_recipe_message(
        {**parameters, "dcg_dcids": [dcids[0]], "dcid": dcids[1]}, gridinfo
    )
    rw = RecipeWrapper(message=m, transport=t)
    send_to = mocker.spy(rw, "send_to")
    with caplog.at_level(logging.DEBUG):
        for i, n_spots in enumerate(spot_counts_p45):
            message = {
                "n_spots_total": n_spots,
                "file-number": i + 1,
                "file-seen-at": time.time(),
            }
            xc.add_pia_result(rw, {"some": "header"}, message)
    assert "Max pixel: (4, 5, 4)" in caplog.text
    assert "Centre of mass:" in caplog.text
    send_to.assert_called_with("success", (4, 5, 4), transaction=mock.ANY)
