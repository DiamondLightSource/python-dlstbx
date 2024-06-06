from __future__ import annotations

from unittest import mock

import workflows.transport.common_transport
from dlstbx.services.per_image_analysis import DLSPerImageAnalysis
from workflows.recipe.wrapper import RecipeWrapper


def generate_recipe_message(parameters, output=None):
    """Helper function for tests."""
    message = {
        "recipe": {
            1: {
                "queue": "per_image_analysis",
                "parameters": parameters,
                "output": output,
            },
            2: {"queue": "transient.output"},
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


def test_per_image_analysis_cbf(dials_data, mocker):
    image = dials_data("x4wide") / "X4_wide_M1S4_2_0001.cbf"
    mock_transport = mock.Mock()
    pia = DLSPerImageAnalysis()
    setattr(pia, "_transport", mock_transport)
    pia.initializing()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    m = generate_recipe_message(
        parameters={
            "d_min": 4,
        },
        output={"any": 1, "select-2": 2},
    )
    payload = {
        "file": image.strpath,
        "file-number": 1,
        "file-pattern-index": 7,
    }
    rw = RecipeWrapper(message=m, transport=t)
    # Spy on the rw.send_to method
    send_to = mocker.spy(rw, "send_to")
    pia.per_image_analysis(rw, {"subscription": mock.sentinel}, payload)
    send_to.assert_called_with(
        "result",
        {
            **{
                k: mock.ANY
                for k in (
                    "d_min_distl_method_1",
                    "d_min_distl_method_2",
                    "estimated_d_min",
                    "n_spots_4A",
                    "n_spots_no_ice",
                    "n_spots_total",
                    "noisiness_method_1",
                    "noisiness_method_2",
                    "total_intensity",
                )
            },
            "file": image.strpath,
            "file-number": 1,
            "file-pattern-index": 7,
        },
        transaction=mock.ANY,
    )


def test_per_image_analysis_h5(dials_data, mocker):
    image = dials_data("vmxi_thaumatin") / "image_15799_master.h5"
    mock_transport = mock.Mock()
    pia = DLSPerImageAnalysis()
    setattr(pia, "_transport", mock_transport)
    pia.initializing()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    m = generate_recipe_message(
        parameters={
            "d_min": 4,
            "scan_range": "3,3",
        },
        output={"any": 1, "select-2": 2},
    )
    payload = {
        "file": image.strpath,
        "file-number": 3,
    }
    rw = RecipeWrapper(message=m, transport=t)
    # Spy on the rw.send_to method
    send_to = mocker.spy(rw, "send_to")
    pia.per_image_analysis(rw, {"subscription": mock.sentinel}, payload)
    send_to.assert_called_with(
        "result",
        {
            **{
                k: mock.ANY
                for k in (
                    "d_min_distl_method_1",
                    "d_min_distl_method_2",
                    "estimated_d_min",
                    "n_spots_4A",
                    "n_spots_no_ice",
                    "n_spots_total",
                    "noisiness_method_1",
                    "noisiness_method_2",
                    "total_intensity",
                )
            },
            "file": image.strpath,
            "file-number": 3,
        },
        transaction=mock.ANY,
    )
