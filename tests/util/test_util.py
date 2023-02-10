from __future__ import annotations

from dlstbx.util import ChainMapWithReplacement


def test_chain_map():

    substitutions = {
        "foo": "bar",
    }

    params = {"abc": 123, "def": 456, "bar": "$foo"}

    other_params = {
        "abc": 321,
        "ghi": 789,
    }

    parameter_map = ChainMapWithReplacement(
        params,
        other_params,
    )
    assert parameter_map["abc"] == 123
    assert parameter_map["def"] == 456
    assert parameter_map["ghi"] == 789
    assert parameter_map["bar"] == "$foo"
    assert "foo" not in parameter_map

    parameter_map = ChainMapWithReplacement(
        other_params,
        params,
        substitutions=substitutions,
    )
    assert parameter_map["bar"] == "bar"
    assert parameter_map["abc"] == 321
