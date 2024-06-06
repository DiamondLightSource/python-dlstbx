from __future__ import annotations

import dataclasses
import itertools

import dlstbx.mimas
import pytest


def test_validation_of_unknown_objects():
    for failing_object in (
        5,
        "string",
        b"bytestring",
        None,
        True,
        False,
        [],
        {},
        dict(),
    ):
        with pytest.raises(ValueError):
            dlstbx.mimas.validate(failing_object)


def test_validation_of_scenario():
    valid_scenario = dlstbx.mimas.MimasScenario(
        DCID=1,
        dcclass=dlstbx.mimas.MimasDCClass.ROTATION,
        event=dlstbx.mimas.MimasEvent.START,
        beamline="i03",
        visit="nt28218-3",
        unitcell=dlstbx.mimas.MimasISPyBUnitCell(
            a=10, b=10.0, c=10, alpha=90.0, beta=90, gamma=90
        ),
        spacegroup=dlstbx.mimas.MimasISPyBSpaceGroup("P41212"),
        getsweepslistfromsamedcg=(
            dlstbx.mimas.MimasISPyBSweep(DCID=1, start=1, end=100),
        ),
        preferred_processing=None,
        runstatus="string",
        detectorclass=dlstbx.mimas.MimasDetectorClass.PILATUS,
    )
    dlstbx.mimas.validate(valid_scenario)

    # replacing individual values should fail validation
    for key, value in [
        ("DCID", "banana"),
        ("dcclass", None),
        ("dcclass", 1),
        ("event", dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="invalid")),
        (
            "getsweepslistfromsamedcg",
            dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="invalid"),
        ),
        (
            "getsweepslistfromsamedcg",
            (dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="invalid"),),
        ),
        (
            "getsweepslistfromsamedcg",
            dlstbx.mimas.MimasISPyBSweep(DCID=1, start=1, end=100),
        ),
        ("getsweepslistfromsamedcg", ""),
        ("getsweepslistfromsamedcg", None),
        ("unitcell", False),
        ("unitcell", (10, 10, 10, 90, 90, 90)),
        (
            "unitcell",
            dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="invalid"),
        ),
        ("detectorclass", "ADSC"),
    ]:
        print(f"testing {key}: {value}")
        invalid_scenario = dataclasses.replace(valid_scenario, **{key: value})
        with pytest.raises(ValueError):
            dlstbx.mimas.validate(invalid_scenario)


def test_validation_of_recipe_invocation():
    valid_invocation = dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="string")
    dlstbx.mimas.validate(valid_invocation)

    # replacing individual values should fail validation
    for key, value in [
        ("DCID", "banana"),
        ("recipe", dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="invalid")),
        ("recipe", ""),
        ("recipe", None),
    ]:
        print(f"testing {key}: {value}")
        invalid_invocation = dataclasses.replace(valid_invocation, **{key: value})
        with pytest.raises(ValueError):
            dlstbx.mimas.validate(invalid_invocation)


def test_validation_of_ispyb_invocation():
    valid_invocation = dlstbx.mimas.MimasISPyBJobInvocation(
        DCID=1,
        autostart=True,
        comment="",
        displayname="",
        parameters=(dlstbx.mimas.MimasISPyBParameter(key="test", value="valid"),),
        recipe="string",
        source="automatic",
        sweeps=(dlstbx.mimas.MimasISPyBSweep(DCID=1, start=1, end=100),),
        triggervariables=(),
    )
    dlstbx.mimas.validate(valid_invocation)

    # replacing individual values should fail validation
    for key, value in [
        ("DCID", "banana"),
        ("autostart", "banana"),
        ("parameters", dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="invalid")),
        ("parameters", (dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="invalid"),)),
        ("parameters", dlstbx.mimas.MimasISPyBParameter(key="test", value="invalid")),
        ("parameters", ""),
        ("parameters", None),
        ("recipe", dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="invalid")),
        ("recipe", ""),
        ("recipe", None),
        ("sweeps", dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="invalid")),
        ("sweeps", (dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="invalid"),)),
        ("sweeps", dlstbx.mimas.MimasISPyBSweep(DCID=1, start=1, end=100)),
        ("sweeps", ""),
        ("sweeps", None),
    ]:
        print(f"testing {key}: {value}")
        invalid_invocation = dataclasses.replace(valid_invocation, **{key: value})
        with pytest.raises(ValueError):
            dlstbx.mimas.validate(invalid_invocation)


def test_validation_of_ispyb_parameters():
    valid = dlstbx.mimas.MimasISPyBParameter(key="key", value="value")
    dlstbx.mimas.validate(valid)

    # replacing individual values should fail validation
    for key, value in [
        ("key", ""),
        ("key", 5),
        ("key", None),
        ("key", False),
        ("value", 5),
        ("value", None),
        ("value", False),
    ]:
        print(f"testing {key}: {value}")
        invalid = dataclasses.replace(valid, **{key: value})
        with pytest.raises(ValueError):
            dlstbx.mimas.validate(invalid)


def test_validation_of_ispyb_sweeps():
    valid = dlstbx.mimas.MimasISPyBSweep(DCID=1, start=10, end=100)
    dlstbx.mimas.validate(valid)

    # replacing individual values should fail validation
    for key, value in [
        ("DCID", ""),
        ("DCID", "1"),
        ("DCID", None),
        ("DCID", 0),
        ("start", ""),
        ("start", "5"),
        ("start", False),
        ("start", -3),
        ("end", ""),
        ("end", "5"),
        ("end", False),
        ("end", -3),
        ("end", 5),
    ]:
        print(f"testing {key}: {value}")
        invalid = dataclasses.replace(valid, **{key: value})
        with pytest.raises(ValueError):
            dlstbx.mimas.validate(invalid)


def test_validation_of_ispyb_unit_cells():
    valid = dlstbx.mimas.MimasISPyBUnitCell(
        a=10, b=11, c=12, alpha=90, beta=91.0, gamma=92
    )
    dlstbx.mimas.validate(valid)
    assert valid.string == "10,11,12,90,91.0,92"

    # replacing individual values should fail validation
    for key, value in itertools.chain(
        itertools.product(
            ("a", "b", "c", "alpha", "beta", "gamma"), (-10, 0, "", False)
        ),
        [("alpha", 180), ("beta", 180), ("gamma", 180)],
    ):
        print(f"testing {key}: {value}")
        invalid = dataclasses.replace(valid, **{key: value})
        with pytest.raises(ValueError):
            dlstbx.mimas.validate(invalid)


def test_validation_of_ispyb_space_groups():
    valid = dlstbx.mimas.MimasISPyBSpaceGroup(symbol="P 41 21 2")
    dlstbx.mimas.validate(valid)
    assert valid.string == "P41212"

    invalid = dlstbx.mimas.MimasISPyBSpaceGroup(symbol="P 5")
    with pytest.raises(ValueError):
        dlstbx.mimas.validate(invalid)


def test_validation_of_ispyb_anomalous_scatterer():
    valid = dlstbx.mimas.MimasISPyBAnomalousScatterer(symbol="S")
    dlstbx.mimas.validate(valid)
    assert valid.string == "S"

    valid = dlstbx.mimas.MimasISPyBAnomalousScatterer(symbol="se")
    dlstbx.mimas.validate(valid)
    assert valid.string == "Se"

    invalid = dlstbx.mimas.MimasISPyBAnomalousScatterer(symbol="X")
    with pytest.raises(ValueError):
        dlstbx.mimas.validate(invalid)

    invalid = dlstbx.mimas.MimasISPyBAnomalousScatterer(symbol="nope")
    with pytest.raises(ValueError):
        dlstbx.mimas.validate(invalid)
