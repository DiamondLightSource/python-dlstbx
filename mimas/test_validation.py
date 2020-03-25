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
        event=dlstbx.mimas.MimasEvent.START,
        beamline="i03",
        unitcell="string",
        spacegroup="string",
        default_recipes="undefined",
        isitagridscan="undefined",
        getsweepslistfromsamedcg="undefined",
        runstatus="string",
    )
    dlstbx.mimas.validate(valid_scenario)

    # replacing individual values should fail validation
    for key, value in {
        "DCID": "banana",
        "event": dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="invalid"),
    }.items():
        print(f"testing {key}: {value}")
        invalid_scenario = valid_scenario._replace(**{key: value})
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
        invalid_invocation = valid_invocation._replace(**{key: value})
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
        sweeps=(),
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
    ]:
        print(f"testing {key}: {value}")
        invalid_invocation = valid_invocation._replace(**{key: value})
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
        invalid = valid._replace(**{key: value})
        with pytest.raises(ValueError):
            dlstbx.mimas.validate(invalid)
