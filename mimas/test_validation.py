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
    for key, value in {
        "DCID": "banana",
        "recipe": dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="invalid"),
        "recipe": "",
        "recipe": None,
    }.items():
        print(f"testing {key}: {value}")
        invalid_invocation = valid_invocation._replace(**{key: value})
        with pytest.raises(ValueError):
            dlstbx.mimas.validate(invalid_invocation)
