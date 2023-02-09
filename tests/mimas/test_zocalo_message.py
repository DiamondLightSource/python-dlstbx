from __future__ import annotations

import dlstbx.mimas


def test_transformation_of_recipe_invocation():
    valid_invocation = dlstbx.mimas.MimasRecipeInvocation(DCID=1, recipe="string")
    zocdata = dlstbx.mimas.zocalo_message(valid_invocation)
    assert isinstance(zocdata, dict)


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
    zocdata = dlstbx.mimas.zocalo_message(valid_invocation)
    assert isinstance(zocdata, dict)


def test_validation_of_ispyb_parameters():
    valid = dlstbx.mimas.MimasISPyBParameter(key="key", value="value")
    zocdata = dlstbx.mimas.zocalo_message(valid)
    assert isinstance(zocdata, dict)

    zoclist = dlstbx.mimas.zocalo_message([valid, valid])
    assert isinstance(zoclist, list)
    assert len(zoclist) == 2
    assert zoclist[0] == zocdata
    assert zoclist[1] == zocdata


def test_validation_of_ispyb_sweeps():
    valid = dlstbx.mimas.MimasISPyBSweep(DCID=1, start=10, end=100)
    zocdata = dlstbx.mimas.zocalo_message(valid)
    assert isinstance(zocdata, dict)

    zoclist = dlstbx.mimas.zocalo_message((valid, valid))
    assert isinstance(zoclist, tuple)
    assert len(zoclist) == 2
    assert zoclist[0] == zocdata
    assert zoclist[1] == zocdata


def test_validation_of_ispyb_unit_cells():
    valid = dlstbx.mimas.MimasISPyBUnitCell(
        a=10, b=11, c=12, alpha=90, beta=91.0, gamma=92
    )
    zocdata = dlstbx.mimas.zocalo_message(valid)
    assert zocdata == (10, 11, 12, 90, 91.0, 92)


def test_validation_of_ispyb_space_groups():
    valid = dlstbx.mimas.MimasISPyBSpaceGroup(symbol="P 41 21 2")
    zocdata = dlstbx.mimas.zocalo_message(valid)
    assert zocdata == "P41212"
