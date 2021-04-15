import dxtbx.model
import dlstbx.nexus.nxmx


def test_get_dxtbx_goniometer(nxmx_example):
    sample = dlstbx.nexus.nxmx.NXmx(nxmx_example).entries[0].samples[0]
    gonio = dlstbx.nexus.get_dxtbx_goniometer(sample)
    assert isinstance(gonio, dxtbx.model.MultiAxisGoniometer)
    assert gonio.get_rotation_axis() == (1.0, 0.0, 0.0)
    assert list(gonio.get_angles()) == [0.0, 0.0, 0.0]
    assert list(gonio.get_axes()) == [(1.0, 0.0, 0.0), (0.0, 0.0, 1.0), (1.0, 0.0, 0.0)]
    assert list(gonio.get_names()) == ["phi", "chi", "omega"]
    assert gonio.get_scan_axis() == 2
