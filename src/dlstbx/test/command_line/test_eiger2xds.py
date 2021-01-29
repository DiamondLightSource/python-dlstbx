from dlstbx.cli import eiger2xds


def test_vmxi_thaumatin(dials_data, tmpdir):
    master_h5 = dials_data("vmxi_thaumatin") / "image_15799_master.h5"
    with tmpdir.as_cwd():
        eiger2xds.run([master_h5.strpath])
    xds_inp = tmpdir / "XDS.INP"
    assert xds_inp.check(file=1)
