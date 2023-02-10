from __future__ import annotations

import procrunner


def test_vmxi_thaumatin(dials_data, tmpdir):
    master_h5 = dials_data("vmxi_thaumatin") / "image_15799_master.h5"
    result = procrunner.run(
        ["eiger2xds", master_h5],
        working_directory=tmpdir,
    )
    result.check_returncode()
    xds_inp = tmpdir / "XDS.INP"
    assert xds_inp.check(file=1)
