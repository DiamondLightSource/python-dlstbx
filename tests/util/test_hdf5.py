from __future__ import annotations

import os

import pytest

import dlstbx.util.hdf5


@pytest.mark.skipif(
    not os.access(
        "/dls/i04-1/data/2022/cm31107-1/auto/TestLysozyme/TEST1/TEST1_1_master.h5",
        os.R_OK,
    ),
    reason="Test images not available",
)
def test_validate_pixel_mask_shape():
    dlstbx.util.hdf5.validate_pixel_mask(
        "/dls/i04-1/data/2022/cm31107-1/auto/TestLysozyme/TEST1/TEST1_1_master.h5"
    )


@pytest.mark.skipif(
    not os.access(
        "/dls/i04-1/data/2022/cm31107-1/stop_all_920/TEST1_2_master.h5", os.R_OK
    ),
    reason="Test images not available",
)
def test_validate_pixel_mask_shape_0_0():
    # This has pixel_mask shape (0, 0)
    with pytest.raises(dlstbx.util.hdf5.ValidationError):
        dlstbx.util.hdf5.validate_pixel_mask(
            "/dls/i04-1/data/2022/cm31107-1/stop_all_920/TEST1_2_master.h5"
        )


@pytest.mark.skipif(
    not os.access(
        "/dls/i04/data/2022/cm31106-1/xraycentring/TestThaumatin/Se_thau_6/Se_thau_6_8_master.h5",
        os.R_OK,
    ),
    reason="Test images not available",
)
def test_validate_pixel_mask_shape_ROI():
    # This has pixel_mask shape for the full image when the data were in ROI mode
    with pytest.raises(dlstbx.util.hdf5.ValidationError):
        dlstbx.util.hdf5.validate_pixel_mask(
            "/dls/i04/data/2022/cm31106-1/xraycentring/TestThaumatin/Se_thau_6/Se_thau_6_8_master.h5"
        )


@pytest.mark.skipif(
    not os.access(
        "/dls/mx/data/cm31104/cm31104-1/020222/RT/ToNV_WT/x_27_master.h5", os.R_OK
    ),
    reason="Test images not available",
)
def test_validate_pixel_mask_shape_int64():
    # This has the pixel mask stored as int64
    with pytest.raises(dlstbx.util.hdf5.ValidationError):
        dlstbx.util.hdf5.validate_pixel_mask(
            "/dls/mx/data/cm31104/cm31104-1/020222/RT/ToNV_WT/x_27_master.h5"
        )
