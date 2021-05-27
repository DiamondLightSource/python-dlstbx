import pytest

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


def test_get_dxtbx_beam(nxmx_example):
    instrument = dlstbx.nexus.nxmx.NXmx(nxmx_example).entries[0].instruments[0]
    beam = dlstbx.nexus.get_dxtbx_beam(instrument.beams[0])
    assert isinstance(beam, dxtbx.model.Beam)
    assert beam.get_wavelength() == 0.976223
    assert beam.get_sample_to_source_direction() == (0.0, 0.0, 1.0)


def test_get_dxtbx_scan(nxmx_example):
    sample = dlstbx.nexus.nxmx.NXmx(nxmx_example).entries[0].samples[0]
    instrument = dlstbx.nexus.nxmx.NXmx(nxmx_example).entries[0].instruments[0]
    scan = dlstbx.nexus.get_dxtbx_scan(sample, instrument.detectors[0])
    assert scan.get_num_images() == 10
    assert scan.get_image_range() == (1, 10)
    assert scan.get_oscillation() == (0.0, 0.1)
    assert scan.get_oscillation_range() == (0.0, 1.0)
    assert list(scan.get_exposure_times()) == [0.1] * 10


def test_get_dxtbx_detector(nxmx_example):
    instrument = dlstbx.nexus.nxmx.NXmx(nxmx_example).entries[0].instruments[0]
    detector = dlstbx.nexus.get_dxtbx_detector(
        instrument.detectors[0], instrument.beams[0]
    )

    assert isinstance(detector, dxtbx.model.Detector)
    assert len(detector) == 1
    panel = detector[0]
    assert panel.get_distance() == 289.3
    assert panel.get_origin() == (-155.985, 166.904, -289.3)
    assert panel.get_material() == "Si"
    assert panel.get_pixel_size() == (0.075, 0.075)
    assert panel.get_slow_axis() == (0.0, -1.0, 0.0)
    assert panel.get_fast_axis() == (1.0, 0.0, 0.0)
    assert panel.get_image_size() == (4148, 4362)
    assert panel.get_image_size_mm() == pytest.approx((311.09999999999997, 327.15))
    assert panel.get_name() == "/entry/instrument/detector"
    assert panel.get_normal() == (0.0, 0.0, -1.0)
    assert panel.get_trusted_range() == (-1, 9266)
    px_mm = panel.get_px_mm_strategy()
    assert px_mm.t0() == panel.get_thickness() == 0.45
    assert px_mm.mu() == panel.get_mu() == pytest.approx(3.9217189904637366)
