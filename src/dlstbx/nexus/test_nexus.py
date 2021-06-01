import h5py
import numpy as np
import pytest

import dxtbx.model
from scitbx.array_family import flex

import dlstbx.nexus.nxmx


def test_get_dxtbx_goniometer(nxmx_example):
    sample = dlstbx.nexus.nxmx.NXmx(nxmx_example).entries[0].samples[0]
    gonio = dlstbx.nexus.get_dxtbx_goniometer(sample)
    assert isinstance(gonio, dxtbx.model.MultiAxisGoniometer)
    assert gonio.get_rotation_axis() == (1.0, 0.0, 0.0)
    assert list(gonio.get_angles()) == [0.0, 0.0, 0.0]
    assert list(gonio.get_axes()) == [
        (1.0, 0.0, 0.0),
        (0.0, 0.0, -1.0),
        (1.0, 0.0, 0.0),
    ]
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
    assert panel.get_type() == "SENSOR_PAD"
    px_mm = panel.get_px_mm_strategy()
    assert px_mm.t0() == panel.get_thickness() == 0.45
    assert px_mm.mu() == panel.get_mu() == pytest.approx(3.9217189904637366)


@pytest.fixture
def detector_with_two_theta():
    with h5py.File(" ", "w", **pytest.h5_in_memory) as f:
        beam = f.create_group("/entry/instrument/beam")
        beam.attrs["NX_class"] = "NXbeam"
        beam["incident_wavelength"] = 0.495937
        beam["incident_wavelength"].attrs["units"] = b"angstrom"

        detector = f.create_group("/entry/instrument/detector")
        detector.attrs["NX_class"] = "NXdetector"
        detector["sensor_material"] = "Silicon"
        detector["sensor_thickness"] = 0.00045
        detector["sensor_thickness"].attrs["units"] = b"m"

        module = detector.create_group("module")
        module.attrs["NX_class"] = "NXdetector_module"
        module.create_dataset("data_size", data=np.array([2162, 2068]))

        fast_pixel_direction = module.create_dataset(
            "fast_pixel_direction", data=7.5e-5
        )
        fast_pixel_direction.attrs["transformation_type"] = "translation"
        fast_pixel_direction.attrs[
            "depends_on"
        ] = "/entry/instrument/detector/module/module_offset"
        fast_pixel_direction.attrs["vector"] = np.array([-1.0, 0.0, 0.0])
        fast_pixel_direction.attrs["units"] = "m"

        slow_pixel_direction = module.create_dataset(
            "slow_pixel_direction", data=7.5e-5
        )
        slow_pixel_direction.attrs["transformation_type"] = "translation"
        slow_pixel_direction.attrs[
            "depends_on"
        ] = "/entry/instrument/detector/module/module_offset"
        slow_pixel_direction.attrs["vector"] = np.array([0, -1.0, 0.0])
        slow_pixel_direction.attrs["units"] = "m"

        module_offset = module.create_dataset("module_offset", data=112.19250476301882)
        module_offset.attrs["transformation_type"] = "translation"
        module_offset.attrs[
            "depends_on"
        ] = "/entry/instrument/detector/transformations/det_z"
        module_offset.attrs["vector"] = np.array([0.72264186, 0.69122265, 0.0])
        module_offset.attrs["units"] = b"mm"

        transformations = detector.create_group("transformations")
        det_z = transformations.create_dataset("det_z", data=120.0)
        det_z.attrs[
            "depends_on"
        ] = b"/entry/instrument/detector/transformations/two_theta"
        det_z.attrs["transformation_type"] = b"translation"
        det_z.attrs["units"] = b"mm"
        det_z.attrs["vector"] = np.array([0.0, 0.0, 1.0])

        two_theta = transformations.create_dataset("two_theta", data=45)
        two_theta.attrs["depends_on"] = b"."
        two_theta.attrs["transformation_type"] = b"rotation"
        two_theta.attrs["units"] = b"deg"
        two_theta.attrs["vector"] = np.array([-1.0, 0.0, 0.0])

        yield f


def test_get_dxtbx_detector_with_two_theta(detector_with_two_theta):
    det = dlstbx.nexus.nxmx.NXdetector(
        detector_with_two_theta["/entry/instrument/detector"]
    )
    beam = dlstbx.nexus.nxmx.NXbeam(detector_with_two_theta["/entry/instrument/beam"])

    detector = dlstbx.nexus.get_dxtbx_detector(det, beam)
    panel = detector[0]
    assert panel.get_fast_axis() == (1.0, 0.0, 0.0)
    assert panel.get_slow_axis() == (0.0, -0.7071067811865475, -0.7071067811865476)
    assert panel.get_origin() == (
        -81.07500032000678,
        139.68894494331983,
        -30.01668254145155,
    )
    assert panel.get_distance() == pytest.approx(120)


@pytest.fixture
def pixel_mask_example():
    with h5py.File(" ", "w", **pytest.h5_in_memory) as f:
        detector = f.create_group("/entry/instrument/detector")
        detector.attrs["NX_class"] = "NXdetector"

        module = detector.create_group("module")
        module.attrs["NX_class"] = "NXdetector_module"
        module.create_dataset("data_origin", data=np.array([0.0, 0.0]))
        module.create_dataset("data_size", data=np.array([4362, 4148]))

        nf = 1028  # module pixels fast
        ns = 512  # module pixels slow
        df = 12  # module gap fast
        ds = 38  # module gap slow

        mask = np.zeros((4362, 4148), dtype="i8")
        for j in range(mask.shape[1] // (nf + df)):
            mask[:, (j + 1) * nf + j * df : (j + 1) * (nf + df)] = 1
        for i in range(mask.shape[0] // (ns + ds)):
            mask[(i + 1) * ns + i * ds : (i + 1) * (ns + ds), :] = 1

        detector.create_dataset("pixel_mask", data=mask)
        yield f


def test_get_static_mask(pixel_mask_example):
    det = dlstbx.nexus.nxmx.NXdetector(pixel_mask_example["/entry/instrument/detector"])
    mask = dlstbx.nexus.get_static_mask(det)
    assert len(mask) == 1
    assert isinstance(mask[0], flex.bool)
    assert mask[0].all() == (4362, 4148)
    assert np.all(
        mask[0].as_numpy_array()
        == (pixel_mask_example["/entry/instrument/detector/pixel_mask"][()] == 0)
    )
