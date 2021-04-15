import h5py
import numpy as np
import pytest

from dlstbx.nexus import nxmx


@pytest.fixture
def nxmx_example(tmp_path):
    filename = tmp_path / "entry.nxs"
    f = h5py.File(filename, mode="w")
    entry = f.create_group("/entry")
    entry.attrs["NX_class"] = "NXentry"
    entry["definition"] = "NXmx"

    source = entry.create_group("source")
    source.attrs["NX_class"] = "NXsource"
    source_name = source.create_dataset("name", data="Diamond")
    source_name.attrs["short_name"] = "DLS"

    instrument = entry.create_group("instrument")
    instrument.attrs["NX_class"] = "NXinstrument"
    name = instrument.create_dataset("name", data=np.string_("DIAMOND BEAMLINE I03"))
    name.attrs["short_name"] = "I03"

    beam = instrument.create_group("beam")
    beam.attrs["NX_class"] = "NXbeam"
    beam["incident_wavelength"] = 0.976223

    detector = instrument.create_group("detector")
    detector.attrs["NX_class"] = "NXdetector"
    detector["beam_center_x"] = 2079.79727597266
    detector["beam_center_y"] = 2225.38773853771
    detector["count_time"] = 0.00285260857097799
    detector["depends_on"] = "/entry/instrument/transformations/det_z"
    detector["description"] = "Eiger 16M"
    detector["distance"] = 0.237015940260233
    data = detector.create_dataset("data", data=np.zeros((100, 100)))
    detector["sensor_material"] = "Silicon"
    detector["sensor_thickness"] = 0.00045
    detector["x_pixel_size"] = 7.5e-05
    detector["y_pixel_size"] = 7.5e-05

    module = detector.create_group("module")
    module.attrs["NX_class"] = "NXdetector_module"
    module.create_dataset("data_origin", data=np.array([0.0, 0.0]))
    module.create_dataset("data_size", data=np.array([4148, 4362]))

    fast_pixel_direction = module.create_dataset("fast_pixel_direction", data=7.5e-5)
    fast_pixel_direction.attrs["transformation_type"] = "translation"
    fast_pixel_direction.attrs[
        "depends_on"
    ] = "/entry/instrument/detector/module/module_offset"
    fast_pixel_direction.attrs["vector"] = np.array([-1.0, 0.0, 0.0])
    fast_pixel_direction.attrs["offset"] = np.array([0.0, 0.0, 0.0])
    fast_pixel_direction.attrs["unit"] = "m"

    slow_pixel_direction = module.create_dataset("slow_pixel_direction", data=7.5e-5)
    slow_pixel_direction.attrs["transformation_type"] = "translation"
    slow_pixel_direction.attrs[
        "depends_on"
    ] = "/entry/instrument/detector/module/module_offset"
    slow_pixel_direction.attrs["vector"] = np.array([0.0, -1.0, 0.0])
    slow_pixel_direction.attrs["offset"] = np.array([0.0, 0.0, 0.0])
    slow_pixel_direction.attrs["unit"] = "m"

    module_offset = module.create_dataset("module_offset", data=0)
    module_offset.attrs["transformation_type"] = "translation"
    module_offset.attrs["depends_on"] = "/entry/instrument/transformations/det_z"
    module_offset.attrs["vector"] = np.array([1.0, 0.0, 0.0])
    module_offset.attrs["offset"] = np.array([0.155985, 0.166904, -0])
    module_offset.attrs["unit"] = "m"

    sample = entry.create_group("sample")
    sample.attrs["NX_class"] = "NXsample"
    sample["name"] = "mysample"
    sample["depends_on"] = b"/entry/sample/transformations/phi"

    transformations = sample.create_group("transformations")
    transformations.attrs["NX_class"] = "NXtransformations"
    omega = transformations.create_dataset("omega", data=np.arange(0, 10))
    omega.attrs["depends_on"] = b"."
    omega.attrs["transformation_type"] = b"rotation"
    omega.attrs["units"] = b"deg"
    omega.attrs["vector"] = np.array([-1.0, 0.0, 0.0])
    omega.attrs["omega_offset"] = np.array([0.0, 0.0, 0.0])

    phi = transformations.create_dataset("phi", data=np.array([0.0]))
    phi.attrs["depends_on"] = b"/entry/sample/transformations/chi"
    phi.attrs["transformation_type"] = b"rotation"
    phi.attrs["units"] = b"deg"
    phi.attrs["vector"] = np.array([-1.0, 0, 0])

    chi = transformations.create_dataset("chi", data=np.array([0.0]))
    chi.attrs["depends_on"] = b"/entry/sample/transformations/omega"
    chi.attrs["transformation_type"] = b"rotation"
    chi.attrs["units"] = b"deg"
    chi.attrs["vector"] = np.array([0, 0, 1])

    data = entry.create_group("data")
    data.attrs["NX_class"] = "NXdata"
    return f


def test_nxentry(nxmx_example):
    nxentry = nxmx.NXentry(nxmx_example["/entry"])
    assert nxentry.definition == "NXmx"

    assert len(nxentry.samples) == 1
    assert isinstance(nxentry.samples[0], nxmx.NXsample)
    assert len(nxentry.instruments) == 1
    assert isinstance(nxentry.instruments[0], nxmx.NXinstrument)
    assert isinstance(nxentry.source, nxmx.NXsource)
    assert len(nxentry.data) == 1
    assert isinstance(nxentry.data[0], nxmx.NXdata)


def test_nxmx(nxmx_example):
    nx = nxmx.NXmx(nxmx_example)
    assert len(nx) == 1
    assert nx.keys() == nxmx_example.keys()
    entries = nx.entries
    assert len(entries) == 1
    nxentry = entries[0]
    assert nxentry.definition == "NXmx"

    samples = nxentry.samples
    assert len(samples) == 1
    sample = samples[0]
    assert sample.name == "mysample"
    assert sample.depends_on.name == "/entry/sample/transformations/phi"
    assert sample.temperature is None

    transformations = sample.transformations
    assert len(transformations) == 1
    axes = transformations[0].axes
    assert len(axes) == 3
    assert set(axes.keys()) == {"chi", "omega", "phi"}
    phi_depends_on = axes["phi"].depends_on
    assert phi_depends_on.name == "/entry/sample/transformations/chi"

    assert len(nxentry.instruments) == 1
    instrument = nxentry.instruments[0]
    assert instrument.name == "DIAMOND BEAMLINE I03"
    assert instrument.short_name == "I03"

    assert len(instrument.beams) == 1
    beam = instrument.beams[0]
    assert beam.incident_wavelength == 0.976223

    assert len(instrument.detectors) == 1
    detector = instrument.detectors[0]
    assert detector.description == "Eiger 16M"
    assert detector.sensor_material == "Silicon"
    assert detector.sensor_thickness == 0.00045

    assert len(detector.modules) == 1
    module = detector.modules[0]
    assert np.all(module.data_origin == [0, 0])
    assert np.all(module.data_size == [4148, 4362])

    assert nxentry.source.name == "Diamond"
    assert nxentry.source.short_name == "DLS"


def test_get_rotation_axes(nxmx_example):
    sample = nxmx.NXmx(nxmx_example).entries[0].samples[0]
    dependency_chain = nxmx.get_dependency_chain(sample.depends_on)
    axes = nxmx.get_rotation_axes(dependency_chain)
    assert np.all(axes.is_scan_axis == [False, False, True])
    assert np.all(axes.names == ["phi", "chi", "omega"])
    assert np.all(axes.angles == [0.0, 0.0, 0.0])
    assert np.all(
        axes.axes == np.array([[-1.0, 0.0, 0.0], [0.0, 0.0, 1.0], [-1.0, 0.0, 0.0]])
    )


def test_get_dependency_chain(nxmx_example):
    sample = nxmx.NXmx(nxmx_example).entries[0].samples[0]
    dependency_chain = nxmx.get_dependency_chain(sample.depends_on)
    assert [d.name for d in dependency_chain] == [
        "/entry/sample/transformations/phi",
        "/entry/sample/transformations/chi",
        "/entry/sample/transformations/omega",
    ]


def test_get_cumulative_transformation(nxmx_example):
    sample = nxmx.NXmx(nxmx_example).entries[0].samples[0]
    dependency_chain = nxmx.get_dependency_chain(sample.depends_on)
    A = nxmx.get_cumulative_transformation(dependency_chain)
    assert A.shape == (10, 4, 4)
    assert np.all(
        A[0]
        == np.array(
            [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
                [0.0, 0.0, 1.0, 0.0],
                [0.0, 0.0, 0.0, 1.0],
            ]
        )
    )
