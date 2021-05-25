import h5py
import numpy as np
import pytest

from dlstbx.nexus import nxmx


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


@pytest.mark.parametrize(
    "scan_data", [np.array(0), np.array([0])], ids=["scalar", "vector"]
)
def test_get_rotation_axis_scalar_or_vector(scan_data):
    """
    Test that single-valued rotation axis positions can be scalar or vector.

    A rotation axis with a single angular position may be recorded in a HDF5 NeXus
    file either as an array data set with a single entry, or as a scalar data set.
    Both are equally valid.  Check that they are handled correctly in get_rotation_axis.
    """
    # Create a basic h5py data set.  A non-empty string file name is required,
    # even though there is no corresponding file.
    with h5py.File(" ", "w", **pytest.h5_in_memory) as f:
        # Create a single data set representing the goniometer axis.
        scan_axis = f.create_dataset("dummy_axis", data=scan_data)
        # Add the attributes of a rotation scan axis aligned with the x axis.
        scan_axis.attrs["transformation_type"] = "rotation"
        scan_axis.attrs["vector"] = (1, 0, 0)
        scan_axis.attrs["units"] = "degrees"

        # Test that we can interpret the rotation axis datum.
        scan_axes = [nxmx.NXtransformationsAxis(scan_axis, None)]
        nxmx.get_rotation_axes(scan_axes)


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
