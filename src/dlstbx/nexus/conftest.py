import h5py
import numpy as np
import pytest


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
