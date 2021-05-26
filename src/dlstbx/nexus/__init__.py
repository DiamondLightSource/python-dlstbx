import numpy as np

import dxtbx.model
from cctbx import eltbx
from scitbx.array_family import flex

from . import nxmx


KNOWN_SENSOR_MATERIALS = {
    "Si": "Si",
    "Silicon": "Si",
    "CdTe": "CdTe",
    "GaAs": "GaAs",
}


# Conversion from the McStas coordinate system as used by NeXus to the imgCIF
# coordinate system conventionally used by dxtbx:
#   https://manual.nexusformat.org/design.html#design-coordinatesystem
#   https://www.iucr.org/__data/iucr/cifdic_html/2/cif_img.dic/Caxis.html
MCSTAS_TO_IMGCIF = np.diag([-1, 1, -1])


def get_dxtbx_goniometer(nxsample: nxmx.NXsample) -> dxtbx.model.Goniometer:
    dependency_chain = nxmx.get_dependency_chain(nxsample.depends_on)
    axes = nxmx.get_rotation_axes(dependency_chain)
    if len(axes.axes) == 1:
        return dxtbx.model.GoniometerFactory.make_goniometer(
            MCSTAS_TO_IMGCIF @ axes.axes[0], np.identity(3)
        )
    else:
        assert np.sum(axes.is_scan_axis) == 1, "only one scan axis is supported"
        return dxtbx.model.GoniometerFactory.make_multi_axis_goniometer(
            flex.vec3_double(MCSTAS_TO_IMGCIF @ axes.axes),
            flex.double(axes.angles),
            flex.std_string(axes.names),
            int(np.where(axes.is_scan_axis)[0][0]),
        )


def get_dxtbx_beam(nxbeam: nxmx.NXbeam) -> dxtbx.model.Beam:
    return dxtbx.model.BeamFactory.make_beam(
        sample_to_source=(0, 0, 1),
        wavelength=nxbeam.incident_wavelength.to("angstrom").magnitude,
    )


def get_dxtbx_detector(
    nxdetector: nxmx.NXdetector, nxbeam: nxmx.NXbeam
) -> dxtbx.model.Detector:
    detector_type = nxdetector.type
    if not detector_type:
        detector_type = "unknown"

    module = nxdetector.modules[0]
    fast_axis = MCSTAS_TO_IMGCIF @ module.fast_pixel_direction.vector
    slow_axis = MCSTAS_TO_IMGCIF @ module.slow_pixel_direction.vector
    dependency_chain = nxmx.get_dependency_chain(module.module_offset)
    A = nxmx.get_cumulative_transformation(dependency_chain)
    origin = MCSTAS_TO_IMGCIF @ A[0, :3, 3]
    pixel_size = (
        module.fast_pixel_direction[()].to("mm").magnitude,
        module.slow_pixel_direction[()].to("mm").magnitude,
    )
    image_size = module.data_size
    underload = (
        nxdetector.underload_value
        if nxdetector.underload_value is not None
        else -0x7FFFFFFF
    )
    overload = (
        nxdetector.overload_value
        if nxdetector.overload_value is not None
        else 0x7FFFFFFF
    )
    trusted_range = (underload, overload)

    material = KNOWN_SENSOR_MATERIALS.get(nxdetector.sensor_material)
    if not material:
        raise ValueError(f"Unknown material: {nxdetector.sensor_material}")
    thickness = nxdetector.sensor_thickness.to("mm").magnitude
    table = eltbx.attenuation_coefficient.get_table(material)
    mu = (
        table.mu_at_angstrom(nxbeam.incident_wavelength.to("angstrom").magnitude) / 10.0
    )
    px_mm = dxtbx.model.ParallaxCorrectedPxMmStrategy(mu, thickness)
    name = nxdetector.path

    return dxtbx.model.DetectorFactory.make_detector(
        detector_type,
        fast_axis,
        slow_axis,
        origin,
        pixel_size,
        image_size,
        trusted_range=trusted_range,
        px_mm=px_mm,
        name=name,
        thickness=thickness,
        material=material,
        mu=mu,
        # gain=None,
        # pedestal=None,
        # identifier="",
    )
