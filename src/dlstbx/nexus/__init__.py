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


def get_dxtbx_goniometer(sample):
    dependency_chain = nxmx.get_dependency_chain(sample.depends_on)
    axes = nxmx.get_rotation_axes(dependency_chain)
    R = np.diag([-1, 1, -1])
    if len(axes.axes) == 1:
        return dxtbx.model.GoniometerFactory.make_goniometer(
            R @ axes.axes[0], np.identity(3)
        )
    else:
        assert np.sum(axes.is_scan_axis) == 1, "only one scan axis is supported"
        return dxtbx.model.GoniometerFactory.make_multi_axis_goniometer(
            flex.vec3_double(R @ axes.axes),
            flex.double(axes.angles),
            flex.std_string(axes.names),
            int(np.where(axes.is_scan_axis)[0][0]),
        )


def get_dxtbx_beam(beam):
    return dxtbx.model.BeamFactory.make_beam(
        sample_to_source=(0, 0, 1),
        wavelength=beam.incident_wavelength.to("angstrom").magnitude,
    )


def get_dxtbx_detector(detector, beam):
    detector_type = detector.type
    if not detector_type:
        detector_type = "unknown"

    module = detector.modules[0]
    fast_axis = module.fast_pixel_direction.vector
    slow_axis = module.slow_pixel_direction.vector
    # origin = module.module_offset.vector
    dependency_chain = nxmx.get_dependency_chain(module.module_offset)
    A = nxmx.get_cumulative_transformation(dependency_chain)
    origin = A[0, :3, 3]
    pixel_size = (
        module.fast_pixel_direction[()].to("mm").magnitude,
        module.slow_pixel_direction[()].to("mm").magnitude,
    )
    image_size = module.data_size
    underload = (
        detector.underload_value
        if detector.underload_value is not None
        else -0x7FFFFFFF
    )
    overload = (
        detector.overload_value if detector.overload_value is not None else 0x7FFFFFFF
    )
    trusted_range = (underload, overload)

    material = KNOWN_SENSOR_MATERIALS.get(detector.sensor_material)
    if not material:
        raise ValueError(f"Unknown material: {detector.sensor_material}")
    thickness = detector.sensor_thickness.to("mm").magnitude
    table = eltbx.attenuation_coefficient.get_table(material)
    mu = table.mu_at_angstrom(beam.incident_wavelength.to("angstrom").magnitude) / 10.0
    px_mm = dxtbx.model.ParallaxCorrectedPxMmStrategy(mu, thickness)
    name = detector.path

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
