from typing import Tuple

import numpy as np

import dxtbx.model
from cctbx import eltbx
from dxtbx.format.nexus import dataset_as_flex
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
        if np.sum(axes.is_scan_axis) == 0:
            # A sequence of still images, choose an arbitrary scan axis
            scan_axis = 0
        else:
            assert np.sum(axes.is_scan_axis) == 1, "only one scan axis is supported"
            scan_axis = int(np.where(axes.is_scan_axis)[0][0])
        return dxtbx.model.GoniometerFactory.make_multi_axis_goniometer(
            flex.vec3_double((MCSTAS_TO_IMGCIF @ axes.axes.T).T),
            flex.double(axes.angles),
            flex.std_string(axes.names),
            scan_axis,
        )


def get_dxtbx_beam(nxbeam: nxmx.NXbeam) -> dxtbx.model.Beam:
    return dxtbx.model.BeamFactory.make_beam(
        sample_to_source=(0, 0, 1),
        wavelength=nxbeam.incident_wavelength.to("angstrom").magnitude,
    )


def get_dxtbx_scan(
    nxsample: nxmx.NXsample, nxdetector: nxmx.NXdetector
) -> dxtbx.model.Scan:
    dependency_chain = nxmx.get_dependency_chain(nxsample.depends_on)
    scan_axis = None
    for t in dependency_chain:
        if (
            t.transformation_type == "rotation"
            and len(t) > 1
            and not np.all(t[()] == t[0])
        ):
            scan_axis = t
            break

    if scan_axis is None:
        scan_axis = nxsample.depends_on

    is_rotation = scan_axis.transformation_type == "rotation"
    num_images = len(scan_axis)
    image_range = (1, num_images)

    if is_rotation and num_images > 1:
        oscillation = (
            float(scan_axis[0].to("degree").magnitude),
            float((scan_axis[1] - scan_axis[0]).to("degree").magnitude),
        )
    else:
        oscillation = (
            float(scan_axis[0].to("degree").magnitude) if is_rotation else 0,
            0,
        )

    if nxdetector.frame_time is not None:
        frame_time = nxdetector.frame_time.to("seconds").magnitude
        exposure_times = flex.double(num_images, frame_time)
        epochs = flex.double_range(0, num_images) * frame_time
    else:
        exposure_times = flex.double(num_images, 0)
        epochs = flex.double(num_images, 0)

    return dxtbx.model.Scan(
        image_range,
        tuple(float(o) for o in oscillation),
        exposure_times,
        epochs,
        batch_offset=0,
        deg=True,
    )


def get_dxtbx_detector(
    nxdetector: nxmx.NXdetector, nxbeam: nxmx.NXbeam
) -> dxtbx.model.Detector:
    module = nxdetector.modules[0]

    # Apply any rotation components of the dependency chain to the fast axis
    fast_axis_depends_on = [
        t
        for t in nxmx.get_dependency_chain(module.fast_pixel_direction.depends_on)
        if t.transformation_type == "rotation"
    ]
    if fast_axis_depends_on:
        R = nxmx.get_cumulative_transformation(fast_axis_depends_on)[0, :3, :3]
    else:
        R = np.identity(3)
    fast_axis = MCSTAS_TO_IMGCIF @ R @ module.fast_pixel_direction.vector

    # Apply any rotation components of the dependency chain to the slow axis
    slow_axis_depends_on = [
        t
        for t in nxmx.get_dependency_chain(module.slow_pixel_direction.depends_on)
        if t.transformation_type == "rotation"
    ]
    if slow_axis_depends_on:
        R = nxmx.get_cumulative_transformation(slow_axis_depends_on)[0, :3, :3]
    else:
        R = np.identity(3)
    slow_axis = MCSTAS_TO_IMGCIF @ R @ module.slow_pixel_direction.vector

    # Apply all components of the dependency chain to the module offset to get the
    # dxtbx panel origin
    dependency_chain = nxmx.get_dependency_chain(module.module_offset)
    A = nxmx.get_cumulative_transformation(dependency_chain)
    origin = MCSTAS_TO_IMGCIF @ A[0, :3, 3]

    pixel_size = (
        module.fast_pixel_direction[()].to("mm").magnitude,
        module.slow_pixel_direction[()].to("mm").magnitude,
    )
    # dxtbx requires image size in the order fast, slow - which is the reverse of what
    # is stored in module.data_size
    image_size = reversed(module.data_size)
    underload = (
        nxdetector.underload_value
        if nxdetector.underload_value is not None
        else -0x7FFFFFFF
    )
    overload = (
        nxdetector.saturation_value
        if nxdetector.saturation_value is not None
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
        "SENSOR_PAD",
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


def get_static_mask(nxdetector: nxmx.NXdetector) -> Tuple[flex.bool]:
    pixel_mask = nxdetector.get("pixel_mask")
    if pixel_mask and pixel_mask.ndim == 2:
        all_slices = [
            tuple(
                slice(int(start), int(start + step), 1)
                for start, step in zip(module.data_origin, module.data_size)
            )
            for module in nxdetector.modules
        ]
        return tuple(dataset_as_flex(pixel_mask, slices) == 0 for slices in all_slices)
