import dataclasses
import h5py
import logging
import numpy as np
import pint
from collections import namedtuple
from collections.abc import Mapping
from functools import cached_property
from scipy.spatial.transform import Rotation
from typing import Iterator, List, Optional, Tuple, Union


ureg = pint.UnitRegistry()


logger = logging.getLogger(__name__)


NXNode = Union[h5py.File, h5py.Group]


def h5str(h5_value: Optional[Union[str, np.string_, bytes]]) -> Optional[str]:
    """
    Convert a value returned an h5py attribute to str.

    h5py can return either a bytes-like (numpy.string_) or str object
    for attribute values depending on whether the value was written as
    fixed or variable length. This function collapses the two to str.
    """
    if hasattr(h5_value, "decode"):
        return h5_value.decode("utf-8")
    return h5_value


def find_classes(
    node: NXNode, *nx_classes: Optional[str]
) -> Tuple[List[h5py.Group], ...]:
    """
    Find instances of multiple NXclass types within the children of the current node.

    Args:
        node: The input h5py node (h5py.File or h5py.Group).
        nx_classes: Names of NXclass types to search for.  If None, search for children
            without an NXclass.

    Returns:
        A list of matching nodes for each of the specified NX_class types.
    """
    results = {nx_class: [] for nx_class in nx_classes}

    for v in filter(None, node.values()):
        class_name = h5str(v.attrs.get("NX_class"))
        if class_name in nx_classes:
            results[class_name].append(v)

    return tuple(results.values())


def find_class(node: NXNode, nx_class: Optional[str]) -> List[h5py.Group]:
    """
    Find instances of a single NXclass type within the children of the current node.

    This is a convenience function, equivalent to calling find_classes with a single
    NXclass type name argument and returning the list of matches.

    Args:
        node: The input h5py node (h5py.File or h5py.Group).
        nx_class: Names of NXclass type to search for.  If None, search for children
            without an NXclass.

    Returns:
        The list of matching nodes for the specified NXclass type.
    """
    return find_classes(node, nx_class)[0]


class H5Mapping(Mapping):
    def __init__(self, handle):
        self._handle = handle

    def __getitem__(self, key):
        return self._handle[key]

    def __iter__(self):
        return iter(self._handle)

    def __len__(self):
        return len(self._handle)

    @cached_property
    def path(self):
        return h5str(self._handle.name)


class NXmx(H5Mapping):
    def __init__(self, handle):
        super().__init__(handle)
        self._entries = [
            entry
            for entry in find_class(handle, "NXentry")
            if "definition" in entry and h5str(entry["definition"][()]) == "NXmx"
        ]

    @cached_property
    def entries(self):
        return [NXentry(entry) for entry in self._entries]


class NXentry(H5Mapping):
    def __init__(self, handle):
        super().__init__(handle)
        self._data, self._instruments, self._samples, self._sources = find_classes(
            handle, "NXdata", "NXinstrument", "NXsample", "NXsource"
        )

    @cached_property
    def instruments(self):
        return [NXinstrument(instrument) for instrument in self._instruments]

    @cached_property
    def samples(self):
        return [NXsample(sample) for sample in self._samples]

    @cached_property
    def data(self):
        return [NXdata(data) for data in self._data]

    @cached_property
    def source(self):
        return NXsource(self._sources[0])

    @cached_property
    def start_time(self):
        return self._handle["start_time"][()]

    @cached_property
    def end_time(self):
        if "end_time" in self._handle:
            return self._handle["end_time"][()]

    @cached_property
    def end_time_estimated(self):
        return self._handle["end_time_estimated"][()]

    @cached_property
    def definition(self):
        return h5str(self._handle["definition"][()])


class NXdata(H5Mapping):
    def __getitem__(self, key):
        return self._handle[key]

    def __iter__(self):
        return iter(self._handle)

    def __len__(self):
        return len(self._handle)

    @cached_property
    def signal(self):
        return self._handle.attrs.get("signal")


class NXtransformations(H5Mapping):
    def __init__(self, handle):
        super().__init__(handle)
        self._axes = {
            k: NXtransformationsAxis(v)
            for k, v in handle.items()
            if isinstance(v, h5py.Dataset)
        }

    @cached_property
    def default(self):
        return h5str(self._handle.attrs.get("default"))

    @cached_property
    def axes(self):
        return self._axes


class NXtransformationsAxis(H5Mapping):
    def __init__(self, handle):
        super().__init__(handle)

    @cached_property
    def units(self):
        return h5str(self._handle.attrs.get("units"))

    @cached_property
    def transformation_type(self):
        return h5str(self._handle.attrs.get("transformation_type"))

    @cached_property
    def vector(self):
        return self._handle.attrs.get("vector")

    @cached_property
    def offset(self) -> pint.quantity:
        if "offset" in self._handle.attrs:
            return self._handle.attrs.get("offset") * ureg(self.offset_units)

    @cached_property
    def offset_units(self):
        if "offset_units" in self._handle.attrs:
            return h5str(self._handle.attrs.get("offset_units"))
        # This shouldn't be the case, but DLS EIGER NeXus files include offset without
        # accompanying offset_units, so use units instead (which should strictly only
        # apply to vector, not offset.
        # See also https://jira.diamond.ac.uk/browse/MXGDA-3668
        logger.warning(
            f"'offset_units' attribute not present for {self.path}, falling back to 'units'"
        )
        return self.units

    @cached_property
    def depends_on(self):
        depends_on = h5str(self._handle.attrs.get("depends_on"))
        if depends_on and depends_on != ".":
            return NXtransformationsAxis(self._handle.parent[depends_on])

    def __getitem__(self, key) -> pint.Quantity:
        return self._handle[key] * ureg(self.units)


class NXsample(H5Mapping):
    def __init__(self, handle):
        super().__init__(handle)
        self._transformations = find_class(handle, "NXtransformations")

    @cached_property
    def name(self) -> str:
        """Descriptive name of sample"""
        return h5str(self._handle["name"][()])

    @cached_property
    def depends_on(self) -> NXtransformationsAxis:
        """The axis on which the sample position depends"""
        depends_on = h5str(self._handle["depends_on"][()])
        if depends_on and depends_on != ".":
            return NXtransformationsAxis(self._handle[depends_on])

    @cached_property
    def temperature(self) -> pint.Quantity:
        if "temperature" in self._handle:
            temperature = self._handle["temperature"]
            units = h5str(temperature.attrs["units"])
            return temperature[()] * ureg(units)

    @cached_property
    def transformations(self) -> NXtransformations:
        """This is the recommended location for sample goniometer and other related axes.

        This is a requirement to describe for any scan experiment. The reason it is
        optional is mainly to accommodate XFEL single shot exposures.

        Use of the depends_on field and the NXtransformations group is strongly
        recommended. As noted above this should be an absolute requirement to have for
        any scan experiment.

        The reason it is optional is mainly to accommodate XFEL single shot exposures.
        """
        return [
            NXtransformations(transformations)
            for transformations in self._transformations
        ]


class NXinstrument(H5Mapping):
    def __init__(self, handle):
        super().__init__(handle)

        (
            self._attenuators,
            self._detector_groups,
            self._detectors,
            self._beams,
            self._transformations,
        ) = find_classes(
            handle,
            "NXattenuator",
            "NXdetector_group",
            "NXdetector",
            "NXbeam",
            "NXtransformations",
        )

    @cached_property
    def name(self):
        return h5str(self._handle["name"][()])

    @cached_property
    def short_name(self):
        return h5str(self._handle["name"].attrs.get("short_name"))

    @cached_property
    def time_zone(self):
        return self._handle.get("time_zone")

    @cached_property
    def attenuators(self):
        return self._attenuators

    @cached_property
    def detector_groups(self):
        return [NXdetector_group(group) for group in self._detector_groups]

    @cached_property
    def detectors(self):
        return [NXdetector(detector) for detector in self._detectors]

    @cached_property
    def beams(self):
        return [NXbeam(beam) for beam in self._beams]

    @cached_property
    def transformations(self):
        return [
            NXtransformations(transformations)
            for transformations in self._transformations
        ]


class NXdetector_group(H5Mapping):
    """Optional logical grouping of detectors.

    Each detector is represented as an NXdetector with its own detector data array. Each
    detector data array may be further decomposed into array sections by use of
    NXdetector_module groups. Detectors can be grouped logically together using
    NXdetector_group. Groups can be further grouped hierarchically in a single
    NXdetector_group (for example, if there are multiple detectors at an endstation or
    multiple endstations at a facility). Alternatively, multiple NXdetector_groups can
    be provided.

    The groups are defined hierarchically, with names given in the group_names field,
    unique identifying indices given in the field group_index, and the level in the
    hierarchy given in the group_parent field. For example if an x-ray detector group,
    DET, consists of four detectors in a rectangular array:

        DTL    DTR
        DLL    DLR

    We could have:

        group_names: ["DET", "DTL", "DTR", "DLL", "DLR"]
        group_index: [1, 2, 3, 4, 5]
        group_parent:  [-1, 1, 1, 1, 1]
    """

    @cached_property
    def group_names(self) -> np.ndarray:
        """
        An array of the names of the detectors or the names of hierarchical groupings of
        detectors.
        """
        return self._handle["group_names"].asstr()[()]

    @cached_property
    def group_index(self) -> np.ndarray:
        """An array of unique identifiers for detectors or groupings of detectors.

        Each ID is a unique ID for the corresponding detector or group named in the
        field group_names. The IDs are positive integers starting with 1.
        """
        return self._handle["group_index"][()]

    @cached_property
    def group_parent(self) -> np.ndarray:
        """
        An array of the hierarchical levels of the parents of detectors or groupings of
        detectors.

        A top-level grouping has parent level -1.
        """
        return self._handle["group_parent"][()]


class NXdetector(H5Mapping):
    def __init__(self, handle):
        super().__init__(handle)
        (self._modules,) = find_classes(handle, "NXdetector_module")

    @cached_property
    def depends_on(self) -> Optional[NXtransformationsAxis]:
        """The axis on which the detector position depends.

        NeXus path to the detector positioner axis that most directly supports the
        detector. In the case of a single-module detector, the detector axis chain may
        start here.
        """
        if "depends_on" in self._handle:
            return NXtransformationsAxis(self._handle[self._handle["depends_on"][()]])

    @cached_property
    def data(self):
        if "data" in self._handle:
            return self._handle["data"][()]

    @cached_property
    def description(self):
        if "description" in self._handle:
            return h5str(self._handle["description"][()])

    @cached_property
    def distance(self):
        if "distance" in self._handle:
            return self._handle["distance"][()]

    @cached_property
    def distance_derived(self):
        if "distance_derived" in self._handle:
            return self._handle["distance_derived"][()]

    @cached_property
    def count_time(self):
        if "count_time" in self._handle:
            return self._handle["count_time"][()]

    @cached_property
    def beam_center_x(self):
        if "beam_center_x" in self._handle:
            return self._handle["beam_center_x"][()]

    @cached_property
    def beam_center_y(self):
        if "beam_center_y" in self._handle:
            return self._handle["beam_center_y"][()]

    @cached_property
    def pixel_mask_applied(self) -> Optional[bool]:
        """
        True when the pixel mask correction has been applied in the electronics, false
        otherwise (optional).
        """
        if "pixel_mask_applied" in self._handle:
            return self._handle["pixel_mask_applied"][()]

    @cached_property
    def pixel_mask(self) -> Optional[np.ndarray]:
        """The 32-bit pixel mask for the detector.

        Can be either one mask for the whole dataset (i.e. an array with indices i, j)
        or each frame can have its own mask (in which case it would be an array with
        indices nP, i, j).

        Contains a bit field for each pixel to signal dead, blind, high or otherwise
        unwanted or undesirable pixels. They have the following meaning:

          - bit 0: gap (pixel with no sensor)

          - bit 1: dead

          - bit 2: under-responding

          - bit 3: over-responding

          - bit 4: noisy

          - bit 5: -undefined-

          - bit 6: pixel is part of a cluster of problematic pixels (bit set in addition
                   to others)

          - bit 7: -undefined-

          - bit 8: user defined mask (e.g. around beamstop)

          - bits 9-30: -undefined-

          - bit 31: virtual pixel (corner pixel with interpolated value)

        Normal data analysis software would not take pixels into account when a bit in
        (mask & 0x0000FFFF) is set. Tag bit in the upper two bytes would indicate
        special pixel properties that normally would not be a sole reason to reject the
        intensity value (unless lower bits are set.

        If the full bit depths is not required, providing a mask with fewer bits is
        permissible.

        If needed, additional pixel masks can be specified by including additional
        entries named pixel_mask_N, where N is an integer. For example, a general bad
        pixel mask could be specified in pixel_mask that indicates noisy and dead
        pixels, and an additional pixel mask from experiment-specific shadowing could be
        specified in pixel_mask_2. The cumulative mask is the bitwise OR of pixel_mask
        and any pixel_mask_N entries.

        If provided, it is recommended that it be compressed.
        """
        if "pixel_mask" in self._handle:
            return self._handle["pixel_mask"][()]

    @cached_property
    def bit_depth_readout(self) -> Optional[int]:
        """How many bits the electronics record per pixel (recommended)."""
        if "bit_depth_readout" in self._handle:
            return self._handle["bit_depth_readout"][()]

    @cached_property
    def sensor_material(self):
        return h5str(self._handle["sensor_material"][()])

    @cached_property
    def sensor_thickness(self) -> pint.Quantity:
        thickness = self._handle["sensor_thickness"]
        units = h5str(thickness.attrs["units"])
        return thickness[()] * ureg(units)

    @cached_property
    def underload_value(self) -> Optional[int]:
        """The lowest value at which pixels for this detector would be reasonably be measured.

        For example, given a saturation_value and an underload_value, the valid pixels
        are those less than or equal to the saturation_value and greater than or equal
        to the underload_value.
        """
        if "underload_value" in self._handle:
            return self._handle["underload_value"][()]

    @cached_property
    def saturation_value(self) -> Optional[int]:
        """The value at which the detector goes into saturation.

        Data above this value is known to be invalid.

        For example, given a saturation_value and an underload_value, the valid pixels
        are those less than or equal to the saturation_value and greater than or equal
        to the underload_value.
        """
        if "saturation_value" in self._handle:
            return self._handle["saturation_value"][()]

    @cached_property
    def modules(self):
        return [NXdetector_module(module) for module in self._modules]

    @cached_property
    def type(self):
        if "type" in self._handle:
            return h5str(self._handle["type"][()])

    @cached_property
    def frame_time(self) -> Optional[pint.Quantity]:
        """This is time for each frame. This is exposure_time + readout time."""
        if "frame_time" in self._handle:
            frame_time = self._handle["frame_time"]
            units = h5str(frame_time.attrs["units"])
            return frame_time[()] * ureg(units)


class NXdetector_module(H5Mapping):
    @cached_property
    def data_origin(self):
        return self._handle["data_origin"][()]

    @cached_property
    def data_size(self):
        return self._handle["data_size"][()]

    @cached_property
    def data_stride(self):
        if "data_stride" in self._handle:
            return self._handle["data_stride"][()]

    @cached_property
    def module_offset(self):
        if "module_offset" in self._handle:
            return NXtransformationsAxis(self._handle["module_offset"])

    @cached_property
    def fast_pixel_direction(self):
        return NXtransformationsAxis(self._handle["fast_pixel_direction"])

    @cached_property
    def slow_pixel_direction(self):
        return NXtransformationsAxis(self._handle["slow_pixel_direction"])


class NXsource(H5Mapping):
    @cached_property
    def name(self):
        return h5str(self._handle["name"][()])

    @cached_property
    def short_name(self):
        return h5str(self._handle["name"].attrs.get("short_name"))


class NXbeam(H5Mapping):
    @cached_property
    def incident_wavelength(self) -> pint.Quantity:
        wavelength = self._handle["incident_wavelength"]
        units = h5str(wavelength.attrs["units"])
        return wavelength[()] * ureg(units)

    @cached_property
    def flux(self):
        if "flux" in self._handle:
            return self._handle["flux"][()]

    @cached_property
    def total_flux(self):
        return self._handle["total_flux"][()]

    @cached_property
    def incident_beam_size(self):
        if "incident_beam_size" in self._handle:
            return self._handle["incident_beam_size"][()]

    @cached_property
    def profile(self):
        if "profile" in self._handle:
            return h5str(self._handle["profile"][()])

    @cached_property
    def incident_polarisation_stokes(self):
        if "incident_polarisation_stokes" in self._handle:
            return self._handle["incident_polarisation_stokes"][()]


class Transformation:
    def __init__(
        self, values, vector, transformation_type, offset=None, depends_on=None
    ):
        self.values = values
        self.vector = np.repeat(vector.reshape(1, vector.size), values.size, axis=0)
        self.transformation_type = transformation_type
        self.offset = offset
        self.depends_on = depends_on

    def compose(self) -> np.ndarray:
        if self.transformation_type == "rotation":
            R = Rotation.from_rotvec(
                self.values[:, np.newaxis] * self.vector
            ).as_matrix()
            T = np.zeros((self.values.size, 3))
        else:
            R = np.identity(3)
            T = self.values[:, np.newaxis] * self.vector
        if self.offset is not None:
            T += self.offset
        A = np.repeat(np.identity(4).reshape((1, 4, 4)), self.values.size, axis=0)
        A[:, :3, :3] = R
        A[:, :3, 3] = T
        if self.depends_on:
            return self.depends_on.compose() @ A
        return A


@dataclasses.dataclass(frozen=True)
class DependencyChain:
    transformations: List[NXtransformationsAxis]

    def __iter__(self) -> Iterator[NXtransformationsAxis]:
        return iter(self.transformations)

    def __getitem__(self, idx) -> NXtransformationsAxis:
        return self.transformations[idx]

    def __len__(self) -> int:
        return len(self.transformations)

    def __str__(self):
        string = []
        for t in self.transformations:
            depends_on = t.depends_on.path if t.depends_on else "."
            string.extend(
                [
                    f"{t.path} = {t[()]:g}",
                    f"  @transformation_type = {t.transformation_type}",
                    f"  @vector = {t.vector}",
                    f"  @offset = {t.offset}",
                    f"  @depends_on = {depends_on}",
                ]
            )
        return "\n".join(string)


def get_dependency_chain(
    transformation: NXtransformationsAxis,
) -> DependencyChain:
    transformations = []
    while transformation is not None:
        transformations.append(transformation)
        transformation = transformation.depends_on
    return DependencyChain(transformations)


def get_cumulative_transformation(
    dependency_chain: DependencyChain,
) -> np.ndarray:
    t = None
    for transformation in reversed(dependency_chain):
        transformation_type = transformation.transformation_type
        if transformation_type == "translation":
            assert transformation.units is not None
        values = np.atleast_1d(transformation[()])
        values = (
            values.to("mm").magnitude
            if transformation_type == "translation"
            else values.to("rad").magnitude
        )
        offset = transformation.offset
        if offset is not None:
            offset = offset.to("mm").magnitude
        t = Transformation(
            values,
            transformation.vector,
            transformation_type,
            offset=offset,
            depends_on=t,
        )
    return t.compose()


Axes = namedtuple("axes", ["axes", "angles", "names", "is_scan_axis"])


def get_rotation_axes(dependency_chain: DependencyChain) -> Axes:
    axes = []
    angles = []
    axis_names = []
    is_scan_axis = []

    for transformation in dependency_chain:
        if transformation.transformation_type != "rotation":
            continue
        values = np.atleast_1d(transformation[()])
        values = values.to("degrees").magnitude
        is_scan = len(values) > 1 and not np.all(values == values[0])
        axes.append(transformation.vector)
        angles.append(values[0])
        axis_names.append(transformation.path.split("/")[-1])
        is_scan_axis.append(is_scan)

    return Axes(
        np.array(axes), np.array(angles), np.array(axis_names), np.array(is_scan_axis)
    )
