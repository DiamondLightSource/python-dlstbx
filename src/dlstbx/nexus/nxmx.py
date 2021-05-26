import h5py
import logging
import numpy as np
import pint
from collections import namedtuple
from scipy.spatial.transform import Rotation
from typing import List, Optional, Tuple, Union

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


from functools import cached_property

from collections.abc import Mapping


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


class NXsample(H5Mapping):
    def __init__(self, handle):
        super().__init__(handle)
        self._transformations = find_class(handle, "NXtransformations")

    @cached_property
    def name(self):
        return h5str(self._handle["name"][()])

    @cached_property
    def depends_on(self):
        return NXtransformationsAxis(self._handle[self._handle["depends_on"][()]])

    @cached_property
    def temperature(self):
        if "temperature" in self._handle:
            return self._handle["temperature"][()]

    @cached_property
    def transformations(self):
        return [
            NXtransformations(transformations)
            for transformations in self._transformations
        ]


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
        return h5str(self._handle.attrs.get("offset_units"))

    @cached_property
    def depends_on(self):
        depends_on = h5str(self._handle.attrs.get("depends_on"))
        if depends_on and depends_on != ".":
            return NXtransformationsAxis(self._handle.parent[depends_on])

    def __getitem__(self, key) -> pint.Quantity:
        return self._handle[key] * ureg(self.units)


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
        return self._detector_groups

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


class NXdetector(H5Mapping):
    def __init__(self, handle):
        super().__init__(handle)
        (self._modules,) = find_classes(handle, "NXdetector_module")

    @cached_property
    def depends_on(self):
        if "depends_on" in self._hande:
            return h5str(self._handle["depends_on"][()])

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
    def pixel_mask(self):
        if "pixel_mask" in self._handle:
            return self._handle["pixel_mask"][()]

    @cached_property
    def sensor_material(self):
        return h5str(self._handle["sensor_material"][()])

    @cached_property
    def sensor_thickness(self) -> pint.Quantity:
        thickness = self._handle["sensor_thickness"]
        units = thickness.attrs["units"]
        return thickness[()] * ureg(units)

    @cached_property
    def underload_value(self):
        if "underload_value" in self._handle:
            return self._handle["underload_value"][()]

    @cached_property
    def overload_value(self):
        if "overload_value" in self._handle:
            return self._handle["overload_value"][()]

    @cached_property
    def modules(self):
        return [NXdetector_module(module) for module in self._modules]

    @cached_property
    def type(self):
        if "type" in self._handle:
            return h5str(self._handle["type"][()])


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
        units = wavelength.attrs["units"]
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


def get_dependency_chain(
    transformation: NXtransformationsAxis,
) -> List[NXtransformationsAxis]:
    dependency_chain = []
    while transformation is not None:
        logging.debug(f"{transformation.path} =>")
        dependency_chain.append(transformation)
        transformation = transformation.depends_on
    return dependency_chain


def get_cumulative_transformation(
    dependency_chain: List[NXtransformationsAxis],
) -> np.ndarray:
    t = None
    for transformation in reversed(dependency_chain):
        transformation_type = transformation.transformation_type
        if transformation_type == "translation":
            assert transformation.units is not None
        values = np.atleast_1d(transformation[()])
        values = (
            values.to("mm")
            if transformation_type == "translation"
            else values.to("rad")
        )
        offset = transformation.offset
        t = Transformation(
            values.magnitude,
            transformation.vector,
            transformation_type,
            offset=offset,
            depends_on=t,
        )
    return t.compose()


Axes = namedtuple("axes", ["axes", "angles", "names", "is_scan_axis"])


def get_rotation_axes(dependency_chain: List[NXtransformationsAxis]) -> Axes:
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
