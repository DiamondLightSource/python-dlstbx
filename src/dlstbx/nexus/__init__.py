import numpy as np

import dxtbx.model
from scitbx.array_family import flex

from . import nxmx


def get_dxtbx_goniometer(sample):
    dependency_chain = nxmx.get_dependency_chain(sample.depends_on)
    axes = nxmx.get_rotation_axes(dependency_chain)
    R = np.array((-1, 0, 0, 0, 1, 0, 0, 0, -1)).reshape((3, 3))
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
