import numpy as np
import pint

from scipy.spatial.transform import Rotation

ureg = pint.UnitRegistry()


class Transformation:
    def __init__(self, values, vector, transformation_type, units, depends_on=None):
        self.name = values.name
        self.values = values[()] * ureg(units)
        self.vector = np.repeat(vector.reshape(1, vector.size), values.size, axis=0)
        self.transformation_type = transformation_type
        self.units = ureg(units)
        self.depends_on = depends_on

    def compose(self):
        if self.transformation_type == "rotation":
            values = (self.values[:, np.newaxis] * self.units.to("rad")).magnitude
            R = Rotation.from_rotvec(values * self.vector).as_matrix()
            T = np.zeros((self.values.size, 3))
        else:
            values = (self.values * self.units.to("mm")).magnitude
            R = np.identity(3)
            T = values[:, np.newaxis] * self.vector
        A = np.repeat(np.identity(4).reshape((1, 4, 4)), self.values.size, axis=0)
        A[:, :3, :3] = R
        A[:, :3, 3] = T
        print(f"{self.name}:\n{A.round(3)[0]}")
        if self.depends_on:
            return self.depends_on.compose() @ A
        return A


def get_dependency_chain(transformation):
    dependency_chain = []
    while True:
        print(f"{transformation.name} =>")
        dependency_chain.append(transformation)
        depends_on = transformation.attrs.get("depends_on", ".")
        if depends_on == ".":
            break
        transformation = transformation.parent[depends_on]
    return dependency_chain


def get_cumulative_transformation(dependency_chain):
    t = None
    for transformation in reversed(dependency_chain):
        t = Transformation(
            transformation,
            transformation.attrs["vector"],
            transformation.attrs["transformation_type"],
            transformation.attrs["units"],
            depends_on=t,
        )
    return t.compose()
