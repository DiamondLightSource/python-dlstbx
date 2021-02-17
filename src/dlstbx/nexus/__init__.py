import numpy as np
import pint

from scipy.spatial.transform import Rotation

ureg = pint.UnitRegistry()


class Transformation:
    def __init__(
        self, values, vector, transformation_type, offset=None, depends_on=None
    ):
        self.values = values
        self.vector = np.repeat(vector.reshape(1, vector.size), values.size, axis=0)
        self.transformation_type = transformation_type
        self.offset = offset
        self.depends_on = depends_on

    def compose(self):
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
        transformation_type = transformation.attrs["transformation_type"]
        values = transformation[()] * ureg(transformation.attrs["units"])
        values = (
            values.to("mm")
            if transformation_type == "translation"
            else values.to("rad")
        )
        offset = transformation.attrs.get("offset")
        t = Transformation(
            values.magnitude,
            transformation.attrs["vector"],
            transformation_type,
            offset=offset,
            depends_on=t,
        )
    return t.compose()
