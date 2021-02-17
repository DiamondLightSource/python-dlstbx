import h5py
import numpy as np
import pytest

from . import get_dependency_chain, get_cumulative_transformation


@pytest.fixture
def nxsample(tmp_path):
    def _nxsample_wrapper(filename, **kwargs):
        def values(key):
            return kwargs.get(key, np.array([0.0]))

        f = h5py.File(filename, mode="w")

        entry = f.create_group("/entry")
        entry.attrs["NX_class"] = "NXentry"

        sample = entry.create_group("sample")
        sample.attrs["NX_class"] = "NXsample"
        sample.create_dataset("depends_on", data=b"/entry/sample/transformations/phi")

        transformations = sample.create_group("transformations")
        transformations.attrs["NX_class"] = b"NXtransformations"

        phi = transformations.create_dataset("phi", data=values("phi"))
        phi.attrs["depends_on"] = b"/entry/sample/transformations/chi"
        phi.attrs["equipment_component"] = b"goniometer"
        phi.attrs["transformation_type"] = b"rotation"
        phi.attrs["units"] = b"deg"
        phi.attrs["vector"] = np.array([-1.0, -0.0025, -0.0056])

        chi = transformations.create_dataset("chi", data=values("chi"))
        chi.attrs["depends_on"] = b"/entry/sample/transformations/sam_x"
        chi.attrs["equipment_component"] = b"goniometer"
        chi.attrs["transformation_type"] = b"rotation"
        chi.attrs["units"] = b"deg"
        chi.attrs["vector"] = np.array([0.006, -0.0264, 0.9996])

        sam_x = transformations.create_dataset("sam_x", data=values("sam_x"))
        sam_x.attrs["depends_on"] = b"/entry/sample/transformations/sam_y"
        sam_x.attrs["equipment_component"] = b"goniometer"
        sam_x.attrs["transformation_type"] = b"translation"
        sam_x.attrs["units"] = b"mm"
        sam_x.attrs["vector"] = np.array([1.0, 0.0, 0.0])

        sam_y = transformations.create_dataset("sam_y", data=values("sam_y"))
        sam_y.attrs["depends_on"] = b"/entry/sample/transformations/sam_z"
        sam_y.attrs["equipment_component"] = b"goniometer"
        sam_y.attrs["transformation_type"] = b"translation"
        sam_y.attrs["units"] = b"mm"
        sam_y.attrs["vector"] = np.array([0.0, 1.0, 0.0])

        sam_z = transformations.create_dataset("sam_z", data=values("sam_z"))
        sam_z.attrs["depends_on"] = b"/entry/sample/transformations/omega"
        sam_z.attrs["equipment_component"] = b"goniometer"
        sam_z.attrs["transformation_type"] = b"translation"
        sam_z.attrs["units"] = b"mm"
        sam_z.attrs["vector"] = np.array([0.0, 0.0, 1.0])

        omega = transformations.create_dataset("omega", data=values("omega"))
        omega.attrs["depends_on"] = b"/entry/sample/transformations/gon_x"
        omega.attrs["equipment_component"] = b"goniometer"
        omega.attrs["transformation_type"] = b"rotation"
        omega.attrs["units"] = b"deg"
        omega.attrs["vector"] = np.array([-1.0, 0.0, 0.0])

        gon_x = transformations.create_dataset("gon_x", data=values("gon_x"))
        gon_x.attrs["depends_on"] = b"/entry/sample/transformations/gon_y"
        gon_x.attrs["equipment_component"] = b"goniometer"
        gon_x.attrs["transformation_type"] = b"translation"
        gon_x.attrs["units"] = b"mm"
        gon_x.attrs["vector"] = np.array([1.0, 0.0, 0.0])

        gon_y = transformations.create_dataset("gon_y", data=values("gon_y"))
        gon_y.attrs["depends_on"] = b"/entry/sample/transformations/gon_z"
        gon_y.attrs["equipment_component"] = b"goniometer"
        gon_y.attrs["transformation_type"] = b"translation"
        gon_y.attrs["units"] = b"mm"
        gon_y.attrs["vector"] = np.array([0.0, 1.0, 0.0])

        gon_z = transformations.create_dataset("gon_z", data=values("gon_z"))
        gon_z.attrs["depends_on"] = b"."
        gon_z.attrs["equipment_component"] = b"goniometer"
        gon_z.attrs["transformation_type"] = b"translation"
        gon_z.attrs["units"] = b"mm"
        gon_z.attrs["vector"] = np.array([0.0, 0.0, 1.0])

    return _nxsample_wrapper


def test(nxsample, tmp_path):
    gon_x = np.array(
        [
            417.619,
            437.619,
            457.619,
            477.619,
            497.619,
            517.619,
            537.619,
            557.619,
            577.619,
            597.619,
            617.619,
            637.619,
            657.619,
            657.619,
            637.619,
            617.619,
            597.619,
            577.619,
            557.619,
            537.619,
            517.619,
            497.619,
            477.619,
            457.619,
            437.619,
            417.619,
            417.619,
            437.619,
            457.619,
            477.619,
            497.619,
            517.619,
            537.619,
            557.619,
            577.619,
            597.619,
            617.619,
            637.619,
            657.619,
            657.619,
            637.619,
            617.619,
            597.619,
            577.619,
            557.619,
            537.619,
            517.619,
            497.619,
            477.619,
            457.619,
            437.619,
            417.619,
            417.619,
            437.619,
            457.619,
            477.619,
            497.619,
            517.619,
            537.619,
            557.619,
            577.619,
            597.619,
            617.619,
            637.619,
            657.619,
            657.619,
            637.619,
            617.619,
            597.619,
            577.619,
            557.619,
            537.619,
            517.619,
            497.619,
            477.619,
            457.619,
            437.619,
            417.619,
            417.619,
            437.619,
            457.619,
            477.619,
            497.619,
            517.619,
            537.619,
            557.619,
            577.619,
            597.619,
            617.619,
            637.619,
            657.619,
            657.619,
            637.619,
            617.619,
            597.619,
            577.619,
            557.619,
            537.619,
            517.619,
            497.619,
            477.619,
            457.619,
            437.619,
            417.619,
            417.619,
            437.619,
            457.619,
            477.619,
            497.619,
            517.619,
            537.619,
            557.619,
            577.619,
            597.619,
            617.619,
            637.619,
            657.619,
            657.619,
            637.619,
            617.619,
            597.619,
            577.619,
            557.619,
            537.619,
            517.619,
            497.619,
            477.619,
            457.619,
            437.619,
            417.619,
            417.619,
            437.619,
            457.619,
            477.619,
            497.619,
            517.619,
            537.619,
            557.619,
            577.619,
            597.619,
            617.619,
            637.619,
            657.619,
        ]
    )
    gon_y = np.array(
        [
            359.073,
            359.073,
            359.073,
            359.073,
            359.073,
            359.073,
            359.073,
            359.073,
            359.073,
            359.073,
            359.073,
            359.073,
            359.073,
            379.073,
            379.073,
            379.073,
            379.073,
            379.073,
            379.073,
            379.073,
            379.073,
            379.073,
            379.073,
            379.073,
            379.073,
            379.073,
            399.073,
            399.073,
            399.073,
            399.073,
            399.073,
            399.073,
            399.073,
            399.073,
            399.073,
            399.073,
            399.073,
            399.073,
            399.073,
            419.073,
            419.073,
            419.073,
            419.073,
            419.073,
            419.073,
            419.073,
            419.073,
            419.073,
            419.073,
            419.073,
            419.073,
            419.073,
            439.073,
            439.073,
            439.073,
            439.073,
            439.073,
            439.073,
            439.073,
            439.073,
            439.073,
            439.073,
            439.073,
            439.073,
            439.073,
            459.073,
            459.073,
            459.073,
            459.073,
            459.073,
            459.073,
            459.073,
            459.073,
            459.073,
            459.073,
            459.073,
            459.073,
            459.073,
            479.073,
            479.073,
            479.073,
            479.073,
            479.073,
            479.073,
            479.073,
            479.073,
            479.073,
            479.073,
            479.073,
            479.073,
            479.073,
            499.073,
            499.073,
            499.073,
            499.073,
            499.073,
            499.073,
            499.073,
            499.073,
            499.073,
            499.073,
            499.073,
            499.073,
            499.073,
            519.073,
            519.073,
            519.073,
            519.073,
            519.073,
            519.073,
            519.073,
            519.073,
            519.073,
            519.073,
            519.073,
            519.073,
            519.073,
            539.073,
            539.073,
            539.073,
            539.073,
            539.073,
            539.073,
            539.073,
            539.073,
            539.073,
            539.073,
            539.073,
            539.073,
            539.073,
            559.073,
            559.073,
            559.073,
            559.073,
            559.073,
            559.073,
            559.073,
            559.073,
            559.073,
            559.073,
            559.073,
            559.073,
            559.073,
        ]
    )

    nxs = tmp_path / "nxsample_omega_0.nxs"
    # nxsample(filename=nxs, gon_x=gon_x, gon_y=gon_y, omega=np.array([0.]))
    nxsample(
        filename=nxs,
        gon_x=np.array([gon_x.mean()]),
        gon_y=np.array([gon_y.mean()]),
        omega=np.array([0.0]),
    )
    f = h5py.File(nxs)
    sample = f["/entry/sample"]
    depends_on = sample["depends_on"][()]
    dependency_chain = get_dependency_chain(f[depends_on])
    print(sample["/entry/sample/transformations/omega"][()])
    A = get_cumulative_transformation(dependency_chain)
    print(f"Final A:\n{A[0].round(3)}")
    x = gon_x
    y = gon_y
    z = sample["/entry/sample/transformations/gon_z"][()]
    # coords = np.vstack([x, y, np.repeat(z, x.size), np.zeros(x.size)]).T
    coords = np.vstack(
        [x - x.mean(), y - y.mean(), np.repeat(z, x.size), np.zeros(x.size)]
    ).T
    coords_o0 = np.array([A[0] @ c for c in coords])

    nxs = tmp_path / "nxsample_omega_45.nxs"
    # nxsample(filename=nxs, gon_x=gon_x, gon_y=gon_y, omega=np.array([45.]))
    nxsample(
        filename=nxs,
        gon_x=np.array([gon_x.mean()]),
        gon_y=np.array([gon_y.mean()]),
        omega=np.array([45.0]),
    )
    f = h5py.File(nxs)
    sample = f["/entry/sample"]
    depends_on = sample["depends_on"][()]
    print(sample["/entry/sample/transformations/omega"][()])
    dependency_chain = get_dependency_chain(f[depends_on])
    A = get_cumulative_transformation(dependency_chain)
    print(f"Final A:\n{A[0].round(3)}")
    x = gon_x
    y = gon_y
    z = sample["/entry/sample/transformations/gon_z"][()]
    # coords = np.vstack([x, y, np.repeat(z, x.size), np.zeros(x.size)]).T
    coords = np.vstack(
        [x - x.mean(), y - y.mean(), np.repeat(z, x.size), np.zeros(x.size)]
    ).T
    coords_o45 = np.array([A[0] @ c for c in coords])

    nxs = tmp_path / "nxsample_omega_90.nxs"
    # nxsample(filename=nxs, gon_x=gon_x, gon_y=gon_y, omega=np.array([90.]))
    nxsample(
        filename=nxs,
        gon_x=np.array([gon_x.mean()]),
        gon_y=np.array([gon_y.mean()]),
        omega=np.array([90.0]),
    )
    f = h5py.File(nxs)
    sample = f["/entry/sample"]
    depends_on = sample["depends_on"][()]
    print(sample["/entry/sample/transformations/omega"][()])
    dependency_chain = get_dependency_chain(f[depends_on])
    A = get_cumulative_transformation(dependency_chain)
    print(f"Final A:\n{A[0].round(3)}")
    x = gon_x
    y = gon_y
    z = sample["/entry/sample/transformations/gon_z"][()]
    # coords = np.vstack([x, y, np.repeat(z, x.size), np.zeros(x.size)]).T
    coords = np.vstack(
        [x - x.mean(), y - y.mean(), np.repeat(z, x.size), np.zeros(x.size)]
    ).T
    coords_o90 = np.array([A[0] @ c for c in coords])

    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

    fig = plt.figure()
    ax = fig.add_subplot(111, projection="3d")
    ax.plot(coords_o0[:, 0], coords_o0[:, 1], coords_o0[:, 2])
    ax.plot(coords_o45[:, 0], coords_o45[:, 1], coords_o45[:, 2])
    ax.plot(coords_o90[:, 0], coords_o90[:, 1], coords_o90[:, 2])
    plt.show()
