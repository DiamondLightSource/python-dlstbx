import h5py
import numpy as np
import pytest
from scipy.spatial.transform import Rotation

from . import get_dependency_chain, get_cumulative_transformation

import dlstbx.nexus


@pytest.fixture
def nxsample(tmp_path):
    def _nxsample_wrapper(filename, **kwargs):
        def values(key):
            return kwargs.get(key, np.array([0.0]))

        f = h5py.File(filename, mode="w")

        entry = f.create_group("/entry")
        entry.attrs["NX_class"] = "NXentry"
        entry["definition"] = "NXmx"

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
        if "phi_offset" in kwargs:
            phi.attrs["offset"] = kwargs["phi_offset"]

        chi = transformations.create_dataset("chi", data=values("chi"))
        chi.attrs["depends_on"] = b"/entry/sample/transformations/sam_x"
        chi.attrs["equipment_component"] = b"goniometer"
        chi.attrs["transformation_type"] = b"rotation"
        chi.attrs["units"] = b"deg"
        chi.attrs["vector"] = np.array([0.006, -0.0264, 0.9996])
        if "chi_offset" in kwargs:
            chi.attrs["offset"] = kwargs["chi_offset"]

        sam_x = transformations.create_dataset("sam_x", data=values("sam_x"))
        sam_x.attrs["depends_on"] = b"/entry/sample/transformations/sam_y"
        sam_x.attrs["equipment_component"] = b"goniometer"
        sam_x.attrs["transformation_type"] = b"translation"
        sam_x.attrs["units"] = b"mm"
        sam_x.attrs["vector"] = np.array([1.0, 0.0, 0.0])
        if "sam_x_offset" in kwargs:
            sam_x.attrs["offset"] = kwargs["sam_x_offset"]

        sam_y = transformations.create_dataset("sam_y", data=values("sam_y"))
        sam_y.attrs["depends_on"] = b"/entry/sample/transformations/sam_z"
        sam_y.attrs["equipment_component"] = b"goniometer"
        sam_y.attrs["transformation_type"] = b"translation"
        sam_y.attrs["units"] = b"mm"
        sam_y.attrs["vector"] = np.array([0.0, 1.0, 0.0])
        if "sam_y_offset" in kwargs:
            sam_y.attrs["offset"] = kwargs["sam_y_offset"]

        sam_z = transformations.create_dataset("sam_z", data=values("sam_z"))
        sam_z.attrs["depends_on"] = b"/entry/sample/transformations/omega"
        sam_z.attrs["equipment_component"] = b"goniometer"
        sam_z.attrs["transformation_type"] = b"translation"
        sam_z.attrs["units"] = b"mm"
        sam_z.attrs["vector"] = np.array([0.0, 0.0, 1.0])
        if "sam_z_offset" in kwargs:
            sam_z.attrs["offset"] = kwargs["sam_z_offset"]

        omega = transformations.create_dataset("omega", data=values("omega"))
        omega.attrs["depends_on"] = b"/entry/sample/transformations/gon_x"
        omega.attrs["equipment_component"] = b"goniometer"
        omega.attrs["transformation_type"] = b"rotation"
        omega.attrs["units"] = b"deg"
        omega.attrs["vector"] = np.array([-1.0, 0.0, 0.0])
        if "omega_offset" in kwargs:
            omega.attrs["offset"] = kwargs["omega_offset"]

        gon_x = transformations.create_dataset("gon_x", data=values("gon_x"))
        gon_x.attrs["depends_on"] = b"/entry/sample/transformations/gon_y"
        gon_x.attrs["equipment_component"] = b"goniometer"
        gon_x.attrs["transformation_type"] = b"translation"
        gon_x.attrs["units"] = b"mm"
        gon_x.attrs["vector"] = np.array([1.0, 0.0, 0.0])
        if "gon_x_offset" in kwargs:
            gon_x.attrs["offset"] = kwargs["gon_x_offset"]

        gon_y = transformations.create_dataset("gon_y", data=values("gon_y"))
        gon_y.attrs["depends_on"] = b"/entry/sample/transformations/gon_z"
        gon_y.attrs["equipment_component"] = b"goniometer"
        gon_y.attrs["transformation_type"] = b"translation"
        gon_y.attrs["units"] = b"mm"
        gon_y.attrs["vector"] = np.array([0.0, 1.0, 0.0])
        if "gon_y_offset" in kwargs:
            gon_y.attrs["offset"] = kwargs["gon_y_offset"]

        gon_z = transformations.create_dataset("gon_z", data=values("gon_z"))
        gon_z.attrs["depends_on"] = b"."
        gon_z.attrs["equipment_component"] = b"goniometer"
        gon_z.attrs["transformation_type"] = b"translation"
        gon_z.attrs["units"] = b"mm"
        gon_z.attrs["vector"] = np.array([0.0, 0.0, 1.0])
        if "gon_z_offset" in kwargs:
            gon_z.attrs["offset"] = kwargs.get("gon_z_offset")

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

    omega_offset = np.array((gon_x.mean(), gon_y.mean(), 0))
    coords = np.vstack(
        [
            gon_x - gon_x.mean(),
            gon_y - gon_y.mean(),
            np.zeros(gon_x.size),
            np.ones(gon_x.size),
        ]
    ).T

    nxs = tmp_path / "nxsample_omega_0.nxs"
    nxsample(
        filename=nxs,
        gon_x=coords[:, 0],
        gon_y=coords[:, 1],
        omega=np.array([0.0]),
        omega_offset=omega_offset,
    )
    f = h5py.File(nxs)
    nxmx = dlstbx.nexus.NXmx(f)
    sample = nxmx.entries[0].samples[0]
    dependency_chain = get_dependency_chain(sample.depends_on)
    A = get_cumulative_transformation(dependency_chain)
    print(f"Final A:\n{A[0].round(3)}")
    coords_o0 = np.array([A[0] @ c for c in coords])

    nxs = tmp_path / "nxsample_omega_45.nxs"
    nxsample(
        filename=nxs,
        gon_x=coords[:, 0],
        gon_y=coords[:, 1],
        omega=np.array([45.0]),
        omega_offset=omega_offset,
    )
    f = h5py.File(nxs)
    nxmx = dlstbx.nexus.NXmx(f)
    sample = nxmx.entries[0].samples[0]
    dependency_chain = get_dependency_chain(sample.depends_on)
    A = get_cumulative_transformation(dependency_chain)
    print(f"Final A:\n{A[0].round(3)}")
    coords_o45 = np.array([A[0] @ c for c in coords])

    nxs = tmp_path / "nxsample_omega_90.nxs"
    nxsample(
        filename=nxs,
        gon_x=coords[:, 0],
        gon_y=coords[:, 1],
        omega=np.array([90.0]),
        omega_offset=omega_offset,
    )
    f = h5py.File(nxs)
    nxmx = dlstbx.nexus.NXmx(f)
    sample = nxmx.entries[0].samples[0]
    dependency_chain = get_dependency_chain(sample.depends_on)
    A = get_cumulative_transformation(dependency_chain)
    print(f"Final A:\n{A[0].round(3)}")
    coords_o90 = np.array([A[0] @ c for c in coords])
    # coords_o90 = np.array([a @ c for a, c in zip(A, coords)])

    print(coords_o0)
    print(coords_o45)
    print(coords_o90)

    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

    fig = plt.figure()
    ax = fig.add_subplot(111, projection="3d")
    ax.plot(coords_o0[:, 0], coords_o0[:, 1], coords_o0[:, 2])
    ax.plot(coords_o45[:, 0], coords_o45[:, 1], coords_o45[:, 2])
    ax.plot(coords_o90[:, 0], coords_o90[:, 1], coords_o90[:, 2])
    plt.show()


def test_smargon_sample_stages(nxsample, tmp_path):
    sam_xyz = np.array(
        [
            [-120.0, -100.0, 0.0],
            [-100.0, -100.0, 0.0],
            [-80.0, -100.0, 0.0],
            [-60.0, -100.0, 0.0],
            [-40.0, -100.0, 0.0],
            [-20.0, -100.0, 0.0],
            [0.0, -100.0, 0.0],
            [20.0, -100.0, 0.0],
            [40.0, -100.0, 0.0],
            [60.0, -100.0, 0.0],
            [80.0, -100.0, 0.0],
            [100.0, -100.0, 0.0],
            [120.0, -100.0, 0.0],
            [120.0, -80.0, 0.0],
            [100.0, -80.0, 0.0],
            [80.0, -80.0, 0.0],
            [60.0, -80.0, 0.0],
            [40.0, -80.0, 0.0],
            [20.0, -80.0, 0.0],
            [0.0, -80.0, 0.0],
            [-20.0, -80.0, 0.0],
            [-40.0, -80.0, 0.0],
            [-60.0, -80.0, 0.0],
            [-80.0, -80.0, 0.0],
            [-100.0, -80.0, 0.0],
            [-120.0, -80.0, 0.0],
            [-120.0, -60.0, 0.0],
            [-100.0, -60.0, 0.0],
            [-80.0, -60.0, 0.0],
            [-60.0, -60.0, 0.0],
            [-40.0, -60.0, 0.0],
            [-20.0, -60.0, 0.0],
            [0.0, -60.0, 0.0],
            [20.0, -60.0, 0.0],
            [40.0, -60.0, 0.0],
            [60.0, -60.0, 0.0],
            [80.0, -60.0, 0.0],
            [100.0, -60.0, 0.0],
            [120.0, -60.0, 0.0],
            [120.0, -40.0, 0.0],
            [100.0, -40.0, 0.0],
            [80.0, -40.0, 0.0],
            [60.0, -40.0, 0.0],
            [40.0, -40.0, 0.0],
            [20.0, -40.0, 0.0],
            [0.0, -40.0, 0.0],
            [-20.0, -40.0, 0.0],
            [-40.0, -40.0, 0.0],
            [-60.0, -40.0, 0.0],
            [-80.0, -40.0, 0.0],
            [-100.0, -40.0, 0.0],
            [-120.0, -40.0, 0.0],
            [-120.0, -20.0, 0.0],
            [-100.0, -20.0, 0.0],
            [-80.0, -20.0, 0.0],
            [-60.0, -20.0, 0.0],
            [-40.0, -20.0, 0.0],
            [-20.0, -20.0, 0.0],
            [0.0, -20.0, 0.0],
            [20.0, -20.0, 0.0],
            [40.0, -20.0, 0.0],
            [60.0, -20.0, 0.0],
            [80.0, -20.0, 0.0],
            [100.0, -20.0, 0.0],
            [120.0, -20.0, 0.0],
            [120.0, 0.0, 0.0],
            [100.0, 0.0, 0.0],
            [80.0, 0.0, 0.0],
            [60.0, 0.0, 0.0],
            [40.0, 0.0, 0.0],
            [20.0, 0.0, 0.0],
            [0.0, 0.0, 0.0],
            [-20.0, 0.0, 0.0],
            [-40.0, 0.0, 0.0],
            [-60.0, 0.0, 0.0],
            [-80.0, 0.0, 0.0],
            [-100.0, 0.0, 0.0],
            [-120.0, 0.0, 0.0],
            [-120.0, 20.0, 0.0],
            [-100.0, 20.0, 0.0],
            [-80.0, 20.0, 0.0],
            [-60.0, 20.0, 0.0],
            [-40.0, 20.0, 0.0],
            [-20.0, 20.0, 0.0],
            [0.0, 20.0, 0.0],
            [20.0, 20.0, 0.0],
            [40.0, 20.0, 0.0],
            [60.0, 20.0, 0.0],
            [80.0, 20.0, 0.0],
            [100.0, 20.0, 0.0],
            [120.0, 20.0, 0.0],
            [120.0, 40.0, 0.0],
            [100.0, 40.0, 0.0],
            [80.0, 40.0, 0.0],
            [60.0, 40.0, 0.0],
            [40.0, 40.0, 0.0],
            [20.0, 40.0, 0.0],
            [0.0, 40.0, 0.0],
            [-20.0, 40.0, 0.0],
            [-40.0, 40.0, 0.0],
            [-60.0, 40.0, 0.0],
            [-80.0, 40.0, 0.0],
            [-100.0, 40.0, 0.0],
            [-120.0, 40.0, 0.0],
            [-120.0, 60.0, 0.0],
            [-100.0, 60.0, 0.0],
            [-80.0, 60.0, 0.0],
            [-60.0, 60.0, 0.0],
            [-40.0, 60.0, 0.0],
            [-20.0, 60.0, 0.0],
            [0.0, 60.0, 0.0],
            [20.0, 60.0, 0.0],
            [40.0, 60.0, 0.0],
            [60.0, 60.0, 0.0],
            [80.0, 60.0, 0.0],
            [100.0, 60.0, 0.0],
            [120.0, 60.0, 0.0],
            [120.0, 80.0, 0.0],
            [100.0, 80.0, 0.0],
            [80.0, 80.0, 0.0],
            [60.0, 80.0, 0.0],
            [40.0, 80.0, 0.0],
            [20.0, 80.0, 0.0],
            [0.0, 80.0, 0.0],
            [-20.0, 80.0, 0.0],
            [-40.0, 80.0, 0.0],
            [-60.0, 80.0, 0.0],
            [-80.0, 80.0, 0.0],
            [-100.0, 80.0, 0.0],
            [-120.0, 80.0, 0.0],
            [-120.0, 100.0, 0.0],
            [-100.0, 100.0, 0.0],
            [-80.0, 100.0, 0.0],
            [-60.0, 100.0, 0.0],
            [-40.0, 100.0, 0.0],
            [-20.0, 100.0, 0.0],
            [0.0, 100.0, 0.0],
            [20.0, 100.0, 0.0],
            [40.0, 100.0, 0.0],
            [60.0, 100.0, 0.0],
            [80.0, 100.0, 0.0],
            [100.0, 100.0, 0.0],
            [120.0, 100.0, 0.0],
        ]
    )
    R45 = Rotation.from_rotvec(np.pi / 4 * np.array((-1, 0, 0))).as_matrix()
    sam_xyz_o45 = np.array([R45 @ xyz for xyz in sam_xyz])
    R90 = Rotation.from_rotvec(np.pi / 2 * np.array((-1, 0, 0))).as_matrix()
    sam_xyz_o90 = np.array([R90 @ xyz for xyz in sam_xyz])
    coords = np.ones((len(sam_xyz), 4))
    coords[:, :-1] = sam_xyz

    nxs = tmp_path / "nxsample_omega_0.nxs"
    nxsample(
        filename=nxs,
        sam_x=sam_xyz[:, 0],
        sam_y=sam_xyz[:, 1],
        sam_z=sam_xyz[:, 2],
    )
    f = h5py.File(nxs)
    nxmx = dlstbx.nexus.NXmx(f)
    sample = nxmx.entries[0].samples[0]
    dependency_chain = get_dependency_chain(sample.depends_on)
    A = get_cumulative_transformation(dependency_chain)
    print(f"Final A:\n{A[0].round(3)}")
    # coords_o0 = (A[0] @ coords.T).T
    coords_o0 = A @ np.array((0, 0, 0, 1))

    nxs = tmp_path / "nxsample_omega_45.nxs"
    nxsample(
        filename=nxs,
        sam_x=sam_xyz_o45[:, 0],
        sam_y=sam_xyz_o45[:, 1],
        sam_z=sam_xyz_o45[:, 2],
    )
    f = h5py.File(nxs)
    nxmx = dlstbx.nexus.NXmx(f)
    sample = nxmx.entries[0].samples[0]
    dependency_chain = get_dependency_chain(sample.depends_on)
    A = get_cumulative_transformation(dependency_chain)
    print(f"Final A:\n{A[0].round(3)}")
    # coords_o45 = (A[0] @ coords.T).T
    coords_o45 = A @ np.array((0, 0, 0, 1))

    nxs = tmp_path / "nxsample_omega_90.nxs"
    nxsample(
        filename=nxs,
        sam_x=sam_xyz_o90[:, 0],
        sam_y=sam_xyz_o90[:, 1],
        sam_z=sam_xyz_o90[:, 2],
    )
    f = h5py.File(nxs)
    nxmx = dlstbx.nexus.NXmx(f)
    sample = nxmx.entries[0].samples[0]
    dependency_chain = get_dependency_chain(sample.depends_on)
    A = get_cumulative_transformation(dependency_chain)
    print(f"Final A:\n{A[0].round(3)}")
    # coords_o90 = (A[0] @ coords.T).T
    coords_o90 = A @ np.array((0, 0, 0, 1))

    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

    fig = plt.figure()
    ax = fig.add_subplot(111, projection="3d")
    ax.plot(coords_o0[:, 0], coords_o0[:, 1], coords_o0[:, 2])
    ax.plot(coords_o45[:, 0], coords_o45[:, 1], coords_o45[:, 2])
    ax.plot(coords_o90[:, 0], coords_o90[:, 1], coords_o90[:, 2])
    plt.show()
