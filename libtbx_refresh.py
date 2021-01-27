import libtbx.pkg_utils

try:
    import dials.precommitbx.nagger

    dials.precommitbx.nagger.nag()
except ImportError:
    pass

# Undefine libtbx.dlstbx entry points
libtbx.pkg_utils.define_entry_points({})

try:
    import dlstbx.requirements

    if dlstbx.requirements.check():
        print(
            "You can run 'python -m dlstbx.requirements' to update all packages indiscriminately."
        )
        print(
            "Note that this will overwrite any 'pip install -e' local installations you may have."
        )
except ModuleNotFoundError:
    print("Could not import dlstbx")


def _install_dlstbx_setup():
    """Install dlstbx as a regular/editable python package"""
    import subprocess
    import sys

    import libtbx.load_env

    # Call pip
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "-e",
            libtbx.env.dist_path("dlstbx"),
        ],
        check=True,
    )


def _show_dlstbx_version():
    try:
        from dlstbx.util.version import dlstbx_version

        # the import implicitly updates the .gitversion file
        print(dlstbx_version())
    except ModuleNotFoundError:
        print("Can't tell dlstbx version")


_install_dlstbx_setup()
_show_dlstbx_version()
