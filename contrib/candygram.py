"""
# Candygram: A sweeter dispatch

For libtbx-based DIALS installs, generate executable scripts that behave
somewhat like libtbx dispatchers, for packages that otherwise rely on
entry_points.console_scripts being able to interact with dials/cctbx.

Usage:

    libtbx.python "/path/to/candygram.py" zocalo dials dlstbx [.....]

## Background

At some point after DIALS 3.21 but before DIALS 3.22, libtbx removed the
functionality that generated libtbx-style dispatchers for other
installed python packages with registered libtbx.dispatcher.script entry
points. This includes dlstbx (as well as pytest, which we explicitly
registered). So, before, running libtbx.refresh would cause a copy of
each of the dlstbx console_scripts to have an entry in build/bin that
had a libtbx-style dispatcher which:

    a) Set up the PYTHONPATH to point to the modules/ modules (e.g.
       dials).
    b) Set up libtbx-specific environment variables to find libtbx.env.
    c) Set LD_LIBRARY_PATH to build/lib for libtbx-built libraries.

Because libtbx deliberately does not install python packages for modules
in conda_base, this meant that anything outside of the libtbx_refresh.py
system couldn't import dials/dxtbx/anything else in cctbx.

## The Solution

For every module passed to candygram:
  - Python scripts with hardcoded interpreter paths are written to
    build/bin for every console_scripts entry. These properly set the
    libtbx environment (e.g. LIBTBX_DISPATCHER_NAME) but otherwise are
    close to the usual pip console_scripts template.
  - Update the site configuration to know what LIBTBX_BUILD is.
  - Write a .pth file pointer to site-packages that points to all of the
    extra PYTHONPATH that libtbx dispatchers normally set.
  - Uses patchelf to rewrite the library RPATH for every library in
    build/lib. This is so that they can find each other, because they
    are built without correct relative entries and the old libtbx
    dispatchers masked this by setting LD_LIBRARY_PATH.
"""

from __future__ import annotations

import importlib.metadata
import os
import shutil
import site
import subprocess
import sys
from argparse import ArgumentParser
from pathlib import Path

try:
    import libtbx.load_env
except ModuleNotFoundError:
    sys.exit("Error: Could not import libtbx. Please run using libtbx.python")

try:
    from rich import print
except ModuleNotFoundError:
    pass

KNOWN_PATCHELF = shutil.which("patchelf") or "/dls_sw/apps/patchelf/0.17.2/bin/patchelf"
if not Path(KNOWN_PATCHELF).is_file():
    sys.exit("Error: Must have patchelf tool installed to run")

parser = ArgumentParser(
    description="Regenerate sweeter dispatchers for console_scripts and other executables."
)
parser.add_argument(
    "packages",
    metavar="PACKAGE",
    help="List of package or script file names to create new dispatchers for.",
    nargs="+",
)
args = parser.parse_args()

# Work out what the distribution is
if "LIBTBX_BUILD" not in os.environ:
    if "LIBTBX_PREFIX" in os.environ:
        sys.exit(
            "Error: No LIBTBX_BUILD but LIBTBX_PREFIX: Will not rewrite environments that look like prebuilt."
        )
    sys.exit("Error: No LIBTBX_BUILD: Please run this script from libtbx.python")
args.distribution = Path(os.environ["LIBTBX_BUILD"]).parent

# Validate the DIALS distribution
build_bin = args.distribution / "build" / "bin"
base_bin = args.distribution / "conda_base" / "bin"
if (
    not args.distribution.joinpath("conda_base").is_dir()
    or not args.distribution.joinpath("build").is_dir()
    or not args.distribution.joinpath("build", "libtbx_env").is_file()
    or not build_bin.is_dir()
):
    print(
        f"Error: {args.distribution} does not appear to be a valid, libtbx distribution"
    )
    sys.exit(1)
print(f"Modifying DIALS distribution at: {args.distribution.resolve()}")


def get_site_packages() -> Path:
    _site_packages = site.getsitepackages()
    assert len(_site_packages) == 1
    return Path(_site_packages[0])


# Make sure that we set up the cctbx import locations in the sitecustomize
def insert_site_customization(code: str) -> None:
    site_packages = get_site_packages()
    code = code.strip()
    # print("Setting up sitecustomize.py")
    sitefile = site_packages / "sitecustomize.py"
    contents = sitefile.read_text() if sitefile.is_file() else ""
    start_line = "# >>> dials-cctbx integration >>>"
    end_line = "# <<< dials-cctbx integration <<<"
    assert start_line != end_line
    if (start_line + "\n") not in contents:
        # Just writing, easy
        prefix = "" if (not contents or contents.endswith("\n")) else "\n\n"
        contents += "\n".join([prefix, start_line, code.strip(), end_line]) + "\n"
        print(f"Writing new section into {sitefile}")
        sitefile.write_text(contents.strip() + "\n")
    else:
        start = contents.index(start_line)
        end = contents.index(end_line)
        existing = contents[start + len(start_line) : end].strip()
        if existing == code:
            print("No updates for sitecustomize.py")
            return
        # We need to replace the existing
        contents = contents[:start] + "\n".join([start_line, code, ""]) + contents[end:]
        print(f"Updating site customization in {sitefile}")
        sitefile.write_text(contents.strip() + "\n")


def write_new_script(entrypoint: importlib.metadata.EntryPoint, bin_path: Path) -> None:
    dest = bin_path / entrypoint.name
    assert ":" in entrypoint.value
    imp, sym = entrypoint.value.split(":")
    script = f"""#!{base_bin.resolve()}/python3
import os
import sys
from {imp} import {sym}

os.environ["LIBTBX_DISPATCHER_NAME"] = "{entrypoint.name}"

if __name__ == "__main__":
    {sym}()
"""
    if dest.is_file() and dest.read_text() == script:
        return

    dest.write_text(script)
    os.chmod(dest, 0o755)
    return


# Get a list of paths listed in the libtbx.env.
# Note that this often _does_ include the site-packages folder, as this
# is redundantly re-injected into the environment.
extra_paths = [Path(abs(x)).resolve() for x in libtbx.env.pythonpath]
build_lib = (args.distribution / "build" / "lib").resolve()
base_lib = (args.distribution / "conda_base" / "lib").resolve()

# Write extra path locations as a pth file
(get_site_packages() / "dials.pth").write_text("\n".join(str(x) for x in extra_paths))

# We need to insert the global LIBTBX_BUILD folder as a site customization
insert_site_customization(f"""
import os
os.environ["LIBTBX_BUILD"] = "{args.distribution.absolute() / "build"}"
""")

for pkg in args.packages:
    print(f"Handling {pkg}")
    # First, handle any package named this
    try:
        dist = importlib.metadata.distribution(pkg)
    except importlib.metadata.PackageNotFoundError:
        pass
    else:
        for script in dist.entry_points.select(group="console_scripts"):
            print(f"  Handling console_scripts {script.name}")
            write_new_script(script, build_bin)
        for script in dist.entry_points.select(group="gui_scripts"):
            print(f"  Handling gui_scripts {script.name}")
            write_new_script(script, build_bin)

# Rewrite RPATH for everything in build/lib
cmd = [
    KNOWN_PATCHELF,
    "--set-rpath",
    f"{base_lib}:{build_lib}",
    *[str(x) for x in build_lib.glob("*.so")],
]
print(f"+ {KNOWN_PATCHELF} --set-rpath {base_lib}:{build_lib} {build_lib}/*.so")
subprocess.run(cmd, check=True)

print("\nAll done!")
