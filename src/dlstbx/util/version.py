from __future__ import annotations

import pathlib

from dials.util.version import get_git_version

# DLSTBX version numbers are constructed from
#  1. a common prefix
__dlstbx_version_format = "dlstbx %s"
#  2. the most recent annotated git tag (or failing that: a default string)
__dlstbx_version_default = "1.dev"
#  3. a dash followed by the number of commits since that tag
#  4. a dash followed by a lowercase 'g' and the current commit id

# When run from a development installation the version information is extracted
# from the git repository. Otherwise it is read from the file '.gitversion' in
# the module directory.


def dlstbx_version():
    """Try to obtain the current git revision number
    and store a copy in .gitversion"""
    version = None

    try:
        dlstbx_path = pathlib.Path(__file__).parents[3]
        version_file = dlstbx_path / ".gitversion"

        # 1. Try to access information in .git directory
        #    Regenerate .gitversion if possible
        if dlstbx_path.joinpath(".git").is_dir():
            try:
                version = get_git_version(dlstbx_path)
                version_file.write_text(version)
            except Exception:
                if version == "":
                    version = None

        # 2. If .git directory missing or 'git describe' failed, read .gitversion
        if (version is None) and version_file.is_file():
            version = version_file.read_text().rstrip()
    except Exception:
        pass

    if version is None:
        version = __dlstbx_version_format % __dlstbx_version_default
    else:
        version = __dlstbx_version_format % version

    return version


if __name__ == "__main__":
    print(dlstbx_version())
