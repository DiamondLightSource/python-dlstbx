# Check conda environment fulfills all requirements

# can also be run directly to install missing packages
# command line options are passed through to the conda install process

import dlstbx
import json
import os
import pathlib
import pkg_resources
import subprocess
import sys

dlstbx_path = dlstbx.__file__

try:
    import conda.cli.python_api
except ImportError:
    conda = None


def _notice(*lines, **context):
    print(
        os.linesep
        + "=" * 80
        + os.linesep
        + os.linesep
        + os.linesep.join(l.format(**context) for l in lines)
        + os.linesep
        + os.linesep
        + "=" * 80
        + os.linesep
    )


if dlstbx_path:
    conda_required = (
        pathlib.Path(dlstbx.__file__)
        .parent.parent.parent.joinpath("requirements.conda.txt")
        .read_text()
        .strip()
        .split("\n")
    )
else:
    conda_required = None


def check():
    """Resolve all dlstbx conda dependencies"""
    # Check we can do anything here
    if conda is None:
        _notice(
            "  WARNING: Can not find conda package in your environment",
            "  You will have to keep track of dependencies yourself",
        )
        return False
    if not conda_required:
        _notice(
            "  WARNING: Could not find dlstbx in your environment",
            "  Make sure dlstbx is configured in your cctbx environment,",
            "  and try another round of 'make reconf'",
        )
        return False

    conda_list, error, return_code = conda.cli.python_api.run_command(
        conda.cli.python_api.Commands.LIST,
        "--json",
        use_exception_handler=True,
    )
    if error or return_code:
        _notice(
            "  WARNING: Could not obtain list of conda packages in your environment",
            error,
        )
        return False
    conda_environment = {
        package["name"]: package["version"] for package in json.loads(conda_list)
    }

    requirements = [
        pkg_resources.Requirement.parse(spec) for spec in sorted(conda_required)
    ]

    # Now we should have an unduplicated set of requirements
    action_list = []
    for requirement in requirements:
        # Check if package is installed in development mode
        try:
            currentversion = pkg_resources.require(requirement.name)[0].version
        except Exception:
            pass
        else:
            location = None
            for path_item in sys.path:
                egg_link = os.path.join(path_item, requirement.name + ".egg-link")
                if os.path.isfile(egg_link):
                    with open(egg_link, "r") as fh:
                        location = fh.readline().strip()
                        break
            if location and currentversion in requirement:
                print(
                    "requires conda package %s, has %s as developer installation"
                    % (requirement, currentversion)
                )
                continue
            elif location and currentversion not in requirement:
                _notice(
                    "    WARNING: Can not update package {package} automatically.",
                    "",
                    "It is installed as editable package for development purposes. The currently",
                    "installed version, {currentversion}, is too old. The required version is {requirement}.",
                    "Please update the package manually in its installed location:",
                    "",
                    "    {location}",
                    package=requirement.name,
                    currentversion=currentversion,
                    requirement=requirement,
                    location=location,
                )
                continue

        # Check if package is installed with conda
        if requirement.name in conda_environment:
            if conda_environment[requirement.name] in requirement:
                print(
                    "requires conda package %s, has %s"
                    % (requirement, conda_environment[requirement.name])
                )
                continue
            print(
                "conda requirement %s is not currently met, current version %s"
                % (requirement, conda_environment[requirement.name])
            )

        # Install/update required
        print(
            "conda requirement %s is not currently met, package not installed"
            % (requirement)
        )
        action_list.append(str(requirement))

    if not action_list:
        print("All conda requirements satisfied")
        return

    _notice(
        "  WARNING: Your conda environment is likely out of date or incomplete.",
        "           Please install/update the following packages:",
        "           " + ", ".join(action_list),
    )

    return action_list


if __name__ == "__main__":
    actions = check()
    if actions is False:
        exit(1)
    if actions:
        subprocess.run(["libtbx.conda", "install", *actions, *sys.argv[1:]], check=True)
