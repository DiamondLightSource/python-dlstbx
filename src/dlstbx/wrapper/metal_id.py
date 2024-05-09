from __future__ import annotations

import math
import os
import pathlib
import re
import shutil
import subprocess

from iotbx import pdb

from dlstbx.wrapper import Wrapper


def view_as_quat(p1, p2):
    """
    Calculate a quaternion representing the rotation necessary to orient a viewer's
    perspective from an initial view direction towards a desired view direction,
    given by the positions p1 and p2, respectively.

    Parameters:
    - p1: tuple or list representing the initial rotation centre (x, y, z) of the viewer.
    - p2: tuple or list representing the desired position (x, y, z) towards which
        the viewer should orient.

    Returns:
    - Quaternion: A tuple representing the quaternion (w, x, y, z) that represents
    the rotation necessary to align the initial view direction with the desired
    view direction. If either p1 or p2 is None, returns the default identity
    quaternion (0., 0., 0., 1.), indicating no rotation.
    """
    if p1 is None or p2 is None:
        return (0.0, 0.0, 0.0, 1.0)
    # Find and normalise direction vector from p1 to p2
    d = (p2[0] - p1[0], p2[1] - p1[1], p2[2] - p1[2])
    length = math.sqrt(d[0] ** 2 + d[1] ** 2 + d[2] ** 2)
    d = (d[0] / length, d[1] / length, d[2] / length)
    # Cross product of d and (0, 0, -1) to view down the direction vector.
    prod = (d[1], -d[0], 0)
    # Generate and normalise quaternion from cross product
    quat = (prod[0], prod[1], prod[2], 1 - d[2])
    qlen = math.sqrt(sum(a * a for a in quat))
    return (quat[0] / qlen, quat[1] / qlen, quat[2] / qlen, quat[3] / qlen)


class MetalIdWrapper(Wrapper):

    _logger_name = "dlstbx.wrap.metal_id"

    def are_pdbs_similar(self, file_1, file_2, tolerances):
        """
        Determine if two pdb files have the same crystal symmetry, the same number
        and type of atoms and sufficiently similar unit cell and atomic coordinates
        within the defined tolerances
        """

        def read_pdb(file):
            """
            Read a pdb file to get crystal symmetry, atom names and atom coordinates
            """
            pdb_obj = pdb.input(file)
            sym = pdb_obj.crystal_symmetry()
            atoms = pdb_obj.atoms()
            atom_names = atoms.extract_name()
            list_atoms = [atom for atom in atom_names]
            atom_coords = atoms.extract_xyz()
            list_coords = [coords for coords in atom_coords]
            return sym, list_atoms, list_coords

        # Read pdb files
        sym_1, atoms_1, coords_1 = read_pdb(file_1)
        sym_2, atoms_2, coords_2 = read_pdb(file_2)

        # Get tolerances

        # Compare symmetry
        is_similar_sym = sym_1.is_similar_symmetry(
            sym_2,
            relative_length_tolerance=tolerances["rel_cell_length"],
            absolute_angle_tolerance=tolerances["abs_cell_angle"],
        )
        if not is_similar_sym:
            self.log.error("PDB file symmetries are too different")
            return False

        # Compare atom type/number
        if atoms_1 != atoms_2:
            self.log.error("Different number or type of atoms in pdb files")

        # Compare atom coordinates
        combined_coords = zip(coords_1, coords_2)
        for xyz_1, xyz_2 in combined_coords:
            # Calculate the distance between xyz_1 and xyz_2
            diff = abs(
                (
                    (xyz_1[0] - xyz_2[0]) ** 2
                    + (xyz_1[1] - xyz_2[1]) ** 2
                    + (xyz_1[2] - xyz_2[2]) ** 2
                )
                ** 0.5
            )
            if diff > tolerances["abs_coord_diff"]:
                self.log.error(
                    f"PDB atom coordinates have difference > tolerance ({tolerances['abs_coord_diff']} Å"
                )
                return False
        return True

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        self.log.debug(
            f"Running recipewrap file {self.recwrap.recipe_step['parameters']['recipewrapper']}"
        )
        # Get parameters from the recipe file
        params = self.recwrap.recipe_step["job_parameters"]

        pha_above = pathlib.Path(params["anode_map_above"])
        pha_below = pathlib.Path(params["anode_map_below"])
        pdb_files = params["pdb"]

        working_directory = pathlib.Path(params["working_directory"])
        working_directory.mkdir(parents=True, exist_ok=True)

        results_directory = pathlib.Path(params["results_directory"])
        results_directory.mkdir(parents=True, exist_ok=True)

        # Check file inputs
        for file_type, file_path in [
            ("AnoDe map above", pha_above),
            ("AnoDe map below", pha_below),
        ]:
            if not os.path.isfile(file_path):
                self.log.error(f"Could not find {file_type}, expected at: {file_path}")
                return False

        # Check and handle pdb file(s)
        if isinstance(pdb_files, str):
            # Handles case where a single pdb file is given, not as a list
            pdb_file = pathlib.Path(pdb_files)
        elif len(pdb_files) > 2:
            self.log.error(
                f"Expected up to two pdb files, instead {len(pdb_files)} were given"
            )
        elif len(pdb_files) == 2:
            self.log.info(
                f"Checking pdb files for similarity. Files: {pdb_files[0]}, {pdb_files[1]}"
            )
            tolerances = params.get("pdb_comparison_tolerances")
            if not tolerances:
                self.log.warning(
                    "PDB similarity tolerances not specified, using default values"
                )
                tolerances = {
                    "rel_cell_length": 0.01,
                    "abs_cell_angle": 1.0,
                    "abs_coord_diff": 3.0,
                }
            pdbs_are_similar = self.are_pdbs_similar(
                pdb_files[0], pdb_files[1], tolerances
            )
            if not pdbs_are_similar:
                self.log.error("PDB files are not similar enough, not running metal_id")
                return False
            self.log.info("PDB files are similar enough, continuing with metal_id")
            pdb_file = pathlib.Path(pdb_files[0])
        else:
            pdb_file = pathlib.Path(pdb_files[0])

        self.log.info("Making double difference map")
        self.log.info(f"Using {pdb_file} as reference coordinates for map")
        map_out = working_directory / "diff.map"
        map_sig_thresh = 8  # Threshold in rmsd for difference map peaks/contours
        coot_script = [
            "#!/usr/bin/env coot",
            "# python script for coot - generated by metal_ID",
            "set_nomenclature_errors_on_read('ignore')",
            f"read_pdb('{pdb_file}')",
            f"map_above = read_phs_and_make_map_using_cell_symm_from_previous_mol('{pha_above}')",
            f"map_below = read_phs_and_make_map_using_cell_symm_from_previous_mol('{pha_below}')",
            "map_diff = difference_map(map_above, map_below, 1)",
            f"difference_map_peaks(3, 0, {map_sig_thresh}, 0.0, 1, 0, 0)",
            f"export_map(map_diff, '{map_out}')",
            "coot_real_exit(0)",
        ]
        coot_script_path = working_directory / "coot_diff_map.py"
        with open(coot_script_path, "w") as script_file:
            for line in coot_script:
                script_file.write(line + "\n")
        self.log.info(f"Running coot script {coot_script_path} to create diff.map")
        coot_command = f"coot --no-guano --no-graphics -s {coot_script_path}"
        result = subprocess.run(
            coot_command, shell=True, capture_output=True, text=True
        )

        with open(working_directory / "metal_id.log", "w") as log_file:
            log_file.write(result.stdout)

        # Regex pattern to match lines containing peaks from coot output in format: "0 dv: 77.94 n-rmsd: 42.52 xyz = (     24.08,     12.31,     28.48)"
        pattern = r"\s*\d+\s+dv:\s*([\d.]+)\s+n-rmsd:\s*([\d.]+)\s+xyz\s*=\s*\(\s*([\d., -]+)\)"

        # Extract peaks from coot output
        matches = re.finditer(pattern, result.stdout)
        electron_densities = []
        rmsds = []
        peak_coords = []
        for match in matches:
            density = float(match.group(1))
            rmsd = float(match.group(2))
            xyz = tuple(map(float, match.group(3).split(",")))
            electron_densities.append(density)
            rmsds.append(rmsd)
            peak_coords.append(xyz)

        # Print the extracted information
        for i, (density, rmsd, xyz) in enumerate(
            zip(electron_densities, rmsds, peak_coords)
        ):
            self.log.info(
                f"Peak {i}: Electron Density = {density}, RMSD = {rmsd}, XYZ = {xyz}"
            )

        # Use regex to get the protein centre of mass coordinates from find-blobs output
        # N.B. Despite the name, find-blobs is not being used to find blobs here.
        self.log.info("Finding protein centre")
        find_blobs_command = f"find-blobs -c {pdb_file}"
        result = subprocess.run(
            find_blobs_command, shell=True, capture_output=True, text=True
        )
        # Regex pattern for extracting coords from find-blobs output in format "Protein mass center: xyz = (     12.37,     23.89,     32.69)"
        pattern = r"Protein mass center: xyz = \(\s*([-+]?\d*\.\d+|\d+\.\d*)\s*,\s*([-+]?\d*\.\d+|\d+\.\d*)\s*,\s*([-+]?\d*\.\d+|\d+\.\d*)\s*\)"
        match = re.search(pattern, result.stdout)
        assert match, "Protein mass center not found"
        centre = tuple(map(float, match.groups()))

        self.log.info(f"Protein mass centre at: {centre}")
        render_script = [
            "#!/usr/bin/env coot",
            "# python script for coot - generated by metal_ID",
            "set_nomenclature_errors_on_read('ignore')",
            f"read_pdb('{pdb_file}')",
            f"read_ccp4_map('{map_out}', 1)",
            f"set_contour_level_in_sigma(1, {map_sig_thresh})",
        ]
        render_paths = []
        for _i, peak in enumerate(peak_coords):
            quat = view_as_quat(peak, centre)
            # Use relative path as explicit paths can exceed render command length limit
            render_path = f"peak_{_i}.r3d"
            mini_script = [
                f"set_rotation_centre{peak}",
                "set_zoom(30.0)",
                f"set_view_quaternion{quat}",
                "graphics_draw()",
                f"raster3d('{str(render_path)}')",
            ]
            render_script.extend(mini_script)
            render_paths.append(render_path)
        render_script.append("coot_real_exit(0)")

        render_script_path = working_directory / "coot_render.py"
        with open(render_script_path, "w") as script_file:
            for line in render_script:
                script_file.write(line + "\n")
        self.log.info(f"Running coot rendering script {render_script_path}")
        render_command = f"coot --no-guano --no-graphics -s {render_script_path}"
        result = subprocess.run(
            render_command,
            shell=True,
            capture_output=True,
            text=True,
            cwd=working_directory,
        )

        # Convert r3d files to pngs
        self.log.info("Converting r3d files to pngs")
        for render_path in render_paths:
            render_png_path = f"{os.path.splitext(render_path)[0]}.png"
            self.log.info(f"Converting {render_path} to {render_png_path}")
            r3d_command = f"cat {render_path} | render -png {render_png_path}"
            result = subprocess.run(
                r3d_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=working_directory,
            )

        for f in working_directory.iterdir():
            if f.name.startswith("."):
                continue
            if any(f.suffix == skipext for skipext in (".r3d")):
                continue
            shutil.copy(f, results_directory)

        self.log.info("Metal_ID script finished")
        return True
