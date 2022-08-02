from __future__ import annotations

import os
import pathlib
import shutil

import procrunner

from dlstbx.wrapper import Wrapper


class EdnaWrapper(Wrapper):

    _logger_name = "dlstbx.wrap.edna"

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = pathlib.Path(params["working_directory"])
        results_directory = pathlib.Path(params["results_directory"])
        working_directory.mkdir(parents=True)
        results_directory.mkdir(parents=True, exist_ok=True)
        self.log.info("working_directory: {working_directory}")
        try:  # set Synchweb to swirl
            (results_directory / "summary.html").touch()
        except OSError:
            pass  # it'll be fine

        if params["image_template"].endswith(".h5"):
            edna_module = "edna/mx-20190213-auto"
            complexity = "min"
            if not self.hdf5_to_cbf():
                return False
        else:
            self.generate_modified_headers()
            edna_module = "edna/20140709-auto"
            complexity = "none"

        sparams = params["strategy"]
        transmission = float(sparams["transmission"])
        wavelength = float(sparams["wavelength"])
        beamline = sparams["beamline"]
        self.log.debug("transmission: %s" % transmission)
        self.log.debug("wavelength: %s" % wavelength)
        lifespan = sparams["lifespan"].get(beamline, sparams["lifespan"]["default"])
        if sparams["gentle"]:
            strategy_lifespan = round(
                (lifespan * (100 / transmission)) * (wavelength / 0.979) ** -3 / 10, 0
            )
        else:
            strategy_lifespan = round(
                (lifespan * (100 / transmission)) * (wavelength / 0.979) ** -3, 0
            )
        self.log.debug("lifespan: %s" % strategy_lifespan)

        min_exposure = sparams["min_exposure"].get(
            beamline, sparams["min_exposure"]["default"]
        )

        multiplicity = sparams["multiplicity"]
        i_over_sig_i = sparams["i_over_sig_i"]
        EDNAStrategy = working_directory / "EDNAStrategy"
        EDNAStrategy.mkdir()
        with open("%s.xml" % EDNAStrategy, "w") as f:
            f.write(
                self.make_edna_xml(
                    complexity=complexity,
                    multiplicity=multiplicity,
                    i_over_sig_i=i_over_sig_i,
                    lifespan=strategy_lifespan,
                    min_osc_range=0.1,
                    min_exposure=min_exposure,
                    anomalous=sparams["anomalous"],
                )
            )
        short_comments = "%s Multiplicity=%s I/sig=%s Maxlifespan=%s s" % (
            sparams["description"],
            multiplicity,
            i_over_sig_i,
            strategy_lifespan,
        )
        (working_directory / "Strategy.txt").write_text(short_comments)

        strategy_xml = working_directory / "EDNAStrategy.xml"
        results_xml = working_directory / "results.xml"
        if beamline == "i24":
            edna_site = "export EDNA_SITE=DLS_i24"
        else:
            edna_site = ""
        wrap_edna_sh = working_directory / "wrap_edna.sh"
        wrap_edna_sh.write_text(
            f"""\
module load global/cluster
module load {edna_module}
export DCID={params["dcid"]}
export COMMENTS="{short_comments}"
export SHORT_COMMENTS="{sparams["name"]}"
{edna_site}
edna-plugin-launcher \
  --execute EDPluginControlInterfacev1_2 --DEBUG \
  --inputFile {strategy_xml} \
  --outputFile {results_xml}"""
        )
        commands = [
            "sh",
            wrap_edna_sh,
            strategy_xml,
            results_xml,
        ]
        self.log.info("Running command: %s", " ".join(str(c) for c in commands))
        result = procrunner.run(
            commands,
            working_directory=EDNAStrategy,
            timeout=params.get("timeout", 3600),
            environment_override={
                "LD_LIBRARY_PATH": "",
                "LOADEDMODULES": "",
                "PYTHONPATH": "",
                "_LMFILES_": "",
            },
        )
        success = not result["exitcode"] and not result["timeout"]
        if success:
            self.log.info("EDNA successful, took %.1f seconds", result["runtime"])
        else:
            self.log.info(
                "EDNA failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            self.log.debug(result["stdout"].decode("latin1"))
            self.log.debug(result["stderr"].decode("latin1"))

        wrap_edna2html_sh = working_directory / "wrap_edna2html.sh"
        edna2html_home = "/dls_sw/apps/edna/edna-20140709"
        edna2html = os.path.join(
            edna2html_home, "libraries/EDNA2html-0.0.10a/EDNA2html"
        )
        wrap_edna2html_sh.write_text(
            f"""\
module load {edna_module}
{edna2html} \
--title="{short_comments}" \
--run_basename={working_directory}/EDNAStrategy \
--portable \
--basename={working_directory}/summary
"""
        )
        commands = ["sh", wrap_edna2html_sh]
        self.log.info("Running command: %s", " ".join(str(c) for c in commands))
        result = procrunner.run(
            commands,
            working_directory=working_directory,
            timeout=params.get("timeout", 3600),
            print_stdout=True,
            print_stderr=True,
            environment_override={
                "LD_LIBRARY_PATH": "",
                "LOADEDMODULES": "",
                "PYTHONPATH": "",
                "_LMFILES_": "",
            },
        )
        success = not result["exitcode"] and not result["timeout"]
        if success:
            self.log.info("EDNA2html successful, took %.1f seconds", result["runtime"])
        else:
            self.log.info(
                "EDNA2html failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            self.log.debug(result["stdout"].decode("latin1"))
            self.log.debug(result["stderr"].decode("latin1"))

        # copy output files to result directory
        self.log.info(
            f"Copying results from {working_directory} to {results_directory}"
        )

        source_dir = working_directory / "EDNAStrategy"
        dest_dir = results_directory / ("EDNA%s" % sparams["name"])
        shutil.copytree(source_dir, dest_dir)
        src = working_directory / "EDNAStrategy.xml"
        dst = results_directory / ("EDNA%s.xml" % sparams["name"])
        shutil.copy(src, dst)
        for fname in ("summary.html", "results.xml"):
            src = working_directory / fname
            dst = results_directory / fname
            if src.is_file() and (not dst.is_file() or dst.stat().st_size == 0):
                shutil.copy(src, dst)
        return success

    def hdf5_to_cbf(self):
        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = pathlib.Path(params["working_directory"])
        if params.get("temporary_directory"):
            tmpdir = pathlib.Path(params["temporary_directory"])
        else:
            tmpdir = working_directory / ".image-tmp"
        tmpdir.mkdir(parents=True, exist_ok=True)
        master_h5 = os.path.join(params["image_directory"], params["image_template"])
        prefix = params["image_template"].split("master.h5")[0]
        params["image_pattern"] = prefix + "%04d.cbf"
        self.log.info("Image pattern: %s", params["image_pattern"])
        self.log.info(
            "Converting %s to %s", master_h5, tmpdir / (params["image_pattern"])
        )
        result = procrunner.run(
            ["dxtbx.dlsnxs2cbf", master_h5, params["image_pattern"]],
            working_directory=tmpdir,
            timeout=params.get("timeout", 3600),
        )
        success = not result["exitcode"] and not result["timeout"]
        if success:
            self.log.info(
                "dxtbx.dlsnxs2cbf successful, took %.1f seconds", result["runtime"]
            )
        else:
            self.log.error(
                "dxtbx.dlsnxs2cbf failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            self.log.debug(result["stdout"].decode("latin1"))
            self.log.debug(result["stderr"].decode("latin1"))
        params["orig_image_directory"] = params["image_directory"]
        params["image_directory"] = str(tmpdir)
        return success

    def generate_modified_headers(
        self,
    ):
        params = self.recwrap.recipe_step["job_parameters"]

        def behead(cif_in, cif_out):
            self.log.info(f"Writing modified file {cif_in} to {cif_out}")
            assert cif_in.exists(), cif_in
            assert not cif_out.exists(), cif_out

            data = cif_in.read_bytes()

            if b"# This and all subsequent lines will" in data:
                head = data.split(b"# This and all subsequent lines will")[0]
                tail = data.split(b"CBF_BYTE_OFFSET little_endian")[-1]
                data = head + tail

            cif_out.write_bytes(data)

        if params.get("temporary_directory"):
            tmpdir = pathlib.Path(params["temporary_directory"])
        else:
            tmpdir = pathlib.Path(params["working_directory"]) / ".image-tmp"
        tmpdir.mkdir(parents=True, exist_ok=True)

        image_directory = pathlib.Path(params["image_directory"])
        template = params["image_template"].replace("#", "?")
        for f in image_directory.glob(template):
            behead(f, tmpdir / f.name)

        params["orig_image_directory"] = params["image_directory"]
        params["image_directory"] = str(tmpdir)

    def make_edna_xml(
        self,
        complexity,
        multiplicity,
        i_over_sig_i,
        lifespan,
        min_osc_range,
        min_exposure,
        anomalous=False,
    ):

        params = self.recwrap.recipe_step["job_parameters"]
        dcid = int(params["dcid"])
        assert dcid > 0, "Invalid data collection ID given."

        anomalous = 1 if anomalous else 0

        # 1) Echo out the header
        output = '<?xml version="1.0" ?><XSDataInputInterfacev2_2>'

        # 2) Echo out the diffractionPlan
        output = (
            output
            + f"""
<diffractionPlan>
  <anomalousData>
    <value>{anomalous}</value>
  </anomalousData>
  <complexity>
    <value>{complexity}</value>
  </complexity>
  <aimedIOverSigmaAtHighestResolution>
    <value>{i_over_sig_i}</value>
  </aimedIOverSigmaAtHighestResolution>
  <aimedMultiplicity>
    <value>{multiplicity}</value>
  </aimedMultiplicity>
  <minExposureTimePerImage>
    <value>{min_exposure}</value>
  </minExposureTimePerImage>
  <maxExposureTimePerDataCollection>
    <value>{lifespan}</value>
  </maxExposureTimePerDataCollection>
"""
        )

        # self.log.info('spacegroup: %s' %params.get('spacegroup'))
        # space_group = params.get('spacegroup')
        # if space_group is not None:
        #  print >> s, """            <forcedSpaceGroup>
        #              <value>%s</value>
        #          </forcedSpaceGroup>""" %space_group
        output = output + "</diffractionPlan>"

        # 3) Echo out the full path for each image.

        self.log.info(str(list(params.keys())))
        image_directory = pathlib.Path(params["image_directory"])
        image_first = int(params["image_first"])
        image_last = int(params["image_last"])

        # image_pattern doesn't work: jira.diamond.ac.uk/browse/SCI-6131
        image_pattern = params["image_pattern"]
        # template = params['image_template']
        # fmt = '%%0%dd' % template.count('#')
        # prefix = template.split('#')[0]
        # suffix = template.split('#')[-1]
        # image_pattern = prefix + fmt + suffix

        self.log.info(f"{image_pattern} {image_first}:{image_last}")
        for i_image in range(image_first, image_last + 1):
            image_file_name = image_directory / (image_pattern % i_image)
            output = (
                output
                + """
<imagePath><path><value>%s</value></path></imagePath>
"""
                % image_file_name
            )

        # 4) Echo out the beam and flux (if we know them)
        flux = params["strategy"]["flux"]
        try:
            flux = float(flux)
        except ValueError:
            flux = None
        try:
            beam_size_x = float(params["strategy"]["beam_size_x"])
            beam_size_y = float(params["strategy"]["beam_size_y"])
        except ValueError:
            beam_size_x = None
            beam_size_y = None
        if flux:
            output = output + "<flux><value>%s</value></flux>" % flux
        if beam_size_x:
            output = output + "<beamSizeX><value>%s</value></beamSizeX>" % beam_size_x
        if beam_size_y:
            output = output + "<beamSizeY><value>%s</value></beamSizeY>" % beam_size_y

        # 5) Echo out omega,kappa,phi (if we know them)
        for axis in ("chi", "kappa", "omega", "phi"):
            angle = params["strategy"].get(axis)
            if angle is not None:
                output = output + f"<{axis}><value>{angle}</value></{axis}>"

        # 6) and close
        output = output + "</XSDataInputInterfacev2_2>"

        return output
