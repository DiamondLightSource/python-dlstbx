import logging
import os
import pathlib
import py

import procrunner
import zocalo.wrapper

logger = logging.getLogger("dlstbx.wrap.edna")


class EdnaWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = py.path.local(params["working_directory"])
        results_directory = py.path.local(params["results_directory"])
        logger.info("working_directory: %s" % working_directory.strpath)
        working_directory.ensure(dir=True)
        try:  # set Synchweb to swirl
            results_directory.join("summary.html").ensure()
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
        logger.debug("transmission: %s" % transmission)
        logger.debug("wavelength: %s" % wavelength)
        lifespan = sparams["lifespan"].get(beamline, sparams["lifespan"]["default"])
        if sparams["gentle"]:
            strategy_lifespan = round(
                (lifespan * (100 / transmission)) * (wavelength / 0.979) ** -3 / 10, 0
            )
        else:
            strategy_lifespan = round(
                (lifespan * (100 / transmission)) * (wavelength / 0.979) ** -3, 0
            )
        logger.debug("lifespan: %s" % strategy_lifespan)

        min_exposure = sparams["min_exposure"].get(
            beamline, sparams["min_exposure"]["default"]
        )

        multiplicity = sparams["multiplicity"]
        i_over_sig_i = sparams["i_over_sig_i"]
        EDNAStrategy = working_directory.join("EDNAStrategy")
        EDNAStrategy.ensure(dir=True)
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
        with working_directory.join("Strategy.txt").open("w") as f:
            f.write(short_comments)

        strategy_xml = working_directory.join("EDNAStrategy.xml")
        results_xml = working_directory.join("results.xml")
        wrap_edna_sh = working_directory.join("wrap_edna.sh")
        with wrap_edna_sh.open("w") as f:
            if beamline == "i24":
                edna_site = "export EDNA_SITE=DLS_i24"
            else:
                edna_site = ""
            f.write(
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
            wrap_edna_sh.strpath,
            strategy_xml.strpath,
            results_xml.strpath,
        ]
        logger.info("Running command: %s", " ".join(commands))
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
            logger.info("EDNA successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "EDNA failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"].decode("latin1"))
            logger.debug(result["stderr"].decode("latin1"))

        wrap_edna2html_sh = working_directory.join("wrap_edna2html.sh")
        edna2html_home = "/dls_sw/apps/edna/edna-20140709"
        edna2html = os.path.join(
            edna2html_home, "libraries/EDNA2html-0.0.10a/EDNA2html"
        )
        with wrap_edna2html_sh.open("w") as f:
            f.write(
                f"""\
module load {edna_module}
{edna2html} \
--title="{short_comments}" \
--run_basename={working_directory}/EDNAStrategy \
--portable \
--basename={working_directory}/summary
"""
            )
        commands = ["sh", wrap_edna2html_sh.strpath]
        logger.info("Running command: %s", " ".join(commands))
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
            logger.info("EDNA2html successful, took %.1f seconds", result["runtime"])
        else:
            logger.info(
                "EDNA2html failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"].decode("latin1"))
            logger.debug(result["stderr"].decode("latin1"))

        # copy output files to result directory
        logger.info(
            "Copying results from %s to %s"
            % (working_directory.strpath, results_directory.strpath)
        )

        source_dir = working_directory / "EDNAStrategy"
        dest_dir = results_directory / ("EDNA%s" % sparams["name"])
        source_dir.copy(dest_dir)
        src = working_directory / "EDNAStrategy.xml"
        dst = results_directory / ("EDNA%s.xml" % sparams["name"])
        src.copy(dst)
        for fname in ("summary.html", "results.xml"):
            src = working_directory / fname
            dst = results_directory / fname
            if src.check() and (not dst.check() or dst.size() == 0):
                src.copy(dst)
        return success

    def hdf5_to_cbf(self):
        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = py.path.local(params["working_directory"])
        if params.get("temporary_directory"):
            tmpdir = py.path.local(params["temporary_directory"])
        else:
            tmpdir = working_directory.join(".image-tmp")
        tmpdir.ensure(dir=True)
        master_h5 = os.path.join(params["image_directory"], params["image_template"])
        prefix = params["image_template"].split("master.h5")[0]
        params["image_pattern"] = prefix + "%04d.cbf"
        logger.info("Image pattern: %s", params["image_pattern"])
        logger.info(
            "Converting %s to %s" % (master_h5, tmpdir.join(params["image_pattern"]))
        )
        result = procrunner.run(
            ["dxtbx.dlsnxs2cbf", master_h5, params["image_pattern"]],
            working_directory=tmpdir,
            timeout=params.get("timeout", 3600),
        )
        success = not result["exitcode"] and not result["timeout"]
        if success:
            logger.info(
                "dxtbx.dlsnxs2cbf successful, took %.1f seconds", result["runtime"]
            )
        else:
            logger.error(
                "dxtbx.dlsnxs2cbf failed with exitcode %s and timeout %s",
                result["exitcode"],
                result["timeout"],
            )
            logger.debug(result["stdout"].decode("latin1"))
            logger.debug(result["stderr"].decode("latin1"))
        params["orig_image_directory"] = params["image_directory"]
        params["image_directory"] = str(tmpdir)
        return success

    def generate_modified_headers(
        self,
    ):
        params = self.recwrap.recipe_step["job_parameters"]

        def behead(cif_in, cif_out):
            logger.info(f"Writing modified file {cif_in} to {cif_out}")
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
        output = output + """
<diffractionPlan>
  <anomalousData>
    <value>%(anomalous)i</value>
  </anomalousData>
  <complexity>
    <value>%(complexity)s</value>
  </complexity>
  <aimedIOverSigmaAtHighestResolution>
    <value>%(i_over_sig_i)s</value>
  </aimedIOverSigmaAtHighestResolution>
  <aimedMultiplicity>
    <value>%(multiplicity)s</value>
  </aimedMultiplicity>
  <minExposureTimePerImage>
    <value>%(min_exposure)s</value>
  </minExposureTimePerImage>
  <maxExposureTimePerDataCollection>
    <value>%(lifespan)s</value>
  </maxExposureTimePerDataCollection>
""" % dict(
            anomalous=anomalous,
            complexity=complexity,
            i_over_sig_i=i_over_sig_i,
            multiplicity=multiplicity,
            min_exposure=min_exposure,
            lifespan=lifespan,
        )

        # logger.info('spacegroup: %s' %params.get('spacegroup'))
        # space_group = params.get('spacegroup')
        # if space_group is not None:
        #  print >> s, """            <forcedSpaceGroup>
        #              <value>%s</value>
        #          </forcedSpaceGroup>""" %space_group
        output = output + "</diffractionPlan>"

        # 3) Echo out the full path for each image.

        logger.info(str(list(params.keys())))
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

        logger.info(f"{image_pattern} {image_first}:{image_last}")
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
