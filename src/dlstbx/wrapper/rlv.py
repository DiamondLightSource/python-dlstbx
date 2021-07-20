import gzip
import logging
import os
import shutil

import procrunner
import zocalo.wrapper

import dlstbx.util.symlink

logger = logging.getLogger("dlstbx.wrap.rlv")


class RLVWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]

        # run in working directory
        working_directory = params["working_directory"]
        if not os.path.exists(working_directory):
            os.makedirs(working_directory)
        os.chdir(working_directory)

        command = [
            "dials.import",
            f"image_range={params['image_first']},{params['image_last']}",
        ]
        template = params["template"]
        if template.endswith((".h5", ".nxs")):
            command.append(template)
        else:
            command.append(f"template={template}")
        "template=%s" % params["template"],
        logger.info("command: %s", " ".join(command))
        result = procrunner.run(command, timeout=params.get("timeout"))
        if result["exitcode"] or result["timeout"]:
            logger.warning(
                "Failed to import files %s with exitcode %s and timeout %s",
                params["template"],
                result["exitcode"],
                result["timeout"],
            )
            return False
        logger.info("Import successful, took %.1f seconds", result["runtime"])

        # then find spots
        command = [
            "dials.find_spots",
            "imported.expt",
            "nproc=" + str(os.getenv("NSLOTS", "20")),
        ]
        logger.info("command: %s", " ".join(command))
        result = procrunner.run(command, timeout=params.get("timeout"))
        if result["exitcode"] or result["timeout"]:
            logger.warning(
                "Spotfinding failed on %s with exitcode %s and timeout %s",
                params["template"],
                result["exitcode"],
                result["timeout"],
            )
            return False
        logger.info("Spotfinding successful, took %.1f seconds", result["runtime"])

        # then map to json file
        command = [
            "dials.export",
            "format=json",
            "json.n_digits=4",
            "json.compact=true",
            "json.filename=rlp.json",
            "imported.expt",
            "strong.refl",
        ]
        logger.info("command: %s", " ".join(command))
        result = procrunner.run(command, timeout=params.get("timeout"))
        if result["exitcode"] or result["timeout"]:
            logger.warning(
                "dials.export format=json failed on %s with exitcode %s and timeout %s",
                params["template"],
                result["exitcode"],
                result["timeout"],
            )
            return False
        logger.info("JSON generation successful, took %.1f seconds", result["runtime"])

        with open("rlp.json", "rb") as fin:
            with gzip.open("rlp.json.gz", "wb") as fout:
                fout.write(fin.read())

        # copy output files to result directory
        results_directory = params["results_directory"]
        if not os.path.exists(results_directory):
            os.makedirs(results_directory)

        defaultfiles = ["rlp.json.gz"]
        foundfiles = []
        success = True
        for filename in params.get("keep_files", defaultfiles):
            if os.path.exists(filename):
                dst = os.path.join(results_directory, filename)
                logger.debug(f"Copying {filename} to {dst}")
                shutil.copy(filename, dst)
                foundfiles.append(dst)
                self.record_result_individual_file(
                    {
                        "file_path": results_directory,
                        "file_name": filename,
                        "file_type": "recip",
                    }
                )
            else:
                logger.warning("Expected output file %s missing", filename)
                success = False

        if foundfiles:
            logger.info("Notifying for found files: %s", str(foundfiles))
            self.record_result_all_files({"filelist": foundfiles})

        if params.get("results_symlink"):
            # Create symbolic link above working directory
            dlstbx.util.symlink.create_parent_symlink(
                results_directory, params["results_symlink"]
            )

        logger.info("Done.")

        return success
