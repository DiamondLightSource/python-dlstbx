import zocalo.wrapper
import logging
import py
import tempfile
import os
import procrunner
import dlstbx.util.symlink

logger = logging.getLogger("dlstbx.wrap.screen19_mx")

clean_environment = {
    "LD_LIBRARY_PATH": "",
    "LOADEDMODULES": "",
    "PYTHONPATH": "",
    "_LMFILES_": "",
}


class Screen19MXWrapper(zocalo.wrapper.BaseWrapper):
    def send_html_email_message(self, msg, email_params, img):
        import smtplib
        import getpass
        import platform
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.image import MIMEImage
        from time import sleep

        try:
            from_addr = "@".join([getpass.getuser(), platform.node()])

            message = MIMEMultipart("related")
            message["From"] = from_addr
            message["To"] = ",".join(email_params["recipients"])
            message["Subject"] = email_params["subject"]
            txt = MIMEText(msg, "html")
            message.attach(txt)

            with open(img, "rb") as fp:
                wilson_plot = MIMEImage(fp.read())
            wilson_plot.add_header("Content-ID", "<image1>")
            message.attach(wilson_plot)

            server = smtplib.SMTP("localhost")
            retry = 5
            for i in range(retry):
                try:
                    server.sendmail(
                        from_addr=from_addr,
                        to_addrs=email_params["recipients"],
                        msg=message.as_string(),
                    )
                    logger.info("Sent email with screen19_mx results")
                    return
                except smtplib.SMTPSenderRefused:
                    sleep(60)
            logger.error("Cannot sending email with screen19_mx processing results")
        except Exception:
            logger.exception("Error sending email with screen19_mx processing results")

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]

        working_directory = py.path.local(params["working_directory"])
        working_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                working_directory.strpath, params["create_symlink"]
            )

        screen19_params = " ".join(
            (f"{k}={v}" for k, v in params.get("minimum_exposure").items())
        )

        try:
            fp = tempfile.NamedTemporaryFile(dir=working_directory.strpath)
            screen19_script = working_directory.join(
                "run_screen19_mx_{}.sh".format(os.path.basename(fp.name))
            )
            fp.close()
            with screen19_script.open("w") as fp:
                fp.writelines(
                    [
                        "#!/bin/bash\n",
                        f"{params['dials_env']}\n",
                        f"screen19.minimum_exposure {screen19_params}\n",
                    ]
                )
        except OSError:
            logger.exception(
                "Could not create screen19 script file in the working directory"
            )
            return False
        try:
            result = procrunner.run(
                ["sh", screen19_script.strpath],
                timeout=params["timeout"],
                working_directory=working_directory,
                environment_override=clean_environment,
            )
            assert result["exitcode"] == 0
            assert result["timeout"] is False
        except AssertionError:
            logger.exception(
                "Process returned an error code when running screen19.minimum_exposure script"
            )
            return False
        except Exception:
            logger.exception("Running screen19.minimum_exposure script has failed")
            return False
        # Create results directory if it doesn't already exist
        results_directory = py.path.local(params["results_directory"])
        results_directory.ensure(dir=True)
        if params.get("create_symlink"):
            dlstbx.util.symlink.create_parent_symlink(
                results_directory.strpath, params["create_symlink"]
            )
        logger.info("Copying screen19_mx results to %s", results_directory.strpath)
        for result_filename, result_type in [
            ("output_file", "result"),
            ("wilson_plot", "graph"),
        ]:
            result_file = py.path.local(params[result_filename])
            if result_file.check():
                try:
                    destination = results_directory.join(result_file.basename)
                    result_file.copy(destination)
                    self.record_result_individual_file(
                        {
                            "file_path": destination.dirname,
                            "file_name": destination.basename,
                            "file_type": result_type,
                        }
                    )
                except Exception:
                    logger.info(
                        "Error copying files into the results directory %s",
                        results_directory.strpath,
                    )
            else:
                logger.error("Results file %s not found", result_file.strpath)
                return False

        with open(params["output_file"]) as fp:
            email_body = fp.read()
        email_message = f"""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
 <head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
  <title>Demystifying Email Design</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
</head>
<body>
<div>
<img src="cid:image1">
</div>
<div>
<a href="https://ispyb-staging.diamond.ac.uk/dc/visit/{params["visit"]}/id/{params["dcid"]}">SynchWeb Visit: {params["visit"]} DCID: {params["dcid"]} </a>
</div>
<pre>
screen19 processingJobId: {params["jobid"]}

Path: {params["results_directory"]}

Datafile: {params["minimum_exposure"]["mtz"]}

screen19 log: {params["output_file"]}

Wilson plot: {params["wilson_plot"]}

{email_body}
</pre>
</body>
</html>
"""
        self.send_html_email_message(
            email_message, params["email"], params["wilson_plot"]
        )
        return True
