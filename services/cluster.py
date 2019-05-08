from __future__ import absolute_import, division, print_function

import errno
import json
import os

import procrunner
import workflows.recipe
from workflows.services.common_service import CommonService


class DLSCluster(CommonService):
    """A service to interface zocalo with functions to start new
     jobs on the clusters."""

    # Human readable service name
    _service_name = "DLS Cluster service"

    # Logger name
    _logger_name = "dlstbx.services.cluster"

    def initializing(self):
        """Subscribe to the cluster submission queue.
       Received messages must be acknowledged."""
        self.log.info("Cluster service starting")

        workflows.recipe.wrap_subscribe(
            self._transport,
            "cluster.submission",
            self.run_submit_job,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    @staticmethod
    def _recursive_mkdir(path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def run_submit_job(self, rw, header, message):
        """Submit cluster job according to message."""

        parameters = rw.recipe_step["parameters"]
        commands = parameters["cluster_commands"]
        if not isinstance(commands, basestring):
            commands = "\n".join(commands)

        cluster = parameters.get("cluster")
        if cluster not in ("cluster", "testcluster", "hamilton"):
            if cluster:
                self.log.warning(
                    "Unknown cluster %s specified, defaulting to normal cluster",
                    cluster,
                )
            cluster = "cluster"
        submission_params = parameters.get("cluster_submission_parameters", "")
        if (
            parameters.get("cluster_project")
            and 1 < len(parameters["cluster_project"].strip())
            and "{" not in parameters["cluster_project"]
        ):
            submission_params = "-P %s %s" % (
                parameters["cluster_project"],
                submission_params,
            )
            self.log.debug(
                "Using cluster project %s for submission", parameters["cluster_project"]
            )
        else:
            self.log.warning(
                "No cluster project set for job (%s)",
                repr(parameters.get("cluster_project")),
            )
        commands = commands.replace("$RECIPEPOINTER", str(rw.recipe_pointer))

        if "recipefile" in parameters:
            recipefile = parameters["recipefile"]
            try:
                self._recursive_mkdir(os.path.dirname(recipefile))
            except OSError as e:
                if e.errno == errno.ENOENT:
                    self.log.error(
                        "Error in underlying filesystem: %s", str(e), exc_info=True
                    )
                    self._transport.nack(header)
                    return
                raise
            self.log.debug("Writing recipe to %s", recipefile)
            commands = commands.replace("$RECIPEFILE", recipefile)
            with open(recipefile, "w") as fh:
                fh.write(rw.recipe.pretty())
        if "recipeenvironment" in parameters:
            recipeenvironment = parameters["recipeenvironment"]
            try:
                self._recursive_mkdir(os.path.dirname(recipeenvironment))
            except OSError as e:
                if e.errno == errno.ENOENT:
                    self.log.error(
                        "Error in underlying filesystem: %s", str(e), exc_info=True
                    )
                    self._transport.nack(header)
                    return
                raise
            self.log.debug("Writing recipe environment to %s", recipeenvironment)
            commands = commands.replace("$RECIPEENV", recipeenvironment)
            with open(recipeenvironment, "w") as fh:
                json.dump(
                    rw.environment, fh, sort_keys=True, indent=2, separators=(",", ": ")
                )
        if "recipewrapper" in parameters:
            recipewrapper = parameters["recipewrapper"]
            try:
                self._recursive_mkdir(os.path.dirname(recipewrapper))
            except OSError as e:
                if e.errno == errno.ENOENT:
                    self.log.error(
                        "Error in underlying filesystem: %s", str(e), exc_info=True
                    )
                    self._transport.nack(header)
                    return
                raise
            self.log.debug("Storing serialized recipe wrapper in %s", recipewrapper)
            commands = commands.replace("$RECIPEWRAP", recipewrapper)
            with open(recipewrapper, "w") as fh:
                json.dump(
                    {
                        "recipe": rw.recipe.recipe,
                        "recipe-pointer": rw.recipe_pointer,
                        "environment": rw.environment,
                        "recipe-path": rw.recipe_path,
                        "payload": rw.payload,
                    },
                    fh,
                    sort_keys=True,
                    indent=2,
                    separators=(",", ": "),
                )

        if "workingdir" not in parameters or not parameters["workingdir"].startswith(
            "/"
        ):
            self.log.error(
                "No absolute working directory specified. Will not run cluster job"
            )
            self._transport.nack(header)
            return
        workingdir = parameters["workingdir"]
        try:
            self._recursive_mkdir(workingdir)
        except OSError as e:
            self.log.error(
                "Could not create working directory: %s", str(e), exc_info=True
            )
            self._transport.nack(header)
            return

        submission = [
            ". /etc/profile.d/modules.sh",
            "module load global/" + cluster,
            "qsub %s << EOF" % submission_params,
            "#!/bin/bash",
            ". /etc/profile.d/modules.sh",
            "cd " + workingdir,
            commands,
            "EOF",
        ]
        self.log.debug("Cluster submission parameters: %s", submission_params)
        self.log.debug("Commands: %s", commands)
        self.log.debug("Working directory: %s", workingdir)
        self.log.debug(str(rw.recipe_step))
        result = procrunner.run(
            ["/bin/bash"], stdin="\n".join(submission), working_directory=workingdir
        )
        assert result["exitcode"] == 0
        assert "has been submitted" in result["stdout"]
        jobnumber = result["stdout"].split()[2]

        # Conditionally acknowledge receipt of the message
        txn = self._transport.transaction_begin()
        self._transport.ack(header, transaction=txn)

        # Send results onwards
        rw.set_default_channel("job_submitted")
        rw.send({"jobid": jobnumber}, transaction=txn)

        # Commit transaction
        self._transport.transaction_commit(txn)
        self.log.info("Submitted job %s to %s", str(jobnumber), cluster)
