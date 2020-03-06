from __future__ import absolute_import, division, print_function

import errno
import json
import os

import procrunner
import six
import workflows.recipe
from workflows.services.common_service import CommonService

cluster_queue_mapping = {
    "cluster": {
        "default": "medium.q",
        "bottom": "bottom.q",
        "low": "low.q",
        "medium": "medium.q",
        "high": "high.q",
        "admin": "admin.q",
        "tempservices": "tempservices.q",
    },
    "testcluster": {
        "default": "test-medium.q",
        "low": "test-low.q",
        "medium": "test-medium.q",
        "high": "test-high.q",
        "admin": "test-admin.q",
    },
    "hamilton": {"default": "all.q"},
}


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
        if not isinstance(commands, six.string_types):
            commands = "\n".join(commands)

        cluster = parameters.get("cluster")
        if cluster not in cluster_queue_mapping:
            if cluster:
                self.log.warning(
                    "Unknown cluster %s specified, defaulting to science cluster",
                    cluster,
                )
            cluster = "cluster"
        submission_params = parameters.get("cluster_submission_parameters", "")
        if (
            parameters.get("cluster_project")
            and 1 < len(parameters["cluster_project"].strip())
            and "{" not in parameters["cluster_project"]
        ):
            if cluster == "hamilton" and parameters["cluster_project"] == "dls":
                self.log.error("Project 'dls' is not allowed on Hamilton")
                self._transport.nack(header)
                return
            submission_params = "-P %s %s" % (
                parameters["cluster_project"],
                submission_params,
            )
            self.log.debug(
                "Using cluster project %s for submission", parameters["cluster_project"]
            )
        elif cluster == "hamilton":
            self.log.error(
                "No cluster project set for job (%s) on Hamilton. "
                "Cluster project is mandatory for submission.",
                repr(parameters.get("cluster_project")),
            )
            self._transport.nack(header)
            return
        else:
            self.log.warning(
                "No cluster project set for job (%s)",
                repr(parameters.get("cluster_project")),
            )
        commands = commands.replace("$RECIPEPOINTER", str(rw.recipe_pointer))

        cluster_queue = parameters.get("cluster_queue")
        if cluster_queue is not None:
            mapped_queue = cluster_queue_mapping[cluster].get(cluster_queue)
            if mapped_queue:
                self.log.debug(
                    "Mapping requested cluster queue %s on cluster %s to %s",
                    cluster_queue,
                    cluster,
                    mapped_queue,
                )
            else:
                mapped_queue = cluster_queue_mapping[cluster].get("default")
                if mapped_queue:
                    self.log.info(
                        "Requested cluster queue %s not available on cluster %s, mapping to %s instead",
                        cluster_queue,
                        cluster,
                        mapped_queue,
                    )
            if mapped_queue:
                submission_params = "-q %s %s" % (mapped_queue, submission_params)
            else:
                self.log.warning(
                    "Requested cluster queue %s not available on cluster %s, no default queue set",
                    cluster_queue,
                    cluster,
                )

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
                else:
                    self.log.error(
                        "Could not create working directory: %s", str(e), exc_info=True
                    )
                self._transport.nack(header)
                return
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
            # thanks to Modules 3.2 weirdness qsub may now be a function
            # calling the real qsub command, but eating up its parameters.
            "unset -f qsub",
            "qsub %s << EOF" % submission_params,
            "#!/bin/bash",
            ". /etc/profile.d/modules.sh",
            "cd " + workingdir,
            commands,
            "EOF",
        ]
        self.log.debug(
            "Cluster (%s) submission parameters: %s", cluster, submission_params
        )
        self.log.debug("Commands: %s", commands)
        self.log.debug("Working directory: %s", workingdir)
        self.log.debug(str(rw.recipe_step))
        result = procrunner.run(
            ["/bin/bash"], stdin="\n".join(submission), working_directory=workingdir
        )
        if result["exitcode"]:
            self.log.error(
                "Could not submit cluster job:\n%s\n%s",
                result["stdout"],
                result["stderr"],
            )
            self._transport.nack(header)
            return
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
