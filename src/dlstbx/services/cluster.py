from __future__ import annotations

import datetime
import errno
import json
import logging
import math
import os
import pathlib
import subprocess
from pprint import pformat
from typing import Optional

import pkg_resources
import pydantic
import requests
import workflows.recipe
import zocalo.configuration
from workflows.services.common_service import CommonService
from zocalo.util import slurm

cluster_queue_mapping: dict[str, dict[str, str]] = {
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
    "htcondor": {},
}


class JobSubmissionParameters(pydantic.BaseModel):
    scheduler: str
    cluster: str
    partition: Optional[str]
    job_name: Optional[str]  #
    environment: Optional[dict[str, str]] = None
    cpus_per_task: Optional[int] = None
    min_memory_per_cpu: Optional[int] = pydantic.Field(
        None, description="Minimum real memory per cpu (MB)"
    )
    max_memory_per_cpu: Optional[int] = pydantic.Field(
        None, description="Maximum real memory per cpu (MB)"
    )  # HTCondor: maximum memory allocated for job
    max_disk_per_cpu: Optional[int] = pydantic.Field(
        None, description="Maximum disk space per cpu (MB)"
    )  # HTCondor: maximum disk space allocated for job
    time_limit: Optional[str] = None
    gpus: Optional[int] = None
    exclusive: bool = False
    account: Optional[str]  # account in slurm terminology
    commands: list[str] | str
    qos: Optional[str]
    queue: Optional[str]  # legacy for grid engine
    qsub_submission_parameters: Optional[str]  # temporary support for legacy recipes
    transfer_input_files: Optional[list[str]]  # HTCondor: list of input objects to
    #           transfer from submitter node
    transfer_output_files: Optional[
        list[str]
    ]  # HTCondor: list of output objects to transfer
    #           transfer to submitter node


class JobSubmissionValidationError(ValueError):
    pass


def submit_to_grid_engine(
    params: JobSubmissionParameters,
    working_directory: pathlib.Path,
    logger: logging.Logger,
    **kwargs,
) -> int | None:
    # validate
    if params.account and 1 < len(params.account.strip()) and "{" not in params.account:
        if params.cluster == "hamilton" and params.account == "dls":
            raise JobSubmissionValidationError(
                "Project 'dls' is not allowed on Hamilton"
            )
    elif params.cluster == "hamilton":
        raise JobSubmissionValidationError(
            f"No cluster project set for job ({params.account}) on Hamilton. "
            "Cluster project is mandatory for submission."
        )

    submission_params: list
    if params.qsub_submission_parameters:
        submission_params = params.qsub_submission_parameters.split()
    else:
        submission_params = ["-N", params.job_name]
        if params.cpus_per_task:
            submission_params.extend(["-pe", "smp", str(params.cpus_per_task)])
        if params.min_memory_per_cpu:
            submission_params.extend(["-l", f"mfree={params.min_memory_per_cpu}M"])
        if params.time_limit:
            submission_params.extend(["-l", f"h_rt={params.time_limit}"])
        if params.exclusive:
            submission_params.extend(["-l", "exclusive"])
        if params.gpus:
            submission_params.extend(["-l", f"gpus={params.gpus}"])
        if params.account:
            submission_params.extend(["-P", params.account])

        cluster_queue = params.queue
        if cluster_queue is not None:
            mapped_queue = cluster_queue_mapping[params.cluster].get(cluster_queue)
            if mapped_queue:
                logger.debug(
                    f"Mapping requested cluster queue {cluster_queue} on cluster {params.cluster} to {mapped_queue}"
                )
            else:
                mapped_queue = cluster_queue_mapping[params.cluster].get("default")
                if mapped_queue:
                    logger.info(
                        f"Requested cluster queue {cluster_queue} not available on cluster {params.cluster}, mapping to {mapped_queue} instead"
                    )
            if mapped_queue:
                submission_params = ["-q", mapped_queue] + submission_params
            else:
                logger.warning(
                    f"Requested cluster queue {cluster_queue} not available on cluster {params.cluster}, no default queue set"
                )

    commands = params.commands
    if not isinstance(commands, str):
        commands = " ".join(commands)

    submission = [
        ". /etc/profile.d/modules.sh",
        "module load global/" + params.cluster,
        # thanks to Modules 3.2 weirdness qsub may now be a function
        # calling the real qsub command, but eating up its parameters.
        "unset -f qsub",
        f"qsub {' '.join(submission_params)} << EOF",
        "#!/bin/bash",
        ". /etc/profile.d/modules.sh",
        "cd " + os.fspath(working_directory),
        commands,
        "EOF",
    ]
    logger.debug(
        f"Cluster ({params.cluster}) submission parameters: {submission_params}"
    )
    logger.debug(f"Commands: {commands}")
    logger.debug(f"Working directory: {working_directory}")
    result = subprocess.run(
        ["/bin/bash"],
        input="\n".join(submission).encode("latin1"),
        cwd=working_directory,
        capture_output=True,
    )
    if result.returncode:
        logger.error(
            "Could not submit cluster job:\n%s\n%s",
            result.stdout.decode("latin1"),
            result.stderr.decode("latin1"),
        )
        return None
    assert b"has been submitted" in result.stdout
    jobnumber = result.stdout.split()[2].decode("latin1")
    return int(jobnumber)


def submit_to_slurm(
    params: JobSubmissionParameters,
    working_directory: pathlib.Path,
    logger: logging.Logger,
    zc: zocalo.configuration,
) -> int | None:
    api = slurm.SlurmRestApi.from_zocalo_configuration(zc)

    script = params.commands
    if not isinstance(script, str):
        script = "\n".join(script)
    script = f"#!/bin/bash\n. /etc/profile.d/modules.sh\n{script}"

    logger.debug(f"Submitting script to Slurm:\n{script}")
    if params.time_limit:
        parsed_time = datetime.datetime.strptime(params.time_limit, "%H:%M:%S")
        time_delta = datetime.timedelta(
            hours=parsed_time.hour,
            minutes=parsed_time.minute,
            seconds=parsed_time.second,
        )
        time_limit_minutes = math.ceil(time_delta.total_seconds() / 60)
    else:
        time_limit_minutes = None

    job_submission = slurm.models.JobSubmission(
        script=script,
        job=slurm.models.JobProperties(
            partition=params.partition,
            name=params.job_name,
            cpus_per_task=params.cpus_per_task,
            environment=os.environ
            if params.environment is None
            else params.environment,
            memory_per_cpu=params.min_memory_per_cpu,
            time_limit=time_limit_minutes,
            gpus=params.gpus,
            exclusive=str(params.exclusive).lower(),
            account=params.account,
            current_working_directory=os.fspath(working_directory),
            qos=params.qos,
        ),
    )
    try:
        response = api.submit_job(job_submission)
    except requests.HTTPError as e:
        logger.error(f"Failed Slurm job submission: {e}\n" f"{e.response.text}")
        return None
    if response.errors:
        error_message = "\n".join(f"{e.errno}: {e.error}" for e in response.errors)
        logger.error(f"Failed Slurm job submission: {error_message}")
        return None
    return response.job_id


def submit_to_htcondor(
    params: JobSubmissionParameters,
    working_directory: pathlib.Path,
    logger: logging.Logger,
    **kwargs,
) -> int | None:
    current_wd = os.getcwd()

    singularity_environment = "SINGULARITY_CACHEDIR=/tmp/singularity SINGULARITY_LOCALCACHEDIR=/tmp/singularity SINGULARITY_TMPDIR=/tmp/singularity"

    commands = params.commands
    if not isinstance(commands, str):
        commands = "\n".join(commands)
    cluster_exec, cluster_args = commands.split("\n", 1)
    logger.info(f"{cluster_exec} {cluster_args}")
    htcondor_submit = {
        "executable": cluster_exec,
        "arguments": cluster_args,
        "universe": "vanilla",
        "environment": singularity_environment
        if params.environment is None
        else params.environment,
        "should_transfer_files": "YES",
        "when_to_transfer_output": "ON_EXIT_OR_EVICT",
        "output": f"{params.job_name}.condor.out",
        "error": f"{params.job_name}.condor.err",
        "log": f"{params.job_name}.condor.log",
        "on_exit_hold": False,
        "on_exit_remove": True,
    }
    if params.cpus_per_task:
        htcondor_submit.update({"request_cpus": str(params.cpus_per_task)})
        if params.max_memory_per_cpu:
            htcondor_submit.update(
                {
                    "request_memory": f"{str(params.max_memory_per_cpu * params.cpus_per_task)}MB"
                }
            )
        if params.max_disk_per_cpu:
            htcondor_submit.update(
                {
                    "request_disk": f"{str(params.max_disk_per_cpu * params.cpus_per_task)}MB"
                }
            )
    if params.transfer_input_files:
        htcondor_submit.update(
            {"transfer_input_files": ",".join(params.transfer_input_files)}
        )
    if params.transfer_output_files:
        htcondor_submit.update(
            {"transfer_output_files": ",".join(params.transfer_output_files)}
        )

    try:
        import htcondor

        coll = htcondor.Collector(htcondor.param["COLLECTOR_HOST"])
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
        schedd = htcondor.Schedd(schedd_ad)
        logger.debug(f"Address of the Schedd is: {str(schedd_ad['MyAddress'])}")
        os.chdir(working_directory)
        htcondor_job = htcondor.Submit(htcondor_submit)

        with schedd.transaction() as txn:
            jobnumber = htcondor_job.queue(txn, count=1)
    except Exception:
        logger.exception(
            f"Could not submit HTCondor job:\n{pformat(htcondor_submit)}",
        )
        return None
    finally:
        os.chdir(current_wd)
    return jobnumber


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

        if not self.environment_is_valid():
            self._request_termination()
            return

        self.schedulers = {
            f.name: f.load()
            for f in pkg_resources.iter_entry_points(
                "zocalo.services.cluster.schedulers"
            )
        }
        self.log.debug(f"Supported schedulers: {', '.join(self.schedulers.keys())}")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "cluster.submission",
            self.run_submit_job,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def environment_is_valid(self):
        """Check that the cluster submission environment is sane. Specifically, that
        there is no ~/.sge_request file interfering with cluster job submissions.
        """
        sge_file = pathlib.Path("~").expanduser() / ".sge_request"
        if sge_file.exists():
            contents = sge_file.read_bytes().strip()
            if contents:
                self.log.error(
                    "Rejecting service initialisation: file %s is not empty. "
                    "This may interfere with service operation. ",
                    str(sge_file),
                )
                return False

            self.log.info(
                "Note: empty file %s found during service startup", str(sge_file)
            )
        return True

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
        if isinstance(parameters.get("cluster"), str):
            self.log.warning(
                f"Legacy cluster parameters encountered in recipe_ID: {rw.environment['ID']}"
            )
            cluster = parameters["cluster"]
            cluster_submission_parameters = parameters.get(
                "cluster_submission_parameters"
            )
            account = parameters.get("cluster_project")
            queue = parameters.get("cluster_queue")
            commands = parameters.get("cluster_commands")
            params = JobSubmissionParameters(
                scheduler="grid_engine",
                cluster=cluster,
                account=account,
                commands=commands,
                qsub_submission_parameters=cluster_submission_parameters,
                queue=queue,
            )
        else:
            params = JobSubmissionParameters(**parameters.get("cluster", {}))

        if not isinstance(params.commands, str):
            params.commands = "\n".join(params.commands)

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
            params.commands = params.commands.replace("$RECIPEFILE", recipefile)
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
            params.commands = params.commands.replace("$RECIPEENV", recipeenvironment)
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
            params.commands = params.commands.replace("$RECIPEWRAP", recipewrapper)
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
        working_directory = pathlib.Path(parameters["workingdir"])
        try:
            working_directory.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            self.log.error(
                "Could not create working directory: %s", str(e), exc_info=True
            )
            self._transport.nack(header)
            return

        submit_to_scheduler = self.schedulers.get(params.scheduler)

        jobnumber = submit_to_scheduler(
            params, working_directory, self.log, zc=self.config
        )
        if not jobnumber:
            self._transport.nack(header)
            return

        # Conditionally acknowledge receipt of the message
        txn = self._transport.transaction_begin(subscription_id=header["subscription"])
        self._transport.ack(header, transaction=txn)

        # Send results onwards
        rw.set_default_channel("job_submitted")
        rw.send({"jobid": jobnumber}, transaction=txn)

        # Commit transaction
        self._transport.transaction_commit(txn)
        self.log.info(f"Submitted job {jobnumber} to {params.cluster}")
