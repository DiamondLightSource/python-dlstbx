from __future__ import annotations

import json
import os
import pathlib
import shutil
import time

import requests
import workflows.recipe
from workflows.services.common_service import CommonService

from dlstbx.services.cluster import DLSCluster
from dlstbx.util.outbox import replay_outbox


class VisitInput:
    def __init__(self, proposalCode, proposalNumber, number):
        self.proposalCode = proposalCode
        self.proposalNumber = int(proposalNumber)
        self.number = int(number)

    def to_dict(self):
        return {
            "proposalCode": self.proposalCode,
            "proposalNumber": self.proposalNumber,
            "number": self.number,
        }


# test command: zocalo.go -n -f /dls/science/wdr83388/dc_sim.json -s scenario="workflows cluster test" -s beamline=i03 -s src_dcid=22409150 -s visitNumber=1 -s proposalCode=cm -s proposalNumber=44137 -s template_name=dc-sim  -e devrmq
# verify at: https://workflows.diamond.ac.uk/workflows/cm44137-1
class DLSWorkflowsCluster(CommonService):
    """A service to interface zocalo with functions to start new jobs on the workflows cluster.

    Workflows-cluster pods have /dls access but no broker access, so a wrapper running
    on one can't send its recipe messages the normal way. Instead it runs with
    dlstbx.util.outbox.OutboxTransport, which writes each outgoing
    message to a file in an outbox directory on /dls instead of sending it.

    This service is responsible for both ends of that gap:
      - run_submit_job: serialises the recipe wrapper to /dls, creates the outbox,
        submits the workflow, and arms a watch for it.
      - watch_job: polls the submitted workflow's status. While it's still running,
        it checkpoints its own watch state back to itself (via rw.checkpoint) rather
        than holding that state in memory - so the watch survives this service being
        restarted or the tick being handled by a different replica, the same way
        dlstbx.services.cloudwatcher.CloudWatcher watches cluster jobs. Once the
        workflow succeeds, it replays the pod's outbox onto the real broker: because
        each replayed message is already a complete, pre-addressed recipe envelope,
        that replay *is* the recipe continuing - no further routing is needed here.
        If the workflow fails or times out, there is no pod-written message to
        replay (a dead pod never got to write one), so this service manufactures
        the recipe's "failure" message itself.
    """

    _service_name = "DLS Workflows Cluster Service"

    _logger_name = "dlstbx.services.cluster"

    _graphql_endpoint = "https://graph.diamond.ac.uk/graphql"

    _SUCCESS_STATUSES = {"WorkflowSucceededStatus"}
    _FAILURE_STATUSES = {"WorkflowFailedStatus", "WorkflowErroredStatus"}
    _RUNNING_STATUSES = {"WorkflowPendingStatus", "WorkflowRunningStatus"}

    def initializing(self):
        """Subscribe to the workflows cluster submission and watch queues.
        Received messages must be acknowledged.
        """
        self.log.info("Cluster service is starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "workflows.submission",
            self.run_submit_job,
            acknowledgement=True,
            log_extender=self.extend_log,
        )
        workflows.recipe.wrap_subscribe(
            self._transport,
            "workflows.watch",
            self.watch_job,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def _graphql_request(self, query: str, variables: dict) -> dict:
        """POST a GraphQL query/mutation and return its 'data' object.

        Raises on HTTP failure or on a GraphQL-level "errors" response, so callers
        don't have to remember to check both.
        """
        response = requests.post(
            self._graphql_endpoint,
            headers={"Authorization": f"Bearer {os.environ['WORKFLOWS_BEARER_TOKEN']}"},
            json={"query": query, "variables": variables},
        )
        response.raise_for_status()
        body = response.json()
        if body.get("errors"):
            raise RuntimeError(f"GraphQL error from {self._graphql_endpoint}: {body['errors']}")
        return body["data"]

    def submit_to_workflows(self, workflow: dict, parameters: dict) -> dict:
        """Submit a workflow template and return the submitWorkflowTemplate result.

        Args:
            workflow: The recipe's "workflow" block - the template to run and the
                visit to run it against.
            parameters: The Argo template parameters, already built and stringified
                by run_submit_job.
        """
        mutation = """
            mutation testTemplateSubmission($templateName: String!, $visitID: VisitInput!, $parameters: JSON!){
                submitWorkflowTemplate(
                name: $templateName,
                visit: $visitID,
                parameters: $parameters
                ) {
                    name
                    id
                    visit {
                        number
                    }
                    status {
                        __typename
                    }
                    creator {
                        creatorId
                    }
                    templateRef
                    }
            }
        """
        visit = workflow["visit"]
        variables = {
            "templateName": workflow["template_name"],
            "visitID": VisitInput(
                visit["proposalCode"], visit["proposalNumber"], visit["number"]
            ).to_dict(),
            "parameters": parameters,
        }
        return self._graphql_request(mutation, variables)["submitWorkflowTemplate"]

    def get_workflow_status(self, ID) -> str | None:
        """Return the raw status __typename for a previously-submitted workflow.

        Returns None when the status isn't known yet: Workflow.status is a nullable
        WorkflowStatus union, and a freshly-submitted workflow has no status member
        for a short while, so None here means "no answer yet", not "lookup failed".
        workflowById is nullable too, so an unknown id gives None the same way.
        """
        query = """
            query workflowStatus($ID: ID!) {
                workflowById(id: $ID) {
                    name
                    status {
                        __typename
                    }
                }
            }
        """
        variables = {
            "ID": ID,
        }
        data = self._graphql_request(query, variables)
        workflow = data.get("workflowById") or {}
        return (workflow.get("status") or {}).get("__typename")

    def _classify_status(self, typename: str | None) -> str:
        """Bucket a raw status __typename into 'running' / 'succeeded' / 'failed'.

        None means the status isn't known yet (see get_workflow_status) and counts as
        still-running without comment. Any other unrecognised typename is also treated
        as still-running - so a schema change makes us keep polling (until the recipe
        step's own timeout) rather than wrongly declare success or failure - but it's
        logged loudly so it doesn't go unnoticed.
        """
        if typename is None:
            return "running"
        if typename in self._SUCCESS_STATUSES:
            return "succeeded"
        if typename in self._FAILURE_STATUSES:
            return "failed"
        if typename not in self._RUNNING_STATUSES:
            self.log.warning(
                "Unrecognised workflow status __typename %r; treating as still "
                "running. Update DLSWorkflowsCluster._*_STATUSES once the real "
                "schema is confirmed.",
                typename,
            )
        return "running"

    def run_submit_job(self, rw, header, message):
        "Submit cluster job according to message."
        job_params = rw.recipe_step["job_parameters"]

        # Unlike DLSCluster, where a recipewrapper is optional, one is required
        # here: the pod has no broker, so this file on /dls is the only way the
        # recipe reaches the wrapper, and the only way the wrapper learns where to
        # send its results. Without it the job could run but the recipe could never
        # continue, so refuse the submission rather than start a job that can only
        # dead-end.
        recipewrapper = job_params.get("recipewrapper")
        if not recipewrapper:
            self.log.error(
                "Rejecting submission for template %s: no recipewrapper in the "
                "recipe step, so the recipe could not be continued",
                job_params.get("workflow", {}).get("template_name"),
            )
            self._transport.nack(header)
            return

        # DLSCluster._write_job_file creates the directory, writes the file, and
        # nacks the message itself on a permission or filesystem error - returning
        # False to say "already rejected, stop here". It is an instance method
        # rather than a static one, but it only touches self.log and
        # self.transport, both of which CommonService gives us, so it is called
        # unbound here rather than duplicating its error handling.
        if not DLSCluster._write_job_file(
            self,
            header,
            recipewrapper,
            "recipe wrapper",
            json.dumps(
                {
                    "recipe": rw.recipe.recipe,
                    "recipe-pointer": rw.recipe_pointer,
                    "environment": rw.environment,
                    "recipe-path": rw.recipe_path,
                    "payload": rw.payload,
                },
                indent=2,
                separators=(",", ": "),
            ),
        ):
            return

        working_directory = pathlib.Path(job_params["workingdir"])
        try:
            working_directory.mkdir(parents=True, exist_ok=True)
        except OSError:
            self.log.exception(
                "Could not create working directory %s", working_directory
            )
            self._transport.nack(header)
            return

        # The pod has /dls but no broker access, so it writes its outgoing recipe
        # messages here (via OutboxTransport) instead of sending them - watch_job
        # replays them once the workflow finishes. The outbox must exist before the
        # pod starts, since OutboxTransport refuses to write into a missing directory.
        outbox_dir = working_directory / "outbox"
        outbox_dir.mkdir(parents=True, exist_ok=True)

        # The Argo template parameters. Only these two need to cross the submission
        # boundary, because they are the only things that appear on the pod's
        # command line - everything else the wrapper needs (the whole recipe, the
        # payload, the job parameters) travels inside the recipewrapper file above
        # and is read straight off /dls.
        #
        # Argo parameter values are strings, so anything the recipe supplies is
        # coerced rather than left to fail somewhere less obvious.
        parameters = {
            # Template-specific extras, if this recipe declares any.
            **{
                key: str(value)
                for key, value in job_params["workflow"].get("parameters", {}).items()
            },
            # Service-owned, and deliberately last so a recipe cannot shadow them.
            # Every workflows-cluster template takes these two, the same way every
            # cluster.submission recipe takes $RECIPEWRAP (see
            # dlstbx.services.cluster.DLSCluster).
            "RECIPEWRAP": str(recipewrapper),
            "OUTBOX": str(outbox_dir),
        }

        try:
            submitted = self.submit_to_workflows(job_params["workflow"], parameters)
        except Exception:
            self.log.exception(
                "Failed to submit workflow for job %s", job_params.get("workflow")
            )
            self._transport.nack(header)
            return

        watch_state = {
            "handle": {
                "name": submitted["name"],
                "id": submitted["id"],
                "visit": job_params["workflow"]["visit"],
            },
            "outbox": str(outbox_dir),
            "recipewrapper": recipewrapper,
            "first-seen": time.time(),
        }

        txn = self._transport.transaction_begin(subscription_id=header["subscription"])
        self._transport.ack(header, transaction=txn)
        rw.send_to("job_submitted", watch_state, transaction=txn)
        self._transport.transaction_commit(txn)
        self.log.info(
            "Submitted workflow %s for visit %s; watching for completion",
            submitted["name"],
            watch_state["handle"]["visit"],
        )

    def watch_job(self, rw, header, message):
        """One polling tick for a previously-submitted workflow.

        Mirrors dlstbx.services.cloudwatcher.CloudWatcher.watch_jobs: the watch
        state is read entirely from the incoming message body, never from this
        service's memory - as state is on the broker. Takes a single status snapshot, then either:
          - still running: rw.checkpoint()s the same state back to this recipe
            step with a delay, so whichever service replica is free next picks up
            the next tick;
          - succeeded: replays the pod's outbox onto the real broker (the replayed
            messages are already complete, addressed recipe envelopes, so this
            *is* the recipe continuing - no rw.send_to needed) and cleans up;
          - failed or timed out: since a dead/stuck pod never wrote a failure
            message of its own, this service manufactures one via rw.send_to.
        """
        txn = self._transport.transaction_begin(subscription_id=header["subscription"])
        self._transport.ack(header, transaction=txn)

        state = message if isinstance(message, dict) else {}
        try:
            handle = state["handle"]
            outbox_dir = state["outbox"]
        except KeyError:
            self.log.error("Rejecting malformed watch message: %r", message)
            self._transport.transaction_commit(txn)
            return
        first_seen = state.get("first-seen", time.time())

        try:
            typename = self.get_workflow_status(handle["id"])
            status = self._classify_status(typename)
        except Exception:
            # A failed status lookup is a transient API problem, not evidence the
            # workflow itself failed - keep polling rather than fail the job on it.
            self.log.exception("Failed to query status for workflow %s", handle["name"])
            status = "running"
            typename = None

        parameters = rw.recipe_step.get("parameters", {})
        timeout = parameters.get("timeout", 3600)
        poll_delay = parameters.get("burst-wait", 30)

        if status == "succeeded":
            try:
                n = replay_outbox(self._transport, outbox_dir, logger=self.log)
            except Exception:
                self.log.exception(
                    "Failed to replay outbox for workflow %s; will retry",
                    handle["name"],
                )
                rw.checkpoint(state, delay=poll_delay, transaction=txn)
                self._transport.transaction_commit(txn)
                return

            if n == 0:
                # Argo reporting success is not evidence the wrapper did its job.
                # dlstbx.wrap catches every exception out of the wrapper, logs it,
                # and still exits 0 - so a wrapper that died produces a "successful"
                # pod and a "succeeded" workflow. Its own failure() call cannot tell
                # us either, because that goes to the recipe's "failure" channel from
                # inside a pod whose only outbound path is this outbox.
                #
                # So a successful workflow that wrote nothing almost always means the
                # wrapper failed silently. Say so loudly, fail the recipe step rather
                # than letting it stop with no explanation, and deliberately skip
                # _cleanup so the outbox and recipewrapper survive for diagnosis.
                self.log.error(
                    "Workflow %s reported success but wrote no outbox messages - the "
                    "wrapper most likely failed (dlstbx.wrap exits 0 even when it "
                    "does). Keeping %s and the recipewrapper for diagnosis; the pod "
                    "log will have the traceback.",
                    handle["name"],
                    outbox_dir,
                )
                rw.send_to(
                    "failure",
                    {"handle": handle, "status": "no-messages", "success": False},
                    transaction=txn,
                )
                self._transport.transaction_commit(txn)
                return

            self.log.info(
                "Workflow %s succeeded; replayed %d outbox message(s)",
                handle["name"],
                n,
            )
            self._cleanup(outbox_dir, state.get("recipewrapper"))
            self._transport.transaction_commit(txn)
            return

        if status == "failed":
            self.log.warning("Workflow %s failed (status: %s)", handle["name"], typename)
            rw.send_to(
                "failure",
                {"handle": handle, "status": typename, "success": False},
                transaction=txn,
            )
            self._transport.transaction_commit(txn)
            return

        if time.time() - first_seen > timeout:
            self.log.warning(
                "Workflow %s timed out after %.1f seconds", handle["name"], timeout
            )
            rw.send_to(
                "failure",
                {"handle": handle, "status": "timeout", "success": False},
                transaction=txn,
            )
            self._transport.transaction_commit(txn)
            return

        # Still running: hand the same state back to ourselves and check again later.
        rw.checkpoint(
            {**state, "first-seen": first_seen}, delay=poll_delay, transaction=txn
        )
        self._transport.transaction_commit(txn)

    def _cleanup(self, outbox_dir, recipewrapper: str | None) -> None:
        """Best-effort removal of the outbox directory and recipewrapper file once
        a workflow's messages have all been replayed. Failures here are logged, not
        raised - the recipe has already moved on via the replayed messages, so a
        leftover file is litter to sweep up later, not a correctness problem."""
        try:
            shutil.rmtree(outbox_dir)
        except OSError:
            self.log.exception("Could not remove outbox directory %s", outbox_dir)
        if recipewrapper:
            try:
                os.remove(recipewrapper)
            except OSError:
                self.log.exception("Could not remove recipewrapper file %s", recipewrapper)
