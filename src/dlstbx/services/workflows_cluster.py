import pathlib

from workflows.services.common_service import CommonService
import workflows.recipe
from dlstbx.services.cluster import DLSCluster
import os, json, getpass, requests

class VisitInput:
    def __init__(self, proposalCode, proposalNumber, number):
        self.proposalCode = proposalCode
        self.proposalNumber = int(proposalNumber)
        self.number = int(number)
    
    def to_dict(self):
        return {
            "proposalCode" : self.proposalCode,
            "proposalNumber": self.proposalNumber,
            "number": self.number
        }
# test command: zocalo.go -n -f /dls/tmp/wdr83388/test-dc_sim.json -s scenario="workflows cluster test" -s beamline=i03 -s src_dcid=22409150 -s visitNumber=1 -s proposalCode=cm -s proposalNumber=44137 -s template_name=example-template  -e devrmq
# verify at: https://workflows.diamond.ac.uk/workflows/cm44137-1
class DLSWorkflowsCluster(CommonService):
    """A service to interface zocalo with functions to start new jobs on the workflows cluster"""

    _service_name = "DLS Workflows Cluster Service"

    _logger_name = "dlstbx.services.cluster"

    def initializing(self):
        """Subscribe to the workflows cluster submission queue.
        Recieved messages must be acknowledged.
        """
        self.log.info("Cluster service is starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "workflows.submission",
            self.run_submit_job,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def submit_to_workflows(self, job_params):
        endpoint = "https://graph.diamond.ac.uk/graphql"
        #intution: mutation nameofAction($variableName: schemaImposedDataType!) {
            #schemaImposedQueryName(
            #schemaImposedParamName:$variableName
            #) {
            #     expectedReturn
            #     expectedReturn {
            #         expectedReturnType
            #     }
            # }
        #}
        mutation = """
            mutation testTemplateSubmission($templateName: String!, $visitID: VisitInput!, $parameters: JSON!){ 
       	        submitWorkflowTemplate(
                name: $templateName,
                visit: $visitID,
                parameters: $parameters
       	        ) {
                    name
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

        variables = {
                "templateName" : job_params["workflow"]["template_name"],
                "visitID" : VisitInput(job_params["workflow"]["visit"]["proposalCode"],job_params["workflow"]["visit"]["proposalNumber"],job_params["workflow"]["visit"]["number"]).to_dict(),
                "parameters" : job_params,
            }
        payload = {
            "query": mutation,
            "variables": variables,
        }    
        response = requests.request(
            headers={"Authorization": f"Bearer {os.environ['WORKFLOWS_BEARER_TOKEN']}"},
            method="POST",
            url=endpoint,
            json=payload,
        )

        return response



    def run_submit_job(self, rw, header, message):
        "Submit cluster job according to message."
        job_params = rw.recipe_step["job_parameters"]

        if "recipewrapper" in job_params:
            recipewrapper = job_params["recipewrapper"]
            try:
                DLSCluster._recursive_mkdir(os.path.dirname(recipewrapper))
            except OSError as e:
                print(e)
                self._transport.nack(header)
                return #TODO: handle this exception
            self.log.debug("Storing shave a erialized recipe wrapper in %s", recipewrapper)
            with open(recipewrapper, 'w') as fh:
                json.dump(
                    {
                        "recipe": rw.recipe.recipe,
                        "recipe-pointer": rw.recipe_pointer,
                        "environment": rw.environment,
                        "recipe-path": rw.recipe_path,
                        "payload": rw.payload,                        
                    }, fh, indent=2, separators=(",",":"),
                )
        working_directory = pathlib.Path(job_params["workingdir"])
        try:
            working_directory.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            print(e)
            return
        response = self.submit_to_workflows(job_params)

        txn = self._transport.transaction_begin(subscription_id=header["subscription"])
        self._transport.ack(header, transaction=txn)
        rw.set_default_channel("job_submitted")
        rw.send({"response": response, "scheduler": job_params}, transaction=txn)
        print('got here')

        self._transport.transaction_commit(txn)
        self.log.info(
            f"Submitted job {response.text} to '{job_params}' on partition '{job_params}'"
        )


    


    
        
            


    