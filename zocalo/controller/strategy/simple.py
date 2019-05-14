from __future__ import absolute_import, division, print_function

import dlstbx.controller.strategyenvironment


def _categorize(instances):
    result = {"preparing": {}, "running": {}, "disappearing": {}}
    sortinghat = {
        dlstbx.controller.strategyenvironment.StrategyEnvironment.S_HOLD: result[
            "preparing"
        ],
        dlstbx.controller.strategyenvironment.StrategyEnvironment.S_PREPARE: result[
            "preparing"
        ],
        dlstbx.controller.strategyenvironment.StrategyEnvironment.S_STARTING: result[
            "preparing"
        ],
        dlstbx.controller.strategyenvironment.StrategyEnvironment.S_RUNNING: result[
            "running"
        ],
        dlstbx.controller.strategyenvironment.StrategyEnvironment.S_HOLDSHDN: result[
            "disappearing"
        ],
        dlstbx.controller.strategyenvironment.StrategyEnvironment.S_SHUTDOWN: result[
            "disappearing"
        ],
    }
    for host, instance in instances.iteritems():
        if instance["status"] in sortinghat:
            sortinghat[instance["status"]][host] = instance
    return result


class SimpleStrategy:
    def __init__(self, service=None, minimum=None, maximum=None, **kwargs):
        self.minimum = minimum
        self.maximum = maximum
        self.service_name = service

        assert self.service_name, "service name not defined"
        if minimum:
            assert int(minimum) >= 0, (
                "minimum instances of service %s must be a positive number" % service
            )
        if maximum:
            assert int(maximum) >= 0, (
                "maximum instances of service %s must be a positive number" % service
            )
        if minimum and maximum:
            assert int(minimum) <= int(maximum), (
                "minimum instances of service %s must be below or equal to maximum"
                % service
            )

    def assess(self, environment):

        assert isinstance(environment, dict), "passed environment is invalid"
        instances = _categorize(
            environment.get("services", {}).get(self.service_name, {})
        )
        instance_count = {k: len(v) for k, v in instances.items()}
        result = {
            "required": {
                "count": instance_count["running"] + instance_count["disappearing"]
            },
            "optional": {},
            "shutdown": {},
        }

        if self.minimum and result["required"]["count"] < self.minimum:
            result["required"]["count"] = self.minimum

        if self.maximum and result["required"]["count"] > self.maximum:
            result["required"]["count"] = self.maximum

        return result
