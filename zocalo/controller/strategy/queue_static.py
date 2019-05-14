from __future__ import absolute_import, division, print_function

import logging

import dlstbx.controller.strategyenvironment


def _filter_active(instances):
    return {
        host: instance
        for host, instance in instances.iteritems()
        if instance["status"]
        in (
            dlstbx.controller.strategyenvironment.StrategyEnvironment.S_HOLD,
            dlstbx.controller.strategyenvironment.StrategyEnvironment.S_PREPARE,
            dlstbx.controller.strategyenvironment.StrategyEnvironment.S_STARTING,
            dlstbx.controller.strategyenvironment.StrategyEnvironment.S_RUNNING,
            dlstbx.controller.strategyenvironment.StrategyEnvironment.S_HOLDSHDN,
            dlstbx.controller.strategyenvironment.StrategyEnvironment.S_SHUTDOWN,
        )
    }


class QueueStaticStrategy:
    def __init__(
        self,
        service=None,
        minimum=None,
        maximum=None,
        queue=None,
        multiplier=0.04,
        **kwargs
    ):
        self.minimum = minimum
        self.maximum = maximum
        self.service_name = service
        self.queue_name = queue
        self.multiplier = multiplier
        self.log = logging.getLogger("dlstbx.controller.strategy.queuestatic")

        assert self.service_name, "service name not defined"
        assert self.queue_name, "no queue defined to watch"
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

    def watch_queues(self):
        return [self.queue_name]

    def assess(self, environment):
        assert isinstance(environment, dict), "passed environment is invalid"
        queue_size = (
            environment.get("queues", {}).get(self.queue_name, {}).get("QueueSize", 0)
        )
        ideal_level = queue_size * self.multiplier
        self.log.debug(
            "Queue size for %s is %d. Estimated number of instances required: %.1f",
            self.queue_name,
            queue_size,
            ideal_level,
        )
        ideal_level = int(ideal_level + 0.9)

        instances = _filter_active(
            environment.get("services", {}).get(self.service_name, {})
        )
        instances = len(instances)

        if instances < ideal_level:
            ideal_level = int((ideal_level + instances) / 2 + 0.6)

        if instances > ideal_level:
            ideal_level = int((ideal_level + instances) / 2)

        if self.minimum and ideal_level < self.minimum:
            ideal_level = self.minimum

        if self.maximum and ideal_level > self.maximum:
            ideal_level = self.maximum

        result = {"required": {"count": ideal_level}, "optional": {}, "shutdown": {}}

        return result
