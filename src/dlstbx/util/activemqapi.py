from __future__ import annotations

import dlstbx.util.jmxstats


class ActiveMQAPI:
    def connect(self, *args, **kwargs):
        self.jmx = dlstbx.util.jmxstats.JMXAPI(*args, **kwargs)

        # List of supported variables:
        # curl -XGET --user rrdapi:**password** http://cs04r-sc-vserv-69:80/api/jolokia/list | python -m json.tool

    def _query(self, **kwargs):
        result = self.jmx.org.apache.activemq(**kwargs)
        if result["status"] == 404:
            return 0
        if result["status"] != 200:
            raise RuntimeError(f"ActiveMQ responded with {result}")
        return result["value"]

    def getStorePercentUsage(self):
        return self._query(
            type="Broker", brokerName="localhost", attribute="StorePercentUsage"
        )

    def getTempPercentUsage(self):
        return self._query(
            type="Broker", brokerName="localhost", attribute="TempPercentUsage"
        )

    def getMemoryPercentUsage(self):
        return self._query(
            type="Broker", brokerName="localhost", attribute="MemoryPercentUsage"
        )

    def getConnectionsCount(self):
        return self._query(
            type="Broker", brokerName="localhost", attribute="CurrentConnectionsCount"
        )

    @property
    def _VMMemoryInfo(self):
        if not hasattr(self, "_vmmeminfo"):
            result = self.jmx.java.lang(
                type="Memory", attribute="HeapMemoryUsage,NonHeapMemoryUsage"
            )
            assert result["status"] == 200
            self._vmmeminfo = result["value"]
        return self._vmmeminfo

    def getHeapMemoryCommitted(self):
        return self._VMMemoryInfo["HeapMemoryUsage"]["committed"]

    def getHeapMemoryInitial(self):
        return self._VMMemoryInfo["HeapMemoryUsage"]["init"]

    def getHeapMemoryMaximum(self):
        return self._VMMemoryInfo["HeapMemoryUsage"]["max"]

    def getHeapMemoryUsed(self):
        return self._VMMemoryInfo["HeapMemoryUsage"]["used"]

    def getNonHeapMemoryCommitted(self):
        return self._VMMemoryInfo["NonHeapMemoryUsage"]["committed"]

    def getNonHeapMemoryInitial(self):
        return self._VMMemoryInfo["NonHeapMemoryUsage"]["init"]

    def getNonHeapMemoryMaximum(self):
        return self._VMMemoryInfo["NonHeapMemoryUsage"]["max"]

    def getNonHeapMemoryUsed(self):
        return self._VMMemoryInfo["NonHeapMemoryUsage"]["used"]

    def getMimasHeldQueueSize(self):
        return self._query(
            type="Broker",
            brokerName="localhost",
            destinationType="Queue",
            destinationName="zocalo.mimas.held",
            attribute="QueueSize",
        )

    def getQueueCount(self):
        return len(
            self._query(
                type="Broker",
                brokerName="localhost",
                destinationType="Queue",
                destinationName="*",
                attribute="ConsumerCount",
            )
        )
