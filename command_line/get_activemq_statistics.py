from __future__ import absolute_import, division, print_function

import logging
import time

import dlstbx.util.jmxstats
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from dlstbx.util.rrdtool import RRDTool

class ActiveMQAPI(object):
  def __init__(self, configuration_file):
    self.jmx = dlstbx.util.jmxstats.JMXAPI(configuration_file)

    # List of supported variables:
    # curl -XGET --user rrdapi:**password** http://cs04r-sc-vserv-69:80/api/jolokia/list | python -m json.tool

  def getStorePercentUsage(self):
    result = self.jmx.org.apache.activemq(type="Broker", brokerName="localhost/StorePercentUsage")
    assert result['status'] == 200
    return result['value']

  def getTempPercentUsage(self):
    result = self.jmx.org.apache.activemq(type="Broker", brokerName="localhost/TempPercentUsage")
    assert result['status'] == 200
    return result['value']

  def getMemoryPercentUsage(self):
    result = self.jmx.org.apache.activemq(type="Broker", brokerName="localhost/MemoryPercentUsage")
    assert result['status'] == 200
    return result['value']

  def getConnectionsCount(self):
    result = self.jmx.org.apache.activemq(type="Broker", brokerName="localhost/CurrentConnectionsCount")
    assert result['status'] == 200
    return result['value']

class ActiveMQRRD(object):
  def __init__(self, path='.', api=None):
    self.rrd = RRDTool(path)
    self.setup_rrd()
    self.api_activemq = api
    self.log = logging.getLogger('dlstbx.command_line.activemq_stats')

  def setup_rrd(self):
    daydata       = [ 'RRA:%s:0.5:1:1440' % cls for cls in ('AVERAGE', 'MAX') ]
    weekdata      = [ 'RRA:%s:0.5:3:3360' % cls for cls in ('AVERAGE', 'MAX') ]
    monthdata     = [ 'RRA:%s:0.5:6:7440' % cls for cls in ('AVERAGE', 'MAX') ]
    self.rrd_activemq = self.rrd.create(
          'activemq-statistics.rrd', [ '--step', '60' ]
        + [ 'DS:storageused:GAUGE:180:0:U',
            'DS:tempused:GAUGE:180:0:U',
            'DS:memoryused:GAUGE:180:0:U',
            'DS:connections:GAUGE:180:0:U',
          ]
        + daydata + weekdata + monthdata
      )

  def update(self):
    update_time = int(time.time())
    self.log.info("Last known data point:    %d", self.rrd_activemq.last_update)
    self.log.info("Current time:             %d", update_time)
    if update_time - self.rrd_activemq.last_update <= 30:
      self.log.info("No update required.")
      return
    if not self.api_activemq:
      self.log.warn("ActiveMQ API not available.")
      return

    self.rrd_activemq.update([ [ update_time,
                                 self.api_activemq.getStorePercentUsage(),
                                 self.api_activemq.getTempPercentUsage(),
                                 self.api_activemq.getMemoryPercentUsage(),
                                 self.api_activemq.getConnectionsCount(),
                               ] ])
    self.log.info("Updated to:               %d", self.rrd_activemq.last_update)

def setup_logging(level=logging.INFO):
  console = ColorStreamHandler()
  console.setLevel(level)
  logger = logging.getLogger()
  logger.setLevel(logging.WARN)
  logger.addHandler(console)
  logging.getLogger('dlstbx').setLevel(level)

setup_logging(logging.INFO)
amq = ActiveMQAPI('/dls_sw/apps/zocalo/secrets/credentials-jmx-access.cfg')
ActiveMQRRD(api=amq).update()
