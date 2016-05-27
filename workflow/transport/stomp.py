from __future__ import absolute_import
from __future__ import division
import stomp
import threading
import time

class Transport():
  '''Abstraction layer for messaging infrastructure. Here we are using ActiveMQ
     with STOMP.'''

  def __init__(self):
    # Set some sensible defaults
    self.defaults = {
      '--stomp-host': 'localhost',
      '--stomp-port': 61613,
      '--stomp-user': 'admin',
      '--stomp-pass': 'password',
      '--stomp-prfx': 'demo'
    }
    # Effective configuration
    self.config = {}

    self._connected = False
    self._namespace = None
    self._lock = threading.RLock()

  def connect(self):
    with self._lock:
      if self._connected:
        return True
      self._conn = stomp.Connection([(
        self.config.get('--stomp-host', self.defaults.get('--stomp-host')),
        int(self.config.get('--stomp-port', self.defaults.get('--stomp-port'))),
        )])
      self._conn.set_listener('', stomp.PrintingListener())
      try:
        self._conn.start()
      except stomp.exception.ConnectFailedException:
        return False
      self._conn.connect(
        self.config.get('--stomp-user', self.defaults.get('--stomp-user')),
        self.config.get('--stomp-pass', self.defaults.get('--stomp-pass')),
        wait=True)
      self._namespace = \
        self.config.get('--stomp-prfx', self.defaults.get('--stomp-prfx'))
      self._connected = True
    return True

  def is_connected(self):
    '''Return connection status'''
    return self._connected

  def broadcast_retain(self, message, channel=None):
    destination = ['/topic/transient']
    if self.get_namespace():
      destination.append(self.get_namespace())
    if channel:
      destination.append(channel)
    destination = '.'.join(destination)
    with self._lock:
      self._conn.send(
          body=message,
          destination=destination,
#         retain=True,
          headers={ 'activemq.retain': True,
#                   'persistent': 'true',
                    'expires': '%d' % int((30 + time.time()) * 1000)
                   })

  def get_namespace(self):
    '''Return the stomp namespace. This is a prefix used for all topic and
       queue names.'''
    return self._namespace

  def _set_parameter(self, option, opt, value, parser):
    '''callback function for optionparser'''
    self.config[opt] = value
    if opt == '--stomp-conf':
      import ConfigParser
      cfgparser = ConfigParser.ConfigParser(allow_no_value=True)
      if not cfgparser.read(value):
        raise RuntimeError('Could not read from configuration file %s' % value)
      for cfgoption, target in [
          ('host', '--stomp-host'),
          ('port', '--stomp-port'),
          ('password', '--stomp-pass'),
          ('username', '--stomp-user'),
          ('prefix', '--stomp-prfx'),
          ]:
        try:
          self.defaults[target] = cfgparser.get('stomp', cfgoption)
        except ConfigParser.NoOptionError:
          pass

  def add_command_line_options(self, optparser):
    '''function to inject command line parameters'''
    optparser.add_option('--stomp-host', metavar='HOST',
      default=self.defaults.get('--stomp-host'),
      help="Stomp broker address, default '%default'",
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-port', metavar='PORT',
      default=self.defaults.get('--stomp-port'),
      help="Stomp broker port, default '%default'",
      type='int', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-user', metavar='USER',
      default=self.defaults.get('--stomp-user'),
      help="Stomp user, default '%default'",
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-pass', metavar='PASS',
      default=self.defaults.get('--stomp-pass'),
      help="Stomp password, default '%default'",
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-prfx', metavar='PRE',
      default=self.defaults.get('--stomp-prfx'),
      help="Stomp namespace prefix, default '%default'",
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-conf', metavar='CNF',
      default=self.defaults.get('--stomp-conf'),
      help='Stomp configuration file containing connection information, disables default values',
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)

