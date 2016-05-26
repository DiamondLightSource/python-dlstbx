from __future__ import division

class Communication():
  '''Abstraction layer for messaging infrastructure. Here we are using ActiveMQ
     with STOMP.'''

  def __init__(self):
    # Set some sensible defaults
    self.defaults = {
      '--stomp-host': '127.0.0.1',
      '--stomp-port': 61613,
      '--stomp-user': 'user',
      '--stomp-pass': 'pass',
      '--stomp-name': 'prefix'
    }
    # Effective configuration
    self.config = {}

    self._connected = False

  def is_connected(self):
    '''Return connection status'''
    return self._connected

  def _set_parameter(self, option, opt, value, parser):
    '''callback function for optionparser'''
    del option, parser # unused
    self.config[opt] = value
    if opt == '--stomp-conf':
      self.defaults = {}

  def add_command_line_options(self, optparser):
    '''function to inject command line parameters'''
    optparser.add_option('--stomp-broker', metavar='HOST',
      default=self.defaults.get('--stomp-broker'),
      help='Stomp broker address, default %default',
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-port', metavar='PORT',
      default=self.defaults.get('--stomp-port'),
      help='Stomp broker port, default %default',
      type='int', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-user', metavar='USER',
      default=self.defaults.get('--stomp-user'),
      help='Stomp user, default %default',
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-pass', metavar='PASS',
      default=self.defaults.get('--stomp-pass'),
      help='Stomp password, default %default',
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-name', metavar='PRE',
      default=self.defaults.get('--stomp-name'),
      help='Stomp namespace prefix, default %default',
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-conf', metavar='CNF',
      default=self.defaults.get('--stomp-conf'),
      help='Stomp configuration file containing connection information, disables default values',
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)

