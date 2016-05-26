from __future__ import division

class Service():
  '''
  Base class for dlstbx services. A service is a piece of software that runs
  in an isolated environment, communicating only via queues with the outside
  world. Units of work are injected via a queue, results, status and log
  messages, etc. are written out via a queue. Any task can be encapsulated
  as a service, for example a service that counts spots on an image passed
  as a filename, and returns the number of counts.

  To instantiate a service three Queue-like objects should be passed to the
  constructors, one to communicate to a frontend, one to the service
  communication layer, and one for incoming commands and work packages.
  '''

  # Overrideable functions ----------------------------------------------------

  def initialize(self):
    '''Service initialization. This function is run before any commands are
       received from the frontend. This is the place to request channel
       subscriptions with the messaging layer, and register callbacks.
       This function can be overridden by specific service implementations.'''
    pass

  def log(self, logmessage):
    '''Pass a log message to the frontend.
       This function can be overridden by specific service implementations.'''
    self._log_send(logmessage)

  def update_status(self, status):
    '''Pass a status update to the frontend.
       This function can be overridden by specific service implementations.'''
    self._update_status(status)

  def shutdown(self):
    '''Service shutdown. This function is run before the service is terminated.
       No more commands are received, but communications can still be sent.
       This function can be overridden by specific service implementations.'''
    pass


  # Internal service status codes ---------------------------------------------
  # These codes will be sent to the frontend to indicate the current state of
  # the main loop regardless of the status text, which can be set freely by
  # the specific service.

  # The state transitions are: (see definition of start() below)
  #  constructor() -> NEW
  #            NEW -> start() being called -> STARTING
  #       STARTING -> self.initialize() -> IDLE
  #           IDLE -> wait for messages on command queue -> PROCESSING
  #     PROCESSING -> process command -> IDLE
  #              \--> shutdown command received -> SHUTDOWN
  #       SHUTDOWN -> self.shutdown() -> END
  #  unhandled exception -> ERROR

  SERVICE_STATUS_NEW, SERVICE_STATUS_STARTING, SERVICE_STATUS_IDLE, \
    SERVICE_STATUS_PROCESSING, SERVICE_STATUS_SHUTDOWN, SERVICE_STATUS_END, \
    SERVICE_STATUS_ERROR = range(7)

  # Not so overrideable functions ---------------------------------------------

  def __init__(self, *args, **kwargs):

    self.__queues = { 'frontend': kwargs.get('frontend'),
                      'messaging': kwargs.get('messaging'),
                      'command': kwargs.get('command') }
    self.__shutdown = False
    self.__callback_register = {}
    self.__update_service_status(self.SERVICE_STATUS_NEW)

  def _log_send(self, data_structure):
    '''Internal function to format and send log messages.'''
    self.__log_send_full({'log': data_structure, 'source': 'other'})

  def __log_send_full(self, data_structure):
    '''Internal function to actually send log messages.'''
    if self.__queues['frontend']:
      self.__queues['frontend'].put(data_structure)

  def _register(self, message_type, callback):
    '''Register a callback function for a specific command message type.'''
    self.__callback_register[message_type] = callback
 
  def _update_status(self, status):
    '''Internal function to actually send status update.'''
    if self.__queues['frontend']:
      self.__queues['frontend'].put({'status': status})

  def __update_service_status(self, statuscode):
    '''Set the internal status of the service object, and notify frontend.'''
    self.__service_status = statuscode
    if self.__queues['frontend']:
      self.__queues['frontend'].put({'statuscode': self.__service_status})

  def start(self):
    '''Start listening to command queue, process commands in main loop,
       set status, etc...
       This function is most likely called by the frontend in a separate
       process.'''
    self.__update_service_status(self.SERVICE_STATUS_STARTING)

    self.initialize()
    self._register('command', self.__process_command)

    while not self.__shutdown: # main loop

      self.__update_service_status(self.SERVICE_STATUS_IDLE)

      message = self.__queues['command'].get()

      self.__update_service_status(self.SERVICE_STATUS_PROCESSING)

      if message and 'channel' in message:
        processor = self.__callback_register.get(message['channel'])
        if processor is None:
          self.__log_send_full({
              'source': 'service',
              'cause': 'received message on unregistered channel',
              'channel': message['channel'],
              'log': message})
        else:
          processor(message.get('payload'))
      else:
        self.__log_send_full({
            'source': 'service',
            'cause': 'received message without channel information',
            'log': message})

    self.__update_service_status(self.SERVICE_STATUS_SHUTDOWN)

    self.shutdown()

    self.__update_service_status(self.SERVICE_STATUS_END)

  def __process_command(self, command):
    '''Process an incoming command message from the frontend.'''
    if command == 'shutdown':
      self.__shutdown = True
