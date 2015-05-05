_debug = False

from junit_xml import TestCase

class Result(TestCase):
  def __init__(self):
    TestCase.__init__(self, None)

    self.name = None
    self.classname = None
    self.elapsed_sec = 0
    self.skipped_message = None # first skipped message
    self.skipped_output  = None # skipped messages
    self.failure_message = None # error message
    self.failure_output  = None # stack trace
                                # to test for failure use is_failure()
    self.stdout = None          # standard output
    self.stderr = None          # standard error
    self.log = []

  def append(self, result=None, message=None, stacktrace=None, stdout=None, stderr=None):
    if _debug:
      print "Result() append:"
      print "  msg: ", message
      print "  trc: ", stacktrace
      print "  out: ", stdout
      print "  err: ", stderr

    if result is not None:
      self.append(message=result.failure_message, stacktrace=result.failure_output, stdout=result.stdout, stderr=result.stderr)

    if self.failure_message is None:
      if (message is not None) and (message is not ""):
        self.failure_message = message
      # otherwise ignore new message

    if (stderr is not None) and (stderr is not ""):
      if self.stderr is None:
        self.stderr = ""
      self.stderr = (self.stderr + "\n" + stderr).lstrip("\n")
      if self.failure_message is None:
        self.failure_message = self.stderr.split('\n', 1)[0]

    if (stacktrace is not None) and (stacktrace is not ""):
      if self.failure_output is None:
        self.failure_output = ""
      self.failure_output = (self.failure_output + "\n" + stacktrace).lstrip("\n")
      if self.failure_message is None:
        self.failure_message = self.failure_output.split('\n')[0]

    if (stdout is not None) and (stdout is not ""):
      if self.stdout is None:
        self.stdout = ""
      self.stdout = (self.stdout + "\n" + stdout).lstrip("\n")


  def log_message(self, text):
    self.log.append((0, text))
    if self.stdout is None:
      self.stdout = text
    else:
      self.stdout = self.stdout + "\n" + text

  def log_skip(self, text):
    self.log.append((1, text))
    if self.skipped_message is None:
      self.skipped_message = text
    if self.skipped_output is None:
      self.skipped_output = text
    else:
      self.skipped_output = self.skipped_output + "\n" + text

  def log_error(self, text):
    self.log.append((2, text))
    if self.failure_message is None:
      self.failure_message = text
    if self.stderr is None:
      self.stderr = text
    else:
      self.stderr = self.stderr + "\n" + text

  def log_trace(self, text):
    self.log.append((3, text))
    if self.failure_message is None:
      self.failure_message = text.split('\n')[0]
    if self.failure_output is None:
      self.failure_output = text
    else:
      self.failure_output = self.failure_output + "\n" + text

  def printResult(self):
    import term
    for (c, t) in self.log:
      term.color(['green', 'yellow', 'red', 'red'][c])
      print t

  def set_name(self, name):
    self.name = name

  def set_classname(self, classname):
    self.classname = classname

  def set_time(self, time):
    self.elapsed_sec = time

  def is_success(self):
    return not self.is_failure() and not self.is_error() and not self.is_skipped()

  def toJUnitTestCase(self):
    return self
#   t = TestCase(self.name, classname=self.classname, elapsed_sec=self.elapsed_sec, stdout=self.stdout, stderr=self.stderr)
#   t.add_failure_info(message=self.failure_message, output=self.failure_output) # None values are ignored
#   t.add_skipped_info(message=self.skipped_message)
#   return t
