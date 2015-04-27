_debug = False

class Result:
  def __init__(self, error=False, message=None, stacktrace=None, stdout=None, stderr=None):
    if _debug:
      print "Result() constructor:"
      print "  err: ", error
      print "  msg: ", message
      print "  trc: ", stacktrace
      print "  out: ", stdout
      print "  err: ", stderr

    if (error == False):
      self.error = False
    else:
      self.error = True
    self.message = None

    if (stderr is not None) and (stderr is not ""):
      self.error = True
      self.stderr = stderr
      self.message = stderr.split('\n', 1)[0]
    else:
      self.stderr = None

    if (stacktrace is not None) and (stacktrace is not ""):
      self.error = True
      self.stacktrace = stacktrace
      self.message = stacktrace.split('\n')[-1]
    else:
      self.stacktrace = None

    if (message is not None) and (message is not ""):
      self.message = message

    if (stdout is not None) and (stdout is not ""):
      self.stdout = stdout
    else:
      self.stdout = None


  def append(self, result=None, error=False, message=None, stacktrace=None, stdout=None, stderr=None):
    if _debug:
      print "Result() append:"
      print "  err: ", error
      print "  msg: ", message
      print "  trc: ", stacktrace
      print "  out: ", stdout
      print "  err: ", stderr

    if result is not None:
      self.append(error=result.error, message=result.message, stacktrace=result.stacktrace, stdout=result.stdout, stderr=result.stderr)

    if (error == True):
      self.error = True

    if self.message is None:
      if (message is not None) and (message is not ""):
        self.message = message
      # otherwise ignore new message

    if (stderr is not None) and (stderr is not ""):
      self.error = True
      if self.stderr is None:
        self.stderr = ""
      self.stderr = (self.stderr + "\n" + stderr).lstrip()
      if self.message is None:
        self.message = self.stderr.split('\n', 1)[0]

    if (stacktrace is not None) and (stacktrace is not ""):
      self.error = True
      if self.stacktrace is None:
        self.stacktrace = ""
      self.stacktrace = (self.stacktrace + "\n" + stacktrace).lstrip()
      if self.message is None:
        self.message = self.stacktrace.split('\n')[0]

    if (stdout is not None) and (stdout is not ""):
      if self.stdout is None:
        self.stdout = ""
      self.stdout = (self.stdout + "\n" + stdout).lstrip()


  def prepend(self, error=False, message=None, stacktrace=None, stdout=None, stderr=None):
    if _debug:
      print "Result() prepend:"
      print "  err: ", error
      print "  msg: ", message
      print "  trc: ", stacktrace
      print "  out: ", stdout
      print "  err: ", stderr

    if (error == True):
      self.error = True

    if self.message is None:
      if (message is not None) and (message is not ""):
        self.message = message
      # otherwise ignore new message

    if (stderr is not None) and (stderr is not ""):
      self.error = True
      if self.stderr is None:
        self.stderr = ""
      self.stderr = (stderr + "\n" + self.stderr).rstrip()
      if self.message is None:
        self.message = self.stderr.split('\n', 1)[0]

    if (stacktrace is not None) and (stacktrace is not ""):
      self.error = True
      if self.stacktrace is None:
        self.stacktrace = ""
      self.stacktrace = (stacktrace + "\n" + self.stacktrace).rstrip()
      if self.message is None:
        self.message = self.stacktrace.split('\n')[-1]

    if (stdout is not None) and (stdout is not ""):
      if self.stdout is None:
        self.stdout = ""
      self.stdout = (stdout + "\n" + self.stdout).rstrip()


  def _print(self, what, colorFunction):
    if what is not None:
      for line in what.split('\n'):
        if colorFunction is not None:
          r = colorFunction(line)
          if r is not None:
            line = r
        print line
      if colorFunction is not None:
        colorFunction(None)

  def printStdout(self, colorFunctionStdout=None, colorFunctionStderr=None, colorFunctionStacktrace=None):
    if self.stdout is None:
      return
    if (colorFunctionStacktrace is None) and (colorFunctionStderr is not None):
      colorFunctionStacktrace = colorFunctionStderr
    stdout = self.stdout.split('\n')
    if self.stderr is None:
      stderr = []
    else:
      stderr = self.stderr.split('\n')
    if self.stacktrace is None:
      stacktrace = []
    else:
      stacktrace = self.stacktrace.split('\n')

    for line in stdout:
      r = None
      if stderr and (line == stderr[0]):
        stderr = stderr[1:]
        if colorFunctionStderr is not None:
          r = colorFunctionStderr(line)
      elif stacktrace and (line == stacktrace[0]):
        stacktrace = stacktrace[1:]
        if colorFunctionStacktrace is not None:
          r = colorFunctionStacktrace(line)
      else:
        if colorFunctionStdout is not None:
          r = colorFunctionStdout(line)
      if r is not None:
        line = r
      print line
    if colorFunctionStdout is not None:
      colorFunctionStdout(None)

  def printStderr(self, colorFunction=None):
    self._print(self.stderr, colorFunction)

  def printStacktrace(self, colorFunction=None):
    self._print(self.stacktrace, colorFunction)


  def toDict(self):
    return { "error": self.error,
             "message": self.message,
             "stacktrace": self.stacktrace,
             "stdout": self.stdout,
             "stderr": self.stderr }

  def toJUnitTestCase(self, name, classname=None, time=None):
    from junit_xml import TestCase
    t = TestCase(name, classname=classname, elapsed_sec=time, stdout=self.stdout, stderr=self.stderr)
    t.add_failure_info(message=self.message, output=self.stacktrace) # None values are ignored
    if (self.message is None) and (self.stacktrace is None) and self.error:
      t.add_failure_info(message="Test failed")
      # If test is marked as failed, then either message or stacktrace need to be set.
    return t
