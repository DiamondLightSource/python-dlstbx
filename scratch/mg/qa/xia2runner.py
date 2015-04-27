import testsuite

def xia2(*args, **kwargs):
  print "=========="
  print "running test ", args, kwargs
  print "=========="

  result = { "resolution.low": 5, "resolution.high": 20 }

  testsuite.storeTestResults(result)
