import testsuite

def xia2(*args, **kwargs):
  print "=========="
  print "running test ", args, kwargs
  print "=========="

  result = { "resolution": 7 }

  testsuite.storeTestResults(result)
