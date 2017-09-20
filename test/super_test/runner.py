
from __future__ import division

def get_env():
  from os import environ
  env = {}
  for k, v in environ.iteritems():
    try:
      env[k.decode("ascii")] = v.decode("ascii")
    except Exception:
      pass
  return env


def find_file(template, image):
  n = template.count('#')
  pfx = template.split('#')[0]
  sfx = template.split('#')[-1]
  template_str = pfx + '%%0%dd' % n + sfx
  return template_str % image


class Runner(object):
  '''
  Class to run the test

  '''

  def __init__(self, datasets, directory, max_running_tasks):
    '''
    Initialise the class

    '''
    self.datasets = datasets
    self.directory = directory
    self.max_running_tasks = max_running_tasks

  def run(self):
    '''
    Run the test

    '''
    import os
    from os.path import join, exists
    from time import gmtime
    import datetime
    import drmaa
    import stat
    import glob

    # Print some output
    print "Running test in %s" % self.directory

    # Write input file
    inputpath = join(self.directory, "input.txt")
    with open(inputpath, "w") as outfile:
      for key, dataset in self.datasets.iteritems():
        template = join(dataset["directory"], dataset["template"])
        file = find_file(template, dataset['image'])
        outfile.write("%d %s\n" % (key, file))

    # Make an output directory
    outputpath = join(self.directory, "output")
    if not exists(outputpath):
      os.makedirs(outputpath)

    # The output format
    output_fmt = join(outputpath, "output.%s")

    # Initialize the drmaa session
    session = drmaa.Session
    session.initialize()

    # Create the job template
    index = drmaa.JobTemplate.PARAMETRIC_INDEX
    job = session.createJobTemplate()
    job.workingDirectory = self.directory
    job.remoteCommand = "dlstbx.super_test_cluster_runner"
    job.jobEnvironment = get_env()
    job.args = [inputpath]
    job.name = "super_test"
    job.outputPath = ":" + (output_fmt % index)
    job.joinFiles = True
    job.nativeSpecification = '-pe smp 12 -tc %d' % self.max_running_tasks

    # The number of jobs
    first = 1
    last = len(self.datasets)

    # Run as a bulk job
    joblist = session.runBulkJobs(job, first, last, 1)
    print "Job submitted between %d and %d" % (first, last)

    # Wait for all job
    session.synchronize(joblist, drmaa.Session.TIMEOUT_WAIT_FOREVER, True)

    # Cleanup dramma stuff
    session.deleteJobTemplate(job)
    session.exit()
