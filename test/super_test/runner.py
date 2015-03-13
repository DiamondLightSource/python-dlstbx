
from __future__ import division

command_template = '''
#!/bin/bash

set -xe

FILE=$1
ID=$SGE_TASK_ID

PATH=$PATH:$SGE_O_PATH

ID_TEMPLATE=$(sed ${ID}'q;d' ${FILE})

ID=$(echo $ID_TEMPLATE | tr -s " " | cut -d " " -f 1)
TEMPLATE=$(echo $ID_TEMPLATE | tr -s " " | cut -d " " -f 2)


mkdir -p ${ID}
pushd ${ID} > /dev/null

dials.import template=${TEMPLATE}

dials.find_spots datablock.json

dials.index datablock.json strong.pickle

dials.refine experiments.json indexed.pickle scan_varying=True output.reflections=refined.pickle

dials.integrate refined_experiments.json refined.pickle

dials.export_mtz refined_experiments.json integrated.pickle

'''

command_resume_template = '''
#!/bin/bash

set -xe

FILE=$1
ID=$SGE_TASK_ID

PATH=$PATH:$SGE_O_PATH

ID_TEMPLATE=$(sed ${ID}'q;d' ${FILE})
ID=$(echo $ID_TEMPLATE | tr -s " " | cut -d " " -f 1)
TEMPLATE=$(echo $ID_TEMPLATE | tr -s " " | cut -d " " -f 2)

mkdir -p ${ID}
pushd ${ID} > /dev/null

if ! [ -e datablock.json ]; then
  dials.import template=${TEMPLATE}
fi

if ! [ -e strong.pickle ]; then
  dials.find_spots datablock.json
fi

if ! [ -e experiments.json ]; then
  dials.index datablock.json strong.pickle
fi

if ! [ -e refined_experiments.json ]; then
  dials.refine experiments.json indexed.pickle scan_varying=True output.reflections=refined.pickle
fi

if ! [ -e integrated.pickle ]; then
  dials.integrate refined_experiments.json refined.pickle
fi

if ! [ -e hklout.mtz ]; then
  dials.export_mtz refined_experiments.json integrated.pickle
fi

'''


class Runner(object):
  '''
  Class to run the test

  '''

  def __init__(self, basepath, runpath):
    '''
    Initialise the class

    '''
    from os.path import join
    import json

    # Set the paths
    self.basepath = basepath
    self.runpath = runpath
    assert(self.basepath != self.runpath)
    self.datasetpath = join(self.basepath, "datasets.json")

    # load the datasets
    with open(self.datasetpath, "r") as infile:
      self.datasets = json.load(infile)

    # Convert keys to integer
    self.datasets = dict((int(k), v) for k, v in self.datasets.iteritems())

    # Check paths are unique
    paths = [join(d['directory'], d['template']) for d in
             self.datasets.itervalues()]
    assert(len(paths) == len(set(paths)))

    # Print some output
    print "Loaded %d datasets" % len(paths)

  def run(self, resume=False, dataset=None, date=None):
    '''
    Run the test

    '''
    import os
    from os.path import join, exists
    from time import gmtime
    import datetime
    import drmaa
    import stat

    if dataset is None or len(dataset) == 0:
      dataset_list = self.datasets
    else:
      dataset_list = dict((i, self.datasets[i]) for i in dataset)

    # Get the time
    time = gmtime()

    # Get the date string
    if date is None:
      date_string = str(datetime.date(time.tm_year, time.tm_mon, time.tm_mday))
    else:
      date_string = date
    # Create the run directory
    self.runpath = join(self.runpath, date_string)
    if not exists(self.runpath):
      os.makedirs(self.runpath)
    return

    # Print some output
    print "Running test in %s" % self.runpath

    # Write input file
    inputpath = join(self.runpath, "input.txt")
    with open(inputpath, "w") as outfile:
      for key, dataset in dataset_list.iteritems():
        outfile.write("%d %s\n" % (
          key,
          join(dataset["directory"], dataset["template"])))

    # Write command file
    command = join(self.runpath, "command.sh")
    with open(command, "w") as outfile:
      if resume is False:
        outfile.write(command_template)
      else:
        outfile.write(command_resume_template)
    os.chmod(command, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

    # Make an output directory
    outputpath = join(self.runpath, "output")
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
    job.workingDirectory = self.runpath
    job.remoteCommand = command
    job.environment = os.environ
    job.args = [inputpath]
    job.name = "super_test"
    job.outputPath = ":" + (output_fmt % index)
    job.joinFiles = True

    # The number of jobs
    first = 1
    last = len(dataset_list)

    # Run as a bulk job
    joblist = session.runBulkJobs(job, first, last, 1)
    print "Job submitted between %d and %d" % (first, last)

    # Wait for all job
    session.synchronize(joblist, drmaa.Session.TIMEOUT_WAIT_FOREVER, True)

    # Cleanup dramma stuff
    session.deleteJobTemplate(job)
    session.exit()
