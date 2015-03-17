
from __future__ import division
from libtbx.phil import parse

phil_scope = parse('''

  mode = run *info
    .type = choice
    .help = "Either run the test or print info from last run"

  dataset = None
    .type = int
    .multiple = True
    .help = "Set which datasets to run"

  max_running_tasks = auto
    .type = int(value_min=1)
    .help = "The maximum running tasks"

  resume = True
    .type = bool
    .help = "Resume last test or start a new one"

  directory = "/dls/mx-scratch/dlstbx/super_test"
    .type = str
    .help = "The base directory to run in"

''')

def load_datasets(path):
  ''' Load the datasets. '''
  import json

  # load the datasets
  with open(path, "r") as infile:
    datasets = json.load(infile)

  # Convert keys to integer
  datasets = dict((int(k), v) for k, v in datasets.iteritems())

  # Check paths are unique
  paths = [join(d['directory'], d['template']) for d in datasets.itervalues()]
  assert(len(paths) == len(set(paths)))

  # Return the datasets
  return datasets


def load_runpath(directory, mode='a'):
  '''
  Load the run path

  '''
  from os.path import join

  # Function to find last directory
  def find_last(directory):
    from os import listdir
    from os.path import isdir
    from time import strptime
    path = None
    date = None
    for item in listdir(directory):
      if isdir(join(directory, item)):
        d = strptime(item, "%Y-%m-%d")
        if date is None:
          date = d
          path = item
        elif date < d:
          date = d
          path = item
    assert(path is not None)
    return join(directory, path)

  # Function to create new directory
  def create_new(directory):
    from os import mkdir
    import datetime
    import time
    d = time.gmtime()
    path = str(datetime.date(d.tm_year, d.tm_mon, d.tm_mday))
    path = join(directory, path)
    mkdir(path)
    return path

  # Choose what to do
  if mode == 'a':
    try:
      runpath = find_last(directory)
    except Exception:
      runpath = create_new(directory)
  elif mode == 'w':
    runpath = create_new(directory)
  elif mode == 'r':
    runpath = find_last(directory)

  # Return the runpath
  return runpath

def rec_split(path):
  from os.path import split
  return path.split('/')

if __name__ == '__main__':
  from dials.util.options import OptionParser
  from dlstbx.test.super_test import Runner, Analyser
  from os.path import join
  from libtbx import Auto

  # Create the option parser
  parser = OptionParser(phil=phil_scope)

  # Get the parameters
  params, options = parser.parse_args()

  # The base and run directory
  basepath = join(params.directory, "bulk")
  runpath = join(params.directory, "run", "bulk")
  dsetpath = join(basepath, "datasets.json")

  # Load the datasets
  datasets = load_datasets(dsetpath)
  print "Loaded %d datasets" % len(datasets)

  # Find the runpath
  if params.mode == 'run':
    if params.resume:
      mode = 'a'
    else:
      mode = 'w'
  else:
    mode = 'r'
  runpath = load_runpath(runpath, mode)

  # Select the subset of datasets
  if params.dataset is not None and len(params.dataset) > 0:
    datasets = dict((i, datasets[i]) for i in params.dataset)
    print "Selected %d datasets" % len(datasets)

  # Find bad datasets if resume
  if params.mode == 'run' and params.resume:
    analyser = Analyser(datasets, runpath)
    analyser.analyse()
    datasets = dict((i, datasets[i]) for i in analyser.failed)
    assert(len(datasets) > 0)

  # Set the max running tasks
  if params.max_running_tasks == Auto:
    params.max_running_tasks = len(datasets)

  # Check if we want to run the test
  if params.mode == 'run':
    runner = Runner(datasets, runpath, params.max_running_tasks)
    runner.run()
  else:
    assert(params.mode == 'info')

  # Analyse the data
  analyser = Analyser(datasets, runpath)
  analyser.analyse()

  # Save a csv file with some info
  if len(analyser.failed) > 0:
    print "Writing %d failed datasets to failed.csv" % len(analyser.failed)
    lines = ['Beamline, Visit, Directory, Error']
    for r in analyser.results:
      if r['processed'] == False:
        i = r['id']
        error = r['error']
        d = datasets[i]
        proc_path = join(runpath, str(i))
        data_path = d['directory']
        path_split = rec_split(data_path)
        beamline = path_split[2]
        visit = path_split[5]
        lines.append('%s, %s, %s, %s' % (beamline, visit, proc_path, error))
    with open("failed.csv", "w") as outfile:
      outfile.write('\n'.join(lines))

  # Save the failed datasets as a phil file
  if len(analyser.failed) > 0:
    print "Writing %d failed datasets to failed.phil" % len(analyser.failed)
    text = '\n'.join('dataset=%d' % i for i in analyser.failed)
    with open("failed.phil", "w") as outfile:
      outfile.write(text)
