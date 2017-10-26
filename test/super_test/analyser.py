
from __future__ import absolute_import, division

class Analyser(object):
  '''
  Class to analyse the results

  '''

  def __init__(self, datasets, runpath):
    '''
    Initialise the class

    '''
    self.datasets = datasets
    self.runpath = runpath
    self.failed = []
    self.results = []

  def analyse(self):
    '''
    Analyse the results

    '''
    from os.path import join, exists
    from textwrap import wrap

    # Find out which failed
    indices = sorted(self.datasets.iterkeys())
    for i in indices:

      dataset = self.datasets[i]

      # Initialize the result
      result = {
        'id'        : i,
        'finished'  : False,
        'processed' : False,
        'section'   : None,
        'error'     : None,
      }

      # The processing path
      directory = join(self.runpath, "%d" % i)
      xia2txt = join(directory, "xia2.txt")

      # See if the xia2.txt file exists
      if exists(xia2txt):
        with open(xia2txt) as infile:
          found_status = False
          section = ""
          for line in infile.readlines():
            if line.strip().startswith("------"):
              section = line.strip().translate(None, '-').strip()
            if line.strip().startswith('Status:'):
              result['error'] = line.strip()
              if line.strip() == 'Status: normal termination':
                result['processed'] = True
                found_status = True
                break
              else:
                found_status = True
                break
          if found_status == False:
            result['error'] = '%s doesn\'t have status line' % xia2txt
          else:
            result['finished'] = True
          result['section'] = section
      else:
        result['error'] = '%s does not exists' % xia2txt

      # Add the result
      self.results.append(result)

    # Print failure table
    rows = [['#', 'Section', 'Error']]
    for r in self.results:
      if r['processed'] == False:
        rows.append([
          str(r['id']),
          str(r['section']),
          str(r['error'])])
        self.failed.append(r['id'])
    from libtbx.table_utils import format as table
    print table(rows, has_header=True)

    # Print the summary
    nprocessed = [r['processed'] for r in self.results].count(True)
    nfinished = [r['finished'] for r in self.results].count(True)
    assert(nfinished >= nprocessed)
    nrunning = len(self.results) - nfinished
    nfailed = nfinished - nprocessed
    print "Num Succeeded: %d" % nprocessed
    print "Num Failed: %d" % nfailed
    print "Num Running %d" % nrunning
