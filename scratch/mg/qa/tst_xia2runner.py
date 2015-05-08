import unittest
import xia2runner

class tbd(unittest.TestCase):
  @unittest.skip('Not implemented yet')
  def test_tbd(self):
    command = ['-parameter']
    workdir = 'workdir'
    datadir = 'datadir'
    archivejson = 'archive.json'
    timeout = 60

    #os.unlink
    #os.makedirs
    #os.chdir

    xia2runner.runxia2(command, workdir, datadir, archivejson, timeout)
