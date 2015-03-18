#!/bin/env python

if __name__ == '__main__':
  import sys
  from os import environ
  from os import chdir, mkdir, getcwd
  from shutil import rmtree
  from os.path import exists, join

  job_id = int(environ["SGE_TASK_ID"]) - 1
  nslots = int(environ["NSLOTS"])

  assert(job_id >= 0)
  assert(len(sys.argv) > 1)

  filename = sys.argv[1]

  cwd = getcwd()
  assert("dlstbx" in cwd)

  with open(filename) as infile:
    lines = infile.readlines()
    assert(job_id < len(lines))
    identifier, template = lines[job_id].strip().split()
    identifier = join(cwd, identifier)

    print identifier
    print template

    if exists(identifier):
      rmtree(identifier)

    mkdir(identifier)
    chdir(identifier)

    command = ["xia2.new", "-dials", "-image", template, "nproc=%d" % nslots]

    from subprocess import call, PIPE
    print "Executing command: ", command
    call(command, env=environ)
