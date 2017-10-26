# LIBTBX_SET_DISPATCHER_NAME dlstbx.plot_reflections
from __future__ import absolute_import, division

import iotbx.phil

master_phil_scope = iotbx.phil.parse("""
oscillation_range = 1
  .type = float(value_min=0)
  .help = "The number of degrees of data to analyse."
nproc = Auto
  .type = int
""")

def run(args):
  from scitbx.array_family import flex
  from libtbx.phil import command_line
  import math
  import os

  from dxtbx.datablock import DataBlockFactory
  unhandled = []
  datablocks = DataBlockFactory.from_args(
    args, verbose=True, unhandled=unhandled)
  assert len(datablocks) == 1
  datablock = datablocks[0]
  sweeps = datablock.extract_sweeps()
  stills = datablock.extract_stills()

  if sweeps is not None and len(sweeps):
    print len(sweeps)
    imagesets = sweeps
  if stills is not None:
    print len(stills)
    imagesets = [stills[i:i+1] for i in range(len(stills))]

  args = unhandled
  cmd_line = command_line.argument_interpreter(master_params=master_phil_scope)
  working_phil = cmd_line.process_and_fetch(args=args)
  params = working_phil.extract()

  commands = []

  for i, imageset in enumerate(imagesets):
    suffix = ''
    if len(imagesets) > 1:
      suffix = '_%i' %(i+1)
    scan = imageset.get_scan()
    oscillation_width = scan.get_oscillation()[1]
    if oscillation_width > 0:
      n = min(int(math.ceil(params.oscillation_range/oscillation_width)), len(imageset))
    else:
      assert len(imageset) == 1
      n = 1
    imageset = imageset[:n]

    from dxtbx.serialize import dump
    dump.imageset(imageset, 'sweep%s.json' %suffix)

    # run dials.spotfinder
    cmd1 = " ".join(["dials.spotfinder",
                    "sweep%s.json" %suffix,
                    "min_spot_size=2",
                    "min_local=10",
                    "-o strong%s.pickle" %suffix,
                    "d_max=40",
                    ])
    if len(imagesets) > 1:
      cmd1 += " --nproc=1"

    # run dials.plot_reflections
    cmd2 = " ".join(["dials.plot_reflections",
                    "sweep%s.json" %suffix,
                    "strong%s.pickle" %suffix,
                    "output.file_name=centroids%s.png" %suffix
                    ])

    commands.append((cmd1, cmd2))

  nproc = params.nproc
  from libtbx import easy_mp
  result = easy_mp.parallel_map(
    func=run_commands,
    iterable=commands,
    processes=nproc)

def run_commands(commands):
  from libtbx import easy_run
  spotfinder_cmd = commands[0]
  plot_cmd = commands[1]

  result = easy_run.fully_buffered(spotfinder_cmd)
  result.show_stdout()
  result.show_stderr()

  result = easy_run.fully_buffered(plot_cmd)
  result.show_stdout()
  result.show_stderr()


if __name__ == '__main__':
  import sys
  run(sys.argv[1:])
