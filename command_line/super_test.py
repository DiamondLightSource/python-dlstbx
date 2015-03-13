
from __future__ import division
from libtbx.phil import parse

phil_scope = parse('''

  mode = run *info
    .type = choice

  info {

    show = *summary tests paths results all
      .type = choice

    print_figures = False
      .type = bool

  }

  tests {

    use = *all bulk
      .type = choice

    resume = True
      .type = bool

  }

  bulk {

    dataset = None
      .type = int
      .multiple = True

  }

  date = None
    .type = str
    .help = "The date of the processing (YYYY-MM-DD), otherwise use today"

  directory = "/dls/mx-scratch/dlstbx/super_test"
    .type = str

''')

if __name__ == '__main__':
  from dials.util.options import OptionParser
  from dlstbx.test import super_test

  # Create the option parser
  parser = OptionParser(phil=phil_scope)

  # Get the parameters
  params, options = parser.parse_args()

  # Choose what to do
  for test in super_test.get_tests(params.tests.use, params.directory):
    if params.mode == 'run':
      test.run(params.tests.resume, params.bulk.dataset, params.date)
    elif params.mode == 'info':
      print test.info(params.info.show)
      if params.info.write_figures:
        test.print_figures()
    else:
      raise RuntimeError('Unknown run mode')
