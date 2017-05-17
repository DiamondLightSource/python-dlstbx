import libtbx.phil

phil_scope = libtbx.phil.parse('''\
statistics_type = outerShell innerShell *overall
  .type = choice
''')

def run(args):

  from dlstbx.ispyb.ispyb import ispyb
  import json


  interp = phil_scope.command_line_argument_interpreter()
  params, unhandled = interp.process_and_fetch(
    args, custom_processor='collect_remaining')
  params = params.extract()

  args = unhandled

  assert len(args) > 0
  dc_id = args[0]
  if len(args) > 1:
    columns = args[1:]
  else:
    columns = None

  ispyb_conn = ispyb()
  field_names, rows = ispyb_conn.get_processing_statistics(dc_id, columns=columns, statistics_type=params.statistics_type)
  rows = [[str(i) for i in r] for r in rows]
  rows.insert(0, field_names)

  from libtbx import table_utils
  print table_utils.format(rows=rows, has_header=True)

if __name__ == '__main__':
  import sys
  args = sys.argv[1:]
  run(args)

