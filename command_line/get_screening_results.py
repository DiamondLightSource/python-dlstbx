def run(args):
  from dlstbx.ispyb.ispyb import ispyb
  import json
  ispyb_conn = ispyb()

  assert len(args) > 0
  dc_ids = []
  columns = []
  for arg in args:
    try:
      dc_ids.append(int(arg))
    except ValueError:
      columns.append(arg)

  if len(columns) == 0:
    columns = None
  elif len(dc_ids) > 1:
    columns.insert(0, 'AutoProcIntegration.dataCollectionId')

  field_names, rows = ispyb_conn.get_screening_results(dc_ids, columns=columns)
  rows = [[str(i) for i in r] for r in rows]
  rows.insert(0, field_names)

  from libtbx import table_utils
  print table_utils.format(rows=rows, has_header=True)

if __name__ == '__main__':
  import sys
  args = sys.argv[1:]
  run(args)

