from __future__ import absolute_import, division, print_function

import libtbx.phil

phil_scope = libtbx.phil.parse('''\
json = None
  .type = path
show = True
  .type = bool
''')


def main(args):
  from dlstbx.ispybtbx import ispybtbx
  ispyb_conn = ispybtbx()

  interp = phil_scope.command_line_argument_interpreter()
  params, unhandled = interp.process_and_fetch(
    args, custom_processor='collect_remaining')
  params = params.extract()

  args = unhandled

  assert len(args) > 0
  dc_ids = []
  columns = []
  for arg in args:
    try:
      dc_ids.append(int(arg))
    except ValueError:
      columns.append(arg)

  assert len(dc_ids) > 0

  if len(columns) == 0:
    columns = None

  results = None

  for dc_id in dc_ids:
    dc_info = ispyb_conn.get_dc_info(dc_id)
    start, end = ispyb_conn.dc_info_to_start_end(dc_info)
    if ispyb_conn.dc_info_is_grid_scan(dc_info):
      field_names, rows = ispyb_conn.get_pia_results([dc_id], columns=columns)
      n_images = end - start + 1
      print(ispyb_conn.dc_info_to_filename(dc_info))
      idx = [s.lower() for s in field_names].index('imagenumber')
      print('%d pia results for %s images' % (len(rows), n_images))
      if idx >= 0:
        image_numbers = set([row[idx] for row in rows])
        missing = set(range(start, end + 1)) - image_numbers
        if len(missing):
          print('Missing results for:')
          for a in sorted(missing):
            print('%s' % ispyb_conn.dc_info_to_filename(dc_info, a))
    else:
      field_names, rows = ispyb_conn.get_pia_results([dc_id], columns=columns)
      n_images = end - start + 1
      print(ispyb_conn.dc_info_to_filename(dc_info))
      print('%d pia results for %s images' % (len(rows), n_images))

    if results is None:
      results = rows
    else:
      results.extend(rows)

  if params.json is not None:
    import json
    d = {}
    for i in range(len(field_names)):
      d[field_names[i]] = [r[i] for r in rows]
    with open(params.json, 'wb') as fh:
      json.dump(d, fh, indent=2)

  if params.show:
    rows = [[str(i) for i in r] for r in results]
    rows.insert(0, field_names)

    from libtbx import table_utils
    print(table_utils.format(rows=rows, has_header=True))

if __name__ == '__main__':
  import sys
  args = sys.argv[1:]
  main(args)
