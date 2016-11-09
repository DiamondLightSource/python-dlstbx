def main(args):
  from dlstbx.ispyb.ispyb import ispyb
  i = ispyb()

  for arg in args:
    dc_id = int(arg)
    dc_info = i.get_dc_info(dc_id)
    start, end = i.dc_info_to_start_end(dc_info)
    print '%s:%d:%d' % (i.dc_info_to_filename(dc_info), start, end)

if __name__ == '__main__':
  import sys
  args = sys.argv[1:]
  main(args)
