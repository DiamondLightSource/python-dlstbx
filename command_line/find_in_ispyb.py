def main(args):
  from dlstbx.ispyb.ispyb import ispyb, ispyb_filter
  import json
  i = ispyb()

  for arg in args:
    dc_id = int(arg)
    dc_info = i.get_dc_info(dc_id)
    start, end = i.dc_info_to_start_end(dc_info)

    message = { }
    parameters = {'ispyb_dcid': dc_id}

    message, parameters = ispyb_filter(message, parameters)

    print json.dumps(parameters)

if __name__ == '__main__':
  import sys
  args = sys.argv[1:]
  main(args)
