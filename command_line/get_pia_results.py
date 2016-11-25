def main(args):
  from dlstbx.ispyb.ispyb import ispyb, ispyb_filter
  i = ispyb()

  for arg in args:
    dc_id = int(arg)
    dc_info = i.get_dc_info(dc_id)
    start, end = i.dc_info_to_start_end(dc_info)
    if i.dc_info_is_grid_scan(dc_info):
      pia = i.get_pia_results(dc_id)
      n_images = end - start + 1
      print i.dc_info_to_filename(dc_info)
      print '%d pia results for %s images' % (len(pia), n_images)
      all = set(range(start, end + 1))
      for p in pia:
        all.discard(p[0])
      if all:
        print 'Missing results for:'
        for a in sorted(all):
          print '%s' % i.dc_info_to_filename(dc_info, a)
    else:
      pia = i.get_pia_results(dc_id)
      n_images = end - start + 1
      print i.dc_info_to_filename(dc_info)
      print '%d pia results for %s images' % (len(pia), n_images)


if __name__ == '__main__':
  import sys
  args = sys.argv[1:]
  main(args)
