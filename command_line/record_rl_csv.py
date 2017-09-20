import sys

from dlstbx.ispybtbx import ispybtbx

i = ispybtbx()
dc_id = int(sys.argv[1])
rl_csv = sys.argv[2]
i.insert_rl_csv(dc_id, rl_csv)
