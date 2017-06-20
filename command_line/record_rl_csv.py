import sys
from dlstbx.ispyb.ispyb import ispyb

i = ispyb()
dc_id = int(sys.argv[1])
rl_csv = sys.argv[2]
i.insert_rl_csv(dc_id, rl_csv)
