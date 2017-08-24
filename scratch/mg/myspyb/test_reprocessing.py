from __future__ import division, absolute_import
import ispyb

api = ispyb.driver(ispyb.Backend.DATABASE)
i = api(config_file='/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')

from pprint import pprint
pprint(i.get_reprocessing_id(73))

pprint(i.get_reprocessing_parameters(73))

sweeps = i.get_reprocessing_sweeps(55)
for sweep in sweeps:
  print sweep
  pprint(i.get_datacollection_template(sweep['dataCollectionId']))


