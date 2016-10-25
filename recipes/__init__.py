import uuid
from workflows.recipe import Recipe

class per_image_analysis(Recipe):
# recipe_id = uuid.uuid4()

  dcid_files = \
    [ "/dls/mx-scratch/dials/example_data/wide_rotation/X4_wide_M1S4_1_%04d.cbf" % x for x in range(1, 91) ]
  beamline = 'i04'

  recipe = \
  {
    1: { 'service': 'DLSFileMonitoring',
         'queue': 'transient.file_monitor',
         'output': { 'all': [ 2, 5 ] },
         'error-notification': [ 3 ],
       },
    2: { 'service': 'DLSPerImageAnalysis',
         'queue': 'per_image_analysis',
         'output': [ 3, 4 ],
       },
    3: { 'topic': 'transient.gda.' + beamline + '-pia-results',
       },
    4: { 'service': 'DLSISPyB',
         'queue': 'transient.ispyb.pia_result',
       },
    5: { 'topic': 'transient.dls.status_monitor',
       },
    'start': [
       (1, { 'files': dcid_files } )
       ],
  }

class simple_run_xia2(Recipe):
# recipe_id = uuid.uuid4()

  dcid_files = \
    [ "/dls/mx-scratch/dials/example_data/wide_rotation/X4_wide_M1S4_1_%04d.cbf" % x for x in range(1, 91) ]

  recipe = \
  {
    1: { 'service': 'DLSFileMonitoring',
         'queue': 'transient.file_monitor',
         'output': { 'last': 2 },
       },
    2: { 'service': 'DLSXia2',
         'queue': 'xia2_processing',
         'parameters': { 'small_molecule=true', 'image=/dls/mx-scratch/dials/example_data/wide_rotation/X4_wide_M1S4_1_0001.cbf' },
       },
    'start': [
       (1, { 'files': dcid_files } )
       ],
  }

print per_image_analysis()
