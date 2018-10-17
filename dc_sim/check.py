from __future__ import absolute_import, division, print_function
import ispyb
import ispyb.model.__future__
import dlstbx.dc_sim.definitions as df

def check_test_outcome(test):

  ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
  db = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')
  
  test_results = []
  values_in_db = [] 
 
  for dcid in test['DCIDs']:
    data_collection = db.get_data_collection(dcid)
  
    # Store separate integration objects
    fast_dp = data_collection.integrations[0].unit_cell
    xia2_3dii = data_collection.integrations[1].unit_cell
    xia2_dials = data_collection.integrations[2].unit_cell
    ap = data_collection.integrations[3].unit_cell
    ap_staraniso = data_collection.integrations[4].unit_cell
   
    # Test against definitions and accumulate test results as a list of booleans
    scenario = test['scenario']
  
    for integration in [fast_dp, xia2_3dii, xia2_dials, ap, ap_staraniso]:
      a = df.tests[scenario]['results']['a'] == integration.a
      b = df.tests[scenario]['results']['b'] == integration.b
      c = df.tests[scenario]['results']['c'] == integration.c
      alpha = df.tests[scenario]['results']['alpha'] == integration.alpha
      beta = df.tests[scenario]['results']['beta'] == integration.beta
      gamma = df.tests[scenario]['results']['gamma'] == integration.gamma  
      test_results.extend([a, b, c, alpha, beta, gamma])
      values_in_db.extend([integration.a, integration.b ,integration.c ,integration.alpha ,integration.beta ,integration.gamma])

           
  # Process the results list after all tests have been completed, decide if value of 'success' is altered
  if all(test_results):
    test['success'] = True
  elif all(values_in_db) and not all(test_results):
    test['success'] = False
  else:
    pass

if __name__ == '__main__':
  check_test_outcome({'time_start': 1539264122.187212, 'scenario': 'native', 'success': None, 'time_end': 1539264176.104456, 'DCIDs': [2960726,2960621], 'beamline': 'i03'})
