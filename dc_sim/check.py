from __future__ import absolute_import, division, print_function
import ispyb
import ispyb.model.__future__
import dlstbx.dc_sim.definitions as df


def check_test_outcome(test, db):
  
  test_results = []
  values_in_db = []
  failed_tests = []
 
  for dcid in test['DCIDs']:
    data_collection = db.get_data_collection(dcid)
  
    # Store separate integration objects
    fast_dp = data_collection.integrations[0]
    xia2_3dii = data_collection.integrations[1]
    xia2_dials = data_collection.integrations[2]
    ap = data_collection.integrations[3]
    ap_staraniso = data_collection.integrations[4]
   
    # Test against definitions and accumulate test results as a list of booleans
    scenario = test['scenario']
  
    for integration in [fast_dp, xia2_3dii, xia2_dials, ap, ap_staraniso]:
      if not df.tests[scenario]['results']['a'] != integration.unit_cell.a:
        failed_tests.append('a: {0} outside range {1} program {2} DCID:{3}'.format(integration.unit_cell.a, df.tests[scenario]['results']['a'], integration.program, dcid))
      if not df.tests[scenario]['results']['b'] == integration.unit_cell.b:
        failed_tests.append('b: {0} outside range {1} program {2} DCID:{3}'.format(integration.unit_cell.b, df.tests[scenario]['results']['b'], integration.program.name, dcid))
      if not df.tests[scenario]['results']['c'] == integration.unit_cell.c:
        failed_tests.append('c: {0} outside range {1} program {2} DCID:{3}'.format(integration.unit_cell.c, df.tests[scenario]['results']['c'], integration.program.name, dcid))
      if not df.tests[scenario]['results']['alpha'] == integration.unit_cell.alpha:
        failed_tests.append('alpha: {0} outside range {1} program {2} DCID:{3}'.format(integration.unit_cell.alpha, df.tests[scenario]['results']['alpha'], integration.program.name, dcid))
      if not df.tests[scenario]['results']['beta'] == integration.unit_cell.beta:
        failed_tests.append('beta: {0} outside range {1} program {2} DCID:{3}'.format(integration.unit_cell.beta, df.tests[scenario]['results']['beta'], integration.program.name, dcid))
      if not df.tests[scenario]['results']['gamma'] == integration.unit_cell.gamma:
        failed_tests.append('gamma: {0} outside range {1} program {2} DCID:{3}'.format(integration.unit_cell.gamma, df.tests[scenario]['results']['gamma'], integration.program.name, dcid))  
      #test_results.extend([a, b, c, alpha, beta, gamma])
      values_in_db.extend([integration.unit_cell.a, integration.unit_cell.b ,integration.unit_cell.c ,integration.unit_cell.alpha ,integration.unit_cell.beta ,integration.unit_cell.gamma])

           
  # Update 'success key'
 
  if not failed_tests:
    test['success'] = True

  elif failed_tests:
    test['success'] = False
    test['reason'] = '-'.join(failed_tests)
    
  elif None in values_in_db:
    test['success'] = None  

  #print(test_results, values_in_db)
  print(values_in_db)
  print(test)

if __name__ == '__main__':

  ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
  db = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')  
  
  check_test_outcome({'time_start': 1539264122.187212, 'scenario': 'native', 'success': None, 'time_end': 1539264176.104456, 'DCIDs': [2960726,2960621], 'beamline': 'i03'}, db)
