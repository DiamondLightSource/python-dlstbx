from __future__ import absolute_import, division, print_function

import dlstbx.dc_sim.definitions as df
import ispyb
import ispyb.model.__future__

def check_test_outcome(test, db):

  failed_tests = []
  all_programs = ['fast_dp', 'xia2 3dii', 'xia2 dials', 'autoPROC', 'autoPROC+STARANISO']

  overall = { program: True for program in all_programs }

  error_explanation = '{variable}: {value} outside range {expected}, program: {program}, DCID:{dcid}'

  for dcid in test['DCIDs']:
    outcomes = { program: { 'success': None } for program in all_programs }

    data_collection = db.get_data_collection(dcid)

    for integration in data_collection.integrations:
      if integration.program.name not in outcomes:
        continue
      if outcomes[integration.program.name]['success'] is True:
        continue

      failure_reasons = []
      expected_outcome = df.tests[test['scenario']]['results']

      for variable in ('a', 'b', 'c', 'alpha', 'beta', 'gamma'):
        if expected_outcome[variable] != getattr(integration.unit_cell, variable):
          failure_reasons.append(error_explanation.format(
              variable=variable,
              value=getattr(integration.unit_cell, variable),
              expected=expected_outcome[variable],
              program=integration.program.name,
              dcid=dcid,
          ))

      if failure_reasons:
        outcomes[integration.program.name]['success'] = False
        outcomes[integration.program.name]['reason'] = failure_reasons
      else:
        outcomes[integration.program.name]['success'] = True
        outcomes[integration.program.name]['reason'] = []

    for program in outcomes:
      if outcomes[program]['success'] is False:
        overall[program] = False
        failed_tests.extend(outcomes[program]['reason'])
      elif outcomes[program]['success'] is None and overall[program] is not False:
        overall[program] = None

  if failed_tests:
    test['success'] = False
    test['reason'] = "\n".join(failed_tests)

  if all(overall.values()):
    test['success'] = True

  print(test)

if __name__ == '__main__':
  ispyb.model.__future__.enable('/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg')
  db = ispyb.open('/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg')

  check_test_outcome({'time_start': 1539264122.187212, 'scenario': 'native', 'success': None, 'time_end': 1539264176.104456, 'DCIDs': [2960726,2960621], 'beamline': 'i03'}, db)
