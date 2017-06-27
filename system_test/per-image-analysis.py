from __future__ import absolute_import, division
from dlstbx.system_test.common import CommonSystemTest
import numbers
import os
from workflows.recipe import Recipe

class PerImageAnalysisService(CommonSystemTest):
  '''Tests for the per-image-analysis service.'''

  def test_image_analysis(self):
    '''Run PIA on the first file of the insulin test data.'''

    recipe = {
        1: { 'service': 'DLS Per-Image-Analysis',
             'queue': 'per_image_analysis',
             'output': 2
           },
        2: { 'service': 'DLS System Test',
             'queue': 'transient.system_test.' + self.guid
           },
        'start': [
           (1, { 'file': '/dls/mx-scratch/zocalo/testdata-insulin/insulin_1_007.img',
                 'file-number': 1,
                 'file-pattern-index': 7 }),
        ]
      }
    recipe = Recipe(recipe)
    recipe.validate()

    self.send_message(
      queue=recipe[1]['queue'],
      message={ 'payload': recipe['start'][0][1],
                'recipe': recipe.recipe,
                'recipe-pointer': '1',
              },
      headers={ 'workflows-recipe': True }
    )

    class PayloadIsValidPIAResult(object):
      '''A helper class to validate incoming results.'''

      def __eq__(self_eq, other):
        '''Comparison function'''
        if not isinstance(other, dict):
          return False

        requirements = [
          { 'name': 'd_min_distl_method_1', 'type': numbers.Number },
          { 'name': 'd_min_distl_method_2', 'type': numbers.Number },
          { 'name': 'estimated_d_min', 'type': numbers.Number },
          { 'name': 'file', 'equals': recipe['start'][0][1]['file'] },
          { 'name': 'file-number', 'equals': recipe['start'][0][1]['file-number'] },
          { 'name': 'file-pattern-index', 'equals': recipe['start'][0][1]['file-pattern-index'] },
          { 'name': 'n_spots_4A', 'type': numbers.Number },
          { 'name': 'n_spots_no_ice', 'type': numbers.Number },
          { 'name': 'n_spots_total', 'type': numbers.Number },
          { 'name': 'noisiness_method_2', 'type': numbers.Number },
          { 'name': 'noisiness_method_1', 'type': numbers.Number },
          { 'name': 'total_intensity', 'min': 6000000, 'max': 8000000 },
        ]

        for r in requirements:
          if r['name'] not in other:
            self.log.warning("Field %s is missing in output", r['name'])
            return False
          if 'min' in r and r['min'] > other[r['name']]:
            self.log.warning("Field %s (%s) is below minimum (%s)", r['name'], str(other[r['name']]), str(r['min']))
            return False
          if 'max' in r and r['max'] < other[r['name']]:
            self.log.warning("Field %s (%s) is above maximum (%s)", r['name'], str(other[r['name']]), str(r['max']))
            return False
          if 'equals' in r and r['equals'] != other[r['name']]:
            self.log.warning("Field %s (%s) does not match %s", r['name'], str(other[r['name']]), str(r['equals']))
            return False
          if 'type' in r and not isinstance(other[r['name']], r['type']):
            self.log.warning("Field %s (%s) is not of type %s", r['name'], str(other[r['name']]), str(r['type']))
            return False

        return True

    self.expect_recipe_message(
      recipe=recipe,
      recipe_path=[ 1 ],
      recipe_pointer=2,
      payload=PayloadIsValidPIAResult(),
      timeout=120,
    )

if __name__ == "__main__":
  PerImageAnalysisService().validate()
