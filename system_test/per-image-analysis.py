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
           (1, { 'file': '/dls/mx-scratch/zocalo/testdata-insulin/insulin_1_001.img' }),
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

      def __eq__(self, other):
        '''Comparison function'''
        if not isinstance(other, dict):
          return False

        requirements = [
          { 'name': 'd_min_distl_method_1', 'type': numbers.Number },
          { 'name': 'd_min_distl_method_2', 'type': numbers.Number },
          { 'name': 'estimated_d_min', 'type': numbers.Number },
          { 'name': 'image', 'equals': recipe['start'][0][1]['file'] },
          { 'name': 'n_spots_4A', 'type': numbers.Number },
          { 'name': 'n_spots_no_ice', 'type': numbers.Number },
          { 'name': 'n_spots_total', 'type': numbers.Number },
          { 'name': 'noisiness_method_2', 'type': numbers.Number },
          { 'name': 'noisiness_method_1', 'type': numbers.Number },
          { 'name': 'total_intensity', 'min': 8000000, 'max': 10000000 },
        ]

        for r in requirements:
          if r['name'] not in other:
            return False
          if 'min' in r and r['min'] > other[r['name']]:
            return False
          if 'max' in r and r['max'] < other[r['name']]:
            return False
          if 'equals' in r and r['equals'] != other[r['name']]:
            return False
          if 'type' in r and not isinstance(other[r['name']], r['type']):
            print "Field %s (%s) is not of type %s" % (r['name'], str(other[r['name']]), r['type'])
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
