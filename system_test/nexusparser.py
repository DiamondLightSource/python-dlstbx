from __future__ import absolute_import, division, print_function

import os

from dlstbx.system_test.common import CommonSystemTest
from workflows.recipe import Recipe

imagepath = '/dls/science/groups/scisoft/DIALS/regression-test-data/vmxi-unknown-stuff/'

class NexusParserService(CommonSystemTest):
  '''Tests for the per-image-analysis service.'''

  def test_find_all_referenced_files(self):
    '''Find all files referenced in an example nexus dataset.'''

    recipe = {
        1: { 'service': 'DLS NexusParser',
             'queue': 'nexusparser.find_related_files',
             'output': 2,
           },
        2: { 'service': 'DLS System Test',
             'queue': 'transient.system_test.' + self.guid,
           },
        'start': [
           (1, { 'file': imagepath + 'image_1424.nxs' }),
        ]
      }
    recipe = Recipe(recipe)
    recipe.validate()

    self.send_message(
      queue=recipe[1]['queue'],
      message={ 'payload': recipe['start'][0][1],
                'recipe': recipe.recipe,
                'recipe-pointer': '1',
                'environment': { 'ID': self.guid },
              },
      headers={ 'workflows-recipe': True }
    )

    self.expect_recipe_message(
      environment={ 'ID': self.guid },
      recipe=recipe,
      recipe_path=[ 1 ],
      recipe_pointer=2,
      payload={ 'filelist': sorted([imagepath + 'image_1424_data_000001.h5',
                                    imagepath + 'image_1424_data_000002.h5',
                                    imagepath + 'image_1424_data_000003.h5',
                                    imagepath + 'image_1424_data_000004.h5',
                                    imagepath + 'image_1424_meta.hdf5',
                                    imagepath + 'image_1424.nxs']),
      },
      timeout=120,
    )

if __name__ == "__main__":
  NexusParserService().validate()
