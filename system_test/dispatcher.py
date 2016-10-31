from __future__ import absolute_import, division
from dlstbx.system_test.common import CommonSystemTest
import json
from workflows.recipe import Recipe

class DispatcherService(CommonSystemTest):
  '''Tests for the dispatcher service (recipe service).'''

  def test_processing_a_trivial_recipe(self):
    '''Passing in a recipe to the service without external dependencies.
       The recipe should be interpreted and a simple message passed back to a
       fixed destination.'''

    recipe = {
        1: { 'service': 'DLS system test',
             'queue': 'transient.system_test.' + self.guid
           },
        'start': [
           (1, { 'purpose': 'trivial test for the recipe parsing service' }),
        ]
      }

    self.send_message(
      queue='processing_recipe',
      message={
        'custom_recipe': recipe,
      }
    )

    self.expect_message(
      queue='transient.system_test.' + self.guid,
      message=recipe['start'][0][1],
      timeout=3,
    )

  def test_parsing_a_recipe_and_replacing_parameters(self):
    '''Passing in a recipe to the service without external dependencies.
       The recipe should be interpreted, the 'guid' placeholder replaced using
       the parameter field, and the message passed back.
       The message should then contain the recipe and a correctly set pointer.'''

    recipe = {
        1: { 'service': 'DLS system test',
             'queue': 'transient.system_test.{guid}'
           },
        'start': [
           (1, { 'purpose': 'test the recipe parsing service' }),
        ]
      }
    parameters = { 'guid': self.guid }

    self.send_message(
      queue='processing_recipe',
      message={
        'parameters': parameters,
        'custom_recipe': recipe,
      }
    )

    expected_recipe = Recipe(recipe)
    expected_recipe.apply_parameters(parameters)
    self.expect_message(
      queue='transient.system_test.' + self.guid,
      message=recipe['start'][0][1],
      headers={ 'recipe': expected_recipe,
                'recipe-pointer': '1',
              },
      timeout=3,
    )

  def test_loading_a_recipe_from_a_file(self):
    '''When a file name is passed to the service the file should be loaded and
       parsed correctly, including parameter replacement.'''

    parameters = { 'guid': self.guid }
    self.send_message(
      queue='processing_recipe',
      message={
        'parameters': parameters,
        'recipes': [ 'system-test-dispatcher' ],
      }
    )

    with open('/dls_sw/apps/mx-scripts/plum-duff/recipes/system-test-dispatcher.json', 'r') as fh:
      recipe = json.loads(fh.read())

    self.expect_message(
      queue='transient.system_test.' + self.guid,
      message=recipe['start'][0][1],
      headers={ 'recipe-pointer': '1' },
      timeout=3,
    )


if __name__ == "__main__":
  DispatcherService().validate()
