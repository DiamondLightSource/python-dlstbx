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

  def test_guid_generation_during_recipe_parsing(self):
    '''The guid parameter should be created during parsing of each recipe.'''

    recipe = {
        1: { 'service': 'DLS system test',
             'queue': 'transient.system_test.' + self.guid
           },
        'start': [
           (1, { 'purpose': 'guid generation test for the recipe parsing service' }),
        ]
      }

    # TODO: The testing framework actually does not support this atm!

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

  def test_combining_recipes(self):
    '''Combine a recipe from a file and a custom recipe.'''

    parameters = { 'guid': self.guid }
    recipe_passed = {
      1: { 'service': 'DLS system test',
           'queue': 'transient.system_test.{guid}'
         },
      'start': [
         (1, { 'purpose': 'test recipe merging' }),
      ]
    }
    self.send_message(
      queue='processing_recipe',
      message={
        'parameters': parameters,
        'custom_recipe': recipe_passed,
        'recipes': [ 'system-test-dispatcher' ],
      }
    )

    with open('/dls_sw/apps/mx-scripts/plum-duff/recipes/system-test-dispatcher.json', 'r') as fh:
      recipe_from_file = json.loads(fh.read())

    self.expect_message(
      queue='transient.system_test.' + self.guid,
      message=recipe_from_file['start'][0][1],
      headers={ 'recipe-pointer': '2' },
      timeout=3,
    )
    self.expect_message(
      queue='transient.system_test.' + self.guid,
      message=recipe_passed['start'][0][1],
      headers={ 'recipe-pointer': '1' },
      timeout=3,
    )

  def test_ispyb_magic(self):
    '''Test the ISPyB magic to see that it does what we think it should do'''

    recipe = {
        1: { 'service': 'DLS system test',
             'queue': 'transient.system_test.' + self.guid
           },
        'start': [
           (1, { 'purpose': 'testing if ISPyB connection works',
                 'parameters': {'image':'{ispyb_image}'}
                 }),
        ]
      }

    self.send_message(
      queue='processing_recipe',
      message={
        'custom_recipe': recipe,
        'parameters':{'ispyb_dcid':1397955}
      }
    )

    self.expect_message(
      queue='transient.system_test.' + self.guid,
      message={'purpose': 'testing if ISPyB connection works',
               'parameters': {'image':'/dls/i03/data/2016/cm14451-4/tmp/2016-10-07/fake113556/TRP_M1S6_4_0001.cbf:1:1800'}
               },
      timeout=3
    )

if __name__ == "__main__":
  DispatcherService().validate()
