from __future__ import absolute_import, division
from dlstbx.system_test.common import CommonSystemTest

class TestRecipeService(CommonSystemTest):
  '''Tests for the recipe service.'''

  def test_recipe_parsing(self):
    '''Passing in a recipe to the service without external dependencies.
       The recipe should be interpreted and a message passed back.
       The message should contain the recipe and a correctly set pointer.'''

    recipe = {
        1: { 'service': 'DLS system test',
             'queue': 'transient.system_test.{guid}'
           },
        'start': [
           (1, { 'purpose': 'test the recipe parsing service' }),
        ]
      }

    self.send_message(
      queue='processing_recipe',
      message={
        'parameters': {},
        'selected_recipes': [],
        'custom_recipe': recipe,
      }
    )
    self.expect_message(
      queue='transient.system_test.{guid}',
      message=recipe['start'][0][1],
      header={ 'recipe': recipe,
               'recipe-pointer': 1,
             },
      timeout=3,
    )

if __name__ == "__main__":
  TestRecipeService().validate()
