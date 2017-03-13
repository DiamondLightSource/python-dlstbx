from __future__ import absolute_import, division
from dlstbx.system_test.common import CommonSystemTest
import os
from workflows.recipe import Recipe

class ArchiverService(CommonSystemTest):
  '''Tests for the archvier service (XML dropfile generator).'''

  def test_processing_a_trivial_recipe(self):
    '''Passing in a recipe to the service without external dependencies.
       The recipe should be interpreted and a simple message passed back to a
       fixed destination.'''

    recipe = {
        1: { 'service': 'DLS Archiver',
             'queue': 'archive.pattern',
             'parameters': { 'pattern': '/dls/mx-scratch/zocalo/testdata-insulin/insulin_1_%03d.img',
                             'pattern-start': 1,
                             'pattern-end': '10',
                           },
             'output': 2
           },
        2: { 'service': 'DLS System Test',
             'queue': 'transient.system_test.' + self.guid
           },
        'start': [
           (1, { 'purpose': 'Generate an XML dropfile for specified files' }),
        ]
      }
    recipe = Recipe(recipe)
    recipe.validate()
    recipe = recipe.serialize()

    self.send_message(
      queue='archive.pattern',
      message='',
      headers={ 'recipe': recipe,
                'recipe-pointer': '1',
              }
    )

    expected_xml = os.path.join(os.path.dirname(__file__), 'archiver.xml')
    with open(expected_xml, 'r') as fh:
      xmldata = fh.read()

    self.expect_message(
      queue='transient.system_test.' + self.guid,
      message={ 'failed': 0, 'success': 10, 'xml': xmldata },
      timeout=10,
    )

if __name__ == "__main__":
  ArchiverService().validate()
