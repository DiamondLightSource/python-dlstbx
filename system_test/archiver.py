from __future__ import absolute_import, division
from dlstbx.system_test.common import CommonSystemTest
import os
from workflows.recipe import Recipe

class ArchiverService(CommonSystemTest):
  '''Tests for the archiver service (XML dropfile generator).'''

  def test_archive_a_set_of_existing_files(self):
    '''Generate a dropfile for a small set of files, and compare against a saved copy.'''

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

    self.send_message(
      queue='archive.pattern',
      message={ 'payload': '',
                'recipe': recipe.recipe,
                'recipe-pointer': '1',
                'environment': { 'ID': self.guid },
              },
      headers={ 'workflows-recipe': True }
    )

    expected_xml = os.path.join(os.path.dirname(__file__), 'archiver-success.xml')
    with open(expected_xml, 'r') as fh:
      xmldata = fh.read()

    self.expect_recipe_message(
      environment={ 'ID': self.guid },
      recipe=recipe,
      recipe_path=[ 1 ],
      recipe_pointer=2,
      payload={ 'failed': 0, 'success': 10, 'xml': xmldata },
      timeout=60,
    )

  def test_split_set_of_existing_files_into_multiple_archives(self):
    '''Generate multiple dropfiles for a set of files, and compare against saved copies.'''

    recipe = {
        1: { 'service': 'DLS Archiver',
             'queue': 'archive.pattern',
             'parameters': { 'pattern': '/dls/mx-scratch/zocalo/testdata-insulin/insulin_1_%03d.img',
                             'pattern-start': 1,
                             'pattern-end': '10',
                             'limit-files': 6,
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

    self.send_message(
      queue='archive.pattern',
      message={ 'payload': '',
                'recipe': recipe.recipe,
                'recipe-pointer': '1',
                'environment': { 'ID': self.guid },
              },
      headers={ 'workflows-recipe': True }
    )

    expected_xml_1 = os.path.join(os.path.dirname(__file__), 'archiver-success-part1.xml')
    expected_xml_2 = os.path.join(os.path.dirname(__file__), 'archiver-success-part2.xml')
    with open(expected_xml_1, 'r') as fh:
      xmldata_1 = fh.read()
    with open(expected_xml_2, 'r') as fh:
      xmldata_2 = fh.read()

    self.expect_recipe_message(
      environment={ 'ID': self.guid },
      recipe=recipe,
      recipe_path=[ 1 ],
      recipe_pointer=2,
      payload={ 'failed': 0, 'success': 6, 'xml': xmldata_1 },
      timeout=60,
    )

    self.expect_recipe_message(
      environment={ 'ID': self.guid },
      recipe=recipe,
      recipe_path=[ 1 ],
      recipe_pointer=2,
      payload={ 'failed': 0, 'success': 4, 'xml': xmldata_2 },
      timeout=60,
    )

  def test_archive_a_set_of_partially_missing_files(self):
    '''Generate a dropfile for a small set of files, some of which are missing, and compare against a saved copy.'''

    recipe = {
        1: { 'service': 'DLS Archiver',
             'queue': 'archive.pattern',
             'parameters': { 'pattern': '/dls/mx-scratch/zocalo/testdata-insulin/insulin_1_%03d.img',
                             'pattern-start': '40',
                             'pattern-end': 50,
                             'log-summary-warning-as-info': True,
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

    self.send_message(
      queue='archive.pattern',
      message={ 'payload': '',
                'recipe': recipe.recipe,
                'recipe-pointer': '1',
                'environment': { 'ID': self.guid },
              },
      headers={ 'workflows-recipe': True }
    )

    expected_xml = os.path.join(os.path.dirname(__file__), 'archiver-partial.xml')
    with open(expected_xml, 'r') as fh:
      xmldata = fh.read()

    self.expect_recipe_message(
      environment={ 'ID': self.guid },
      recipe=recipe,
      recipe_path=[ 1 ],
      recipe_pointer=2,
      payload={ 'failed': 5, 'success': 6, 'xml': xmldata },
      timeout=60,
    )

if __name__ == "__main__":
  ArchiverService().validate()
