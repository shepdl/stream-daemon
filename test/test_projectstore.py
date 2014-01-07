import unittest
from MockRiak import MockRiak

import riak

from Project import ProjectStore, Project

class TestProjectStore(unittest.TestCase):
    def test_create(self):
        mock_db = riak.RiakClient()
        mock_bucket = mock_db.bucket("message")
        mock_id = "proj-1"
        instance = ProjectStore(mock_db)
        (project, id) = instance.create("Sample", ["sample1", "sample2",], id_key=mock_id)
        self.assertEqual(project.name, "Sample")
        self.assertTrue(project.id, mock_id)

    def test_stop(self):
        mock_db = riak.RiakClient()
        instance = ProjectStore(mock_db)
        (project, id) = instance.create("Sample", ["sample1", "sample2",])
        self.assertEqual(instance.active_projects[id], project)
        instance.stop(id)
        self.assertEqual(instance.active_projects, {})

    def test_list(self):
        mock_db = riak.RiakClient()
        instance = ProjectStore(mock_db)
        (project, id) = instance.create("Sample", ["sample1", "sample2",])
        self.assertEqual(instance.active_projects[id], project)
        instance.stop(id)
        self.assertEqual(instance.active_projects, {})