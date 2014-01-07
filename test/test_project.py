import unittest

import riak

from Project import Project, ProjectStore

class TestProjectLogic(unittest.TestCase):

    def test_initial_tweet_percentages(self):
        mock_db = riak.RiakClient()
        mock_bucket = mock_db.bucket("message")
        project = Project("Sample", ["test1", "test2",], mock_bucket)
        self.assertEqual(project.keyword_counts["test1"], 100)
        self.assertEqual(project.keyword_counts["test2"], 100)

    def test_calculate_new_keywords(self):
        mock_db = riak.RiakClient()
        mock_bucket = mock_db.bucket("message")
        project = Project("Sample", ["test1", "test2", ], mock_bucket)
        def sample_algorithm(keyword_counts, total):
            return [k for (k,v) in keyword_counts.iteritems() if v / total >= 1]
        self.assertEqual(project.calculate_new_keywords(sample_algorithm), ["test1", "test2",])

    def test_examine_and_save_message(self):
        mock_db = riak.RiakClient()
        mock_bucket = mock_db.bucket("message")
        ps = ProjectStore(mock_db)
        project, id = ps.create("Sample", ["test1", "test2", ], mock_bucket)
        def sample_algorithm(keyword_counts, total):
            return [k for (k,v) in keyword_counts.iteritems() if v / total >= 1]
        self.assertEqual(project.calculate_new_keywords(sample_algorithm), ["test1", "test2",])
        sample_message1 = {
            "message" : "A sample test1",
            "sent_at" : "now",
        }
        sample_message2 = {
            "message" : "A sample test2",
            "sent_at" : "now",
        }
        self.assertTrue(project.examine_and_save_message(sample_message1, ["test1", "fake1",]))
        self.assertFalse(project.examine_and_save_message(sample_message2, ["fake2", "fake1",]))
        # TODO: incorporate sample tweet data

if __name__ == "__main__":
    unittest.main()