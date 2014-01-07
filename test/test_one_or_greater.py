import unittest

from Monitor import five_or_greater

class MockProject(object):
    def __init__(self, message_count, keyword_counts):
        self.message_count = message_count
        self.keyword_counts = keyword_counts

class TestOneOrGreater(unittest.TestCase):
    def test_some_above_some_below(self):
        total = 1000
        sample_dataset = {
            "keep1" : 1000,
            "keep2" :  800,
            "not1" : 5,
            "keep3" : 100,
            "not2" : 1,
        }
        project = MockProject(total, sample_dataset)
        self.assertEquals(five_or_greater(project), ["keep1", "keep2", "keep3",])