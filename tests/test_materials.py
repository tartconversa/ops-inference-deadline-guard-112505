import unittest

from materials import list_feeds, read_feed

FIELDS = {
    'fleet': ('worker', 'pool', 'slots', 'work_capacity', 'variant', 'p95_ms', 'return_ms'),
    'reservations': ('tenant', 'pool', 'model_family', 'start_ms', 'end_ms', 'units'),
    'requests': ('at_ms', 'op', 'request_id', 'tenant', 'model_family', 'deadline_ms', 'units', 'allow_small', 'units_ceiling'),
    'callbacks': ('at_ms', 'occurred_ms', 'worker', 'request_id', 'attempt_no', 'event', 'units'),
}


class MaterialTest(unittest.TestCase):
    def test_feed_fields(self):
        self.assertEqual(list_feeds(), sorted(FIELDS))
        for name, fields in FIELDS.items():
            with self.subTest(feed=name):
                records = list(read_feed(name))
                self.assertTrue(records)
                self.assertTrue(all(set(row) == set(fields) for row in records))
