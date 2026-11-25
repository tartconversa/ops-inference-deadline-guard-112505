import unittest

from materials import read_feed


class InputsTest(unittest.TestCase):
    def test_cost_and_delivery_evidence(self):
        fleet = {r["worker"]: r for r in read_feed("fleet")}
        requests = {r["request_id"]: r for r in read_feed("requests") if r["op"] == "submit"}
        callbacks = list(read_feed("callbacks"))
        for row in callbacks:
            self.assertIn(row["worker"], fleet)
            self.assertLessEqual(row["occurred_ms"], row["at_ms"])
            if row["request_id"]:
                self.assertIn(row["request_id"], requests)
        timeout = next(r for r in callbacks if r["request_id"] == "c21" and r["event"] == "timeout")
        first = next(r for r in callbacks if r["request_id"] == "c21" and r["event"] == "first_chunk")
        self.assertLess(first["occurred_ms"], timeout["occurred_ms"])
        self.assertGreater(first["at_ms"], timeout["at_ms"])
        report = next(r for r in callbacks if r["event"] == "usage")
        submitted = requests[report["request_id"]]
        self.assertLess(submitted["units"], report["units"])
        self.assertLessEqual(report["units"], submitted["units_ceiling"])
        self.assertTrue(any(r["event"] == "sink_closed_empty" for r in callbacks))

