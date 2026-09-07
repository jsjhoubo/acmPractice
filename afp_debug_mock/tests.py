import unittest
import numpy as np
from telemetry import (parse_records, daily_totals, moving_average,
                       clean_stats, top_k_users, count_sessions,
                       sample_users, normalize, Record)


class TestTelemetry(unittest.TestCase):
    def test_parse_records(self):
        lines = ["alice,3,2.5", "", "bob,0,1.0\n"]
        recs = parse_records(lines)
        self.assertEqual(recs, [Record("alice", 3, 2.5), Record("bob", 0, 1.0)])

    def test_daily_totals_shape_and_values(self):
        recs = [Record("a", 0, 1.0), Record("b", 2, 2.0), Record("c", 2, 0.5)]
        out = daily_totals(recs, 5)
        self.assertEqual(len(out), 5)
        np.testing.assert_allclose(out, [1.0, 0.0, 2.5, 0.0, 0.0])

    def test_moving_average(self):
        out = moving_average([1.0, 2.0, 3.0, 4.0], 2)
        np.testing.assert_allclose(out, [1.5, 2.5, 3.5])
        out1 = moving_average([5.0, 7.0], 1)
        np.testing.assert_allclose(out1, [5.0, 7.0])

    def test_clean_stats(self):
        mean, mx = clean_stats([1.0, np.nan, 3.0])
        self.assertAlmostEqual(mean, 2.0)
        self.assertAlmostEqual(mx, 3.0)
        self.assertEqual(clean_stats([np.nan, np.nan]), (0.0, 0.0))

    def test_top_k_users_ties(self):
        totals = {"bob": 5.0, "amy": 5.0, "cat": 7.0}
        self.assertEqual(top_k_users(totals, 3),
                         [("cat", 7.0), ("amy", 5.0), ("bob", 5.0)])
        self.assertEqual(top_k_users(totals, 1), [("cat", 7.0)])

    def test_count_sessions(self):
        tree = {"sessions": 1, "children": [
            {"sessions": 2},
            {"sessions": 3, "children": [{"sessions": 4}]},
        ]}
        self.assertEqual(count_sessions(tree), 10)

    def test_sample_users_no_mutation_and_deterministic(self):
        users = ["a", "b", "c", "d", "e"]
        snapshot = list(users)
        s1 = sample_users(users, 3, seed=7)
        self.assertEqual(users, snapshot)          # input must be untouched
        s2 = sample_users(users, 3, seed=7)
        self.assertEqual(s1, s2)                   # deterministic per seed
        self.assertEqual(len(set(s1)), 3)          # distinct
        self.assertTrue(set(s1).issubset(set(users)))

    def test_normalize(self):
        np.testing.assert_allclose(normalize([2.0, 4.0, 6.0]), [0.0, 0.5, 1.0])
        np.testing.assert_allclose(normalize([3.0, 3.0]), [0.0, 0.0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
