import unittest
import numpy as np
from pipeline import (TokenBucket, EventStore, sessionize, percentile,
                      parse_kv_log, TTLCache, backoff_delays, robust_std,
                      top_events, normalize01, ewma, dedupe_preserve_order,
                      chunk_by_size)


class TestTokenBucket(unittest.TestCase):
    def test_bucket_0(self):
        """capacity=8, rate=0.25: refill must cap at capacity."""
        b = TokenBucket(8, 0.25)
        self.assertEqual(b.allow(0.1, 1), True)
        self.assertEqual(b.allow(0.3, 1), True)
        self.assertEqual(b.allow(0.5, 1), True)
        self.assertEqual(b.allow(3.0, 3), True)
        self.assertEqual(b.allow(3.1, 3), False)
        self.assertEqual(b.allow(3.6, 1), True)
        self.assertEqual(b.allow(3.7, 1), True)
        self.assertEqual(b.allow(3.9, 1), False)
        self.assertEqual(b.allow(4.9, 3), False)
        self.assertEqual(b.allow(5.0, 3), False)
        self.assertEqual(b.allow(5.2, 3), False)
        self.assertEqual(b.allow(5.7, 1), True)

    def test_bucket_1(self):
        """capacity=4, rate=2.0: refill must cap at capacity."""
        b = TokenBucket(4, 2.0)
        self.assertEqual(b.allow(0.4, 1), True)
        self.assertEqual(b.allow(6.4, 1), True)
        self.assertEqual(b.allow(8.9, 2), True)
        self.assertEqual(b.allow(9.3, 1), True)
        self.assertEqual(b.allow(9.5, 1), True)
        self.assertEqual(b.allow(15.5, 1), True)
        self.assertEqual(b.allow(15.6, 1), True)
        self.assertEqual(b.allow(16.1, 1), True)
        self.assertEqual(b.allow(16.5, 1), True)
        self.assertEqual(b.allow(17.5, 1), True)
        self.assertEqual(b.allow(23.5, 1), True)
        self.assertEqual(b.allow(26.0, 2), True)

    def test_bucket_2(self):
        """capacity=5, rate=0.25: refill must cap at capacity."""
        b = TokenBucket(5, 0.25)
        self.assertEqual(b.allow(0.5, 1), True)
        self.assertEqual(b.allow(1.5, 1), True)
        self.assertEqual(b.allow(7.5, 3), True)
        self.assertEqual(b.allow(13.5, 1), True)
        self.assertEqual(b.allow(14.5, 1), True)
        self.assertEqual(b.allow(17.0, 1), True)
        self.assertEqual(b.allow(17.1, 1), True)
        self.assertEqual(b.allow(23.1, 1), True)
        self.assertEqual(b.allow(23.2, 1), False)
        self.assertEqual(b.allow(29.2, 1), True)
        self.assertEqual(b.allow(29.7, 1), True)
        self.assertEqual(b.allow(30.2, 1), False)

    def test_bucket_3(self):
        """capacity=2, rate=1.0: refill must cap at capacity."""
        b = TokenBucket(2, 1.0)
        self.assertEqual(b.allow(0.4, 1), True)
        self.assertEqual(b.allow(2.9, 1), True)
        self.assertEqual(b.allow(5.4, 1), True)
        self.assertEqual(b.allow(6.4, 1), True)
        self.assertEqual(b.allow(7.4, 1), True)
        self.assertEqual(b.allow(7.6, 2), False)
        self.assertEqual(b.allow(8.1, 1), True)
        self.assertEqual(b.allow(10.6, 3), False)
        self.assertEqual(b.allow(10.8, 1), True)
        self.assertEqual(b.allow(16.8, 1), True)
        self.assertEqual(b.allow(17.0, 1), True)
        self.assertEqual(b.allow(23.0, 1), True)

    def test_bucket_4(self):
        """capacity=4, rate=1.0: refill must cap at capacity."""
        b = TokenBucket(4, 1.0)
        self.assertEqual(b.allow(0.1, 1), True)
        self.assertEqual(b.allow(1.1, 1), True)
        self.assertEqual(b.allow(1.3, 2), True)
        self.assertEqual(b.allow(1.8, 2), False)
        self.assertEqual(b.allow(2.0, 1), True)
        self.assertEqual(b.allow(2.2, 1), True)
        self.assertEqual(b.allow(4.7, 3), False)
        self.assertEqual(b.allow(5.7, 1), True)
        self.assertEqual(b.allow(8.2, 3), True)
        self.assertEqual(b.allow(8.7, 3), False)
        self.assertEqual(b.allow(9.2, 1), True)
        self.assertEqual(b.allow(9.4, 1), True)

    def test_bucket_5(self):
        """capacity=5, rate=1.5: refill must cap at capacity."""
        b = TokenBucket(5, 1.5)
        self.assertEqual(b.allow(0.1, 1), True)
        self.assertEqual(b.allow(6.1, 1), True)
        self.assertEqual(b.allow(6.3, 1), True)
        self.assertEqual(b.allow(12.3, 2), True)
        self.assertEqual(b.allow(13.3, 1), True)
        self.assertEqual(b.allow(13.8, 2), True)
        self.assertEqual(b.allow(14.8, 2), True)
        self.assertEqual(b.allow(15.8, 1), True)
        self.assertEqual(b.allow(16.8, 1), True)
        self.assertEqual(b.allow(19.3, 1), True)
        self.assertEqual(b.allow(21.8, 3), True)
        self.assertEqual(b.allow(27.8, 1), True)

    def test_bucket_6(self):
        """capacity=10, rate=5.0: refill must cap at capacity."""
        b = TokenBucket(10, 5.0)
        self.assertEqual(b.allow(0.4, 1), True)
        self.assertEqual(b.allow(0.8, 2), True)
        self.assertEqual(b.allow(1.0, 2), True)
        self.assertEqual(b.allow(1.1, 1), True)
        self.assertEqual(b.allow(2.1, 1), True)
        self.assertEqual(b.allow(3.1, 1), True)
        self.assertEqual(b.allow(9.1, 1), True)
        self.assertEqual(b.allow(15.1, 3), True)
        self.assertEqual(b.allow(16.1, 1), True)
        self.assertEqual(b.allow(16.3, 1), True)
        self.assertEqual(b.allow(22.3, 1), True)
        self.assertEqual(b.allow(23.3, 3), True)

    def test_bucket_7(self):
        """capacity=1, rate=2.0: refill must cap at capacity."""
        b = TokenBucket(1, 2.0)
        self.assertEqual(b.allow(0.4, 2), False)
        self.assertEqual(b.allow(0.5, 1), True)
        self.assertEqual(b.allow(0.9, 1), False)
        self.assertEqual(b.allow(1.1, 1), True)
        self.assertEqual(b.allow(1.3, 3), False)
        self.assertEqual(b.allow(1.4, 1), False)
        self.assertEqual(b.allow(3.9, 2), False)
        self.assertEqual(b.allow(9.9, 1), True)
        self.assertEqual(b.allow(15.9, 3), False)
        self.assertEqual(b.allow(21.9, 1), True)
        self.assertEqual(b.allow(22.1, 2), False)
        self.assertEqual(b.allow(23.1, 1), True)

    def test_bucket_8(self):
        """capacity=3, rate=2.0: refill must cap at capacity."""
        b = TokenBucket(3, 2.0)
        self.assertEqual(b.allow(6.0, 3), True)
        self.assertEqual(b.allow(6.5, 1), True)
        self.assertEqual(b.allow(7.5, 1), True)
        self.assertEqual(b.allow(10.0, 1), True)
        self.assertEqual(b.allow(10.5, 1), True)
        self.assertEqual(b.allow(11.0, 3), True)
        self.assertEqual(b.allow(11.5, 1), True)
        self.assertEqual(b.allow(11.7, 1), False)
        self.assertEqual(b.allow(11.8, 1), False)
        self.assertEqual(b.allow(11.9, 3), False)
        self.assertEqual(b.allow(12.9, 1), True)
        self.assertEqual(b.allow(13.9, 1), True)

    def test_bucket_9(self):
        """capacity=1, rate=0.25: refill must cap at capacity."""
        b = TokenBucket(1, 0.25)
        self.assertEqual(b.allow(2.5, 1), True)
        self.assertEqual(b.allow(2.7, 1), False)
        self.assertEqual(b.allow(2.8, 1), False)
        self.assertEqual(b.allow(2.9, 3), False)
        self.assertEqual(b.allow(3.1, 1), False)
        self.assertEqual(b.allow(5.6, 2), False)
        self.assertEqual(b.allow(5.8, 3), False)
        self.assertEqual(b.allow(6.0, 3), False)
        self.assertEqual(b.allow(7.0, 2), False)
        self.assertEqual(b.allow(7.2, 2), False)
        self.assertEqual(b.allow(13.2, 2), False)
        self.assertEqual(b.allow(13.4, 1), True)

    def test_bucket_10(self):
        """capacity=1, rate=5.0: refill must cap at capacity."""
        b = TokenBucket(1, 5.0)
        self.assertEqual(b.allow(0.5, 1), True)
        self.assertEqual(b.allow(1.0, 2), False)
        self.assertEqual(b.allow(1.5, 1), True)
        self.assertEqual(b.allow(4.0, 1), True)
        self.assertEqual(b.allow(4.1, 2), False)
        self.assertEqual(b.allow(6.6, 1), True)
        self.assertEqual(b.allow(12.6, 1), True)
        self.assertEqual(b.allow(12.8, 1), True)
        self.assertEqual(b.allow(13.0, 3), False)
        self.assertEqual(b.allow(13.5, 1), True)
        self.assertEqual(b.allow(14.0, 1), True)
        self.assertEqual(b.allow(14.4, 2), False)

    def test_bucket_11(self):
        """capacity=2, rate=0.25: refill must cap at capacity."""
        b = TokenBucket(2, 0.25)
        self.assertEqual(b.allow(0.5, 3), False)
        self.assertEqual(b.allow(0.6, 1), True)
        self.assertEqual(b.allow(3.1, 3), False)
        self.assertEqual(b.allow(9.1, 1), True)
        self.assertEqual(b.allow(9.2, 1), True)
        self.assertEqual(b.allow(9.4, 2), False)
        self.assertEqual(b.allow(9.9, 2), False)
        self.assertEqual(b.allow(10.1, 2), False)
        self.assertEqual(b.allow(10.2, 1), False)
        self.assertEqual(b.allow(10.7, 1), False)
        self.assertEqual(b.allow(11.2, 1), False)
        self.assertEqual(b.allow(17.2, 2), True)

    def test_bucket_12(self):
        """capacity=3, rate=1.5: refill must cap at capacity."""
        b = TokenBucket(3, 1.5)
        self.assertEqual(b.allow(2.5, 3), True)
        self.assertEqual(b.allow(5.0, 2), True)
        self.assertEqual(b.allow(5.2, 1), True)
        self.assertEqual(b.allow(5.6, 1), False)
        self.assertEqual(b.allow(5.7, 3), False)
        self.assertEqual(b.allow(8.2, 3), True)
        self.assertEqual(b.allow(8.3, 1), False)
        self.assertEqual(b.allow(8.4, 1), False)
        self.assertEqual(b.allow(9.4, 2), False)
        self.assertEqual(b.allow(10.4, 3), True)
        self.assertEqual(b.allow(10.6, 1), False)
        self.assertEqual(b.allow(11.6, 1), True)

    def test_bucket_13(self):
        """capacity=10, rate=0.5: refill must cap at capacity."""
        b = TokenBucket(10, 0.5)
        self.assertEqual(b.allow(0.1, 3), True)
        self.assertEqual(b.allow(0.2, 1), True)
        self.assertEqual(b.allow(0.7, 1), True)
        self.assertEqual(b.allow(1.7, 1), True)
        self.assertEqual(b.allow(2.7, 3), True)
        self.assertEqual(b.allow(2.8, 3), False)
        self.assertEqual(b.allow(2.9, 2), True)
        self.assertEqual(b.allow(5.4, 3), False)
        self.assertEqual(b.allow(6.4, 3), False)
        self.assertEqual(b.allow(6.8, 1), True)
        self.assertEqual(b.allow(7.0, 1), True)
        self.assertEqual(b.allow(7.2, 1), False)

    def test_bucket_14(self):
        """capacity=4, rate=0.5: refill must cap at capacity."""
        b = TokenBucket(4, 0.5)
        self.assertEqual(b.allow(2.5, 1), True)
        self.assertEqual(b.allow(3.0, 1), True)
        self.assertEqual(b.allow(9.0, 1), True)
        self.assertEqual(b.allow(9.1, 2), True)
        self.assertEqual(b.allow(10.1, 3), False)
        self.assertEqual(b.allow(10.2, 1), True)
        self.assertEqual(b.allow(11.2, 1), True)
        self.assertEqual(b.allow(12.2, 1), False)
        self.assertEqual(b.allow(12.4, 1), False)
        self.assertEqual(b.allow(12.5, 1), False)
        self.assertEqual(b.allow(12.9, 1), False)
        self.assertEqual(b.allow(13.1, 2), False)

    def test_bucket_15(self):
        """capacity=10, rate=2.0: refill must cap at capacity."""
        b = TokenBucket(10, 2.0)
        self.assertEqual(b.allow(2.5, 1), True)
        self.assertEqual(b.allow(3.5, 3), True)
        self.assertEqual(b.allow(3.6, 3), True)
        self.assertEqual(b.allow(4.0, 1), True)
        self.assertEqual(b.allow(4.2, 1), True)
        self.assertEqual(b.allow(4.3, 1), True)
        self.assertEqual(b.allow(6.8, 3), True)
        self.assertEqual(b.allow(7.0, 1), True)
        self.assertEqual(b.allow(7.4, 3), True)
        self.assertEqual(b.allow(7.6, 1), True)
        self.assertEqual(b.allow(7.8, 1), True)
        self.assertEqual(b.allow(8.8, 2), True)



class TestEventStore(unittest.TestCase):
    def test_store_0_0(self):
        """Inclusive [t0, t1] window on dataset 0."""
        s = EventStore()
        for t, v in [(0.0, 0.92), (1.5, 4.23), (1.5, 0.52), (2.0, 3.37), (3.0, 1.23), (4.5, 2.98), (6.0, 3.02), (6.0, 0.84), (6.5, 2.96), (7.5, 3.12)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(1.5, 6.0), 16.19, places=6)
        self.assertEqual(s.query_count(1.5, 6.0), 7)

    def test_store_0_1(self):
        """Inclusive [t0, t1] window on dataset 0."""
        s = EventStore()
        for t, v in [(0.0, 0.92), (1.5, 4.23), (1.5, 0.52), (2.0, 3.37), (3.0, 1.23), (4.5, 2.98), (6.0, 3.02), (6.0, 0.84), (6.5, 2.96), (7.5, 3.12)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(0.0, 1.5), 5.67, places=6)
        self.assertEqual(s.query_count(0.0, 1.5), 3)

    def test_store_0_2(self):
        """Inclusive [t0, t1] window on dataset 0."""
        s = EventStore()
        for t, v in [(0.0, 0.92), (1.5, 4.23), (1.5, 0.52), (2.0, 3.37), (3.0, 1.23), (4.5, 2.98), (6.0, 3.02), (6.0, 0.84), (6.5, 2.96), (7.5, 3.12)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(3.0, 4.5), 4.21, places=6)
        self.assertEqual(s.query_count(3.0, 4.5), 2)

    def test_store_0_3(self):
        """Inclusive [t0, t1] window on dataset 0."""
        s = EventStore()
        for t, v in [(0.0, 0.92), (1.5, 4.23), (1.5, 0.52), (2.0, 3.37), (3.0, 1.23), (4.5, 2.98), (6.0, 3.02), (6.0, 0.84), (6.5, 2.96), (7.5, 3.12)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(0.0, 4.5), 13.25, places=6)
        self.assertEqual(s.query_count(0.0, 4.5), 6)

    def test_store_0_4(self):
        """Inclusive [t0, t1] window on dataset 0."""
        s = EventStore()
        for t, v in [(0.0, 0.92), (1.5, 4.23), (1.5, 0.52), (2.0, 3.37), (3.0, 1.23), (4.5, 2.98), (6.0, 3.02), (6.0, 0.84), (6.5, 2.96), (7.5, 3.12)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(0.79, 2.71), 8.12, places=6)
        self.assertEqual(s.query_count(0.79, 2.71), 3)

    def test_store_0_5(self):
        """Inclusive [t0, t1] window on dataset 0."""
        s = EventStore()
        for t, v in [(0.0, 0.92), (1.5, 4.23), (1.5, 0.52), (2.0, 3.37), (3.0, 1.23), (4.5, 2.98), (6.0, 3.02), (6.0, 0.84), (6.5, 2.96), (7.5, 3.12)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(-0.13, 6.6), 20.07, places=6)
        self.assertEqual(s.query_count(-0.13, 6.6), 9)

    def test_store_0_6(self):
        """Inclusive [t0, t1] window on dataset 0."""
        s = EventStore()
        for t, v in [(0.0, 0.92), (1.5, 4.23), (1.5, 0.52), (2.0, 3.37), (3.0, 1.23), (4.5, 2.98), (6.0, 3.02), (6.0, 0.84), (6.5, 2.96), (7.5, 3.12)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(6.52, 7.32), 0.0, places=6)
        self.assertEqual(s.query_count(6.52, 7.32), 0)

    def test_store_0_7(self):
        """Inclusive [t0, t1] window on dataset 0."""
        s = EventStore()
        for t, v in [(0.0, 0.92), (1.5, 4.23), (1.5, 0.52), (2.0, 3.37), (3.0, 1.23), (4.5, 2.98), (6.0, 3.02), (6.0, 0.84), (6.5, 2.96), (7.5, 3.12)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(4.28, 4.93), 2.98, places=6)
        self.assertEqual(s.query_count(4.28, 4.93), 1)

    def test_store_0_8(self):
        """Inclusive [t0, t1] window on dataset 0."""
        s = EventStore()
        for t, v in [(0.0, 0.92), (1.5, 4.23), (1.5, 0.52), (2.0, 3.37), (3.0, 1.23), (4.5, 2.98), (6.0, 3.02), (6.0, 0.84), (6.5, 2.96), (7.5, 3.12)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(6.9, 8.28), 3.12, places=6)
        self.assertEqual(s.query_count(6.9, 8.28), 1)

    def test_store_0_9(self):
        """Inclusive [t0, t1] window on dataset 0."""
        s = EventStore()
        for t, v in [(0.0, 0.92), (1.5, 4.23), (1.5, 0.52), (2.0, 3.37), (3.0, 1.23), (4.5, 2.98), (6.0, 3.02), (6.0, 0.84), (6.5, 2.96), (7.5, 3.12)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(7.3, 8.27), 3.12, places=6)
        self.assertEqual(s.query_count(7.3, 8.27), 1)

    def test_store_0_10(self):
        """Inclusive [t0, t1] window on dataset 0."""
        s = EventStore()
        for t, v in [(0.0, 0.92), (1.5, 4.23), (1.5, 0.52), (2.0, 3.37), (3.0, 1.23), (4.5, 2.98), (6.0, 3.02), (6.0, 0.84), (6.5, 2.96), (7.5, 3.12)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(6.49, 6.54), 2.96, places=6)
        self.assertEqual(s.query_count(6.49, 6.54), 1)

    def test_store_0_11(self):
        """Inclusive [t0, t1] window on dataset 0."""
        s = EventStore()
        for t, v in [(0.0, 0.92), (1.5, 4.23), (1.5, 0.52), (2.0, 3.37), (3.0, 1.23), (4.5, 2.98), (6.0, 3.02), (6.0, 0.84), (6.5, 2.96), (7.5, 3.12)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(5.26, 6.34), 3.86, places=6)
        self.assertEqual(s.query_count(5.26, 6.34), 2)

    def test_store_1_0(self):
        """Inclusive [t0, t1] window on dataset 1."""
        s = EventStore()
        for t, v in [(0.5, 1.7), (0.5, 2.22), (0.5, 4.36), (1.0, 1.4), (2.5, 2.07), (3.0, 1.5), (3.5, 2.29), (4.5, 4.39), (5.5, 2.08), (7.5, 2.3)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(3.0, 5.5), 10.26, places=6)
        self.assertEqual(s.query_count(3.0, 5.5), 4)

    def test_store_1_1(self):
        """Inclusive [t0, t1] window on dataset 1."""
        s = EventStore()
        for t, v in [(0.5, 1.7), (0.5, 2.22), (0.5, 4.36), (1.0, 1.4), (2.5, 2.07), (3.0, 1.5), (3.5, 2.29), (4.5, 4.39), (5.5, 2.08), (7.5, 2.3)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(0.5, 0.5), 8.28, places=6)
        self.assertEqual(s.query_count(0.5, 0.5), 3)

    def test_store_1_2(self):
        """Inclusive [t0, t1] window on dataset 1."""
        s = EventStore()
        for t, v in [(0.5, 1.7), (0.5, 2.22), (0.5, 4.36), (1.0, 1.4), (2.5, 2.07), (3.0, 1.5), (3.5, 2.29), (4.5, 4.39), (5.5, 2.08), (7.5, 2.3)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(0.5, 2.5), 11.75, places=6)
        self.assertEqual(s.query_count(0.5, 2.5), 5)

    def test_store_1_3(self):
        """Inclusive [t0, t1] window on dataset 1."""
        s = EventStore()
        for t, v in [(0.5, 1.7), (0.5, 2.22), (0.5, 4.36), (1.0, 1.4), (2.5, 2.07), (3.0, 1.5), (3.5, 2.29), (4.5, 4.39), (5.5, 2.08), (7.5, 2.3)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(2.5, 7.5), 14.63, places=6)
        self.assertEqual(s.query_count(2.5, 7.5), 6)

    def test_store_1_4(self):
        """Inclusive [t0, t1] window on dataset 1."""
        s = EventStore()
        for t, v in [(0.5, 1.7), (0.5, 2.22), (0.5, 4.36), (1.0, 1.4), (2.5, 2.07), (3.0, 1.5), (3.5, 2.29), (4.5, 4.39), (5.5, 2.08), (7.5, 2.3)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(-0.67, 4.8), 19.93, places=6)
        self.assertEqual(s.query_count(-0.67, 4.8), 8)

    def test_store_1_5(self):
        """Inclusive [t0, t1] window on dataset 1."""
        s = EventStore()
        for t, v in [(0.5, 1.7), (0.5, 2.22), (0.5, 4.36), (1.0, 1.4), (2.5, 2.07), (3.0, 1.5), (3.5, 2.29), (4.5, 4.39), (5.5, 2.08), (7.5, 2.3)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(1.94, 7.1), 12.33, places=6)
        self.assertEqual(s.query_count(1.94, 7.1), 5)

    def test_store_1_6(self):
        """Inclusive [t0, t1] window on dataset 1."""
        s = EventStore()
        for t, v in [(0.5, 1.7), (0.5, 2.22), (0.5, 4.36), (1.0, 1.4), (2.5, 2.07), (3.0, 1.5), (3.5, 2.29), (4.5, 4.39), (5.5, 2.08), (7.5, 2.3)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(2.71, 8.41), 12.56, places=6)
        self.assertEqual(s.query_count(2.71, 8.41), 5)

    def test_store_1_7(self):
        """Inclusive [t0, t1] window on dataset 1."""
        s = EventStore()
        for t, v in [(0.5, 1.7), (0.5, 2.22), (0.5, 4.36), (1.0, 1.4), (2.5, 2.07), (3.0, 1.5), (3.5, 2.29), (4.5, 4.39), (5.5, 2.08), (7.5, 2.3)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(-0.02, 7.64), 24.31, places=6)
        self.assertEqual(s.query_count(-0.02, 7.64), 10)

    def test_store_1_8(self):
        """Inclusive [t0, t1] window on dataset 1."""
        s = EventStore()
        for t, v in [(0.5, 1.7), (0.5, 2.22), (0.5, 4.36), (1.0, 1.4), (2.5, 2.07), (3.0, 1.5), (3.5, 2.29), (4.5, 4.39), (5.5, 2.08), (7.5, 2.3)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(0.62, 0.97), 0.0, places=6)
        self.assertEqual(s.query_count(0.62, 0.97), 0)

    def test_store_1_9(self):
        """Inclusive [t0, t1] window on dataset 1."""
        s = EventStore()
        for t, v in [(0.5, 1.7), (0.5, 2.22), (0.5, 4.36), (1.0, 1.4), (2.5, 2.07), (3.0, 1.5), (3.5, 2.29), (4.5, 4.39), (5.5, 2.08), (7.5, 2.3)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(2.71, 5.72), 10.26, places=6)
        self.assertEqual(s.query_count(2.71, 5.72), 4)

    def test_store_1_10(self):
        """Inclusive [t0, t1] window on dataset 1."""
        s = EventStore()
        for t, v in [(0.5, 1.7), (0.5, 2.22), (0.5, 4.36), (1.0, 1.4), (2.5, 2.07), (3.0, 1.5), (3.5, 2.29), (4.5, 4.39), (5.5, 2.08), (7.5, 2.3)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(5.86, 7.67), 2.3, places=6)
        self.assertEqual(s.query_count(5.86, 7.67), 1)

    def test_store_1_11(self):
        """Inclusive [t0, t1] window on dataset 1."""
        s = EventStore()
        for t, v in [(0.5, 1.7), (0.5, 2.22), (0.5, 4.36), (1.0, 1.4), (2.5, 2.07), (3.0, 1.5), (3.5, 2.29), (4.5, 4.39), (5.5, 2.08), (7.5, 2.3)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(6.99, 8.1), 2.3, places=6)
        self.assertEqual(s.query_count(6.99, 8.1), 1)

    def test_store_2_0(self):
        """Inclusive [t0, t1] window on dataset 2."""
        s = EventStore()
        for t, v in [(1.0, 2.44), (2.0, 3.3), (2.0, 3.74), (3.0, 2.78), (4.5, 1.97), (5.5, 2.99), (6.0, 2.39), (7.5, 3.55)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(2.0, 4.5), 11.79, places=6)
        self.assertEqual(s.query_count(2.0, 4.5), 4)

    def test_store_2_1(self):
        """Inclusive [t0, t1] window on dataset 2."""
        s = EventStore()
        for t, v in [(1.0, 2.44), (2.0, 3.3), (2.0, 3.74), (3.0, 2.78), (4.5, 1.97), (5.5, 2.99), (6.0, 2.39), (7.5, 3.55)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(1.0, 6.0), 19.61, places=6)
        self.assertEqual(s.query_count(1.0, 6.0), 7)

    def test_store_2_2(self):
        """Inclusive [t0, t1] window on dataset 2."""
        s = EventStore()
        for t, v in [(1.0, 2.44), (2.0, 3.3), (2.0, 3.74), (3.0, 2.78), (4.5, 1.97), (5.5, 2.99), (6.0, 2.39), (7.5, 3.55)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(4.5, 4.5), 1.97, places=6)
        self.assertEqual(s.query_count(4.5, 4.5), 1)

    def test_store_2_3(self):
        """Inclusive [t0, t1] window on dataset 2."""
        s = EventStore()
        for t, v in [(1.0, 2.44), (2.0, 3.3), (2.0, 3.74), (3.0, 2.78), (4.5, 1.97), (5.5, 2.99), (6.0, 2.39), (7.5, 3.55)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(3.0, 6.0), 10.13, places=6)
        self.assertEqual(s.query_count(3.0, 6.0), 4)

    def test_store_2_4(self):
        """Inclusive [t0, t1] window on dataset 2."""
        s = EventStore()
        for t, v in [(1.0, 2.44), (2.0, 3.3), (2.0, 3.74), (3.0, 2.78), (4.5, 1.97), (5.5, 2.99), (6.0, 2.39), (7.5, 3.55)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(5.68, 7.39), 2.39, places=6)
        self.assertEqual(s.query_count(5.68, 7.39), 1)

    def test_store_2_5(self):
        """Inclusive [t0, t1] window on dataset 2."""
        s = EventStore()
        for t, v in [(1.0, 2.44), (2.0, 3.3), (2.0, 3.74), (3.0, 2.78), (4.5, 1.97), (5.5, 2.99), (6.0, 2.39), (7.5, 3.55)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(1.74, 4.73), 11.79, places=6)
        self.assertEqual(s.query_count(1.74, 4.73), 4)

    def test_store_2_6(self):
        """Inclusive [t0, t1] window on dataset 2."""
        s = EventStore()
        for t, v in [(1.0, 2.44), (2.0, 3.3), (2.0, 3.74), (3.0, 2.78), (4.5, 1.97), (5.5, 2.99), (6.0, 2.39), (7.5, 3.55)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(4.74, 6.66), 5.38, places=6)
        self.assertEqual(s.query_count(4.74, 6.66), 2)

    def test_store_2_7(self):
        """Inclusive [t0, t1] window on dataset 2."""
        s = EventStore()
        for t, v in [(1.0, 2.44), (2.0, 3.3), (2.0, 3.74), (3.0, 2.78), (4.5, 1.97), (5.5, 2.99), (6.0, 2.39), (7.5, 3.55)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(5.75, 8.39), 5.94, places=6)
        self.assertEqual(s.query_count(5.75, 8.39), 2)

    def test_store_2_8(self):
        """Inclusive [t0, t1] window on dataset 2."""
        s = EventStore()
        for t, v in [(1.0, 2.44), (2.0, 3.3), (2.0, 3.74), (3.0, 2.78), (4.5, 1.97), (5.5, 2.99), (6.0, 2.39), (7.5, 3.55)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(5.26, 7.39), 5.38, places=6)
        self.assertEqual(s.query_count(5.26, 7.39), 2)

    def test_store_2_9(self):
        """Inclusive [t0, t1] window on dataset 2."""
        s = EventStore()
        for t, v in [(1.0, 2.44), (2.0, 3.3), (2.0, 3.74), (3.0, 2.78), (4.5, 1.97), (5.5, 2.99), (6.0, 2.39), (7.5, 3.55)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(1.41, 6.12), 17.17, places=6)
        self.assertEqual(s.query_count(1.41, 6.12), 6)

    def test_store_2_10(self):
        """Inclusive [t0, t1] window on dataset 2."""
        s = EventStore()
        for t, v in [(1.0, 2.44), (2.0, 3.3), (2.0, 3.74), (3.0, 2.78), (4.5, 1.97), (5.5, 2.99), (6.0, 2.39), (7.5, 3.55)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(4.26, 4.66), 1.97, places=6)
        self.assertEqual(s.query_count(4.26, 4.66), 1)

    def test_store_2_11(self):
        """Inclusive [t0, t1] window on dataset 2."""
        s = EventStore()
        for t, v in [(1.0, 2.44), (2.0, 3.3), (2.0, 3.74), (3.0, 2.78), (4.5, 1.97), (5.5, 2.99), (6.0, 2.39), (7.5, 3.55)]:
            s.add(t, v)
        self.assertAlmostEqual(s.query_sum(7.09, 7.42), 0.0, places=6)
        self.assertEqual(s.query_count(7.09, 7.42), 0)



class TestSessionize(unittest.TestCase):
    def test_sessions_0(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 3.0, 6.0], 3.0), [(0.0, 6.0)])

    def test_sessions_1(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0], 2.0), [(0.0, 0.0)])

    def test_sessions_2(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 13.0], 6.5), [(0.0, 0.0), (13.0, 13.0)])

    def test_sessions_3(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0], 6.5), [(0.0, 0.0)])

    def test_sessions_4(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 3.1, 6.2, 9.3, 12.3], 3.0), [(0.0, 0.0), (3.1, 3.1), (6.2, 6.2), (9.3, 12.3)])

    def test_sessions_5(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 1.0], 2.0), [(0.0, 1.0)])

    def test_sessions_6(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 3.0, 9.0, 12.1, 13.1], 3.0), [(0.0, 3.0), (9.0, 9.0), (12.1, 13.1)])

    def test_sessions_7(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 1.0, 4.1, 7.1, 10.2, 16.2, 22.2], 3.0), [(0.0, 1.0), (4.1, 7.1), (10.2, 10.2), (16.2, 16.2), (22.2, 22.2)])

    def test_sessions_8(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 5.1, 15.1, 25.1, 30.2, 40.2, 45.3], 5.0), [(0.0, 0.0), (5.1, 5.1), (15.1, 15.1), (25.1, 25.1), (30.2, 30.2), (40.2, 40.2), (45.3, 45.3)])

    def test_sessions_9(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 6.6], 6.5), [(0.0, 0.0), (6.6, 6.6)])

    def test_sessions_10(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 3.0, 9.0], 3.0), [(0.0, 3.0), (9.0, 9.0)])

    def test_sessions_11(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 3.0, 6.1, 7.1, 10.1], 3.0), [(0.0, 3.0), (6.1, 10.1)])

    def test_sessions_12(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 5.0], 5.0), [(0.0, 5.0)])

    def test_sessions_13(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 2.0, 4.0], 2.0), [(0.0, 4.0)])

    def test_sessions_14(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 6.5], 6.5), [(0.0, 6.5)])

    def test_sessions_15(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 1.0, 4.1, 7.2, 10.2, 16.2, 19.3, 22.4, 23.4], 3.0), [(0.0, 1.0), (4.1, 4.1), (7.2, 10.2), (16.2, 16.2), (19.3, 19.3), (22.4, 23.4)])

    def test_sessions_16(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 6.6], 6.5), [(0.0, 0.0), (6.6, 6.6)])

    def test_sessions_17(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 4.0, 6.1, 8.2, 9.2, 11.2, 13.2], 2.0), [(0.0, 0.0), (4.0, 4.0), (6.1, 6.1), (8.2, 13.2)])

    def test_sessions_18(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 13.0, 26.0, 39.0, 45.5], 6.5), [(0.0, 0.0), (13.0, 13.0), (26.0, 26.0), (39.0, 45.5)])

    def test_sessions_19(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 3.0, 6.1, 9.2, 10.2], 3.0), [(0.0, 3.0), (6.1, 6.1), (9.2, 10.2)])

    def test_sessions_20(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 5.1, 10.1, 15.2, 20.2], 5.0), [(0.0, 0.0), (5.1, 10.1), (15.2, 20.2)])

    def test_sessions_21(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 2.1, 6.1, 10.1, 11.1, 12.1, 14.2], 2.0), [(0.0, 0.0), (2.1, 2.1), (6.1, 6.1), (10.1, 12.1), (14.2, 14.2)])

    def test_sessions_22(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 6.5], 6.5), [(0.0, 6.5)])

    def test_sessions_23(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0], 5.0), [(0.0, 0.0)])

    def test_sessions_24(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 5.0, 10.1, 15.1, 20.1], 5.0), [(0.0, 5.0), (10.1, 15.1), (20.1, 20.1)])

    def test_sessions_25(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 5.1, 10.1, 11.1, 16.2], 5.0), [(0.0, 0.0), (5.1, 11.1), (16.2, 16.2)])

    def test_sessions_26(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0], 2.0), [(0.0, 0.0)])

    def test_sessions_27(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 1.0, 2.0], 3.0), [(0.0, 2.0)])

    def test_sessions_28(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0], 3.0), [(0.0, 0.0)])

    def test_sessions_29(self):
        """delta == gap stays in-session; the LAST session must be emitted."""
        self.assertEqual(sessionize([0.0, 4.0], 2.0), [(0.0, 0.0), (4.0, 4.0)])



class TestPercentile(unittest.TestCase):
    def test_pct_0_0(self):
        self.assertAlmostEqual(percentile([23.85, 47.36, 11.44, 94.78], 0), 11.44, places=6)

    def test_pct_0_5(self):
        self.assertAlmostEqual(percentile([23.85, 47.36, 11.44, 94.78], 5), 13.3015, places=6)

    def test_pct_0_10(self):
        self.assertAlmostEqual(percentile([23.85, 47.36, 11.44, 94.78], 10), 15.163, places=6)

    def test_pct_0_25(self):
        self.assertAlmostEqual(percentile([23.85, 47.36, 11.44, 94.78], 25), 20.7475, places=6)

    def test_pct_0_50(self):
        self.assertAlmostEqual(percentile([23.85, 47.36, 11.44, 94.78], 50), 35.605, places=6)

    def test_pct_0_75(self):
        self.assertAlmostEqual(percentile([23.85, 47.36, 11.44, 94.78], 75), 59.215, places=6)

    def test_pct_0_90(self):
        self.assertAlmostEqual(percentile([23.85, 47.36, 11.44, 94.78], 90), 80.554, places=6)

    def test_pct_0_95(self):
        self.assertAlmostEqual(percentile([23.85, 47.36, 11.44, 94.78], 95), 87.667, places=6)

    def test_pct_0_100(self):
        self.assertAlmostEqual(percentile([23.85, 47.36, 11.44, 94.78], 100), 94.78, places=6)

    def test_pct_1_0(self):
        self.assertAlmostEqual(percentile([69.95, 76.69, 16.78, 60.72, 74.79, 11.45, 81.93], 0), 11.45, places=6)

    def test_pct_1_5(self):
        self.assertAlmostEqual(percentile([69.95, 76.69, 16.78, 60.72, 74.79, 11.45, 81.93], 5), 13.049, places=6)

    def test_pct_1_10(self):
        self.assertAlmostEqual(percentile([69.95, 76.69, 16.78, 60.72, 74.79, 11.45, 81.93], 10), 14.648, places=6)

    def test_pct_1_25(self):
        self.assertAlmostEqual(percentile([69.95, 76.69, 16.78, 60.72, 74.79, 11.45, 81.93], 25), 38.75, places=6)

    def test_pct_1_50(self):
        self.assertAlmostEqual(percentile([69.95, 76.69, 16.78, 60.72, 74.79, 11.45, 81.93], 50), 69.95, places=6)

    def test_pct_1_75(self):
        self.assertAlmostEqual(percentile([69.95, 76.69, 16.78, 60.72, 74.79, 11.45, 81.93], 75), 75.74, places=6)

    def test_pct_1_90(self):
        self.assertAlmostEqual(percentile([69.95, 76.69, 16.78, 60.72, 74.79, 11.45, 81.93], 90), 78.786, places=6)

    def test_pct_1_95(self):
        self.assertAlmostEqual(percentile([69.95, 76.69, 16.78, 60.72, 74.79, 11.45, 81.93], 95), 80.358, places=6)

    def test_pct_1_100(self):
        self.assertAlmostEqual(percentile([69.95, 76.69, 16.78, 60.72, 74.79, 11.45, 81.93], 100), 81.93, places=6)

    def test_pct_2_0(self):
        self.assertAlmostEqual(percentile([10.81, 2.57, 31.2, 67.73, 95.82], 0), 2.57, places=6)

    def test_pct_2_5(self):
        self.assertAlmostEqual(percentile([10.81, 2.57, 31.2, 67.73, 95.82], 5), 4.218, places=6)

    def test_pct_2_10(self):
        self.assertAlmostEqual(percentile([10.81, 2.57, 31.2, 67.73, 95.82], 10), 5.866, places=6)

    def test_pct_2_25(self):
        self.assertAlmostEqual(percentile([10.81, 2.57, 31.2, 67.73, 95.82], 25), 10.81, places=6)

    def test_pct_2_50(self):
        self.assertAlmostEqual(percentile([10.81, 2.57, 31.2, 67.73, 95.82], 50), 31.2, places=6)

    def test_pct_2_75(self):
        self.assertAlmostEqual(percentile([10.81, 2.57, 31.2, 67.73, 95.82], 75), 67.73, places=6)

    def test_pct_2_90(self):
        self.assertAlmostEqual(percentile([10.81, 2.57, 31.2, 67.73, 95.82], 90), 84.584, places=6)

    def test_pct_2_95(self):
        self.assertAlmostEqual(percentile([10.81, 2.57, 31.2, 67.73, 95.82], 95), 90.202, places=6)

    def test_pct_2_100(self):
        self.assertAlmostEqual(percentile([10.81, 2.57, 31.2, 67.73, 95.82], 100), 95.82, places=6)

    def test_pct_3_0(self):
        self.assertAlmostEqual(percentile([94.15, 19.83, 59.21, 83.06, 24.28, 69.72, 30.16], 0), 19.83, places=6)

    def test_pct_3_5(self):
        self.assertAlmostEqual(percentile([94.15, 19.83, 59.21, 83.06, 24.28, 69.72, 30.16], 5), 21.165, places=6)

    def test_pct_3_10(self):
        self.assertAlmostEqual(percentile([94.15, 19.83, 59.21, 83.06, 24.28, 69.72, 30.16], 10), 22.5, places=6)

    def test_pct_3_25(self):
        self.assertAlmostEqual(percentile([94.15, 19.83, 59.21, 83.06, 24.28, 69.72, 30.16], 25), 27.22, places=6)

    def test_pct_3_50(self):
        self.assertAlmostEqual(percentile([94.15, 19.83, 59.21, 83.06, 24.28, 69.72, 30.16], 50), 59.21, places=6)

    def test_pct_3_75(self):
        self.assertAlmostEqual(percentile([94.15, 19.83, 59.21, 83.06, 24.28, 69.72, 30.16], 75), 76.39, places=6)

    def test_pct_3_90(self):
        self.assertAlmostEqual(percentile([94.15, 19.83, 59.21, 83.06, 24.28, 69.72, 30.16], 90), 87.496, places=6)

    def test_pct_3_95(self):
        self.assertAlmostEqual(percentile([94.15, 19.83, 59.21, 83.06, 24.28, 69.72, 30.16], 95), 90.823, places=6)

    def test_pct_3_100(self):
        self.assertAlmostEqual(percentile([94.15, 19.83, 59.21, 83.06, 24.28, 69.72, 30.16], 100), 94.15, places=6)

    def test_pct_4_0(self):
        self.assertAlmostEqual(percentile([60.04, 12.11, 98.38, 78.26, 34.72, 42.84, 37.06, 50.6, 34.12, 84.96, 82.23], 0), 12.11, places=6)

    def test_pct_4_5(self):
        self.assertAlmostEqual(percentile([60.04, 12.11, 98.38, 78.26, 34.72, 42.84, 37.06, 50.6, 34.12, 84.96, 82.23], 5), 23.115, places=6)

    def test_pct_4_10(self):
        self.assertAlmostEqual(percentile([60.04, 12.11, 98.38, 78.26, 34.72, 42.84, 37.06, 50.6, 34.12, 84.96, 82.23], 10), 34.12, places=6)

    def test_pct_4_25(self):
        self.assertAlmostEqual(percentile([60.04, 12.11, 98.38, 78.26, 34.72, 42.84, 37.06, 50.6, 34.12, 84.96, 82.23], 25), 35.89, places=6)

    def test_pct_4_50(self):
        self.assertAlmostEqual(percentile([60.04, 12.11, 98.38, 78.26, 34.72, 42.84, 37.06, 50.6, 34.12, 84.96, 82.23], 50), 50.6, places=6)

    def test_pct_4_75(self):
        self.assertAlmostEqual(percentile([60.04, 12.11, 98.38, 78.26, 34.72, 42.84, 37.06, 50.6, 34.12, 84.96, 82.23], 75), 80.245, places=6)

    def test_pct_4_90(self):
        self.assertAlmostEqual(percentile([60.04, 12.11, 98.38, 78.26, 34.72, 42.84, 37.06, 50.6, 34.12, 84.96, 82.23], 90), 84.96, places=6)

    def test_pct_4_95(self):
        self.assertAlmostEqual(percentile([60.04, 12.11, 98.38, 78.26, 34.72, 42.84, 37.06, 50.6, 34.12, 84.96, 82.23], 95), 91.67, places=6)

    def test_pct_4_100(self):
        self.assertAlmostEqual(percentile([60.04, 12.11, 98.38, 78.26, 34.72, 42.84, 37.06, 50.6, 34.12, 84.96, 82.23], 100), 98.38, places=6)

    def test_pct_5_0(self):
        self.assertAlmostEqual(percentile([43.35, 36.22, 89.16], 0), 36.22, places=6)

    def test_pct_5_5(self):
        self.assertAlmostEqual(percentile([43.35, 36.22, 89.16], 5), 36.933, places=6)

    def test_pct_5_10(self):
        self.assertAlmostEqual(percentile([43.35, 36.22, 89.16], 10), 37.646, places=6)

    def test_pct_5_25(self):
        self.assertAlmostEqual(percentile([43.35, 36.22, 89.16], 25), 39.785, places=6)

    def test_pct_5_50(self):
        self.assertAlmostEqual(percentile([43.35, 36.22, 89.16], 50), 43.35, places=6)

    def test_pct_5_75(self):
        self.assertAlmostEqual(percentile([43.35, 36.22, 89.16], 75), 66.255, places=6)

    def test_pct_5_90(self):
        self.assertAlmostEqual(percentile([43.35, 36.22, 89.16], 90), 79.998, places=6)

    def test_pct_5_95(self):
        self.assertAlmostEqual(percentile([43.35, 36.22, 89.16], 95), 84.579, places=6)

    def test_pct_5_100(self):
        self.assertAlmostEqual(percentile([43.35, 36.22, 89.16], 100), 89.16, places=6)



class TestParseLog(unittest.TestCase):
    def test_log_0(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=12:30:05 level=INFO msg=disk almost full'), {'ts': '12:30:05', 'level': 'INFO', 'msg': 'disk almost full'})

    def test_log_1(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('level=ERROR code=500 msg=boom'), {'level': 'ERROR', 'code': '500', 'msg': 'boom'})

    def test_log_2(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('a=1 b=2 c=3'), {'a': '1', 'b': '2', 'c': '3'})

    def test_log_3(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('msg=only message here'), {'msg': 'only message here'})

    def test_log_4(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('k=v msg=trailing spaces  keep  them'), {'k': 'v', 'msg': 'trailing spaces  keep  them'})

    def test_log_5(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=09:15:00 level=DEBUG msg='), {'ts': '09:15:00', 'level': 'DEBUG', 'msg': ''})

    def test_log_6(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=07:12:36 level=INFO host=web-9 msg=shard limit exceeded hot'), {'ts': '07:12:36', 'level': 'INFO', 'host': 'web-9', 'msg': 'shard limit exceeded hot'})

    def test_log_7(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=06:19:34 level=WARN host=web-4 msg=later exceeded'), {'ts': '06:19:34', 'level': 'WARN', 'host': 'web-4', 'msg': 'later exceeded'})

    def test_log_8(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=03:17:39 level=ERROR host=web-7 msg=disk exceeded gc full'), {'ts': '03:17:39', 'level': 'ERROR', 'host': 'web-7', 'msg': 'disk exceeded gc full'})

    def test_log_9(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=07:13:35 level=WARN host=web-6 msg=shard later limit disk'), {'ts': '07:13:35', 'level': 'WARN', 'host': 'web-6', 'msg': 'shard later limit disk'})

    def test_log_10(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=08:13:31 level=INFO host=web-7 msg=limit retry exceeded hot shard'), {'ts': '08:13:31', 'level': 'INFO', 'host': 'web-7', 'msg': 'limit retry exceeded hot shard'})

    def test_log_11(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=00:11:34 level=INFO host=web-7 msg=later shard gc'), {'ts': '00:11:34', 'level': 'INFO', 'host': 'web-7', 'msg': 'later shard gc'})

    def test_log_12(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=07:18:38 level=WARN host=web-7 msg=gc exceeded later limit'), {'ts': '07:18:38', 'level': 'WARN', 'host': 'web-7', 'msg': 'gc exceeded later limit'})

    def test_log_13(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=04:13:31 level=ERROR host=web-4 msg=almost limit full retry'), {'ts': '04:13:31', 'level': 'ERROR', 'host': 'web-4', 'msg': 'almost limit full retry'})

    def test_log_14(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=03:17:34 level=ERROR host=web-9 msg=almost retry later shard'), {'ts': '03:17:34', 'level': 'ERROR', 'host': 'web-9', 'msg': 'almost retry later shard'})

    def test_log_15(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=05:12:34 level=INFO host=web-9 msg=later disk shard'), {'ts': '05:12:34', 'level': 'INFO', 'host': 'web-9', 'msg': 'later disk shard'})

    def test_log_16(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=08:14:32 level=ERROR host=web-8 msg=disk shard'), {'ts': '08:14:32', 'level': 'ERROR', 'host': 'web-8', 'msg': 'disk shard'})

    def test_log_17(self):
        """msg= is last and swallows the rest of the line, spaces included."""
        self.assertEqual(parse_kv_log('ts=04:17:37 level=WARN host=web-6 msg=disk later exceeded'), {'ts': '04:17:37', 'level': 'WARN', 'host': 'web-6', 'msg': 'disk later exceeded'})



class TestTTLCache(unittest.TestCase):
    def test_ttl_0(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(1.0)
        self.assertEqual(c.get('b', 0.1), None)
        self.assertEqual(c.get('k', 1.11), None)
        self.assertEqual(c.get('a', 1.61), None)
        self.assertEqual(c.get('a', 2.11), None)
        self.assertEqual(c.get('k', 2.21), None)
        self.assertEqual(c.get('k', 3.22), None)
        c.set('k', 57, 3.72)
        self.assertEqual(c.get('k', 4.22), 57)
        self.assertEqual(c.get('k', 4.72), 57)

    def test_ttl_1(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(10.0)
        self.assertEqual(c.get('a', 0.1), None)
        c.set('k', 31, 5.1)
        c.set('k', 1, 5.6)
        self.assertEqual(c.get('b', 15.6), None)
        c.set('b', 37, 25.6)
        self.assertEqual(c.get('k', 30.6), None)
        c.set('k', 34, 30.7)
        self.assertEqual(c.get('k', 40.71), None)
        c.set('b', 29, 41.21)

    def test_ttl_2(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(2.0)
        c.set('a', 22, 1.0)
        self.assertEqual(c.get('k', 2.0), None)
        c.set('b', 60, 4.01)
        c.set('k', 35, 5.01)
        self.assertEqual(c.get('k', 7.02), None)
        c.set('k', 56, 7.12)
        c.set('k', 12, 8.12)
        self.assertEqual(c.get('k', 8.62), 12)
        self.assertEqual(c.get('k', 10.63), None)

    def test_ttl_3(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(3.0)
        self.assertEqual(c.get('a', 3.01), None)
        self.assertEqual(c.get('b', 3.51), None)
        c.set('b', 75, 6.51)
        self.assertEqual(c.get('k', 9.51), None)
        c.set('b', 43, 9.61)
        c.set('k', 21, 11.11)
        self.assertEqual(c.get('b', 12.61), None)
        self.assertEqual(c.get('k', 14.11), None)
        self.assertEqual(c.get('a', 17.12), None)

    def test_ttl_4(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(3.0)
        c.set('b', 99, 1.5)
        self.assertEqual(c.get('k', 4.5), None)
        self.assertEqual(c.get('k', 4.6), None)
        c.set('b', 67, 7.6)
        self.assertEqual(c.get('k', 9.1), None)
        c.set('a', 71, 12.1)
        c.set('b', 90, 12.6)
        c.set('a', 81, 15.6)
        self.assertEqual(c.get('a', 18.61), None)

    def test_ttl_5(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(3.0)
        self.assertEqual(c.get('a', 3.01), None)
        self.assertEqual(c.get('a', 3.11), None)
        self.assertEqual(c.get('b', 3.21), None)
        self.assertEqual(c.get('b', 3.71), None)
        self.assertEqual(c.get('k', 5.21), None)
        self.assertEqual(c.get('b', 8.21), None)
        self.assertEqual(c.get('b', 8.71), None)
        self.assertEqual(c.get('a', 11.71), None)
        self.assertEqual(c.get('k', 14.72), None)

    def test_ttl_6(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(1.0)
        c.set('b', 65, 0.5)
        c.set('a', 39, 1.0)
        self.assertEqual(c.get('k', 2.01), None)
        c.set('a', 62, 3.01)
        self.assertEqual(c.get('b', 3.51), None)
        self.assertEqual(c.get('b', 4.52), None)
        self.assertEqual(c.get('a', 5.02), None)
        c.set('k', 53, 5.52)
        self.assertEqual(c.get('b', 5.62), None)

    def test_ttl_7(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(5.0)
        self.assertEqual(c.get('k', 5.0), None)
        self.assertEqual(c.get('b', 5.5), None)
        self.assertEqual(c.get('k', 6.0), None)
        self.assertEqual(c.get('a', 8.5), None)
        self.assertEqual(c.get('a', 13.5), None)
        self.assertEqual(c.get('a', 18.5), None)
        self.assertEqual(c.get('k', 23.5), None)
        self.assertEqual(c.get('b', 23.6), None)
        self.assertEqual(c.get('b', 26.1), None)

    def test_ttl_8(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(5.0)
        self.assertEqual(c.get('b', 0.1), None)
        self.assertEqual(c.get('b', 5.11), None)
        c.set('k', 9, 10.11)
        self.assertEqual(c.get('k', 10.61), 9)
        self.assertEqual(c.get('a', 13.11), None)
        self.assertEqual(c.get('a', 18.11), None)
        c.set('b', 39, 18.21)
        c.set('a', 8, 18.31)
        c.set('b', 19, 20.81)

    def test_ttl_9(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(2.0)
        self.assertEqual(c.get('b', 2.01), None)
        c.set('a', 79, 2.51)
        self.assertEqual(c.get('k', 4.51), None)
        c.set('k', 60, 6.51)
        c.set('b', 2, 7.51)
        self.assertEqual(c.get('b', 9.51), None)
        c.set('a', 45, 10.01)
        self.assertEqual(c.get('b', 12.02), None)
        c.set('k', 39, 14.02)

    def test_ttl_10(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(2.0)
        c.set('b', 49, 2.0)
        self.assertEqual(c.get('b', 4.01), None)
        self.assertEqual(c.get('a', 5.01), None)
        c.set('b', 88, 7.01)
        self.assertEqual(c.get('k', 7.11), None)
        self.assertEqual(c.get('a', 8.11), None)
        self.assertEqual(c.get('a', 9.11), None)
        self.assertEqual(c.get('b', 11.12), None)
        c.set('k', 81, 11.62)

    def test_ttl_11(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(1.0)
        c.set('b', 47, 0.5)
        self.assertEqual(c.get('a', 1.0), None)
        c.set('k', 26, 1.5)
        self.assertEqual(c.get('k', 2.0), 26)
        self.assertEqual(c.get('k', 2.5), None)
        c.set('a', 62, 3.0)
        self.assertEqual(c.get('k', 3.5), None)
        self.assertEqual(c.get('b', 3.6), None)
        self.assertEqual(c.get('a', 4.1), None)

    def test_ttl_12(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(5.0)
        c.set('b', 51, 5.01)
        self.assertEqual(c.get('b', 5.11), 51)
        self.assertEqual(c.get('b', 10.11), None)
        c.set('k', 82, 12.61)
        self.assertEqual(c.get('a', 15.11), None)
        self.assertEqual(c.get('a', 20.11), None)
        self.assertEqual(c.get('b', 25.12), None)
        c.set('k', 60, 25.62)
        c.set('k', 18, 28.12)

    def test_ttl_13(self):
        """Entry is alive strictly while t - t_set < ttl (boundary expires)."""
        c = TTLCache(1.0)
        self.assertEqual(c.get('b', 0.1), None)
        c.set('a', 69, 0.2)
        self.assertEqual(c.get('b', 0.7), None)
        self.assertEqual(c.get('b', 1.71), None)
        self.assertEqual(c.get('b', 2.21), None)
        c.set('k', 36, 3.21)
        c.set('k', 57, 3.31)
        self.assertEqual(c.get('a', 4.31), None)
        c.set('k', 83, 4.41)



class TestBackoff(unittest.TestCase):
    def test_backoff_0(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(1.0, 1.0, 5, 30.0), [1.0, 1.0, 1.0, 1.0, 1.0], rtol=1e-9)

    def test_backoff_1(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(0.5, 1.0, 5, 5.0), [0.5, 0.5, 0.5, 0.5, 0.5], rtol=1e-9)

    def test_backoff_2(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(0.5, 10.0, 1, 5.0), [0.5], rtol=1e-9)

    def test_backoff_3(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(1.0, 1.5, 2, 100.0), [1.0, 1.5], rtol=1e-9)

    def test_backoff_4(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(0.5, 1.0, 6, 10.0), [0.5, 0.5, 0.5, 0.5, 0.5, 0.5], rtol=1e-9)

    def test_backoff_5(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(3.0, 1.5, 2, 30.0), [3.0, 4.5], rtol=1e-9)

    def test_backoff_6(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(0.5, 10.0, 1, 30.0), [0.5], rtol=1e-9)

    def test_backoff_7(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(0.5, 1.5, 6, 30.0), [0.5, 0.75, 1.125, 1.6875, 2.53125, 3.796875], rtol=1e-9)

    def test_backoff_8(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(0.5, 1.0, 7, 5.0), [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5], rtol=1e-9)

    def test_backoff_9(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(0.5, 1.0, 4, 10.0), [0.5, 0.5, 0.5, 0.5], rtol=1e-9)

    def test_backoff_10(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(3.0, 2.0, 1, 10.0), [3.0], rtol=1e-9)

    def test_backoff_11(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(1.0, 1.0, 2, 50.0), [1.0, 1.0], rtol=1e-9)

    def test_backoff_12(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(3.0, 1.0, 7, 5.0), [3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0], rtol=1e-9)

    def test_backoff_13(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(2.0, 3.0, 4, 100.0), [2.0, 6.0, 18.0, 54.0], rtol=1e-9)

    def test_backoff_14(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(3.0, 1.0, 5, 100.0), [3.0, 3.0, 3.0, 3.0, 3.0], rtol=1e-9)

    def test_backoff_15(self):
        """delay_i = min(cap, base * factor**i), i starting at 0."""
        np.testing.assert_allclose(backoff_delays(0.5, 10.0, 1, 100.0), [0.5], rtol=1e-9)



class TestRobustStd(unittest.TestCase):
    def test_std_0(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([22.9, 48.2, 3.04]), 22.634542923, places=6)

    def test_std_1(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([42.36, 21.31, 5.4, 35.62, 22.18]), 14.310687615, places=6)

    def test_std_2(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([16.11]), 0.0, places=6)

    def test_std_3(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([3.28, 13.75]), 7.403407999, places=6)

    def test_std_4(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([29.27, 35.61, 19.05, 29.87, 14.74, 25.28, 21.51, 39.65, 5.72, 32.74, 43.89, 27.57]), 10.725790922, places=6)

    def test_std_5(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([21.5, 44.4]), 16.192745289, places=6)

    def test_std_6(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([16.95, 22.67, 20.8, 4.75, 21.34]), 7.332009956, places=6)

    def test_std_7(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([12.74, 47.68, 34.34, 23.71, 4.56, 4.27, 21.59, 37.23, 18.63, 6.51, 3.0, 47.83]), 16.597423381, places=6)

    def test_std_8(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([16.48, 6.11, 17.68, 33.27, 37.51, 43.4, 36.05, 48.42]), 14.773098137, places=6)

    def test_std_9(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([15.62, 5.18, 25.37, 7.74, 24.11, 42.36, 17.51, 27.81]), 11.940076274, places=6)

    def test_std_10(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([38.13]), 0.0, places=6)

    def test_std_11(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([11.31, 21.46, 28.05, 38.37, 31.09, 33.75, 27.84, 30.45]), 8.256192481, places=6)

    def test_std_12(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([41.47, 13.37, 9.02, 35.13, 15.45, 16.99, 0.31, 43.49, 28.32, 20.04, 7.09, 31.66]), 13.993154062, places=6)

    def test_std_13(self):
        """Sample std, ddof=1."""
        self.assertAlmostEqual(robust_std([4.59]), 0.0, places=6)



class TestTopEvents(unittest.TestCase):
    def test_top_0(self):
        self.assertEqual(top_events({'d': 7, 'g': 8, 'f': 2, 'a': 3}, 2), [('g', 8), ('d', 7)])

    def test_top_1(self):
        self.assertEqual(top_events({'a': 2, 'e': 4, 'g': 8, 'h': 7, 'b': 6}, 4), [('g', 8), ('h', 7), ('b', 6), ('e', 4)])

    def test_top_2(self):
        self.assertEqual(top_events({'e': 1}, 1), [('e', 1)])

    def test_top_3(self):
        self.assertEqual(top_events({'a': 1, 'd': 3, 'e': 5, 'f': 2}, 1), [('e', 5)])

    def test_top_4(self):
        self.assertEqual(top_events({'g': 1, 'c': 7, 'd': 9, 'f': 2}, 4), [('d', 9), ('c', 7), ('f', 2), ('g', 1)])

    def test_top_5(self):
        self.assertEqual(top_events({'h': 2, 'f': 7}, 2), [('f', 7), ('h', 2)])

    def test_top_6(self):
        self.assertEqual(top_events({'e': 2, 'g': 1, 'f': 3, 'h': 6, 'b': 7}, 1), [('b', 7)])

    def test_top_7(self):
        self.assertEqual(top_events({'g': 7, 'b': 7, 'e': 6}, 1), [('b', 7)])



class TestNormalize(unittest.TestCase):
    def test_norm_0(self):
        np.testing.assert_allclose(normalize01([3.0, 3.0]), [0.0, 0.0], rtol=1e-6, atol=1e-9)

    def test_norm_1(self):
        np.testing.assert_allclose(normalize01([0.1, 4.06, -1.51, 2.27, 3.19]), [0.289048474, 1.0, 0.0, 0.678635548, 0.843806104], rtol=1e-6, atol=1e-9)

    def test_norm_2(self):
        np.testing.assert_allclose(normalize01([-2.64, -3.54]), [1.0, 0.0], rtol=1e-6, atol=1e-9)

    def test_norm_3(self):
        np.testing.assert_allclose(normalize01([-3.27, -3.47]), [1.0, 0.0], rtol=1e-6, atol=1e-9)

    def test_norm_4(self):
        np.testing.assert_allclose(normalize01([-4.25, 4.51, 1.28, -0.36, 0.64]), [0.0, 1.0, 0.631278539, 0.444063927, 0.558219178], rtol=1e-6, atol=1e-9)

    def test_norm_5(self):
        np.testing.assert_allclose(normalize01([-0.51, 4.24, 0.64, 1.35, 1.25]), [0.0, 1.0, 0.242105263, 0.391578947, 0.370526316], rtol=1e-6, atol=1e-9)

    def test_norm_6(self):
        np.testing.assert_allclose(normalize01([-1.84, -0.6, -0.31, 1.31, 2.96]), [0.0, 0.258333333, 0.31875, 0.65625, 1.0], rtol=1e-6, atol=1e-9)

    def test_norm_7(self):
        np.testing.assert_allclose(normalize01([-4.44, 0.07, -1.9, -0.48, -4.43]), [0.0, 1.0, 0.563192905, 0.87804878, 0.002217295], rtol=1e-6, atol=1e-9)



class TestEwma(unittest.TestCase):
    def test_ewma_0(self):
        np.testing.assert_allclose(ewma([0.77, 8.64, 8.55, 6.15, 5.07], 0.9), [0.77, 7.853, 8.4803, 6.38303, 5.201303], rtol=1e-9)

    def test_ewma_1(self):
        np.testing.assert_allclose(ewma([4.5], 0.3), [4.5], rtol=1e-9)

    def test_ewma_2(self):
        np.testing.assert_allclose(ewma([6.05, 5.01, 9.58, 4.51, 8.11], 0.5), [6.05, 5.53, 7.555, 6.0325, 7.07125], rtol=1e-9)

    def test_ewma_3(self):
        np.testing.assert_allclose(ewma([5.05], 0.3), [5.05], rtol=1e-9)

    def test_ewma_4(self):
        np.testing.assert_allclose(ewma([2.48], 0.9), [2.48], rtol=1e-9)

    def test_ewma_5(self):
        np.testing.assert_allclose(ewma([5.24, 6.1, 3.64, 9.18, 3.87, 7.74, 6.79], 0.1), [5.24, 5.326, 5.1574, 5.55966, 5.390694, 5.6256246, 5.74206214], rtol=1e-9)

    def test_ewma_6(self):
        np.testing.assert_allclose(ewma([0.66, 0.95, 6.78, 2.84, 7.24], 0.3), [0.66, 0.747, 2.5569, 2.64183, 4.021281], rtol=1e-9)

    def test_ewma_7(self):
        np.testing.assert_allclose(ewma([0.82, 6.64, 9.17, 3.1, 6.56], 0.9), [0.82, 6.058, 8.8588, 3.67588, 6.271588], rtol=1e-9)



class TestDedupe(unittest.TestCase):
    def test_dedupe_0(self):
        self.assertEqual(dedupe_preserve_order(['v', 'x', 'z', 'v']), ['v', 'x', 'z'])

    def test_dedupe_1(self):
        self.assertEqual(dedupe_preserve_order(['z', 'y', 'v', 'x', 'u', 'v']), ['z', 'y', 'v', 'x', 'u'])

    def test_dedupe_2(self):
        self.assertEqual(dedupe_preserve_order(['x', 'z', 'z', 'y', 'y']), ['x', 'z', 'y'])

    def test_dedupe_3(self):
        self.assertEqual(dedupe_preserve_order(['u', 'y', 'y', 'y', 'y']), ['u', 'y'])

    def test_dedupe_4(self):
        self.assertEqual(dedupe_preserve_order(['z', 'x', 'v']), ['z', 'x', 'v'])

    def test_dedupe_5(self):
        self.assertEqual(dedupe_preserve_order(['v', 'v', 'x', 'z', 'v', 'y', 'v', 'u', 'y']), ['v', 'x', 'z', 'y', 'u'])



class TestChunk(unittest.TestCase):
    def test_chunk_0(self):
        self.assertEqual(chunk_by_size([3, 3, 8, 1], 12, lambda x: x), [[3, 3], [8, 1]])

    def test_chunk_1(self):
        self.assertEqual(chunk_by_size([4, 8, 5, 8, 4, 9, 4], 10, lambda x: x), [[4], [8], [5], [8], [4], [9], [4]])

    def test_chunk_2(self):
        self.assertEqual(chunk_by_size([4, 6, 8, 8, 5, 7, 9, 9, 7], 8, lambda x: x), [[4], [6], [8], [8], [5], [7], [9], [9], [7]])

    def test_chunk_3(self):
        self.assertEqual(chunk_by_size([3, 5, 1, 8, 6], 5, lambda x: x), [[3], [5], [1], [8], [6]])

    def test_chunk_4(self):
        self.assertEqual(chunk_by_size([5, 2, 3], 10, lambda x: x), [[5, 2, 3]])

    def test_chunk_5(self):
        self.assertEqual(chunk_by_size([9, 3, 7, 2, 4, 8, 6, 1, 7], 5, lambda x: x), [[9], [3], [7], [2], [4], [8], [6], [1], [7]])

    def test_chunk_6(self):
        self.assertEqual(chunk_by_size([9, 6, 4, 7, 2, 6, 4, 1], 10, lambda x: x), [[9], [6, 4], [7, 2], [6, 4], [1]])

    def test_chunk_7(self):
        self.assertEqual(chunk_by_size([6, 3, 3], 5, lambda x: x), [[6], [3], [3]])



if __name__ == "__main__":
    unittest.main(verbosity=1)
