import sys, os, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from solution import InMemoryDB

class TestLevel3(unittest.TestCase):
    def test_ttl_alive_window(self):
        db = InMemoryDB()
        db.set_at_with_ttl("A", "f", "v1", 1, 5)      # alive [1, 6)
        self.assertEqual(db.get_at("A", "f", 1), "v1")
        self.assertEqual(db.get_at("A", "f", 5), "v1")
        self.assertIsNone(db.get_at("A", "f", 6))

    def test_set_at_no_expiry(self):
        db = InMemoryDB()
        db.set_at("A", "g", "v2", 3)
        self.assertEqual(db.get_at("A", "g", 1000000), "v2")

    def test_delete_at(self):
        db = InMemoryDB()
        db.set_at("A", "g", "v2", 1)
        self.assertTrue(db.delete_at("A", "g", 2))
        self.assertIsNone(db.get_at("A", "g", 3))
        self.assertFalse(db.delete_at("A", "g", 4))
        db.set_at_with_ttl("A", "h", "x", 5, 2)       # alive [5,7)
        self.assertFalse(db.delete_at("A", "h", 8))   # already expired

    def test_overwrite_replaces_ttl(self):
        db = InMemoryDB()
        db.set_at_with_ttl("C", "f", "v", 1, 3)       # alive [1,4)
        db.set_at("C", "f", "w", 2)                   # no ttl now
        self.assertEqual(db.get_at("C", "f", 100), "w")

    def test_scan_at(self):
        db = InMemoryDB()
        db.set_at("B", "b1", "x", 1)
        db.set_at_with_ttl("B", "a1", "y", 2, 3)      # alive [2,5)
        self.assertEqual(db.scan_at("B", 4), "a1(y), b1(x)")
        self.assertEqual(db.scan_at("B", 5), "b1(x)")
        self.assertEqual(db.scan_by_prefix_at("B", "a", 4), "a1(y)")
        self.assertEqual(db.scan_by_prefix_at("B", "a", 5), "")

if __name__ == "__main__":
    unittest.main()
