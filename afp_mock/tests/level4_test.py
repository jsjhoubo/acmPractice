import sys, os, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from solution import InMemoryDB

class TestLevel4(unittest.TestCase):
    def test_backup_count_and_restore_ttl(self):
        db = InMemoryDB()
        db.set_at_with_ttl("A", "f", "v", 1, 10)      # expires at 11
        db.set_at("B", "g", "w", 2)
        self.assertEqual(db.backup(5), 2)             # both records alive
        self.assertTrue(db.delete_at("B", "g", 6))
        self.assertEqual(db.backup(7), 1)             # only A alive (remaining ttl = 4)
        db.restore(10)                                 # latest backup <= 10 is backup(7)
        # A restored: remaining 4s from t=10 -> expires at 14
        self.assertEqual(db.get_at("A", "f", 13), "v")
        self.assertIsNone(db.get_at("A", "f", 14))
        # B was deleted before backup(7): stays gone
        self.assertIsNone(db.get_at("B", "g", 12))

    def test_restore_picks_correct_backup(self):
        db = InMemoryDB()
        db.set_at("K", "f", "v1", 1)
        self.assertEqual(db.backup(2), 1)
        db.set_at("K", "f", "v2", 3)
        self.assertEqual(db.backup(4), 1)
        db.set_at("K", "f", "v3", 5)
        db.restore(4)                                  # -> snapshot at t=4: f = v2
        self.assertEqual(db.get_at("K", "f", 6), "v2")

    def test_no_ttl_restored_without_ttl(self):
        db = InMemoryDB()
        db.set_at("A", "f", "v", 1)
        self.assertEqual(db.backup(2), 1)
        db.delete_at("A", "f", 3)
        db.restore(9)
        self.assertEqual(db.get_at("A", "f", 1000000), "v")

if __name__ == "__main__":
    unittest.main()
