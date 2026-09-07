import sys, os, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from solution import InMemoryDB

class TestLevel1(unittest.TestCase):
    def test_set_get(self):
        db = InMemoryDB()
        db.set("A", "field1", "v1")
        self.assertEqual(db.get("A", "field1"), "v1")
        self.assertIsNone(db.get("A", "field2"))
        self.assertIsNone(db.get("B", "field1"))

    def test_overwrite(self):
        db = InMemoryDB()
        db.set("A", "f", "v1"); db.set("A", "f", "v2")
        self.assertEqual(db.get("A", "f"), "v2")

    def test_delete(self):
        db = InMemoryDB()
        db.set("A", "f", "v1")
        self.assertTrue(db.delete("A", "f"))
        self.assertIsNone(db.get("A", "f"))
        self.assertFalse(db.delete("A", "f"))
        self.assertFalse(db.delete("B", "x"))

if __name__ == "__main__":
    unittest.main()
