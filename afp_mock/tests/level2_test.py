import sys, os, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from solution import InMemoryDB

class TestLevel2(unittest.TestCase):
    def test_scan_sorted(self):
        db = InMemoryDB()
        db.set("A", "f3", "v3"); db.set("A", "f1", "v1"); db.set("A", "f2", "v2")
        self.assertEqual(db.scan("A"), "f1(v1), f2(v2), f3(v3)")

    def test_scan_missing(self):
        db = InMemoryDB()
        self.assertEqual(db.scan("nope"), "")
        db.set("A", "f", "v"); db.delete("A", "f")
        self.assertEqual(db.scan("A"), "")

    def test_scan_by_prefix(self):
        db = InMemoryDB()
        db.set("A", "foo1", "a"); db.set("A", "foo2", "b"); db.set("A", "bar", "c")
        self.assertEqual(db.scan_by_prefix("A", "foo"), "foo1(a), foo2(b)")
        self.assertEqual(db.scan_by_prefix("A", "zzz"), "")
        self.assertEqual(db.scan_by_prefix("B", "foo"), "")

if __name__ == "__main__":
    unittest.main()
