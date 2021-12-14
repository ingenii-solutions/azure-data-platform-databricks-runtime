from unittest import TestCase

from ingenii_databricks.enums import StageObj, Stages


class TestStagesEnum(TestCase):

    def test_greater_than(self):
        self.assertGreater(Stages.COMPLETED, Stages.INSERTED)
        self.assertGreater(Stages.INSERTED, Stages.NEW)
        self.assertGreater(Stages.STAGED, Stages.ARCHIVED)
        self.assertGreater(Stages.COMPLETED, Stages.NEW)
        self.assertGreater(Stages.COMPLETED, "inserted")
        self.assertGreater(Stages.INSERTED, "new")
        self.assertGreater(Stages.STAGED, "archived")
        self.assertGreater(Stages.COMPLETED, "new")

    def test_greater_than_or_equal(self):
        self.assertGreaterEqual(Stages.COMPLETED, Stages.INSERTED)
        self.assertGreaterEqual(Stages.INSERTED, Stages.NEW)
        self.assertGreaterEqual(Stages.STAGED, Stages.ARCHIVED)
        self.assertGreaterEqual(Stages.COMPLETED, Stages.NEW)
        self.assertGreaterEqual(Stages.COMPLETED, Stages.COMPLETED)
        self.assertGreaterEqual(Stages.INSERTED, Stages.INSERTED)
        self.assertGreaterEqual(Stages.COMPLETED, "inserted")
        self.assertGreaterEqual(Stages.INSERTED, "new")
        self.assertGreaterEqual(Stages.STAGED, "archived")
        self.assertGreaterEqual(Stages.COMPLETED, "new")
        self.assertGreaterEqual(Stages.COMPLETED, "completed")
        self.assertGreaterEqual(Stages.INSERTED, "inserted")

    def test_less_than(self):
        self.assertLess(Stages.INSERTED, Stages.COMPLETED)
        self.assertLess(Stages.NEW, Stages.INSERTED)
        self.assertLess(Stages.ARCHIVED, Stages.STAGED)
        self.assertLess(Stages.NEW, Stages.COMPLETED)
        self.assertLess(Stages.INSERTED, "completed")
        self.assertLess(Stages.NEW, "inserted")
        self.assertLess(Stages.ARCHIVED, "staged")
        self.assertLess(Stages.NEW, "completed")

    def test_less_than_or_equal(self):
        self.assertLessEqual(Stages.INSERTED, Stages.COMPLETED)
        self.assertLessEqual(Stages.NEW, Stages.INSERTED)
        self.assertLessEqual(Stages.ARCHIVED, Stages.STAGED)
        self.assertLessEqual(Stages.NEW, Stages.COMPLETED)
        self.assertLessEqual(Stages.COMPLETED, Stages.COMPLETED)
        self.assertLessEqual(Stages.INSERTED, Stages.INSERTED)
        self.assertLessEqual(Stages.INSERTED, "completed")
        self.assertLessEqual(Stages.NEW, "inserted")
        self.assertLessEqual(Stages.ARCHIVED, "staged")
        self.assertLessEqual(Stages.NEW, "completed")
        self.assertLessEqual(Stages.COMPLETED, "completed")
        self.assertLessEqual(Stages.INSERTED, "inserted")

    def test_equal(self):
        self.assertEqual(Stages.COMPLETED, Stages.COMPLETED)
        self.assertEqual(Stages.INSERTED, Stages.INSERTED)
        self.assertEqual(Stages.COMPLETED, "completed")
        self.assertEqual(Stages.INSERTED, "inserted")

    def test_different(self):
        self.assertNotEqual(Stages.INSERTED, Stages.COMPLETED)
        self.assertNotEqual(Stages.NEW, Stages.INSERTED)
        self.assertNotEqual(Stages.ARCHIVED, Stages.STAGED)
        self.assertNotEqual(Stages.NEW, Stages.COMPLETED)
        self.assertNotEqual(Stages.INSERTED, "completed")
        self.assertNotEqual(Stages.NEW, "inserted")
        self.assertNotEqual(Stages.ARCHIVED, "staged")
        self.assertNotEqual(Stages.NEW, "completed")

    def test_type_comparison(self):
        self.assertRaises(Exception, Stages.INSERTED.__eq__, 12)
        self.assertRaises(Exception, Stages.INSERTED.__eq__, True)
        self.assertRaises(Exception, Stages.INSERTED.__eq__, {})
