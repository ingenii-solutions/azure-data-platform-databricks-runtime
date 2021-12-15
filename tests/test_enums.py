from unittest import TestCase

from ingenii_databricks.enums import Stage


class TestStageEnum(TestCase):

    def test_greater_than(self):
        self.assertGreater(Stage.COMPLETED, Stage.INSERTED)
        self.assertGreater(Stage.INSERTED, Stage.NEW)
        self.assertGreater(Stage.STAGED, Stage.ARCHIVED)
        self.assertGreater(Stage.COMPLETED, Stage.NEW)
        self.assertGreater(Stage.COMPLETED, "inserted")
        self.assertGreater(Stage.INSERTED, "new")
        self.assertGreater(Stage.STAGED, "archived")
        self.assertGreater(Stage.COMPLETED, "new")

    def test_greater_than_or_equal(self):
        self.assertGreaterEqual(Stage.COMPLETED, Stage.INSERTED)
        self.assertGreaterEqual(Stage.INSERTED, Stage.NEW)
        self.assertGreaterEqual(Stage.STAGED, Stage.ARCHIVED)
        self.assertGreaterEqual(Stage.COMPLETED, Stage.NEW)
        self.assertGreaterEqual(Stage.COMPLETED, Stage.COMPLETED)
        self.assertGreaterEqual(Stage.INSERTED, Stage.INSERTED)
        self.assertGreaterEqual(Stage.COMPLETED, "inserted")
        self.assertGreaterEqual(Stage.INSERTED, "new")
        self.assertGreaterEqual(Stage.STAGED, "archived")
        self.assertGreaterEqual(Stage.COMPLETED, "new")
        self.assertGreaterEqual(Stage.COMPLETED, "completed")
        self.assertGreaterEqual(Stage.INSERTED, "inserted")

    def test_less_than(self):
        self.assertLess(Stage.INSERTED, Stage.COMPLETED)
        self.assertLess(Stage.NEW, Stage.INSERTED)
        self.assertLess(Stage.ARCHIVED, Stage.STAGED)
        self.assertLess(Stage.NEW, Stage.COMPLETED)
        self.assertLess(Stage.INSERTED, "completed")
        self.assertLess(Stage.NEW, "inserted")
        self.assertLess(Stage.ARCHIVED, "staged")
        self.assertLess(Stage.NEW, "completed")

    def test_less_than_or_equal(self):
        self.assertLessEqual(Stage.INSERTED, Stage.COMPLETED)
        self.assertLessEqual(Stage.NEW, Stage.INSERTED)
        self.assertLessEqual(Stage.ARCHIVED, Stage.STAGED)
        self.assertLessEqual(Stage.NEW, Stage.COMPLETED)
        self.assertLessEqual(Stage.COMPLETED, Stage.COMPLETED)
        self.assertLessEqual(Stage.INSERTED, Stage.INSERTED)
        self.assertLessEqual(Stage.INSERTED, "completed")
        self.assertLessEqual(Stage.NEW, "inserted")
        self.assertLessEqual(Stage.ARCHIVED, "staged")
        self.assertLessEqual(Stage.NEW, "completed")
        self.assertLessEqual(Stage.COMPLETED, "completed")
        self.assertLessEqual(Stage.INSERTED, "inserted")

    def test_equal(self):
        self.assertEqual(Stage.COMPLETED, Stage.COMPLETED)
        self.assertEqual(Stage.INSERTED, Stage.INSERTED)
        self.assertEqual(Stage.COMPLETED, "completed")
        self.assertEqual(Stage.INSERTED, "inserted")

    def test_different(self):
        self.assertNotEqual(Stage.INSERTED, Stage.COMPLETED)
        self.assertNotEqual(Stage.NEW, Stage.INSERTED)
        self.assertNotEqual(Stage.ARCHIVED, Stage.STAGED)
        self.assertNotEqual(Stage.NEW, Stage.COMPLETED)
        self.assertNotEqual(Stage.INSERTED, "completed")
        self.assertNotEqual(Stage.NEW, "inserted")
        self.assertNotEqual(Stage.ARCHIVED, "staged")
        self.assertNotEqual(Stage.NEW, "completed")

    def test_type_comparison(self):
        self.assertRaises(Exception, Stage.INSERTED.__eq__, 12)
        self.assertRaises(Exception, Stage.INSERTED.__eq__, True)
        self.assertRaises(Exception, Stage.INSERTED.__eq__, {})
