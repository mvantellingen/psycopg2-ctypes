from unittest import TestCase

from psycopg2ct.extensions import Notify


class TestNotify(TestCase):
    def test_compare(self):

        self.assertTrue(Notify(1, 'foo') == Notify(1, 'foo'))
        self.assertFalse(Notify(1, 'foo') != Notify(1, 'foo'))

        self.assertTrue(Notify(1, 'foo') != Notify(1, 'bar'))

        self.assertTrue(Notify(1, 'foo') != Notify(2, 'foo'))
        self.assertTrue(Notify(1, 'foo') != Notify(2, 'bar'))
        self.assertTrue(Notify(1, 'foo') != Notify(2, 'bar'))

        self.assertTrue(Notify(1, 'foo') != Notify(2, 'bar'))

    def test_compare_payload(self):
        self.assertTrue(Notify(1, 'foo', 'baz') == Notify(1, 'foo', 'baz'))
        self.assertTrue(Notify(1, 'foo') == Notify(1, 'foo', ''))
        self.assertTrue(Notify(1, 'foo', 'foo') != Notify(1, 'foo', 'bar'))

    def test_compare_tuple(self):
        self.assertTrue(Notify(1, 'foo') == (1, 'foo'))
        self.assertTrue(Notify(1, 'foo') != (1, 'bar'))
        self.assertTrue(Notify(1, 'foo') != (2, 'foo'))

    def test_compare_tuple_payload(self):
        self.assertTrue(Notify(1, 'foo', 'baz') == (1, 'foo'))
        self.assertTrue(Notify(1, 'foo', 'baz') != (1, 'bar'))
        self.assertTrue(Notify(1, 'foo', 'baz') != (2, 'foo'))

    def test_indexing(self):
        n = Notify(1, 'foo', 'baz')
        self.assertEqual(n[0], 1)
        self.assertEqual(n[1], 'foo')
        self.assertRaises(IndexError, n.__getitem__, 2)

    def test_len(self):
        n = Notify(1, 'foo', 'baz')
        self.assertEqual(len(n), 2)



