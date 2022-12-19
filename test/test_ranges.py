import unittest

from pagio import PGInt4Range


class RangeTest(unittest.TestCase):

    def test_int4_default_range(self):
        int_range = PGInt4Range(5, 10)
        self.assertTrue(7 in int_range)
        self.assertTrue(5 in int_range)
        self.assertFalse(10 in int_range)
        self.assertFalse(4 in int_range)

    def test_int4_lower_ex_range(self):
        int_range = PGInt4Range(5, 10, bounds='()')
        self.assertTrue(7 in int_range)
        self.assertFalse(5 in int_range)
        self.assertFalse(10 in int_range)
        self.assertFalse(4 in int_range)
        self.assertEqual(int_range, PGInt4Range(6, 10))

    def test_int4_upper_inc_range(self):
        int_range = PGInt4Range(5, 10, bounds='[]')
        self.assertTrue(7 in int_range)
        self.assertTrue(5 in int_range)
        self.assertTrue(10 in int_range)
        self.assertFalse(4 in int_range)
        self.assertFalse(11 in int_range)
        self.assertEqual(int_range, PGInt4Range(5, 11))

    def test_int4_no_lower_bound(self):
        int_range = PGInt4Range(None, 10)
        self.assertTrue(7 in int_range)
        self.assertTrue(5 in int_range)
        self.assertFalse(10 in int_range)
        self.assertTrue(4 in int_range)
        self.assertFalse(11 in int_range)

    def test_int4_no_upper_bound(self):
        int_range = PGInt4Range(5, None)
        self.assertTrue(7 in int_range)
        self.assertTrue(5 in int_range)
        self.assertTrue(10 in int_range)
        self.assertFalse(4 in int_range)
        self.assertTrue(11 in int_range)

    def test_int4_no_bounds(self):
        int_range = PGInt4Range(None, None)
        self.assertTrue(7 in int_range)
        self.assertTrue(5 in int_range)
        self.assertTrue(10 in int_range)
        self.assertTrue(4 in int_range)
        self.assertTrue(11 in int_range)

    def test_int4_empty(self):
        int_range = PGInt4Range.empty()
        self.assertFalse(7 in int_range)
        self.assertFalse(5 in int_range)
        self.assertFalse(10 in int_range)
        self.assertFalse(4 in int_range)
        self.assertFalse(11 in int_range)

    def test_invalid_int4_range(self):
        with self.assertRaises(ValueError):
            PGInt4Range(-0x80000001, 0)
        int_range = PGInt4Range(None, None)
        with self.assertRaises(ValueError):
            0x80000000 in int_range
