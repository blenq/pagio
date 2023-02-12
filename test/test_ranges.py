from datetime import datetime, date
import unittest

from pagio import (
    PGInt4Range, PGTimestampRange, PGDateRange, PGInt8Range, PGInt4MultiRange)


class RangeTest(unittest.TestCase):

    def test_invalid_bounds(self):
        with self.assertRaises(ValueError):
            PGInt4Range(5, 10, None)
        with self.assertRaises(ValueError):
            PGInt4Range('empty', 10)
        with self.assertRaises(ValueError):
            PGInt4Range(5, 10, "[)}")
        with self.assertRaises(ValueError):
            PGInt4Range(5, 10, "[")
        with self.assertRaises(ValueError):
            PGInt4Range(5, 10, "ab")
        with self.assertRaises(ValueError):
            PGInt4Range(10, 5)

    def test_invalid_eq(self):
        self.assertFalse(
            PGInt4Range(5, 10) == PGDateRange(date(2020, 1, 1), None))

    def test_bounds(self):
        val = PGInt4Range(5, 10)
        self.assertEqual("[)", val.bounds)
        val = PGTimestampRange(
            datetime(2020, 2, 2, 14), datetime(2020, 2, 5, 18), '(]')
        self.assertEqual("(]", val.bounds)
        self.assertIsNone(PGInt4Range('empty').bounds)

    def test_empty(self):
        val = PGInt4Range('empty')
        self.assertTrue(val.is_empty)
        val = PGInt4Range.empty()
        self.assertTrue(val.is_empty)
        self.assertTrue(val.is_empty)
        self.assertEqual(str(val), "empty")
        self.assertEqual(repr(val), 'PGInt4Range(None, None, None)')
        self.assertEqual(val, PGInt4Range(None, None, None))
        self.assertNotEqual(val, PGInt4Range(None, None))

    def test_int4_default_range(self):
        int_range = PGInt4Range(5, 10)
        self.assertTrue(7 in int_range)
        self.assertTrue(5 in int_range)
        self.assertFalse(10 in int_range)
        self.assertFalse(4 in int_range)

    def test_range_str(self):
        self.assertEqual(str(PGInt4Range(5, 10)), "[5,10)")
        self.assertEqual(str(PGInt4Range(None, None)), "(,)")
        self.assertEqual(repr(PGInt4Range(5, 10)), "PGInt4Range(5, 10, '[)')")
        self.assertEqual(repr(PGInt4Range(None, None)),
                         "PGInt4Range(None, None, '()')")

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

    def test_int4_contains_int4(self):
        range1 = PGInt4Range(3, 8)
        self.assertTrue(PGInt4Range(5, 7) in range1)
        self.assertTrue(PGInt4Range(3, 8) in range1)
        self.assertFalse(PGInt4Range(3, None) in range1)
        self.assertFalse(PGInt4Range(None, 5) in range1)
        self.assertTrue(PGInt4Range(5, 5) in range1)
        self.assertFalse(PGInt4Range(5, 8) in PGInt4Range('empty'))
        self.assertTrue(range1 in PGInt4Range(3, None))
        self.assertTrue(range1 in PGInt4Range(None, 10))
        self.assertTrue(range1 in PGInt4Range(None, None))
        self.assertFalse(PGInt4Range(None, None) in range1)
        self.assertTrue(5 in range1)
        self.assertFalse(8 in range1)

    def test_timestamp_contains_timestamp(self):
        check_range = PGTimestampRange(
            datetime(2020, 2, 2, 14), datetime(2020, 2, 5, 18), '[)')
        self.assertTrue(check_range in check_range)
        self.assertFalse(PGTimestampRange(
            datetime(2020, 2, 2, 14), datetime(2020, 2, 5, 18), '[]'
        ) in check_range)
        check_range = PGTimestampRange(
            datetime(2020, 2, 2, 14), datetime(2020, 2, 5, 18), '(]')
        self.assertFalse(PGTimestampRange(
            datetime(2020, 2, 2, 14), datetime(2020, 2, 2, 18), '[]'
        ) in check_range)
        self.assertTrue(datetime(2020, 2, 3) in check_range)
        self.assertFalse(
            PGTimestampRange(
                datetime(2020, 2, 2, 14), datetime(2020, 2, 5, 18), '[]') in
            PGTimestampRange(
                datetime(2020, 2, 2, 14), datetime(2020, 2, 10, 18), '(]'),
        )
        self.assertFalse(
            PGTimestampRange(
                datetime(2020, 2, 2, 14), datetime(2020, 2, 5, 18), '[]') in
            PGTimestampRange(
                datetime(2020, 1, 1, 14), datetime(2020, 2, 5, 18), '[)'),
        )

    def test_overlap(self):
        self.assertFalse(PGInt4Range(5, 10).overlaps(PGInt4Range(2, 4)))
        self.assertFalse(PGInt4Range(2, 4).overlaps(PGInt4Range(5, 10)))

        self.assertFalse(PGInt4Range(5, 10).overlaps(PGInt4Range(2, 5)))
        self.assertFalse(PGInt4Range(2, 5).overlaps(PGInt4Range(5, 10)))

        self.assertTrue(PGInt4Range(5, 10).overlaps(PGInt4Range(2, 5, '[]')))
        self.assertTrue(PGInt4Range(2, 5, '[]').overlaps(PGInt4Range(5, 10)))

        self.assertTrue(PGInt4Range(5, 10).overlaps(PGInt4Range(2, 7)))
        self.assertTrue(PGInt4Range(5, 10).overlaps(PGInt4Range(2, 12)))
        self.assertTrue(PGInt4Range(5, 10).overlaps(PGInt4Range(7, 12)))
        self.assertTrue(PGInt4Range(5, 10, '[]').overlaps(PGInt4Range(10, 12)))
        self.assertFalse(PGInt4Range(5, 10).overlaps(PGInt4Range(10, 12)))
        self.assertFalse(PGInt4Range(5, 10).overlaps(PGInt4Range(12, 20)))
        self.assertFalse(PGInt4Range(5, 10).overlaps(PGInt4Range('empty')))

    def test_is_adjacent(self):
        self.assertTrue(PGInt4Range(5, 10).is_adjacent_to(PGInt4Range(10, 12)))
        self.assertFalse(
            PGInt4Range(5, 10, '[]').is_adjacent_to(PGInt4Range(10, 12)))
        self.assertFalse(
            PGInt4Range(5, 10).is_adjacent_to(PGInt4Range(10, 12, '()')))

        self.assertTrue(PGInt4Range(10, 12).is_adjacent_to(PGInt4Range(5, 10)))
        self.assertFalse(PGInt4Range(11, 12).is_adjacent_to(PGInt4Range(5, 10)))
        self.assertFalse(
            PGInt4Range(11, 12).is_adjacent_to(PGInt4Range('empty')))
        self.assertFalse(
            PGInt4Range('empty').is_adjacent_to(PGInt4Range(5, 10)))
        self.assertTrue(
            PGInt4Range(None, 10).is_adjacent_to(PGInt4Range(10, None)))

    def test_union(self):
        self.assertEqual(
            PGInt4Range(5, 10) | PGInt4Range(7, 12), PGInt4Range(5, 12))
        self.assertEqual(
            PGInt4Range(5, 10) | PGInt4Range(10, 12), PGInt4Range(5, 12))
        self.assertEqual(
            PGInt4Range(5, 10) | PGInt4Range('empty'), PGInt4Range(5, 10))
        self.assertEqual(
            PGInt4Range('empty') | PGInt4Range(5, 10), PGInt4Range(5, 10))
        self.assertEqual(
            PGInt4Range('empty') | PGInt4Range('empty'), PGInt4Range('empty'))

        self.assertEqual(
            PGInt4Range(5, 10) | PGInt4Range(None, 7), PGInt4Range(None, 10))
        self.assertEqual(
            PGInt4Range(5, 10) | PGInt4Range(6, None), PGInt4Range(5, None))

        with self.assertRaises(ValueError):
            PGInt4Range(5, 10) | PGInt4Range(12, 14)

        with self.assertRaises(TypeError):
            PGInt4Range(5, 10) | PGInt8Range(7, 12)

    def test_multirange(self):
        num = PGInt4MultiRange(PGInt4Range(5, 10), (8, 12))
        self.assertEqual(num, PGInt4MultiRange((5, 12)))
        num = PGInt4MultiRange(PGInt4Range(5, 10), (12, 18), (8, 14))
        self.assertEqual(num, PGInt4MultiRange((5, 18)))
        num = PGInt4MultiRange(PGInt4Range(5, 10), (12, 18))
        self.assertEqual(num, PGInt4MultiRange((5, 10), (12, 18)))
        num = PGInt4MultiRange((5, 10), (12, 18), ('empty',), (20, 24), (3, 7))
        self.assertEqual(num, PGInt4MultiRange((3, 10), (12, 18), (20, 24)))
        num = PGInt4MultiRange(('empty',))
        self.assertEqual(num, PGInt4MultiRange())
        num = PGInt4MultiRange(
            (5, 10), (12, 18), (None, None), ('empty',), (20, 24), (3, 7))
        self.assertEqual(num, PGInt4MultiRange((None, None)))
        num = PGInt4MultiRange(('empty',))
        self.assertEqual(num, PGInt4MultiRange())

    def test_multirange_contains(self):
        num = PGInt4MultiRange(PGInt4Range(5, 10), (12, None))
        self.assertTrue(PGInt4Range(14, 18) in num)
        self.assertTrue(7 in num)
        self.assertTrue(PGInt4MultiRange((6, 8)) in num)
        self.assertTrue(PGInt4MultiRange() in num)

        self.assertFalse(PGInt4Range(11, 18) in num)
        self.assertFalse(11 in num)
        self.assertFalse(PGInt4MultiRange((6, 11)) in num)
