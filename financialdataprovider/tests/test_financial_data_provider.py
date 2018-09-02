import unittest
import financialdataprovider as fpd


class TestFPD(unittest.TestCase):

    def setUp(self):
        self.ec = earningscalendar.EarningsCalendar()

    def test_earnings_announcements_for_date(self):

        actual = self.ec.earnings_announcements_for_date("2017-03-30")

        self.assertEqual(actual[0]['ticker'], 'AEHR')
        self.assertEqual(actual[0]['when'], 'amc')

        self.assertEqual(actual[1]['ticker'], 'ANGO')
        self.assertEqual(actual[1]['when'], 'bmo')

        self.assertEqual(actual[2]['ticker'], 'BSET')
        self.assertEqual(actual[2]['when'], '--')

        self.assertEqual(actual[3]['ticker'], 'FC')
        self.assertEqual(actual[3]['when'], 'amc')

        self.assertEqual(actual[4]['ticker'], 'LNN')
        self.assertEqual(actual[4]['when'], 'bmo')

        self.assertEqual(actual[5]['ticker'], 'SAIC')
        self.assertEqual(actual[5]['when'], 'bmo')

        self.assertEqual(actual[6]['ticker'], 'TITN')
        self.assertEqual(actual[6]['when'], 'bmo')


if __name__ == '__main__':
    unittest.main()
