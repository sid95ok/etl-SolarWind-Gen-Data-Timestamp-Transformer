import unittest
from unittest.mock import patch
from datetime import datetime

from commons.util import (
    get_date_today, remove_str_whitespaces, convert_utc_datetime_to_naive_datetime,
    convert_naive_timestamp_to_utc_datetime, get_wind_data, get_solar_data,
    get_latest_week_date_range, request_api_call
)


class UtilTest(unittest.TestCase):

    def setUp(self):
        self.expected_week_range = [
            '2024-01-01',
            '2023-12-31',
            '2023-12-30',
            '2023-12-29',
            '2023-12-28',
            '2023-12-27',
            '2023-12-26'
        ]
        self.expected_solar_data = [
            {'Naive_Timestamp ': 1704585600000, ' Variable': 999,
             'value': 45.7621881297, 'Last Modified utc': 1704585600000}]
        self.expected_wind_data = "2024-01-07 00:00:00+00:00,761,-29.28958042287806,2024-01-07 00:00:00+00:00"

    def test_get_date_today(self):
        date = get_date_today()
        match_format_flag = bool(datetime.strptime(date, "%Y-%m-%d"))
        self.assertTrue(match_format_flag)
        expected_date = datetime.utcnow().strftime("%Y-%m-%d")
        self.assertEqual(expected_date, date)

    def test_remove_str_whitespaces(self):
        string = " Apple is   sweet "
        new_string = remove_str_whitespaces(string)
        expected_string = "Apple is sweet"
        self.assertEqual(new_string, expected_string)

    def test_remove_str_whitespaces_empty_string(self):
        string = ""
        new_string = remove_str_whitespaces(string)
        expected_string = ""
        self.assertEqual(new_string, expected_string)

    def test_remove_str_whitespaces_spaces_string(self):
        string = "   "
        new_string = remove_str_whitespaces(string)
        expected_string = ""
        self.assertEqual(new_string, expected_string)

    def test_remove_str_whitespaces_none_string(self):
        new_string = remove_str_whitespaces(None)
        self.assertIsNone(new_string)
    
    def test_convert_utc_datetime_to_naive_datetime(self):
        datetime_str = "2024-01-01 23:00:09+00:00"
        new_datetime_str = convert_utc_datetime_to_naive_datetime(datetime_str)
        expected_datetime_str = "2024-01-01 23:00:09"
        self.assertEqual(expected_datetime_str, new_datetime_str)

    def test_convert_utc_datetime_to_naive_datetime_wrong_format(self):
        datetime_str = "2024-01-01 23:00:09"
        new_datetime_str = convert_utc_datetime_to_naive_datetime(datetime_str)
        self.assertIsNone(new_datetime_str)

    def test_convert_utc_datetime_to_naive_datetime_none(self):
        new_datetime_str = convert_utc_datetime_to_naive_datetime(None)
        self.assertIsNone(new_datetime_str)

    def test_convert_naive_timestamp_to_utc_datetime_string(self):
        timestamp_str = "1704602536"
        new_datetime_str = convert_naive_timestamp_to_utc_datetime(timestamp_str)
        expected_datetime_str = "2024-01-07 04:42:16+00:00"
        self.assertEqual(expected_datetime_str, new_datetime_str)

    def test_convert_naive_timestamp_to_utc_datetime_int(self):
        timestamp_str = 1704602536
        new_datetime_str = convert_naive_timestamp_to_utc_datetime(timestamp_str)
        expected_datetime_str = "2024-01-07 04:42:16+00:00"
        self.assertEqual(expected_datetime_str, new_datetime_str)

    def test_convert_naive_timestamp_to_utc_datetime_13_string(self):
        timestamp_str = "1704602536000"
        new_datetime_str = convert_naive_timestamp_to_utc_datetime(timestamp_str)
        expected_datetime_str = "2024-01-07 04:42:16+00:00"
        self.assertEqual(expected_datetime_str, new_datetime_str)

    def test_convert_naive_timestamp_to_utc_datetime_13_int(self):
        timestamp_str = 1704602536000
        new_datetime_str = convert_naive_timestamp_to_utc_datetime(timestamp_str)
        expected_datetime_str = "2024-01-07 04:42:16+00:00"
        self.assertEqual(expected_datetime_str, new_datetime_str)

    def test_convert_naive_timestamp_to_utc_datetime_none(self):
        new_datetime_str = convert_naive_timestamp_to_utc_datetime(None)
        self.assertIsNone(new_datetime_str)

    def test_convert_naive_timestamp_to_utc_datetime_wrong(self):
        timestamp_str = "Wrong Timestamp Format"
        new_datetime_str = convert_naive_timestamp_to_utc_datetime(timestamp_str)
        self.assertIsNone(new_datetime_str)

    @patch("commons.util.request_api_call")
    def test_get_wind_data(self, mock_request_api_call):
        mock_request_api_call.return_value = self.expected_wind_data
        wind_data = get_wind_data("2024-01-01")
        self.assertEqual(self.expected_wind_data, wind_data)

    @patch("commons.util.request_api_call")
    def test_get_solar_data(self, mock_request_api_call):
        mock_request_api_call.return_value = self.expected_solar_data
        solar_data = get_solar_data("2024-01-01")
        self.assertEqual(self.expected_solar_data, solar_data)

    def test_get_latest_week_date_range(self):
        week_range = get_latest_week_date_range("2024-01-01")
        self.assertEqual(self.expected_week_range, week_range)

    def test_get_latest_week_date_range_wrong_format(self):
        week_range = get_latest_week_date_range("2024-31-01")
        self.assertIsNone(week_range)

    def test_get_latest_week_date_range_int(self):
        week_range = get_latest_week_date_range(123)
        self.assertIsNone(week_range)

    @patch("commons.util.date")
    def test_get_latest_week_date_range_none(self, mock_date):
        mock_date.today.return_value = datetime(2024, 1, 1)
        week_range = get_latest_week_date_range(None)
        self.assertEqual(week_range, self.expected_week_range)

    def test_request_api_call_missing_env_vars(self):
        response = request_api_call(None)
        self.assertIsNone(response)


if __name__ == '__main__':
    unittest.main()
