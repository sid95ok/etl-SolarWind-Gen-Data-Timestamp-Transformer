import unittest
from unittest.mock import patch
import pandas as pd
from io import StringIO
import csv
from etl_components.extract import extract_generation_data
from etl_components.load import write_solar_data, write_wind_data
from etl_components.transform import (
    transform_column_names, transform_solar_data, transform_wind_data
)


class ComponentsTest(unittest.TestCase):

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
        self.expected_week_range_2 = [
            '2023-01-01',
            '2022-12-31',
            '2022-12-30',
            '2022-12-29',
            '2022-12-28',
            '2022-12-27',
            '2022-12-26'
        ]
        self.expected_solar_data_1 = [{'Naive_Timestamp ': 1704585600000, ' Variable': 999, 'value': 45.7621881297, 'Last Modified utc': 1704585600000}]
        self.expected_solar_data_2 = [
            {'Naive_Timestamp ': 1704585600000, ' Variable': 999, 'value': 45.7621881297, 'Last Modified utc': 1704585600000},
            {'Naive_Timestamp ': 1704585600000, ' Variable': 999, 'value': 45.7621881297, 'Last Modified utc': 1704585600000},
            {'Naive_Timestamp ': 1704585600000, ' Variable': 999, 'value': 45.7621881297, 'Last Modified utc': 1704585600000},
            {'Naive_Timestamp ': 1704585600000, ' Variable': 999, 'value': 45.7621881297, 'Last Modified utc': 1704585600000},
            {'Naive_Timestamp ': 1704585600000, ' Variable': 999, 'value': 45.7621881297, 'Last Modified utc': 1704585600000},
            {'Naive_Timestamp ': 1704585600000, ' Variable': 999, 'value': 45.7621881297, 'Last Modified utc': 1704585600000},
            {'Naive_Timestamp ': 1704585600000, ' Variable': 999, 'value': 45.7621881297, 'Last Modified utc': 1704585600000}]
        self.transformed_gen_data = pd.DataFrame([
            {'naive_timestamp': 1704585600000, 'variable': 999,
             'value': 45.7621881297, 'last_modified_utc': 1704585600000,
             'utc_timestamp': '2024-01-01 00:00:00+00:00'}])

        self.solar_data_sample_df = pd.DataFrame(
            [[1704585600000, 999, 45.7621881297, 1704585600000]],
            columns=['naive_timestamp', 'variable', 'value', 'last_modified_utc']
        )
        self.wind_data_sample_df = pd.DataFrame(
            [["2024-01-07 00:00:00+00:00", 999, 45.7621881297, "2024-01-07 00:00:00+00:00"]],
            columns=['naive_timestamp', 'variable', 'value',
                     'last_modified_utc']
        )

        output = StringIO()
        fieldnames = ["Naive_Timestamp ", " Variable", "value", "Last Modified utc"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)

        writer.writeheader()
        for item in self.expected_solar_data_1:
            writer.writerow(item)

        self.expected_wind_data = output.getvalue()

    @patch("etl_components.extract.get_wind_data")
    @patch("etl_components.extract.get_solar_data")
    def test_extract_generation_data(self, mock_solar_data, mock_wind_data):
        date = "2024-01-01"
        mock_solar_data.return_value = self.expected_solar_data_1
        mock_wind_data.return_value = self.expected_wind_data
        solar_df, wind_df, date_range = extract_generation_data(date)

        expected_df = pd.DataFrame(self.expected_solar_data_2)

        self.assertEqual(True, expected_df.equals(solar_df))
        self.assertEqual(True, expected_df.equals(wind_df))
        self.assertEqual(self.expected_week_range, date_range)

    def test_extract_generation_data_wrong_format(self):
        date = "2024-31-01"
        solar_df, wind_df, date_range = extract_generation_data(date)
        self.assertIsNone(date_range)
        self.assertIsNone(solar_df)
        self.assertIsNone(wind_df)

    def test_extract_generation_data_none(self):
        solar_df, wind_df, date_range = extract_generation_data(None)
        self.assertEqual(7, len(date_range))
        self.assertIsNotNone(solar_df)
        self.assertIsNotNone(wind_df)

    def test_write_solar_data(self):
        is_written = write_solar_data(self.transformed_gen_data, self.expected_week_range)
        self.assertTrue(is_written)

    def test_write_solar_data_date_mismatch(self):
        is_written = write_solar_data(self.transformed_gen_data, self.expected_week_range_2)
        self.assertFalse(is_written)

    def test_write_solar_data_none(self):
        is_written = write_solar_data(None, None)
        self.assertFalse(is_written)

    def test_write_solar_data_empty(self):
        is_written = write_solar_data(pd.DataFrame(), [])
        self.assertFalse(is_written)

    def test_write_wind_data(self):
        is_written = write_wind_data(self.transformed_gen_data, self.expected_week_range)
        self.assertTrue(is_written)

    def test_write_wind_data_date_mismatch(self):
        is_written = write_wind_data(self.transformed_gen_data, self.expected_week_range_2)
        self.assertFalse(is_written)

    def test_write_wind_data_none(self):
        is_written = write_wind_data(None, None)
        self.assertFalse(is_written)

    def test_write_wind_data_empty(self):
        is_written = write_wind_data(pd.DataFrame(), [])
        self.assertFalse(is_written)

    def test_transform_column_names(self):
        df = pd.DataFrame(self.expected_solar_data_1)
        new_df = transform_column_names(df)
        self.assertListEqual(list(df.columns), list(new_df.columns))

    def test_transform_column_names_none(self):
        new_df = transform_column_names(None)
        self.assertIsNone(new_df)

    def test_transform_column_names_empty(self):
        new_df = transform_column_names(pd.DataFrame())
        self.assertEqual(True, pd.DataFrame().equals(new_df))

    def test_transform_solar_data(self):
        new_df = transform_solar_data(self.solar_data_sample_df)
        expected_df = new_df.assign(utc_timestamp="2024-01-07 00:00:00+00:00")
        self.assertTrue(expected_df.equals(new_df))

    def test_transform_solar_data_none(self):
        new_df = transform_solar_data(None)
        self.assertIsNone(new_df)

    def test_transform_solar_data_empty(self):
        new_df = transform_solar_data(pd.DataFrame())
        self.assertTrue(new_df.empty)

    def test_transform_wind_data(self):
        new_df = transform_wind_data(self.wind_data_sample_df)
        expected_df = new_df.assign(utc_timestamp="2024-01-07 00:00:00+00:00")
        self.assertTrue(expected_df.equals(new_df))

    def test_transform_wind_data_none(self):
        new_df = transform_wind_data(None)
        self.assertIsNone(new_df)

    def test_transform_wind_data_empty(self):
        new_df = transform_wind_data(pd.DataFrame())
        self.assertTrue(new_df.empty)


if __name__ == '__main__':
    unittest.main()
