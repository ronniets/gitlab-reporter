#!/usr/bin/python3

import unittest
import pandas as pd
import numpy as np
import sys

from gitlab_reporter import (
    load_csv,
    find_null_values_in_account_label,
    get_time_for_null_values,
    get_user_for_null_values,
    create_list_of_time_reports,
    total_sum,
    user_dataframe,
    total_dataframe,
    calculate_user_time,
    calculate_final,
)

class UnitTest(unittest.TestCase):
    def setUp(self):
        self.file_path = sys.argv[1] if len(sys.argv) == 2 else None

    ##Hämtning av test data
    def get_test_data(self):
        if self.file_path:
            df = pd.read_csv(self.file_path)
            test_data = [(row['time_spent (hours)'], row['user']) for _, row in df.iterrows()]
            return test_data

    ##Få vägen till filen    
    def get_file_path(self):
        if self.file_path:
            return (self.file_path)
    
    ##load_csv
    def test_load_csv(self):
        self.assertIsInstance(load_csv(self.get_file_path()), pd.DataFrame)

    ##find_null_values_in_account_label
    def test_find_null_values_in_account_label(self):
        test_data = self.get_test_data()
        for df in test_data:
            null_values_df = find_null_values_in_account_label(df)
            self.assertTrue(null_values_df is None or isinstance(null_values_df, pd.DataFrame))

    def test_check_if_column_exists_in_account_label(self):
        test_data = self.get_test_data()
        expected_columns = ['user', 'time spent (hours)', 'account_label']
        for df in test_data:
            null_values_df = find_null_values_in_account_label(df)
            if null_values_df is not None:
                self.assertListEqual(list(null_values_df.columns), expected_columns)

    ##get_time_for_null_values
    def test_valid_key_in_time_for_null_values(self):
        test_data = self.get_test_data()
        for ts, _ in test_data:
            nv = {'time_spent (hours)': ts}
            result = get_time_for_null_values(nv)
            self.assertEqual(result, ts)

    def test_false_key_in_time_for_null_values(self):
        nv = {'random_key': 5}
        result = get_time_for_null_values(nv)
        self.assertIsNone(result)

    def test_empty_time_for_null_values(self):
        nv = {}
        result = get_time_for_null_values(nv)
        self.assertIsNone(result)

    def test_positive_value_time_for_null_values(self):
        value = {'time_spent (hours)': 5}
        result = get_time_for_null_values(value)
        self.assertEqual(result, 5)

    def test_negative_value_time_for_null_values(self):
        value = {'time_spent (hours)': -3}
        result = get_time_for_null_values(value)
        self.assertEqual(result, -3)

    def test_zero_value_time_for_null_values(self):
        value = {'time_spent (hours)': 0}
        result = get_time_for_null_values(value)
        self.assertEqual(result, 0)

    def test_non_dict_time_for_null_values(self):
        value = 42
        result = get_time_for_null_values(value)
        self.assertIsNone(result)

    ##get_user_for_null_values
    def test_valid_key_in_user_for_null_values(self):
        test_data = self.get_test_data()
        for _, user in test_data:
            nv = {'user': user}
            result = get_user_for_null_values(nv)
            self.assertEqual(result, user)
    
    def test_false_key_in_user_for_null_values(self):
        nv = {'random_key' : 'User1'}
        result = get_user_for_null_values(nv)
        self.assertIsNone(result)

    def test_empty_user_for_null_values(self):
        nv = {}
        result = get_user_for_null_values(nv)
        self.assertIsNone(result)

    def test_non_dict_user_for_null_values(self):
        value = 42
        result = get_user_for_null_values(value)
        self.assertIsNone(result)

    ##create_list_of_time_reports
    def test_create_dataframe_success(self):
        test_data = self.get_test_data()
        for ts, user in test_data:
            result = create_list_of_time_reports([ts], [user])
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 1)

    def test_create_dataframe_exception_time(self):
        test_data = self.get_test_data()
        ts = []
        for _, user in test_data:
            result = create_list_of_time_reports(ts, user)
            self.assertIsInstance(result, pd.DataFrame)

    def test_create_dataframe_exception_user(self):
        test_data = self.get_test_data()
        user = []
        for ts, _ in test_data:
            result = create_list_of_time_reports(ts, user)
            self.assertIsInstance(result, pd.DataFrame)

    ##total_sum
    def test_empty_total_sum(self):
        df = pd.DataFrame({'Time Spent': []})
        result = total_sum(df)
        self.assertEqual(result, 0)

    def test_positive_values_total_sum(self):
        df = pd.DataFrame({'Time Spent': [1,2,3,4,5]})
        result = total_sum(df)
        self.assertEqual(result, 15)

    def test_negative_values_total_sum(self):
        df = pd.DataFrame({'Time Spent': [-1,-2,-3,-4,-5]})
        result = total_sum(df)
        self.assertEqual(result, -15)

    def test_random_values_total_sum(self):
        df = pd.DataFrame({'Time Spent': [-4, -1, 0, 3, 5]})
        result = total_sum(df)
        self.assertEqual(result, 3)

    def test_edges_total_sum(self):
        df = pd.DataFrame({'Time Spent': [-1,0,1]})
        result = total_sum(df)
        self.assertEqual(result, 0)

    ##user_dataframe
    def test_valid_user_dataframe(self):
        test_data = self.get_test_data()
        individual_sums = []

        for ts, user in test_data:
            data = [{'User': user, 'Time Spent': ts}]
            rs = pd.DataFrame(data)
            individual_sums.append(rs['Time Spent'].values[0])
            result = user_dataframe(rs)
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(result.columns.tolist(), ['User', 'Time Spent'])
            self.assertEqual(result.iloc[0]['Time Spent'], ts)

    ##total_dataframe
    def test_total_dataframe(self):
        test_data = self.get_test_data()
        individual_sums = []

        for ts, user in test_data:
            data = [{'User': user, 'Time Spent': ts}]
            rs = pd.DataFrame(data)
            individual_sums.append(rs['Time Spent'].values[0])

        total = sum(individual_sums)
        result = total_dataframe(total, rs)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.columns.tolist(), ['User', 'Time Spent'])

    ##calculate_user_time
    def test_calculate_user_time(self):
        csv = load_csv(self.get_file_path())
        df = pd.DataFrame(csv)
        result = calculate_user_time(df)
        self.assertIsNotNone(result)

    def test_exceptions_calculate_user_time(self):
        test_data = self.get_test_data()

        for ts, us in test_data:
            data = [{'Time Spent': ts, 'User': us}]
            df = pd.DataFrame(data)
            result = calculate_user_time(df)
            self.assertEqual(result, None)

    #calculate_time
    def test_calculate_final_from_file(self):
        csv = load_csv(self.get_file_path())
        df = pd.DataFrame(csv)
        result_df = calculate_user_time(df)
        result = calculate_final(result_df)
        self.assertIsInstance(result, pd.DataFrame)
    
    def test_calculate_final_data(self):
        test_data = self.get_test_data()

        for ts, us in test_data:
            data = [{'Time Spent': ts, 'User': us}]
            df = pd.DataFrame(data)
            result_df = calculate_user_time(df)
            result = calculate_final(result_df)
            self.assertIsInstance(result, pd.DataFrame)

    def test_empty_calculate_final_input(self):
        empty_df = pd.DataFrame(columns=['Time Spent', 'User'])
        result = calculate_final(empty_df)
        self.assertIsInstance(result, pd.DataFrame)

    def test_final_with_missing_values(self):
        data = {'Time Spent': [1, 2, np.nan, 4], 'User': ['A', 'B', 'C', 'D']}
        df = pd.DataFrame(data)
        result = calculate_final(df)
        self.assertIsInstance(result, pd.DataFrame)

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)
