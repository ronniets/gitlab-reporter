#!/usr/bin/python3

import unittest
import unittest.mock as mock
import pandas as pd
import numpy as np
import os

from gitlab_reporter import (
    load_csv,
    create_path_for_csv,
    find_null_values_in_label,
    get_data_from_label,
    create_list_of_time_reports,
    total_sum,
    format_dataframe,
    total_dataframe,
    calculate_user_time,
    calculate_final_list_empty_accounts,
    calculate_final_reported_time_for_user,
    dataframe_to_csv,
    remove_column,
    get_agg_methods,
    summarize_data,
    get_list_issues,
    convert_date,
)

class UnitTest(unittest.TestCase):
    def setUp(self):
        self.file_path = os.environ.get('CSV_FILE_PATH')

    #Get test data from CSV-file
    def get_test_data(self):
        if self.file_path:
            df = pd.read_csv(self.file_path)
            test_data = [(row['time_spent (hours)'], row['user']) for _, row in df.iterrows()]
            return test_data

    #Get the file path for the provided CSV-file   
    def get_file_path(self):
        if self.file_path:
            return (self.file_path)

    #Get a list for the Aggregation Tests
    def get_agg_data(self):
        return {
            'A': [1, 2, 3],
            'B': [4.5, 5.6, 6.7],
            'C': ['x', 'y', 'z'],
            'D': [True, False, True]
        }
    
    #load_csv
    def test_load_csv(self):
        self.assertIsInstance(load_csv(self.get_file_path()), pd.DataFrame)
    
    #create_path_for_csv
    def test_create_path_for_csv(self):
        path = 'csv/test'
        result = create_path_for_csv(path)
        self.assertEqual(result, 'csv/test.csv')

    def test_create_path_for_csv_null(self):
        path = ""
        result = create_path_for_csv(path)
        self.assertEqual(result, None)

    #find_null_values_in_label
    def test_find_null_values_in_label(self):
        test_data = self.get_test_data()
        for df in test_data:
            null_values_df = find_null_values_in_label(df, 'account_label')
            self.assertTrue(null_values_df is None or isinstance(null_values_df, pd.DataFrame))

    def test_check_if_column_exists_in_account_label(self):
        test_data = self.get_test_data()
        expected_columns = ['user', 'time spent (hours)', 'account_label']
        for df in test_data:
            for label in expected_columns:
                null_values_df = find_null_values_in_label(df, label)
                if null_values_df is not None:
                    self.assertListEqual(list(null_values_df.columns), expected_columns)

    #get_data_from_label
    def test_valid_key_in_time_for_null_values(self):
        label_name = 'time spent (hours)'
        data = []

        test_data = {
            label_name: data
        }
            
        nv = pd.DataFrame(test_data)
        result = get_data_from_label(nv, label_name)

        if len(data) > 0:
            self.assertEqual(result, data)
        else:
            self.assertTrue(result.empty)

    def test_false_key_in_time_for_null_values(self):
        key = 'random key'
        nv = 5
        result = get_data_from_label(nv, key)
        self.assertIsNone(result)

    def test_empty_time_for_null_values(self):
        label_name = ''
        nv = {}
        result = get_data_from_label(nv, label_name)
        self.assertIsNone(result)

    def test_positive_value_time_for_null_values(self):
        label_name = 'time spent (hours)'
        test_data = {
            label_name: [3]
            }
        
        df = pd.DataFrame(test_data)
        result = get_data_from_label(df, label_name)
        self.assertEqual(result.iloc[0], 3)

    def test_negative_value_time_for_null_values(self):
        label_name = 'time spent (hours)'
        test_data = {
            label_name: [-3]
            }
        
        df = pd.DataFrame(test_data)
        result = get_data_from_label(df, label_name)
        self.assertEqual(result.iloc[0], -3)

    def test_zero_value_time_for_null_values(self):
        label_name = 'time spent (hours)'
        test_data = {
            label_name: [0]
            }
        
        df = pd.DataFrame(test_data)
        result = get_data_from_label(df, label_name)
        self.assertEqual(result.iloc[0], 0)

    def test_non_dict_time_for_null_values(self):
        label_name = 'time spent (hours)'
        value = 42
        result = get_data_from_label(value, label_name)
        self.assertIsNone(result)

    #create_list_of_time_reports
    def test_create_dataframe_success(self):
        test_data = self.get_test_data()
        for ts, user in test_data:
            test_list = {
                'Time Spent': [ts],
                'User': [user]
            }
            result = create_list_of_time_reports(test_list)
            self.assertIsNotNone(result)
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 1)

    def test_create_dataframe_exception_time(self):
        test_data = self.get_test_data()
        ts = []
        for _, user in test_data:
            test_list = {
                'Time Spent': [ts],
                'User': [user]
            }
            result = create_list_of_time_reports(test_list)
            self.assertIsNotNone(result)
            self.assertIsInstance(result, pd.DataFrame)

    def test_create_dataframe_exception_user(self):
        test_data = self.get_test_data()
        user = []
        for ts, _ in test_data:
            test_list = {
                'Time Spent': [ts],
                'User': [user]
            }
            result = create_list_of_time_reports(test_list)
            self.assertIsNotNone(result)
            self.assertIsInstance(result, pd.DataFrame)

    #total_sum
    def test_empty_total_sum(self):
        label_name = 'Time Spent'
        df = pd.DataFrame({label_name: []})
        result = total_sum(df, label_name)
        self.assertEqual(result, 0)

    def test_positive_values_total_sum(self):
        label_name = 'Time Spent'
        df = pd.DataFrame({label_name: [1,2,3,4,5]})
        result = total_sum(df, label_name)
        self.assertEqual(result, 15)

    def test_negative_values_total_sum(self):
        label_name = 'Time Spent'
        df = pd.DataFrame({label_name: [-1,-2,-3,-4,-5]})
        result = total_sum(df, label_name)
        self.assertEqual(result, -15)

    def test_random_values_total_sum(self):
        label_name = 'Time Spent'
        df = pd.DataFrame({label_name: [-4, -1, 0, 3, 5]})
        result = total_sum(df, label_name)
        self.assertEqual(result, 3)

    def test_edges_total_sum(self):
        label_name = 'Time Spent'
        df = pd.DataFrame({label_name: [-1,0,1]})
        result = total_sum(df, label_name)
        self.assertEqual(result, 0)

    #format_dataframe
    def test_valid_format_dataframe(self):
        test_data = self.get_test_data()
        individual_sums = []

        for ts, user in test_data:
            data = [{'User': user, 'Time Spent': ts}]
            rs = pd.DataFrame(data)
            individual_sums.append(rs['Time Spent'].values[0])
            result = format_dataframe(rs, 'User', 'Time Spent')
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(result.columns.tolist(), ['User', 'Time Spent'])
            self.assertEqual(result.iloc[0]['Time Spent'], ts)

    #total_dataframe
    def test_total_dataframe(self):
        test_data = self.get_test_data()
        individual_sums = []

        for ts, user in test_data:
            data = [{'User': user, 'Time Spent': ts}]
            rs = pd.DataFrame(data)
            individual_sums.append(rs['Time Spent'].values[0])

        total = sum(individual_sums)
        result = total_dataframe(total, rs, 'User', 'Time Spent')
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.columns.tolist(), ['User', 'Time Spent'])

    #calculate_user_time
    def test_calculate_user_time(self):
        csv = load_csv(self.get_file_path())
        df = pd.DataFrame(csv)
        result = calculate_user_time(df)
        self.assertIsNotNone(result)

    #calculate_time
    def test_calculate_final_from_file(self):
        csv = load_csv(self.get_file_path())
        df = pd.DataFrame(csv)
        result_df = calculate_user_time(df)
        result = calculate_final_list_empty_accounts(result_df)
        self.assertIsInstance(result, pd.DataFrame)
    
    def test_calculate_final_data(self):
        test_data = self.get_test_data()

        for ts, us in test_data:
            data = [{'Time Spent': ts, 'User': us}]
            df = pd.DataFrame(data)
            result_df = calculate_user_time(df)
            result = calculate_final_list_empty_accounts(result_df)
            self.assertIsInstance(result, pd.DataFrame)

    def test_empty_calculate_final_input(self):
        empty_df = pd.DataFrame(columns=['Time Spent', 'User'])
        result = calculate_final_list_empty_accounts(empty_df)
        self.assertIsInstance(result, pd.DataFrame)

    def test_final_with_missing_values(self):
        data = {'Time Spent': [1, 2, np.nan, 4], 'User': ['A', 'B', 'C', 'D']}
        df = pd.DataFrame(data)
        result = calculate_final_list_empty_accounts(df)
        self.assertIsInstance(result, pd.DataFrame)

    #calculate_final_reported_time_for_user    
    def test_calculate_final_report_data(self):
        test_list = {'Time Spent': [1,2,3], 'Account Label': ['Test', 'Test 2', 'Test 3']}
        df = pd.DataFrame(test_list)
        result = calculate_final_reported_time_for_user(df)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_empty_calculate_final_report_input(self):
        empty_df = pd.DataFrame(columns=['Time Spent', 'Account Label'])
        result = calculate_final_reported_time_for_user(empty_df)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_final_report_with_missing_values(self):
        data = {'Time Spent': [1, 2, np.nan, 4], 'Account Label': ['A', 'B', 'C', 'D']}
        df = pd.DataFrame(data)
        result = calculate_final_reported_time_for_user(df)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    #dataframe_to_csv
    def test_dataframe_to_csv(self):
        data = self.get_test_data()
        for ts, us in data:
            dt = [{'Time Spent': ts, 'User': us}]
            test_df = pd.DataFrame(dt)
            path = 'csv/output'
            with mock.patch.object(test_df, "to_csv") as to_csv_mock:
                dataframe_to_csv(test_df, path)
                to_csv_mock.assert_called_with('csv/output.csv', index=False)

    #remove_column
    def test_remove_column_true(self):
        data = self.get_test_data()
        for ts, us in data:
            dt = {'Time Spent': [ts], 'User': [us]}
            test_df = pd.DataFrame(dt)
            expected_result = pd.DataFrame({'User': [us]})
            actual_result = remove_column(test_df, 'Time Spent')
            self.assertCountEqual(actual_result.columns, expected_result.columns)
            self.assertTrue(actual_result.equals(expected_result))
    
    def test_remove_column_empty(self):
        column_name = 'Test'
        df = pd.DataFrame({column_name: []})
        result = remove_column(df, column_name)
        self.assertIs(result, df)

    def test_remove_column_int(self):
        column_name = 'Test'
        df = pd.DataFrame({column_name: [1]})
        result = remove_column(df, column_name)
        self.assertIs(result, df)

    #get_agg_methods
    def test_agg_methods_non_num(self):
        data = self.get_agg_data()
        df = pd.DataFrame(data)
        result = get_agg_methods(df, 'A')
        self.assertEqual(result.get('C'), 'first')
    
    def test_agg_methods_num(self):
        data = self.get_agg_data()
        df = pd.DataFrame(data)
        result = get_agg_methods(df, 'A')
        self.assertEqual(result.get('B'), 'sum')
    
    def test_agg_methods_bool(self):
        data = self.get_agg_data()
        df = pd.DataFrame(data)
        result = get_agg_methods(df, 'A')
        self.assertEqual(result['D'], 'sum')
    
    def test_agg_methods_none(self):
        df = pd.DataFrame()
        result = get_agg_methods(df, 'A')
        self.assertEqual(result, {})

    def test_agg_methods_no_df(self):
        result = get_agg_methods(None, 'A')
        self.assertEqual(result, {})

    #summarize_data
    def test_summarize_data_columns(self):
        test_data = {
            'Test': [1, 2, 3],
            'Working': [4, 5, 6],
        }
        df = pd.DataFrame(test_data)
        result = summarize_data(df)
        expected = ['Test', 'Working']
        result_col = result.columns.tolist()
        self.assertEqual(result_col, expected)
    
    def test_summarize_data_invalid(self):
        invalid = [1, 2, 3]
        result = summarize_data(invalid)
        self.assertIsNone(result)

    def test_summarize_data_empty(self):
        df = pd.DataFrame()
        result = summarize_data(df)
        self.assertIsNone(result)
    
    #get_list_issues
    def test_get_list_issues(self):
        test_data = {
            'Test': [1, 2, 3],
            'Working': [4, 5, 6],
            'Third': [7, 8, 9]
        }

        expected_data = {
            'Third': [24]
        }

        test_columns_list = ['Test', 'Working']

        df = pd.DataFrame(test_data)
        result_df = get_list_issues(df, test_columns_list)
        expected_df = pd.DataFrame(expected_data)
        if isinstance(result_df, pd.DataFrame):
            self.assertTrue(result_df.equals(expected_df))

    def test_get_list_issues_empty_df(self):
        test_columns_list = ['Test', 'Working']
        df = pd.DataFrame()
        result = get_list_issues(df, test_columns_list)
        self.assertIsNone(result)

    def test_get_list_issues_empty_list(self):
        test_data = {
            'Test': [1, 2, 3],
            'Working': [4, 5, 6],
        }

        test_columns_list = []
        df = pd.DataFrame(test_data)
        result = get_list_issues(df, test_columns_list)
        self.assertIsNone(result)

    #convert_date
    def test_convert_date_valid(self):
        data = {
            'Date': ['2023-09-01', '2023-09-02', '2023-09-03'],
            'User': ['Test', 'Anon', 'Ronald']
        }

        df = pd.DataFrame(data)
        start_date='2023-09-01'
        end_date='2023-09-04'
        label = 'Date'

        result_df = convert_date(df, start_date, end_date, label)
        self.assertTrue(result_df['Date'].dtype == 'datetime64[ns]')
        self.assertEqual(len(result_df), 3)
    
    def test_convert_date_invalid(self):
        data = {
            'Date': ['no_date', '2023-01-28', '2023-01-29'],
            'User': ['Test', 'Anon', 'Ronald']
        }

        df = pd.DataFrame(data)
        start_date='2023-01-01'
        end_date='2023-02-01'
        label = 'Date'

        result_df = convert_date(df, start_date, end_date, label)
        self.assertFalse(result_df['Date'].dtype == 'datetime64[ns]')
        self.assertEqual(len(result_df), 3)

    def test_convert_date_empty_df(self):
        df = pd.DataFrame()
        start_date = '2023-01-01'
        end_date = '2023-02-28'
        label = 'Date'

        result_df = convert_date(df, start_date, end_date, label)

        self.assertTrue(result_df.empty)


if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)
