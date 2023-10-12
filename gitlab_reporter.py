#!/usr/bin/python3

import pandas as pd
import sys

OUTPUT_PATH = 'output.csv'

##Load CSV-File with <file_path>
def load_csv(file_path):
    try:
        with open(file_path, 'rb') as file:
            return pd.read_csv(file)
    except FileNotFoundError:
        print("The filepath is incorrect or the file doesn't exist.")
        sys.exit(1)

##Write to CSV-File
def dataframe_to_csv(df):
    try:
        if isinstance(df, pd.DataFrame):
            df.to_csv(OUTPUT_PATH, index=False)
    except:
        print("Couldn't write to CSV")

##Get all null values in account label
def find_null_values_in_label(df, label_name):
    try:
        return df[df[label_name].isnull()]
    except:
        print(f"There is no null values for label: {label_name}")

##Get all values in account label
def get_data_from_label(df, label_name):
    try:
        return df[label_name]
    except:
        print(f"There is no label named: {label_name}")

##Create a DataFrame of reported time for each user.
def create_list_of_time_reports(value_1, value_2, first_label, second_label):
    try:
        return pd.DataFrame({first_label: value_1, second_label: value_2})
    except:
        print("Couldn't create time report.")

##Removes a column in a DataFrame
def remove_column(df, column_name):
    try:
        if isinstance(df, pd.DataFrame):
            for col in df.columns:
                if column_name in col:
                    del df[column_name]
        
        return df
    except:
        print("Couldn't remove column")

##Gets the aggregation methods used in the formating of the DataFrame
def get_agg_methods(df, column):
    try:
        agg_methods = {}

        if isinstance(df, pd.DataFrame):
            for col in df.columns:
                if col != column:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        agg_methods[col] = 'sum'
                    else:
                        agg_methods[col] = 'first'

        return agg_methods
    except:
        print("Couldn't get aggregation methods")

##Summarizes data based on the key column
def summarize_data(df):
    try:
        if isinstance(df, pd.DataFrame):
            column = df.columns[0]
            agg_methods = get_agg_methods(df, column)
            print(agg_methods)
            new_df = df.groupby(column).agg(agg_methods).reset_index()
            return new_df
    except:
        print("Couldn't summarize data")

##Formats the DataFrame for the specific assignment
def get_list_issues(df):
    try:
        remove_column(df, 'date_of_work')
        remove_column(df, 'user')
        return summarize_data(df)
    except:
        print("Couldn't get DataFrame")

##Formats and lists the issues in a CSV
def list_issues(file_path):
    try:
        df = load_csv(file_path)
        new_df = get_list_issues(df)
        print(new_df)
        dataframe_to_csv(new_df)
    except:
        print("Program not working")

##Calculation of the total sum of the reported time. 
def total_sum(df, label_name):
    try:
        return df[label_name].sum()
    except:
        print(f"Couldn't summarize the total value for column: {label_name}")

##Formatting of columns in a new DataFrame: 'User' 'Time Spent'.
def format_dataframe(rs, first_label, second_label):
    try:
        df = rs.groupby(first_label)[second_label].agg(['sum']).reset_index()
        df.rename(columns={'sum': second_label}, inplace=True)
        return df
    except:
        print("Couldn't create a sorted list with users and reported time.")

##Add the total time to the user DataFrame.
def total_dataframe(total, df, first_label, second_label):
    try:
        total_row = pd.DataFrame({first_label: ['Total Result'], second_label: [total]})
        return pd.concat([df, total_row], ignore_index=True)
    except:
        print(f"Couldn't create a sorted list with the total value of column: {second_label}")

##Creation of a user/time spent DataFrame with all information.
def calculate_user_time(df):
    try:
        nv = find_null_values_in_label(df, 'account_label')
        ts = get_data_from_label(nv, 'time_spent (hours)')
        user = get_data_from_label(nv, 'user')
        return create_list_of_time_reports(ts, user, 'Time Spent', 'User')
    except:
        return pd.DataFrame(columns=['User', 'Time Spent'])

##Creation of a account_label/time spent DataFrame with all information.
def calculate_account_time(df):
    try:
        al = get_data_from_label(df, 'account_label')
        ts = get_data_from_label(df, 'time_spent (hours)')
        return create_list_of_time_reports(ts, al, 'Time Spent', 'Account Label')
    except:
        return pd.DataFrame(columns=['Account Label', 'Time Spent'])

##Creation of list with total time included.
def calculate_final_list_empty_accounts(result_df):
    try:
        df = format_dataframe(result_df, 'User', 'Time Spent')
        total = total_sum(result_df, 'Time Spent')
        return total_dataframe(total, df, 'User', 'Time Spent')
    except:
        print("Couldn't create a list with total time.")

##Creation of list with total time included.
def calculate_final_reported_time_for_user(result_df):
    try:
        df = format_dataframe(result_df, 'Account Label', 'Time Spent')
        total = total_sum(result_df, 'Time Spent')
        return total_dataframe(total, df, 'Account Label', 'Time Spent')
    except:
        print("Couldn't create a list with total time.")

##Prints out DataFrame without index values.
def print_df(df):
    print(df.to_string(index=False))

##Lists users with time reports for all empty accounts.
def list_empty_accounts(file_path):
    try:
        df = load_csv(file_path)
        result_df = calculate_user_time(df)
        total_df = calculate_final_list_empty_accounts(result_df)
        print_df(total_df)

    except:
        print("Couldn't start the program.")
        sys.exit(1)

##Lists reported time for all accounts
def list_reported_time_per_account(file_path):
    try:
        df = load_csv(file_path)
        result_df = calculate_account_time(df)
        total_df = calculate_final_reported_time_for_user(result_df)
        print_df(total_df)
    except:
        print("Couldn't start the program.")
        sys.exit(1)

def main():
    if len(sys.argv) != 3:
        print("Correct format: python3 gitlab_reporter.py --method_name <csv_file>")
    else:
        file_path = sys.argv[2]
        if sys.argv[1] == "--list_empty_accounts":
            list_empty_accounts(file_path)
        elif sys.argv[1] == "--list_reported_time_per_account":
            list_reported_time_per_account(file_path)
        elif sys.argv[1] == "--list_issues":
            list_issues(file_path)
            print(f"CSV-File created for list issues: {OUTPUT_PATH}")
            print("Test")
        else:
            print("Correct format: python3 gitlab_reporter.py --method_name <csv_file>")
    
if __name__ == "__main__":
    main()