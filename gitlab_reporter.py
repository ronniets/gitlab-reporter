#!/usr/bin/python3

import pandas as pd
import sys

##Load CSV-File with <file_path>
def load_csv(file_path):
    try:
        with open(file_path, 'rb') as file:
            return pd.read_csv(file)
    except FileNotFoundError:
        print("The filepath is incorrect or the file doesn't exist.")
        sys.exit(1)

##Get all null values in account label
def find_null_values_in_account_label(df):
    try:
        return df[df['account_label'].isnull()]
    except:
        print("There is no null values.")

##Get all values in account label
def get_account_label(df):
    try:
        return df['account_label']
    except:
        print("There is no account labels.")

##Get reported time for all null values.
def get_time_for_column(nv):
    try:
        return nv['time_spent (hours)']
    except:
        print("There is no reported time.")

##Get users for all null values·
def get_user_for_column(nv):
    try:
        return nv['user']
    except:
        print("No user has reported time for this null value.")

##Creata a DataFrame of reported time for each user.
def create_list_of_time_reports(ts, user):
    try:
        return pd.DataFrame({'Time Spent': ts, 'User': user})
    except:
        print("Couldn't create time report.")

##Creata a DataFrame of reported time for each account.
def create_list_of_time_reports_per_account(ts, al):
    try:
        return pd.DataFrame({'Time Spent': ts, 'Account Label': al})
    except:
        print("Couldn't create time report.")

##Calculation of the total sum of the reported time. 
def total_sum(df):
    try:
        total = df['Time Spent'].sum()
        return total
    except:
        print("Couldn't summarize the time.")

##Formatting of columns in a new DataFrame: 'User' 'Time Spent'.
def user_dataframe(rs):
    try:
        df = rs.groupby('User')['Time Spent'].agg(['sum']).reset_index()
        df.rename(columns={'sum': 'Time Spent'}, inplace=True)
        return df
    except:
        print("Couldn't create a sorted list with users and reported time.")

##Formatting of columns in a new DataFrame: 'Account Label' 'Time Spent'.
def account_dataframe(rs):
    try:
        df = rs.groupby('Account Label')['Time Spent'].agg(['sum']).reset_index()
        df.rename(columns={'sum': 'Time Spent'}, inplace=True)
        return df
    except:
        print("Couldn't create a sorted list with accounts and reported time.")

##Add the total time to the user DataFrame.
def total_dataframe(total, df):
    try:
        total_row = pd.DataFrame({'User': ['Total Result'], 'Time Spent': [total]})
        return pd.concat([df, total_row], ignore_index=True)
    except:
        print("Couldn't create a sorted list with the total time.")

##Add the total time to the account DataFrame.
def total_account_dataframe(total, df):
    try:
        total_row = pd.DataFrame({'Account Label': ['Total Result'], 'Time Spent': [total]})
        return pd.concat([df, total_row], ignore_index=True)
    except:
        print("Couldn't create a sorted list with the total time.")

##Creation of a user/time spent DataFrame with all information.
def calculate_user_time(df):
    try:
        nv = find_null_values_in_account_label(df)
        tn = get_time_for_column(nv)
        user = get_user_for_column(nv)
        return create_list_of_time_reports(tn, user)
    except:
        return pd.DataFrame(columns=['User', 'Time Spent'])

##Creation of a account_label/time spent DataFrame with all information.
def calculate_account_time(df):
    try:
        al = get_account_label(df)
        tn = get_time_for_column(df)
        return create_list_of_time_reports_per_account(tn, al)
    except:
        return pd.DataFrame(columns=['Account Label', 'Time Spent'])

##Creation of list with total time included.
def calculate_final_list_empty_accounts(result_df):
    try:
        df = user_dataframe(result_df)
        total = total_sum(result_df)
        return total_dataframe(total, df)
    except:
        print("Couldn't create a list with total time.")

##Creation of list with total time included.
def calculate_final_reported_time_for_user(result_df):
    try:
        df = account_dataframe(result_df)
        total = total_sum(result_df)
        return total_account_dataframe(total, df)
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

def main():
    if len(sys.argv) != 3:
        print("Correct format: python3 gitlab_reporter.py --method_name <csv_file>")
    else:
        if sys.argv[1] == "--list_empty_accounts":
            file_path = sys.argv[2]
            list_empty_accounts(file_path)
        elif sys.argv[1] == "--list_reported_time_per_person":
            file_path = sys.argv[2]
            list_reported_time_per_account(file_path)
        else:
            print("Correct format: python3 gitlab_reporter.py --method_name <csv_file>")
    
if __name__ == "__main__":
    main()