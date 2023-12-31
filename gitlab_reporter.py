#!/usr/bin/python3

import pandas as pd
import sys

#Load CSV-File with <file_path>
def load_csv(file_path):
    try:
        if file_path is None:
            print("Filepath does not exist.")
        else:
            with open(file_path, 'rb') as file:
                return pd.read_csv(file)
    except FileNotFoundError as e:
        print(f"{file_path} is incorrect {e}")
        sys.exit(1)

#Write to CSV-File
def dataframe_to_csv(df, path):
    try:
        output_path = create_path_for_csv(path)
        print(output_path)

        if df is None:
            print("There is no DataFrame")
        else:
            if isinstance(df, pd.DataFrame):
                if output_path is None:
                    print("Please provide a name and path to store the CSV-file in.")
                
                df.to_csv(output_path, index=False)
    
    except Exception as e:
        print(f"Couldn't convert {df} to CSV, {e}")

#Creates the path and name for the output CSV file for method: list_issues.
def create_path_for_csv(path):
    try:
        if path is None:
            print("Please provide a name for the file.")
        
        if path:
            return path + '.csv'
    
    except Exception as e:
        print(f"Couldn't create filepath for {path}.csv, {e}")

#Get all null values in account label
def find_null_values_in_label(df, label_name):
    try:
        if df is None:
            print("Please provide a DataFrame")
        
        if label_name is None:
            print("Please provide a name for the label")
        
        if isinstance(df, pd.DataFrame):
            return df[df[label_name].isnull()]
    except:
        print(f"There is no null values for label: {label_name}")

#Get all values in account label
def get_data_from_label(df, label_name):
    try:
        if df is None:
            print("Please provide a DataFrame")
        
        if label_name is None:
            print("Please provide a name for the label")

        if isinstance(df, pd.DataFrame):
            return df[label_name]
    except:
        print(f"There is no label named: {label_name}")

#Create a DataFrame of reported time for each user.
def create_list_of_time_reports(report_list):
    try:
        if report_list is None:
            print("Please provide a report list")

        return pd.DataFrame(report_list)
    except:
        print("Couldn't create time report.")

#Removes a column in a DataFrame
def remove_column(df, column_name):
    try:
        if df is None:
            print("Please provide a valid DataFrame")

        if column_name is None:
            print("Please provide a valid column name")

        if isinstance(df, pd.DataFrame):
            for col in df.columns:
                if column_name in col:
                    del df[column_name]
        
        return df
    except Exception as e:
        print(f"Couldn't remove column with name: {column_name}, {e}")

#Gets the aggregation methods used in the formating of the DataFrame
def get_agg_methods(df, column):
    try:
        agg_methods = {}

        if df is None:
            print("Please provide a valid DataFrame")

        if column is None:
            print("Please provide a valid column name")

        if isinstance(df, pd.DataFrame):
            for col in df.columns:
                if col != column:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        agg_methods[col] = 'sum'
                    else:
                        agg_methods[col] = 'first'

        return agg_methods
    except Exception as e:
        print(f"Couldn't get aggregation methods for {df} with the column: {column}, {e}")

#Summarizes data based on the key column
def summarize_data(df):
    try:
        if df is None:
            print("Please provide a DataFrame")
        
        if isinstance(df, pd.DataFrame):
            column = df.columns[0]
            agg_methods = get_agg_methods(df, column)
            new_df = df.groupby(column).agg(agg_methods).reset_index()
            return new_df
    except Exception as e:
        print(f"Couldn't summarize data in {df}, {e}")

#Formats the DataFrame for the specific assignment
def get_list_issues(df, columns_list):
    try:
        if df is None:
            print("Please provide a DataFrame")
        
        if columns_list is None:
            print("Please provide a columns list")

        if isinstance(df, pd.DataFrame):
            if not columns_list:
                return None
            for column in columns_list:
                remove_column(df, column)
            return summarize_data(df)
    except Exception as e:
        print(f"Couldn't get {df} and remove columns, {e}")

#Converts the date and filters the start and end dates
def convert_date(df, start_date, end_date, label):
        try:
            if df is None:
                print("Please provide a DataFrame")
            
            if start_date is None:
                print("Please provide a valid Date")
            
            if end_date is None:
                print("Please provide a valid Date")

            if label is None:
                print("Please provide a valid label")
                 
            if isinstance(df, pd.DataFrame):
                df[label] = pd.to_datetime(df[label])
                df[label] = df[label].dt.tz_localize(None)
                mask = (df[label] >= start_date) & (df[label] <= end_date)
                return df[mask]
        except Exception as e:
            print(f"Error converting {label} column to datetime: {e}")

        return df

#Calculation of the total sum of the reported time. 
def total_sum(df, label_name):
    try:
        if df is None:
            print("Please provide DataFrame")
        
        if label_name is None:
            print("Please provide a label name")

        if isinstance(df, pd.DataFrame):
            return df[label_name].sum()
    except:
        print(f"Couldn't summarize the total value for column: {label_name}")

#Formatting of columns in a new DataFrame: 'User' 'Time Spent'.
def format_dataframe(rs, first_label, second_label):
    try:
        if rs is None:
            print("Please provide a DataFrame")

        if first_label is None:
            print("Please provide a label")
        
        if second_label is None:
            print("Please provide a label")

        if isinstance(rs, pd.DataFrame):
            df = rs.groupby(first_label, dropna=False)[second_label].agg(['sum']).reset_index()
            df.rename(columns={'sum': second_label}, inplace=True)
            return df
    except:
        print("Couldn't create a sorted list with users and reported time.")

#Add the total time to the user DataFrame.
def total_dataframe(total, df, first_label, second_label):
    try:
        if total is None:
            print("Please provide a total value")
        
        if df is None:
            print("Please provide a DataFrame")
        
        if first_label is None:
            print("Please provide a label")
        
        if second_label is None:
            print("Please provide a label")

        if isinstance(df, pd.DataFrame):
            total_row = pd.DataFrame({first_label: ['Total Result'], second_label: [total]})
            return pd.concat([df, total_row], ignore_index=True)
    except:
        print(f"Couldn't create a sorted list with the total value of column: {second_label}")

#Creation of a user/time spent DataFrame with all information.
def calculate_user_time(df):
    try:
        if df is None:
            print("Please provide a DataFrame")

        if isinstance(df, pd.DataFrame):
            nv = find_null_values_in_label(df, 'account_label')
            ts = get_data_from_label(nv, 'time_spent (hours)')
            user = get_data_from_label(nv, 'user')

            report_list = {
                    'Time Spent': ts.tolist(),
                    'User': user.tolist()
            }

            return create_list_of_time_reports(report_list)
    except:
        return pd.DataFrame(columns=['User', 'Time Spent'])

#Creation of a account_label/time spent DataFrame with all information.
def calculate_account_time(df):
    try:
        if df is None:
            print("Please provide a DataFrame")

        if isinstance(df, pd.DataFrame):
            al = get_data_from_label(df, 'account_label')
            ts = get_data_from_label(df, 'time_spent (hours)')

            report_list = {
                'Account Label': al.tolist(),
                'Time Spent': ts.tolist()
            }

            return create_list_of_time_reports(report_list)
    except:
        return pd.DataFrame(columns=['Account Label', 'Time Spent'])

#Creation of list with total time included.
def calculate_final_list_empty_accounts(df):
    try:
        if df is None:
            print("Please provide a DataFrame")

        if isinstance(df, pd.DataFrame):
            df = format_dataframe(df, 'User', 'Time Spent')
            total = total_sum(df, 'Time Spent')
            return total_dataframe(total, df, 'User', 'Time Spent')
    except:
        print("Couldn't create a list with total time.")

#Creation of list with total time included.
def calculate_final_reported_time_for_user(df):
    try:
        if df is None:
            print("Please provide a DataFrame")

        if isinstance(df, pd.DataFrame):
            result_df = format_dataframe(df, 'Account Label', 'Time Spent')
            total = total_sum(result_df, 'Time Spent')
            return total_dataframe(total, result_df, 'Account Label', 'Time Spent')
    except:
        print("Couldn't create a list with total time.")

#Prints out DataFrame without index values.
def print_df(df):
    try:
        if df is None:
            print("Please provide a DataFrame")

        if isinstance(df, pd.DataFrame):
            print(df.to_string(index=False))
    except Exception as e:
        print(f"Couldnt print DataFrame: {df}, {e}")

#Lists users with time reports for all empty accounts.
def list_empty_accounts(file_path):
    try:
        if file_path is None:
            print("Please provide a file_path")
        
        df = load_csv(file_path)
        
        if isinstance(df, pd.DataFrame):
            result_df = calculate_user_time(df)
            total_df = calculate_final_list_empty_accounts(result_df)
            print_df(total_df)
    except Exception as e:
        print(f"Couldn't start the program with file_path: {file_path}, {e}")
        sys.exit(1)

#Lists reported time for all accounts
def list_reported_time_per_account(file_path):
    try:
        if file_path is None:
            print("Please provide a file_path")
        
        df = load_csv(file_path)

        if isinstance(df, pd.DataFrame):
            result_df = calculate_account_time(df)
            total_df = calculate_final_reported_time_for_user(result_df)
            print_df(total_df)
    except Exception as e:
        print(f"Couldn't start the program with file_path: {file_path}, {e}")
        sys.exit(1)

#Formats and lists the issues in a CSV
def list_issues(file_path, start_date, end_date, output_path):
    try:
        if file_path is None:
            print("Please provide a file_path")
        
        if start_date is None:
            print("Please provide a start_date")

        if end_date is None:
            print("Please provide a end date")

        df = load_csv(file_path)
        
        if isinstance(df, pd.DataFrame):
            converted_df = convert_date(df, start_date, end_date, 'date_of_work')
            columns_list = ['date_of_work', 'user', 'timelog_note']
            formated_df = get_list_issues(converted_df, columns_list)
            print(formated_df)
            dataframe_to_csv(formated_df, output_path)
        
    except FileNotFoundError as e:
        print(f"Couldn't find file: {file_path}, {e}")
    except Exception as e:
        print(f"Couldn't create DataFrame for file: {file_path}")

def main():
    try:
        if len(sys.argv) < 3:
            print("Correct format: python3 gitlab_reporter.py --method_name <csv_file>")
            return

        file_path = sys.argv[2]
        
        if sys.argv[1] == "--list_empty_accounts" or sys.argv[1] == "--list_reported_time_per_account":
            if sys.argv[1] == "--list_empty_accounts":
                list_empty_accounts(file_path)
            else:
                list_reported_time_per_account(file_path)

        elif sys.argv[1] == "--list_issues":
            if "--start=" in sys.argv[2] and "--end=" in sys.argv[3]:
                start_date = sys.argv[2].split('=')[1]
                end_date = sys.argv[3].split('=')[1]
                file_path = sys.argv[4]
                output_path = sys.argv[5]
                list_issues(file_path, start_date, end_date, output_path)
                print(f"A CSV file called output.csv with the listed issues, has been created for file: {file_path} in the folder: {output_path}")
            else:
                print("Correct format: python3 gitlab_reporter.py --list_issues --start=<start_date> --end=<end_date> <csv_file> <output_path>")
                sys.exit(1)
            
        else:
            print("Correct format: python3 gitlab_reporter.py --method_name <csv_file>")
    except Exception as e:
        print(f"Please provide the correct amount of arguments, {e}")

if __name__ == "__main__":
    main()