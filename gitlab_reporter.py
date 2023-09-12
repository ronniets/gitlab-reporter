#!/usr/bin/python3

import pandas as pd
import sys

##Ladda in CSV fil.
def load_csv(file_path):
    try:
        return pd.read_csv(open(file_path, 'rb'))
    except FileNotFoundError:
        print("Filen existerar inte.")
        sys.exit(1)

##Hitta alla värden i 'account_label' som är null.
def find_null_values_in_account_label(df):
    try:
        return df[df['account_label'].isnull()]
    except:
        print("Det finns inga tomma kolumner.")

##Få rapporterad tid för alla null värden i 'account_label'.
def get_time_for_null_values(nv):
    try:
        return(nv['time_spent (hours)'])
    except:
        print("Det finns ingen rapporterad tid.")

##Få alla användare för alla null värden i 'account_label'.
def get_user_for_null_values(nv):
    try:
        return(nv['user'])
    except:
        print("Det finns inga användare som tidrapporterat.")

##Skapa en DataFrame av rapporterad tid för respektive användare.
def create_list_of_time_reports(ts, user):
    try:
        data = {'Time Spent': ts, 'User': user}
        return pd.DataFrame(data=data)
    except:
        print("Kunde inte skapa tidrapport")

##Beräkning av den totala summan av den rapporterade tiden.
def total_sum(df):
    try:
        total = df['Time Spent'].sum()
        return total
    except:
        print("Kunde inte summera tider.")

##Formatering av kolumner i ny dataFrame.
def user_dataframe(rs):
    try:
        df = rs.groupby('User')['Time Spent'].agg(['sum']).reset_index()
        df.rename(columns={'sum': 'Time Spent'}, inplace=True)
        return df
    except:
        print("Kunde inte skapa en sorterad lista med användare och tidrapporter.")

##Lägg till det totala resultet i DataFrame
def total_dataframe(total, df):
    try:
        total_row = pd.DataFrame({'User': ['Total Result'], 'Time Spent': [total]})
        return pd.concat([df, total_row], ignore_index=True)
    except:
        print("Kunde inte skapa sorterad lista med den totala tiden.")

##Beräkna debiteringsgrad
def calculate_profit_margin():
    pass

##Skapande av lista med all information.
def calculate_user_time(df):
    try:
        nv = find_null_values_in_account_label(df)
        tn = get_time_for_null_values(nv)
        user = get_user_for_null_values(nv)
        return create_list_of_time_reports(tn, user)
    except:
        return pd.DataFrame(columns=['User', 'Time Spent'])

##Skapande av lista med den totala tiden inkluderad.
def calculate_final(result_df):
    try:
        df = user_dataframe(result_df)
        total = total_sum(result_df)
        total_df = total_dataframe(total, df)
        return(total_df)
    except:
        print("Kunde inte skapa en lista med total tid.")

##Printar ut DataFrame utan Index värden
def print_df(df):
    print(df.to_string(index=False))

##Lista tomma konton
def list_empty_accounts(file_path):
    try:
        df = load_csv(file_path)
        result_df = calculate_user_time(df)
        total_df = calculate_final(result_df)
        print_df(total_df)

    except:
        print("Kunde inte starta programmet.")
        sys.exit(1)

def main():
    if len(sys.argv) != 3:
        print("Ange: python3 gitlab_reporter.py --list_empty_accounts <csv_file>")
    else:
        if sys.argv[1] == "--list_empty_accounts":
            file_path = sys.argv[2]
            list_empty_accounts(file_path)
        else:
            print("Ange: python3 gitlab_reporter.py --list_empty_accounts <csv_file>")
    
if __name__ == "__main__":
    main()