#!/usr/bin/env python3

import pandas as pd
import numpy as np
import argparse
import sys

##Ladda in CSV fil.
def load_csv(file_path):
    try:
        return pd.read_csv(open(file_path, 'rb'), index_col=False)
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
        data = {'Sum - Time Spent': ts, 'User': user}
        return pd.DataFrame(data=data)
    except:
        print("Kunde inte skapa tidrapport")

##Formatering av kolumner i ny dataFrame.
def user_dataframe(rs):
    try:
        summary = rs.groupby('User')['Sum - Time Spent'].agg(['sum']).reset_index()
        summary.rename(columns={'sum': 'Sum - Time Spent'}, inplace=True)
        return summary
    except:
        print("Kunde inte skapa en sorterad lista med användare och tidrapporter.")

##Lägg till det totala resultet i DataFrame
def total_dataframe(summary):
    try:
        total_sum = summary['Sum - Time Spent'].sum()
        total_row = pd.DataFrame({'User': ['Total Result'], 'Sum - Time Spent': [total_sum]})
        return pd.concat([summary, total_row], ignore_index=True)
    except:
        print("Kunde inte skapa sorterad lista med den totala tiden.")

##Beräkna debiterings grad
def calculate_profit_margin():
    pass

##Skapande av lista med all information.
def calculate_user_time(df):
    try:
        nv = find_null_values_in_account_label(df=df)
        tn = get_time_for_null_values(nv=nv)
        user = get_user_for_null_values(nv=nv)
        return create_list_of_time_reports(tn, user)
    except:
        return pd.DataFrame(columns=['User', 'Sum Time - Spent'])

##Skapande av lista med den totala tiden inkluderad.
def calculate_final(result_df):
    try:
        summary = user_dataframe(result_df)
        total_df = total_dataframe(summary=summary)
        return(total_df)
    except:
        print("Kunde inte skapa en lista med total tid.")

def main():
    try:
        parser = argparse.ArgumentParser(description="Gitlab Reporter")
        parser.add_argument('csv', help="CSV file path")

        args = parser.parse_args()
        file_path = args.csv

        df = load_csv(file_path=file_path)
        result_df = calculate_user_time(df=df)
        total_df = calculate_final(result_df)
        print(total_df)
    except:
        print("Kunde inte starta programmet.")
    
if __name__ == "__main__":
    main()