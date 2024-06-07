# Imports
import pandas as pd
import os
from datetime import datetime


# New Lead function
def new_lead(df, current_data_path) -> pd.DataFrame:
    # Read in the Current data
    df_current = pd.read_csv(current_data_path)
    
    # Subset for only the lead id column
    df_current = df_current[['lead id']]

    # Find new Lead IDs that are not in the existing lead list
    new_leads = df_current[~df_current['lead id'].isin(df['lead id'])].copy()

    # Add today's date to the 'Datestamp' column for new leads
    new_leads['datestamp'] = datetime.now().strftime('%Y-%m-%d')
    #new_leads['datestamp'] = '2023-10-01'

    # Append new leads to the existing lead list
    df_new_lead = pd.concat([df, new_leads], ignore_index=True)

    # Remove duplicate lead IDs
    df_new_lead.drop_duplicates(subset='lead id', inplace=True)

    # Remove rows without a consignee
    df_new_lead = df_new_lead[df_new_lead['lead id'].notnull()]

    return df_new_lead


def main():
    file_path = '/Users/carlybackstrand/Desktop/HomeBase/Projects/Lead_Generation/'
    lead_list_path = os.path.join(file_path, 'total_lead_list.csv')
    current_data_path = os.path.join(file_path, 'total_current_data.csv')

    # Read the existing lead list
    df = pd.read_csv(lead_list_path)

    # Call function that appends any new leads to the current list
    df_new_lead = new_lead(df, current_data_path)

    # Save updated new lead csv
    df_new_lead.to_csv(lead_list_path, index=False)


if __name__ == '__main__':
    main()
