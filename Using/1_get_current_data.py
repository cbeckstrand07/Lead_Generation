import pandas as pd
import os
from datetime import date
from lookups import COMPANY_ALTERNATE_NAMES
from datetime import datetime, timedelta
from clean_lookups import drop_lookup_values, lookup_list

def lead_id(df) -> pd.DataFrame:
    # Add a column 'Lead ID' that adds a unique lead id based on some columns
    df['lead id'] = (
            df['company'] +
            df['consignee'] +
            df['consignee state'] +
            df['consignee city'] +
            df['consignee zip'].astype(str) +
            df['mode'] +
            df['sales rep']
    )

    # Remove spaces from 'Lead ID' column
    df['lead id'] = df['lead id'].str.replace(' ', '')

    return df


def fix_main_data() -> pd.DataFrame:
    print("Loading Flat World Database...")
    data = pd.read_csv(f'/Users/carlybackstrand/Desktop/Flat World Dashboard Database.csv')
    # Convert the 'Date Generated' column to datetime
    data['Date Generated'] = pd.to_datetime(data['Date Generated'])

    # Filter data for when 'Date Generated' is greater than 'date'
    # data = data[data['Date Generated'] < '2024-01-01']

    print("DATA\n", data)

    # Subset the df down to just the specified columns
    data = data[['Bill of Lading','Company', 'Consignee', 'Consignee State', 'Date Generated',
                 'Consignee City', 'Consignee Zip', 'Mode']]
    
    print("Data Shipment Count: ", len(data))

    # Call the Sales function that creates the sales table with sales rep and company
    df_sales = concatenate_csv_files(folder_path)

    # Save the sales data to sales dictionary with all the companies and their corresponding sales rep
    df_sales.to_csv(f'{file_path}Sales_Dictionary.csv', index=False)

    # merge the sales reps with the companies to create the df_merged df
    df_merged = data.merge(df_sales, on='Company', how='left')
    
    print("Merged Shipment Count 1: ", len(df_merged))

    # Subset the df down to just the specified columns
    df = df_merged[['Bill of Lading', 'Company', 'Consignee', 'Consignee State', 'Date Generated',
                 'Consignee City', 'Consignee Zip', 'Mode', 'Sales Rep']]

    print("Merged Shipment Count 2: ", len(df))

    # Fill missing values in 'Sales Rep' column with 'Unassigned'
    df['Sales Rep'].fillna('Unassigned', inplace=True)

    # Clean df
    col_list = ['Company', 'Consignee', 'Consignee State', 'Consignee City', 'Mode', 'Sales Rep']
    df = clean_columns(df, col_list)
    print("Cleaned Shipment Count: ", len(df))

    # print("Saving df...")
    # df.to_csv(f'{file_path}df.csv', index=False)

    return df

def concatenate_csv_files(folder_path):
    # Get the list of file names in the folder
    file_names = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

    # List to store DataFrames
    dfs = []

    # Loop through the file names, read CSV files, and append them to the list
    for file_name in file_names:
        file_path = os.path.join(folder_path, file_name)
        df = pd.read_csv(file_path)
        dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    df_sales = pd.concat(dfs, ignore_index=True)

    df_sales['Sales Rep'] = df_sales['Sales Rep'].str.replace('Dave Helm', 'David Helm')

    new_rows = [
        {'Sales Rep': 'David Helm', 'Company': 'Herff Jones Arcola'},
        {'Sales Rep': 'David Helm', 'Company': 'Herff Jones Champaign'},
        {'Sales Rep': 'Charlie Conners', 'Company': 'BGR'},
        {'Sales Rep': 'Charlie Conners', 'Company': 'BGR Label Printing'},
        {'Sales Rep': 'House', 'Company': 'Clover Imaging Group OTR'}
    ]

    print(type(df_sales))

    # Append the new rows to the df_sales DataFrame
    df_sales = pd.concat([df_sales, pd.DataFrame(new_rows)], ignore_index=True)

    # # Append the new rows to the df_sales DataFrame
    # df_sales = df_sales.append(new_rows, ignore_index=True)

    # Iterate through the 'Company' column in df_sales
    for main_name, alternate_names in COMPANY_ALTERNATE_NAMES.items():
        for alternate_name in alternate_names:
            df_sales['Company'] = df_sales['Company'].replace(alternate_name, main_name)

    df_sales = df_sales[['Sales Rep', 'Company']]
    
    # Remove duplicates from the concatenated DataFrame
    df_sales.drop_duplicates(inplace=True)

    return df_sales

def clean_columns(df, col_list) -> pd.DataFrame:
    # Clean column values
    for column in col_list:
        df[column] = df[column].str.strip().str.lower()
        replacements = {
            '  ': ' ','√Ç‚Ç¨¬¢': '', '*': '', '!': '', ' - ': '-', ' -': '-', ',': '',
            '.': '', '/': '', '\\': '', "'": '', 'inc': '', 'llc': '', ' corporation': ' corp',
            ' company': ' co', 'world wide': 'worldwide', 'university': 'univ',
            ' sales': ' sale', ' centre': ' center', ' communcations': ' communications',
            ' salt lake city': ' salt lake', '(': '', ')': '', 'states': 'state',
            'trucks': 'truck', 'division': 'div', 'hj-herff': 'herff', '%': '', '#': '', ']': '',
            '[':'', '--': '', ' co ': ''
        }
        for old_str, new_str in replacements.items():
            df[column] = df[column].str.replace(old_str, new_str)

        df[column] = df[column].str.strip().str.lower()

    # Remove rows without a consignee
    df = df[df['Consignee'].notnull()]

    # Remove rows with consignees that are all numbers or symbols
    df = df[~df['Consignee'].str.match(r'^[0-9!@#$%^&*()_+{}\[\]:;"\'<>,.?/\|\\-]+$"')]

    # Drop lookup values from specific columns
    for column in col_list:
        df = drop_lookup_values(df, column)  # Call the drop_lookup_values function

    # Convert column headers to lowercase
    df.columns = df.columns.str.lower()

    return df

def add_sales_rep_states(df, file_path) -> pd.DataFrame:
    df_state = pd.read_csv(f'{file_path}State_Assignments.csv')
    print("DF STATE: ")
    print(df_state)
    
    # Merge data frames based on 'consignee state' and 'State'
    merged_df = pd.merge(df, df_state, how='left', left_on='consignee state', right_on='State Abbreviation')
    
    # Drop the redundant 'State' column
    merged_df.drop('State Abbreviation', axis=1, inplace=True)
    
    # Display the updated data frame
    print(merged_df)

    return merged_df
   

def main():
    print('---------------------------------------------------------')
    print('-----             Monthly Lead Generation           -----')
    print('---------------------------------------------------------')
    
    df = fix_main_data()

    # Call function that adds the Lead ID
    df = lead_id(df)

    # Add the State for each sales rep
    merged_df = add_sales_rep_states(df, file_path)
    print("Add State Shipment Count: ", len(merged_df))

    print("Processing completed.")
    merged_df.to_csv(f'{file_path}total_current_data.csv', index=False)

if __name__ == '__main__':
    folder_path = '/Users/carlybackstrand/Desktop/GeneralTools/Sales_Reps_Dictionaries/'
    file_path = '/Users/carlybackstrand/Desktop/HomeBase/Projects/Lead_Generation/'
    main()