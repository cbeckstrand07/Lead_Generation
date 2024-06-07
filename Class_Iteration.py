import pandas as pd
import os
from datetime import date
from lookups import COMPANY_ALTERNATE_NAMES
from datetime import datetime, timedelta
import re
from clean_lookups import drop_lookup_values

folder_path = '/Users/carlybackstrand/Desktop/GeneralTools/Sales_Reps_Dictionaries/'
file_path = '/Users/carlybackstrand/Desktop/HomeBase/Projects/Lead_Generation/'

def fix_main_data() -> pd.DataFrame:
    print("Loading Flat World Database...")
    data = pd.read_csv(f'/Users/carlybackstrand/Desktop/Flat World Dashboard Database.csv')

    # Subset the df down to just the specified columns
    data = data[['Company', 'Shipper', 'Consignee', 'Shipper State', 'Consignee State', 'Date Generated',
                 'Consignee City', 'Consignee Zip', 'Shipper City', 'Shipper Zip', 'Mode']]

    # Call the Sales function that creates the sales table with sales rep and company
    df_sales = concatenate_csv_files(folder_path)

    # Save the sales data to sales dictionary with all the companies and their corresponding sales rep
    df_sales.to_csv(f'{file_path}Sales_Dictionary.csv', index=False)

    # merge the sales reps with the companies to create the df_merged df
    df_merged = data.merge(df_sales, on='Company', how='left')

    # Subset the df down to just the specified columns
    df = df_merged[['Company', 'Shipper', 'Consignee', 'Shipper State', 'Consignee State', 'Date Generated',
                    'Consignee City', 'Consignee Zip', 'Shipper City', 'Shipper Zip', 'Mode', 'Sales Rep']]

    # Fill missing values in 'Sales Rep' column with 'Unassigned'
    df['Sales Rep'].fillna('Unassigned', inplace=True)

    # Clean df
    col_list = ['Consignee', 'Shipper', 'Shipper City', 'Consignee City']
    df = clean_columns(df, col_list)

    print("Saving df...")
    df.to_csv(f'{file_path}df.csv', index=False)

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

    # Append the new rows to the df_sales DataFrame
    df_sales = df_sales.append(new_rows, ignore_index=True)

    # Iterate through the 'Company' column in df_sales
    for main_name, alternate_names in COMPANY_ALTERNATE_NAMES.items():
        for alternate_name in alternate_names:
            df_sales['Company'] = df_sales['Company'].replace(alternate_name, main_name)

    # Remove duplicates from the concatenated DataFrame
    df_sales.drop_duplicates(inplace=True)

    df_sales = df_sales[['Sales Rep', 'Company']]

    return df_sales

def clean_columns(df, col_list) -> pd.DataFrame:
    # Clean column values
    for column in col_list:
        df[column] = df[column].str.strip().str.lower()
        replacements = {
            '  ': ' ','√Ç‚Ç¨¬¢': '', '*': '', '!': '', ' - ': '-', ' -': '-', ',': '',
            '.': '', '/': '', '\\': '', "'": '', 'inc': '', 'llc': '', ' corporation': ' corp',
            ' company': ' co', ' co': '', 'world wide': 'worldwide', 'university': 'univ',
            ' sales': ' sale', ' centre': ' center', ' communcations': ' communications',
            ' salt lake city': ' salt lake', '(': '', ')': '', 'states': 'state',
            'trucks': 'truck', 'division': 'div', 'hj-herff': 'herff', '%': '', '#': '', ']': '',
            '[':'', '--': ''
        }
        for old_str, new_str in replacements.items():
            df[column] = df[column].str.replace(old_str, new_str)
        df[column] = df[column].str.strip().str.lower()

        # # Remove leading numbers/letters (starting with '00-' and at least 6 characters after that) in the Consignee column
        # df[column] = df[column].str.replace(r'^00-[A-Za-z0-9]\s*', '', regex=True)

        # Remove rows where Consignee contains only numbers or symbols
        pattern = r'^[0-9!@#$%^&*()_+{}\[\]:;"\'<>,.?/\|\\-]+$"'
        df = df[~df[column].str.contains(pattern, regex=True, na=False)]

        # Drop rows containing lookup values
        df = drop_lookup_values(df, column)

    return df

def starting_df(current_date, df):
    print("Compiling Existing Customers...")

    # Convert 'Date Generated' column to pandas Timestamp objects
    df['Date Generated'] = pd.to_datetime(df['Date Generated'])

    twenty_days_ago = current_date - timedelta(days=20)

    ex_cust_df = df[(df['Date Generated'] < twenty_days_ago)]

    ex_cust_df['Count'] = 1
    ex_cust_df['New Customer Date'] = None

    ex_cust_df = ex_cust_df[['Company', 'Shipper', 'Consignee', 'Shipper State', 'Consignee State','Consignee City',
             'Consignee Zip', 'Shipper City', 'Shipper Zip', 'Mode', 'Sales Rep', 'Count', 'New Customer Date']]

    print("Saving starting_df!\n", ex_cust_df)

    ex_cust_df.to_csv(f'{file_path}ex_cust_df.csv', index=False)

def newest_existing_customers(current_date, df):
    print("Compiling Newest Customers...")

    # Convert 'Date Generated' column to pandas Timestamp objects
    df['Date Generated'] = pd.to_datetime(df['Date Generated'])

    df = df[(df['Date Generated'] <= current_date)]
    print("df in new customer function\n", df)

    # Subset the df down to just the specified columns
    new_cust_df = df[['Company', 'Shipper', 'Consignee', 'Shipper State', 'Consignee State', 'Consignee City',
             'Consignee Zip', 'Shipper City', 'Shipper Zip', 'Mode', 'Sales Rep']]

    new_cust_df['Count'] = 1
    new_cust_df['New Customer Date'] = None

    new_cust_df = new_cust_df[['Company', 'Shipper', 'Consignee', 'Shipper State', 'Consignee State','Consignee City',
             'Consignee Zip', 'Shipper City', 'Shipper Zip', 'Mode', 'Sales Rep', 'Count', 'New Customer Date']]

    new_cust_df.to_csv(f'{file_path}new_cust_df.csv', index=False)
    print("new_cust_df in function\n", new_cust_df)


def compare_cust_df(ex_cust_df, new_cust_df) -> pd.DataFrame:
    # Convert dataframes to lists of customer objects
    ex_customers = [customer(*row) for row in ex_cust_df.values]
    new_customers = [customer(*row) for row in new_cust_df.values]

    # Create a set of unique customer attributes from ex_cust_df
    ex_customer_attributes = {(customer.company, customer.shipper, customer.consignee, customer.shipper_state,
                               customer.consignee_state, customer.consignee_city, customer.consignee_zip, customer.shipper_city,
                               customer.shipper_zip, customer.mode, customer.sales_rep)
                              for customer in ex_customers}

    # Create a list of customer statuses for each customer in new_customers
    customer_status = [(customer.company, customer.shipper, customer.consignee, customer.shipper_state,
                        customer.consignee_state, customer.consignee_city, customer.consignee_zip,
                        customer.shipper_city, customer.shipper_zip, customer.mode, customer.sales_rep)
                       not in ex_customer_attributes for customer in new_customers]

    # Add the 'Customer Status' column to new_cust_df
    new_cust_df['Customer Status'] = customer_status

    print("During comparison\n", new_cust_df)

    # new_cust_df['Customer Status'] = new_cust_df['Customer Status'].map({False: 0, True: 1})

    new_cust_df = new_cust_df[['Company', 'Shipper', 'Consignee', 'Shipper State', 'Consignee State','Consignee City',
             'Consignee Zip', 'Shipper City', 'Shipper Zip', 'Mode', 'Sales Rep', 'Count', 'New Customer Date', 'Customer Status']]

    print("after comparison\n", new_cust_df)

    true_count = new_cust_df['Customer Status'].sum()
    print("Number of True values in 'Customer Status' column:", true_count)

    new_cust_df.to_csv(f'{file_path}comparison_df.csv', index=False)
    return new_cust_df

class customer:
    def __init__(self, Company, Shipper, Consignee, Shipper_State, Consignee_State, Consignee_City,
               Consignee_Zip, Shipper_City, Shipper_Zip, Mode, Sales_Rep, Count, New_Customer_Date):
        self.company = Company
        self.shipper = Shipper
        self.consignee = Consignee
        self.shipper_state = Shipper_State
        self.consignee_state = Consignee_State
        self.consignee_city = Consignee_City
        self.consignee_zip = Consignee_Zip
        self.shipper_city = Shipper_City
        self.shipper_zip = Shipper_Zip
        self.mode = Mode
        self.sales_rep = Sales_Rep
        self.count = Count
        self.new_customer_Date = New_Customer_Date


def save_ex_cust_df(ex_cust_df, current_date):
    # Convert the date to a string in the format YYYY-MM-DD
    date_str = date.strftime('%Y-%m-%d')

    # Save the 'ex_cust_df' DataFrame as a CSV file with the generated filename
    ex_cust_df.to_csv(f'{file_path}ex_cust_df_{current_date_str}.csv', index=False)

    print(f"ex_cust_df saved for date {date_str} as ex_cust_df_{current_date_str}.csv")


####################################################################################


def main():
    print("Reading in Data...")
    print('---------------------------------------------------------')
    print('-----             Monthly Lead Generation           -----')
    print('---------------------------------------------------------')
    df = fix_main_data()

    # Get input start_date from the user
    start_date_str = input("Please enter the start date (YYYY-MM-DD): ")

    # Convert start_date to a datetime object
    start_date = pd.to_datetime(start_date_str)

    # Loop through each day from the start_date until today
    today = pd.to_datetime('2023-07-01')
    # today = pd.to_datetime('today')
    current_date = start_date

    # existing_customers(current_date, df)
    starting_df(current_date, df)


    while current_date <= today:
        print(f"Processing data for {current_date.strftime('%Y-%m-%d')}...")

        # Get 'ex_cust_df' for the original data
        ex_cust_df = pd.read_csv(f'{file_path}ex_cust_df.csv')

        # Get 'new_cust_df' for the current date
        newest_existing_customers(current_date, df)

        new_cust_df = pd.read_csv(f'{file_path}new_cust_df.csv')
        print("new_cust_df.csv in while loop\n", new_cust_df)

        # Compare 'ex_cust_df' and 'new_cust_df' for the current date
        new_cust_df = compare_cust_df(ex_cust_df, new_cust_df)

        new_cust_df = new_cust_df.reset_index(drop=True)

        # Assuming 'Customer Status' column contains string representations of True and False
        new_cust_df['Customer Status'] = new_cust_df['Customer Status'].astype(bool)

        # Set 'New Customer Date' to the current date for new customers
        new_cust_df.loc[new_cust_df['Customer Status'], 'New Customer Date'] = current_date

        # Order the DataFrame by 'New Customer Date' column in descending order
        new_cust_df= new_cust_df.sort_values(by='New Customer Date', ascending=False)

        new_cust_df.to_csv(f'{file_path}CustomerStatus_new_cust_df.csv', index=False)

        # Step 1: Filter the DataFrame to get only rows where 'Customer Status' is True
        filtered_df = new_cust_df[new_cust_df['Customer Status'] == True]

        # Step 2: Create a new DataFrame using the filtered rows (optional if you want a separate DataFrame)
        new_df = pd.DataFrame(filtered_df)

        # Step 3: Save the new DataFrame to a CSV file
        new_df.to_csv(f'{file_path}Only_TRUE_Customers.csv', index=False)

        print(new_cust_df['Customer Status'])
        # Assuming you have a DataFrame named new_cust_df with a column 'Customer Status'
        true_count = new_cust_df['Customer Status'].sum()
        print("Number of True values in 'Customer Status' column:", true_count)
        false_count = new_cust_df["Customer Status"].apply(lambda x: not x).sum()
        print("Number of False values in 'Customer Status' column:", false_count)
        # Assuming you have a DataFrame named new_cust_df with a column 'Customer Status'
        column_data_type = new_cust_df['Customer Status'].dtype
        print("Data type of 'Customer Status' column:", column_data_type)

        new_cust_df.drop('Customer Status', axis=1, inplace=True)

        new_df.drop('Customer Status', axis=1, inplace=True)

        # Concatenate the two dataframes vertically
        combined_df = pd.concat([ex_cust_df, new_df], ignore_index=True)

        # Sort the combined dataframe by "New Customer Date" in descending order
        combined_df.sort_values(by="New Customer Date", ascending=False, inplace=True)

        # Optional: Reset the index after sorting
        combined_df.reset_index(drop=True, inplace=True)


        # ex_cust_df = new_cust_df[
        #     ['Company', 'Shipper', 'Consignee', 'Shipper State', 'Consignee State', 'Consignee City',
        #      'Consignee Zip', 'Shipper City', 'Shipper Zip', 'Mode', 'Sales Rep', 'Count', 'New Customer Date']]

        # # Group by customer attributes and sum the 'Count' values
        # ex_cust_df = ex_cust_df.groupby(['Company', 'Shipper', 'Consignee', 'Shipper State',
        #                                   'Consignee State', 'Consignee City', 'Consignee Zip',
        #                                   'Shipper City', 'Shipper Zip', 'Mode', 'Sales Rep', 'New Customer Date'],
        #                                  as_index=False).agg({'Count': 'sum'}).reset_index()
        # print("After the grouping\n", ex_cust_df)

        # Save the updated 'ex_cust_df' for the current date
        save_ex_cust_df(ex_cust_df, current_date)

        # # Convert 'New Customer Date' column to datetime type
        # ex_cust_df['New Customer Date'] = pd.to_datetime(new_cust_df['New Customer Date'])

        # Set 'ex_cust_df' to be the updated DataFrame for the next iteration
        ex_cust_df.to_csv(f'{file_path}ex_cust_df.csv', index=False)

        # Move to the next day
        current_date += pd.Timedelta(days=20)

    print("Processing completed.")


if __name__ == '__main__':
    main()

