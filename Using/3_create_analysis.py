import pandas as pd

def main():
    df_current_data = pd.read_csv(f'{file_path}total_current_data.csv')
    df_lead_list = pd.read_csv(f'{file_path}total_lead_list.csv')

    # Merge df_current_data with df_lead_list on 'lead id'
    merged_df = df_current_data.merge(df_lead_list, on='lead id', how='left')
    
    # Format Values
    merged_df['company'] = merged_df['company'].str.title()
    merged_df['consignee'] = merged_df['consignee'].str.title()
    merged_df['consignee state'] = merged_df['consignee state'].str.upper()
    merged_df['consignee city'] = merged_df['consignee city'].str.title()
    merged_df['mode'] = merged_df['mode'].str.upper()
    merged_df['sales rep'] = merged_df['sales rep'].str.title()
    merged_df.columns = [col.title() for col in merged_df.columns]
    
    # Create a Consignee Address
    merged_df['Consignee Zip'] = merged_df['Consignee Zip'].astype(str)
    merged_df['Consignee State'] = merged_df['Consignee State'].astype(str)
    merged_df['Consignee City'] = merged_df['Consignee City'].astype(str)
    merged_df['Consignee Address'] = merged_df['Consignee City'] + ', ' + merged_df['Consignee State'] + ', ' + merged_df['Consignee Zip']
    
    # # Keep only the most recent datestamp
    # merged_df['Datestamp'] = pd.to_datetime(merged_df['Datestamp'])
    # max_date = merged_df['Datestamp'].max()
    # df_max_date = merged_df[merged_df['Datestamp'] == max_date]
    # df_max_date.to_csv(f'{file_path}lead_analysis.csv', index=False)
    
    # Delete any blank rows
    merged_df = merged_df.dropna(subset=['Datestamp'])
    
    merged_df.to_csv(f'{file_path}total_lead_analysis.csv', index=False)

if __name__ == "__main__":
    file_path = '/Users/carlybackstrand/Desktop/HomeBase/Projects/Lead_Generation/'
    main()