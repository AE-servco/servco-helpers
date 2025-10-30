import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from google.auth import default

def get_sheets_data_date_filtered(spreadsheet_name, worksheet_name, date_in):
    # Set up the connection
    creds, _ = default(scopes=[
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ])
    
    client = gspread.authorize(creds)

    # Open the Google Sheet
    spreadsheet = client.open(spreadsheet_name)
    worksheet = spreadsheet.worksheet(worksheet_name)  # or use .get_worksheet(0)

    # Get data as a DataFrame
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    df['ads_date'] = pd.to_datetime(df['ads_date'])
    filtered_df = df[df['ads_date'].dt.date == date_in]
    return filtered_df

def reformat_sheets_dict(data):
    output = {}
    for k, v in data.items():
        if 'state' in v:
            state_key = 'state'
        else:
            state_key = 'ads_state'
        if v[state_key]:
            output[v[state_key]] = v
            del output[v[state_key]][state_key]
    
    return output