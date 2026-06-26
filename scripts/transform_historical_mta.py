import pandas as pd
import json
from prefect import task, flow
import os
import glob
from datetime import datetime
from zoneinfo import ZoneInfo

@task
def read_latest_historical_bronze_mta_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    bronze_dir = os.path.join(base_dir, "..", "data_lakehouse", "bronze")
    search_pattern = os.path.join(bronze_dir, "historical_mta_data_*.json")

    list_of_files = glob.glob(search_pattern)
    if not list_of_files:
        print("No Historical MTA data files found in the bronze directory.")
        return None
    
    latest_file = max(list_of_files, key=os.path.getctime)
    print(f"Latest Historical MTA data file found: {latest_file}")
    with open(latest_file, 'r') as f:
        read_data = json.load(f)
        if not read_data:
            print("No Historical MTA data found in the latest file.")
            return None
        else:
            return pd.DataFrame(read_data)

@task
def transform_historical_mta_data(mta_data):

    if mta_data is None or mta_data.empty:
        return pd.DataFrame()
    
    # Rename columns to match the Live MTA Data Contract
    clean_hist = mta_data.rename(columns={
        'status_id': 'entity_id',
        'date': 'start',
        'affected': 'routeId',
        'header': 'header_text',
        'description': 'description_text'
    })

    # Add UTC timezone awareness to start time
    if 'start' in clean_hist.columns:
        clean_hist['start'] = pd.to_datetime(clean_hist['start'], errors='coerce', utc=True)

    # Add the missing 'end' column so the schemas match exactly
    if 'end' not in clean_hist.columns:
        clean_hist['end'] = clean_hist['start'] + pd.Timedelta(hours=1)

    # Clean and explode the 'routeId' (Handling things like "L | N | Q")
    clean_hist['routeId'] = clean_hist['routeId'].fillna('Unknown')

    #Clean and split affected into lists
    clean_hist['routeId'] = clean_hist['routeId'].astype(str).str.split(" | ", regex=False)
    clean_hist = clean_hist.explode('routeId')
    
    # Clean up whitespace and drop any accidental empty strings
    clean_hist['routeId'] = clean_hist['routeId'].str.strip()
    clean_hist = clean_hist[clean_hist['routeId'] != '']

    # Enforce the final required column order
    required_columns = ['entity_id', 'routeId', 'start', 'end', 'header_text', 'description_text']
    
    # Return only the columns we need, dropping 'agency' and 'status_label'
    return clean_hist[required_columns]

@task  
def save_historical_mta_clean(final_df):

    # Generate a timestamped filename for the forecast data
    timestamp = datetime.now(tz=ZoneInfo('America/New_York')).strftime("%Y-%m-%d_%H-%M-%S")

    # Construct the full file path for saving the forecast data in the data lakehouse
    base_dir = os.path.abspath(os.path.dirname(__file__))
    filename = f"historical_mta_data_{timestamp}.csv"
    silver_dir = os.path.join(base_dir, "..", "data_lakehouse", "silver")
    output_file_path = os.path.join(silver_dir, filename)

    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    
    # Save the mta data to the specified file path
    final_df.to_csv(output_file_path, index=False)
    print(f"Transformed historical mta data saved to {filename}")

@flow
def historical_mta_silver_pipeline():

    raw_df = read_latest_historical_bronze_mta_data()

    if raw_df is not None and not raw_df.empty:
        final_df = transform_historical_mta_data(raw_df)
        # clean schema and drop duplicate rows
        if not final_df.empty:
            final_df = final_df.drop_duplicates()
            save_historical_mta_clean(final_df)
            print("✅ Historical Silver Pipeline completed successfully.")
        else:
            print("Transformation resulted in an empty DataFrame.")
    else:
        print("Pipeline Stopped. Could not retrieve or transform mta data.")

if __name__ == "__main__":
    historical_mta_silver_pipeline()
