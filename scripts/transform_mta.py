import pandas as pd
import json
from prefect import task, flow
import os
import glob
from datetime import datetime, timedelta
import re
from zoneinfo import ZoneInfo

@task
def read_latest_bronze_mta_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    bronze_dir = os.path.join(base_dir, "..", "data_lakehouse", "bronze")
    search_pattern = os.path.join(bronze_dir, "mta_alerts_*.json")

    list_of_files = glob.glob(search_pattern)
    if not list_of_files:
        print("No MTA data files found in the bronze directory.")
        return None
    
    latest_file = max(list_of_files, key=os.path.getctime)
    print(f"Latest MTA data file found: {latest_file}")
    with open(latest_file, 'r') as f:
        read_data = json.load(f)
        if not read_data:
            print("No MTA data found in the latest file.")
            return None
        else:
            return read_data

@task
def transform_mta_data():
    mta_data = read_latest_bronze_mta_data()
    all_alerts = []

    # Convert the relevant part of the MTA data to a DataFrame for analysis
    for alert in mta_data.get("entity", []):

        # Extract the header text from the MTA data, handling potential missing fields gracefully
        header_obj = alert.get("alert", {}).get("headerText", {}).get("translation", [])
        header_text = header_obj[0].get("text", "") if header_obj else ""

        # Extract the description text from the MTA data, handling potential missing fields gracefully
        desc_obj = alert.get("alert", {}).get("descriptionText", {}).get("translation", [])
        desc_text = desc_obj[0].get("text", "") if desc_obj else ""

        # Extract the "id" field from the MTA data, defaulting to an empty string if not found, to use as a key for merging informedEntity and activePeriod DataFrames
        id = alert.get("id", "") # Get the "id" field from the MTA data, default to an empty string if not found
        
        # Extract the informedEntity and activePeriod data from the MTA data, converting them to DataFrames for merging, and handling potential missing fields gracefully
        informedEntity = pd.DataFrame(alert.get("alert", {}).get("informedEntity", []))
        activePeriod = pd.DataFrame(alert.get("alert", {}).get("activePeriod", []))

        for col in ['start', 'end']:
            if col not in activePeriod.columns:
                activePeriod[col] = None

        # Add the "id" field to both DataFrames to use as a key for merging
        informedEntity['entity_id'] = id
        activePeriod['entity_id'] = id

        # Merge the informedEntity and activePeriod DataFrames on the "id" field, using an inner join to ensure we only keep rows that have matching "id" values in both DataFrames, and handle potential missing fields gracefully
        merged_df = pd.merge(informedEntity, activePeriod, on='entity_id', how='left')

        # Add the extracted header text and description text as new columns in the merged DataFrame, ensuring that we handle potential missing fields gracefully
        merged_df['header_text'] = header_text
        merged_df['description_text'] = desc_text

        # Convert the "start" and "end" columns from Unix timestamps to datetime format, handling potential missing fields gracefully and ensuring that any invalid timestamps are handled without causing errors
        if 'start' in merged_df.columns and 'end' in merged_df.columns:
            merged_df['start'] = pd.to_numeric(merged_df['start'], errors="coerce") # Force to numeric first to clear the object-type trap
            merged_df['start'] = pd.to_datetime(merged_df['start'], unit='s', errors="coerce") # Handle potential invalid timestamps gracefully

            merged_df['end'] = pd.to_numeric(merged_df['end'], errors="coerce") # Force to numeric first to clear the object-type trap 
            merged_df['end'] = pd.to_datetime(merged_df['end'], unit='s', errors="coerce") # Handle potential invalid timestamps gracefully
        elif 'start' in merged_df.columns:
            merged_df['start'] = pd.to_datetime(merged_df['start'], unit='s', errors="coerce") # Handle potential invalid timestamps gracefully
        elif 'end' in merged_df.columns:
            merged_df['end'] = pd.to_datetime(merged_df['end'], unit='s', errors="coerce") # Handle potential invalid timestamps gracefully

        # Check if there are any null values in the "routeId" column of the merged DataFrame, and if so, attempt to extract a route ID from the header text using a regular expression pattern that matches typical route ID formats, and fill in the "routeId" column for the rows where it is null with the extracted route ID or "SYSTEM_WIDE" if no route ID is found, ensuring that we handle potential missing fields gracefully and provide informative output if no route ID is found in the header text
        if merged_df['routeId'].isnull().any():
            mask = merged_df['routeId'].isnull() # Create a mask to identify rows where "routeId" is null
            pattern = r'\[([1-7A-Z]+)\]'  # Pattern to match route IDs that are typically alphanumeric and may include numbers and letters
            
            # Use the regular expression pattern to search for a route ID in the header text, and if a match is found, extract the route ID and fill in the "routeId" column for the rows where it is null, ensuring that we handle potential missing fields gracefully and provide informative output if no route ID is found in the header text
            match = re.search(pattern, header_text)
            if match:
                extracted_route_id = match.group(1) # Extract the route ID from the header text using the regular expression pattern
                merged_df.loc[mask, 'routeId'] = extracted_route_id # Fill in the "routeId" column for the rows where it is null with the extracted route ID, ensuring that we handle potential missing fields gracefully
            else:
                merged_df.loc[mask, 'routeId'] = "SYSTEM_WIDE" # If no route ID is found in the header text, fill in the "routeId" column for the rows where it is null with "SYSTEM_WIDE", ensuring that we handle potential missing fields gracefully and provide informative output

        # Fill any missing values in the "routeId" column with "System Wide" to indicate that the alert applies to the entire system, ensuring that we handle potential missing fields gracefully
        merged_df['routeId'] = merged_df['routeId'].fillna("System Wide")

        # Append the merged DataFrame for the current alert to the all_alerts list, ensuring that we only append valid DataFrames that contain data and handle potential issues gracefully
        if not merged_df.empty:
            all_alerts.append(merged_df)

    # After processing all alerts, concatenate the individual DataFrames in the all_alerts list into a single DataFrame, ensuring that we handle the case where no valid alert pairs were found to combine
    if all_alerts:
        final_df = pd.concat(all_alerts, ignore_index=True)
        return final_df
    else:
        print("No valid alert pairs were found to combine.")
        return None

@task  
def save_mta_clean(final_df):

    # Generate a timestamped filename for the forecast data
    timestamp = datetime.now(tz=ZoneInfo('America/New_York')).strftime("%Y-%m-%d_%H-%M-%S")

    # Construct the full file path for saving the forecast data in the data lakehouse
    base_dir = os.path.abspath(os.path.dirname(__file__))
    filename = f"mta_alerts_{timestamp}.csv"
    silver_dir = os.path.join(base_dir, "..", "data_lakehouse", "silver")
    output_file_path = os.path.join(silver_dir, filename)

    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    
    # Save the mta data to the specified file path
    final_df.to_csv(output_file_path, index=False)
    print(f"Transformed mta data saved to {filename}")

@flow
def mta_silver_pipeline():

    final_df = transform_mta_data()

    if final_df is not None:

        # clean schema and drop duplicate rows
        final_df = final_df.drop(columns=['stopId'], errors='ignore').drop_duplicates()
        save_mta_clean(final_df)
    else:
        print("Pipeline Stopped. Could not retrieve or transform mta data.")

if __name__ == "__main__":
    mta_silver_pipeline()

