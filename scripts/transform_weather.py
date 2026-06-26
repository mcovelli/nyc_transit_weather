import pandas as pd
import json
from prefect import task, flow
import os
import glob
from datetime import datetime
from zoneinfo import ZoneInfo

@task
def read_latest_bronze_weather_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    bronze_dir = os.path.join(base_dir, "..", "data_lakehouse", "bronze")
    search_pattern = os.path.join(bronze_dir, "weather_data_*.json")

    list_of_files = glob.glob(search_pattern)
    if not list_of_files:
        print("No weather data files found in the bronze directory.")
        return None
    
    latest_file = max(list_of_files, key=os.path.getctime)
    print(f"Latest weather data file found: {latest_file}")
    with open(latest_file, 'r') as f:
        read_data = json.load(f)
        if not read_data:
            print("No weather data found in the latest file.")
            return None
        else:
            return read_data

@task
def transform_weather_data():
    weather_data = read_latest_bronze_weather_data()
    if weather_data:
        # Convert the relevant part of the weather data to a DataFrame for analysis
        df = pd.DataFrame(weather_data.get("properties", {}).get("periods", [])) # Prevent KeyError if "properties" or "periods" is missing
        df = df.drop(['icon', 'temperatureTrend', 'number', 'name', 'dewpoint', 'relativeHumidity', 'windDirection'], errors='ignore')
        if 'startTime' in df.columns:
            df['startTime'] = pd.to_datetime(df['startTime'], errors='coerce')
        if 'endTime' in df.columns:
            df['endTime'] = pd.to_datetime(df['endTime'], errors='coerce')
        print(df.head())
        return df
    else:
        print("No weather data available to transform.")
        return None

@task  
def save_forecast_clean(df):

    # Generate a timestamped filename for the forecast data
    timestamp = datetime.now(tz=ZoneInfo('America/New_York')).strftime("%Y-%m-%d_%H-%M-%S")

    # Construct the full file path for saving the forecast data in the data lakehouse
    base_dir = os.path.abspath(os.path.dirname(__file__))
    filename = f"weather_data_{timestamp}.csv"
    silver_dir = os.path.join(base_dir, "..", "data_lakehouse", "silver")
    output_file_path = os.path.join(silver_dir, filename)

    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    
    # Save the forecast data to the specified file path
    df.to_csv(output_file_path, index=False)
    print(f"Transformed weather data saved to {filename}")

@flow
def weather_silver_pipeline():

    #Fetch grid points URL
    df = transform_weather_data()
    if df is not None:
        save_forecast_clean(df)
    else:
        print("Pipeline Stopped. Could not retrieve or transform weather data.")

if __name__ == "__main__":
    weather_silver_pipeline()
