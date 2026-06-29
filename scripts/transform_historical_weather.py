import pandas as pd
import json
from prefect import task, flow
import os
import glob
from datetime import datetime
from zoneinfo import ZoneInfo

@task
def read_latest_historical_bronze_weather():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    bronze_dir = os.path.join(base_dir, "..", "data_lakehouse", "bronze")
    files = glob.glob(os.path.join(bronze_dir, "historical_weather_*.json"))
    if not files: return None
    
    latest_file = max(files, key=os.path.getctime)
    with open(latest_file, 'r') as f:
        return json.load(f)

@task
def transform_historical_weather(raw_data):
    if not raw_data: return pd.DataFrame()
    
    # Open-Meteo stores hourly data in parallel lists; we need to zip them into a DataFrame
    hourly = raw_data.get('hourly', {})
    df = pd.DataFrame({
        'startTime': hourly.get('time', []),
        'temperature': hourly.get('temperature_2m', []),
        'precipitation': hourly.get('precipitation', []),
        'weather_code': hourly.get('weather_code', [])
    })
    
    # 1. Format timestamps and create an endTime (1 hour blocks)
    df['startTime'] = pd.to_datetime(df['startTime'], utc=True)
    df['endTime'] = df['startTime'] + pd.Timedelta(hours=1)

    # convert temp to fahrenheit and round to 2 decimal places
    df['temperature'] = round((df['temperature'] * 1.8) + 32, 2)
    
    # 2. Map WMO Weather Codes to your 'shortForecast' text so it matches NWS!
    # (Simplified mapping for standard NYC weather events)
    def map_weather(code):
        if code in [0, 1, 2]: return 'Clear'
        if code in [3]: return 'Cloudy'
        if code in [51, 53, 55, 61, 63, 65, 80, 81, 82]: return 'Rain'
        if code in [71, 73, 75, 85, 86]: return 'Snow'
        if code in [95, 96, 99]: return 'Storm'
        return 'Cloudy/Other'
        
    df['shortForecast'] = df['weather_code'].apply(map_weather)
    
    # Return exactly the columns your Gold pipeline expects!
    return df[['startTime', 'endTime', 'temperature', 'shortForecast']]

@task
def save_historical_weather_clean(df):
    timestamp = datetime.now(tz=ZoneInfo('America/New_York')).strftime("%Y-%m-%d_%H-%M-%S")
    base_dir = os.path.abspath(os.path.dirname(__file__))
    filename = f"historical_weather_{timestamp}.csv"
    filepath = os.path.join(base_dir, "..", "data_lakehouse", "silver", filename)
    
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df.to_csv(filepath, index=False)
    print(f"✅ Transformed historical weather saved to {filename}")

@flow
def historical_weather_silver_pipeline():
    raw_data = read_latest_historical_bronze_weather()
    clean_df = transform_historical_weather(raw_data)
    if not clean_df.empty:
        save_historical_weather_clean(clean_df)

if __name__ == "__main__":
    historical_weather_silver_pipeline()