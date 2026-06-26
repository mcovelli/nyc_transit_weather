import requests
import json
from prefect import flow, task
import os
from datetime import datetime
from zoneinfo import ZoneInfo

# Open-Meteo Historical API for NYC (2012)
# We are grabbing hourly temperature, precipitation, and weather codes
URL = "https://archive-api.open-meteo.com/v1/archive?latitude=40.7128&longitude=-74.0061&start_date=2012-01-01&end_date=2019-12-31&hourly=temperature_2m,precipitation,weather_code"

@task
def fetch_historical_weather():
    print("Fetching historical weather data for 2012 through 2019...")
    response = requests.get(URL)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch: {response.status_code}")
        return None

@task
def save_historical_weather_raw(data):
    timestamp = datetime.now(tz=ZoneInfo('America/New_York')).strftime("%Y-%m-%d_%H-%M-%S")
    base_dir = os.path.abspath(os.path.dirname(__file__))
    filename = f"historical_weather_{timestamp}.json"
    filepath = os.path.join(base_dir, "..", "data_lakehouse", "bronze", filename)
    
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f)
    print(f"Historical weather saved to {filename}")

@flow
def historical_weather_bronze_pipeline():
    data = fetch_historical_weather()
    if data:
        save_historical_weather_raw(data)

if __name__ == "__main__":
    historical_weather_bronze_pipeline()