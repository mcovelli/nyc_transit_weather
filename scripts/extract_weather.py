import requests
from datetime import datetime, timedelta
import json
from prefect import flow, task
import os
from zoneinfo import ZoneInfo

# API endpoint for the National Weather Service grid points for NYC
url = "https://api.weather.gov/points/40.7128,-74.0061"

# Custom User-Agent header to identify the application making the request
headers = {
    "User-Agent": "NYC Transit Weather Pipeline (mcovelli@duck.com)"
}

# Task to fetch grid points and forecast URL from the NWS API
@task
def fetch_grid_points():
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        print(data)

        properties = data.get("properties", {})
        forecast_url = properties.get("forecastHourly")
        return forecast_url
    else:
        print(f"Failed to fetch grid points: {response.status_code}")
        return None

# Task to fetch the hourly forecast data from the NWS API using the forecast URL    
@task
def fetch_forecast_data(forecast_url):
    if forecast_url:
        forecast_response = requests.get(forecast_url, headers=headers)
        if forecast_response.status_code == 200:
            forecast_data = forecast_response.json()
            return forecast_data
        else:
            print(f"Failed to fetch forecast data: {forecast_response.status_code}")
            return None

# Task to save the raw forecast data to a JSON file in the data lakehouse
@task
def save_forecast_raw(forecast_data):

    # Generate a timestamped filename for the forecast data
    timestamp = datetime.now(tz=ZoneInfo('America/New_York')).strftime("%Y-%m-%d_%H-%M-%S")

    # Construct the full file path for saving the forecast data in the data lakehouse
    relative_path = os.path.abspath(os.path.dirname(__file__))
    filename = f"weather_data_{timestamp}.json"
    full_file_path = os.path.join(relative_path, "..", "data_lakehouse", "bronze", filename)

    # Ensure the directory exists
    os.makedirs(os.path.dirname(full_file_path), exist_ok=True)
    
    # Save the forecast data to the specified file path
    with open(full_file_path, "w") as f:
            json.dump(forecast_data, f)
            print(f"Weather data saved to {filename}")


# Define the Prefect flow to orchestrate the tasks
@flow
def weather_bronze_pipeline():

    #Fetch grid points URL
    forecast_url = fetch_grid_points()
    if not forecast_url:
        print("Pipeline Stopped. Could not retrieve forecast URL.")
        return  
    
    #Fetch true raw forcast data
    forecast_data = fetch_forecast_data(forecast_url)
    if not forecast_data:
        print("Pipeline Stopped. Could not retrieve forecast data.")
        return
    
    #commit raw data to data lakehouse
    save_forecast_raw(forecast_data)    

if __name__ == "__main__":
    weather_bronze_pipeline()
