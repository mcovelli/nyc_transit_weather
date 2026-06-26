import requests
from datetime import datetime, timedelta
import json
from prefect import flow, task
import os
from zoneinfo import ZoneInfo

# API endpoint for Historical MTA Subway Delays
url = "https://data.ny.gov/resource/3h5b-5ktz.json"

# Custom User-Agent header to identify the application making the request
headers = {
    "User-Agent": "NYC Transit Weather Pipeline (mcovelli@duck.com)"
}

# Task to fetch Historical MTA alerts from the API
@task
def fetch_mta_data():
    all_rows = []
    offset = 0
    limit = 50000
    
    while True:

        params = {
            "$where": "agency = 'Subway' AND ((header LIKE '%storm%' OR header LIKE '%snow%' OR header LIKE '%flood%' OR header LIKE '%weather%' OR header LIKE '%wind%' OR header LIKE '%blizzard%' OR header LIKE '%hurricane%' OR header LIKE '%heavy rain%' OR header LIKE '% rain%') OR (desciption LIKE '%storm%' OR desciption LIKE '%snow%' OR desciption LIKE '%flood%' OR desciption LIKE '%weather%' OR desciption LIKE '%wind%' OR desciption LIKE '%blizzard%' OR desciption LIKE '%hurricane%' OR desciption LIKE '%heavy rain%' OR desciption LIKE '% rain%'))",
            "$limit": 50000,
            "$offset": offset
        }
        
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            break
        else:
            data = response.json()
            all_rows.extend(data)
        if len(data) < limit:
            break
        else:
            offset += limit
    return all_rows

# Task to save the raw forecast data to a JSON file in the data lakehouse
@task
def save_historical_mta_raw(mta_data):
    # Generate a timestamped filename for the forecast data
    timestamp = datetime.now(tz=ZoneInfo('America/New_York')).strftime("%Y-%m-%d_%H-%M-%S")

    # Construct the full file path for saving the forecast data in the data lakehouse
    relative_path = os.path.abspath(os.path.dirname(__file__))
    filename = f"historical_mta_data_{timestamp}.json"
    full_file_path = os.path.join(relative_path, "..", "data_lakehouse", "bronze", filename)

    # Ensure the directory exists
    os.makedirs(os.path.dirname(full_file_path), exist_ok=True)
    
    # Save the forecast data to the specified file path
    with open(full_file_path, "w") as f:
            json.dump(mta_data, f)
            print(f"Historical MTA data saved to {filename}")


# Define the Prefect flow to orchestrate the tasks
@flow
def historical_mta_bronze_pipeline():

    #Fetch true raw historical MTA data
    mta_data = fetch_mta_data()
    if not mta_data:
        print("Pipeline Stopped. Could not retrieve MTA data.")
        return
    
    #commit raw data to data lakehouse
    save_historical_mta_raw(mta_data)    

if __name__ == "__main__":
    historical_mta_bronze_pipeline()
