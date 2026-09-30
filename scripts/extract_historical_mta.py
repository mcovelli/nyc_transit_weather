import requests
from datetime import datetime, timedelta
import json
from prefect import flow, task
import os
from zoneinfo import ZoneInfo

# NY State publishes MTA alert history as two separate, non-overlapping datasets:
# - "MTA Service Alerts: 2012 - 2020" (3h5b-5ktz): frozen, agency = 'Subway',
#   id field = status_id (a UUID)
# - "MTA Service Alerts: Beginning April 2020" (7kct-peq7): actively published
#   but lags real time by ~6-7 weeks, agency = 'NYCT Subway', id field =
#   alert_id (a plain number)
# Fetching only one of these (as this script used to) silently caps historical
# coverage at whichever dataset you picked. Fetch both every time so a single
# backfill run always produces the full available range.
DATASETS = [
    {
        "url": "https://data.ny.gov/resource/3h5b-5ktz.json",
        "agency": "Subway",
    },
    {
        "url": "https://data.ny.gov/resource/7kct-peq7.json",
        "agency": "NYCT Subway",
    },
]

# Custom User-Agent header to identify the application making the request
headers = {
    "User-Agent": "NYC Transit Weather Pipeline (mcovelli@duck.com)"
}

_WEATHER_TERMS_WHERE = (
    "(header LIKE '%storm%' OR header LIKE '%snow%' OR header LIKE '%flood%' OR header LIKE '%weather%' "
    "OR header LIKE '%wind%' OR header LIKE '%blizzard%' OR header LIKE '%hurricane%' OR header LIKE '%heavy rain%' "
    "OR header LIKE '% rain%' OR header LIKE '%icy%' OR header LIKE '%icing%') "
    "OR (description LIKE '%storm%' OR description LIKE '%snow%' OR description LIKE '%flood%' OR description LIKE '%weather%' "
    "OR description LIKE '%wind%' OR description LIKE '%blizzard%' OR description LIKE '%hurricane%' OR description LIKE '%heavy rain%' "
    "OR description LIKE '% rain%' OR description LIKE '%icy%' OR description LIKE '%icing%')"
)

# Task to fetch Historical MTA alerts from the API
@task
def fetch_mta_data():
    all_rows = []

    for dataset in DATASETS:
        offset = 0
        limit = 50000

        while True:
            params = {
                "$where": f"agency = '{dataset['agency']}' AND ({_WEATHER_TERMS_WHERE})",
                "$limit": limit,
                "$offset": offset
            }

            response = requests.get(dataset["url"], headers=headers, params=params)
            if response.status_code != 200:
                break
            data = response.json()
            all_rows.extend(data)
            if len(data) < limit:
                break
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
