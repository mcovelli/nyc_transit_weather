import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
from datetime import datetime, timedelta
import os
from prefect import task, flow
from prefect.cache_policies import NO_CACHE
from zoneinfo import ZoneInfo

#API endpoint for MTA subway alerts in GTFS-realtime format
url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts"

# Custom User-Agent header to identify the application making the request
headers = {
        "User-Agent": "NYC Transit Weather Pipeline (mcovelli@duck.com)"
    }

# Task to fetch MTA alerts from the API
@task
def fetch_mta_alerts():
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.content
        return data
    else:
        print(f"Failed to fetch MTA alerts: {response.status_code}")
        return None

# Task to parse the GTFS-realtime data and convert it to JSON format
@task(cache_policy=NO_CACHE)
def parse_mta_alerts(raw_data):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_data)
    print(f"Successfully parsed {len(feed.entity)} live MTA alert updates.")
    return feed

# Task to save the raw MTA alert data to a JSON file in the data lakehouse
@task(cache_policy=NO_CACHE)
def save_mta_raw(feed):
    json_data = MessageToJson(feed)
    timestamp = datetime.now(tz=ZoneInfo('America/New_York')).strftime("%Y-%m-%d_%H-%M-%S")
    relative_path = os.path.abspath(os.path.dirname(__file__))
    filename = f"mta_alerts_{timestamp}.json"
    full_file_path = os.path.join(relative_path, "..", "data_lakehouse", "bronze", filename)

    with open(full_file_path, "w") as f:
            f.write(json_data)
            print(f"MTA alert data saved to {filename}")

# Define the Prefect flow to orchestrate the tasks
@flow
def mta_bronze_pipeline():
    raw_data = fetch_mta_alerts()
    if not raw_data:
        print("Pipeline stopped. Could not retrieve MTA alerts.")
        return
    feed = parse_mta_alerts(raw_data)
    save_mta_raw(feed)

# Entry point to run the Prefect flow
if __name__ == "__main__":
    mta_bronze_pipeline()
