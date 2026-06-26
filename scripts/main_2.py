from prefect import flow

# Import the flows from your separate script files
from extract_mta import mta_bronze_pipeline
from extract_weather import weather_bronze_pipeline
from transform_mta import mta_silver_pipeline
from transform_weather import weather_silver_pipeline
from gold_pipeline import gold_pipeline

@flow(name="Master MTA Flow")
def master_mta_flow():
    mta_bronze_pipeline()
    mta_silver_pipeline()

@flow(name="Master Weather Flow")
def master_weather_flow():
    weather_bronze_pipeline()
    weather_silver_pipeline()

@flow(name= "Master Gold Flow")
def master_gold_flow():
    gold_pipeline()


if __name__ == "__main__":
    master_mta_flow()
    master_weather_flow()
    master_gold_flow()