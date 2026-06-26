import time
from datetime import datetime
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

@flow(name="Master Gold Flow")
def master_gold_flow():
    gold_pipeline()

if __name__ == "__main__":
    print("==================================================")
    print("🚀 LAUNCHING CUSTOM AUTOMATED INGESTION ENGINE... ")
    print("==================================================")
    
    # Initialize trackers (Set to 0 so they trigger instantly on the very first loop)
    last_mta_run = 0
    last_weather_run = 0
    last_gold_run = 0
    
    while True:
        current_time = time.time()
        print(f"\n⏱️ [{datetime.now().strftime('%H:%M:%S')}] Checking Schedule Boundaries...")
        
        # 1. MTA runs every 5 minutes (300 seconds)
        if current_time - last_mta_run >= 300:
            print("➡️  Triggering Automated MTA Pipeline...")
            master_mta_flow()
            last_mta_run = current_time
        
        # 2. Weather runs every 1 hour (3600 seconds)
        if current_time - last_weather_run >= 3600:
            print("➡️  Triggering Automated Weather Pipeline...")
            master_weather_flow()
            last_weather_run = current_time
            
        # 3. Gold runs every 10 minutes (600 seconds)
        if current_time - last_gold_run >= 600:
            print("➡️  Triggering Automated Gold Analytics Pipeline...")
            master_gold_flow()
            last_gold_run = current_time
            
        print("\n💤 Cycle complete. Engine sleeping for 60 seconds...")
        time.sleep(60)