import os
from prefect import task, flow
import glob
import pandas as pd
from sqlite3 import connect
from contextlib import closing
import gspread
import json

base_dir = os.path.dirname(os.path.abspath(__file__))
silver_dir = os.path.join(base_dir, "..", "data_lakehouse", "silver")

@task
def read_latest_silver_mta_data():
    silver_dir = os.path.join(base_dir, "..", "data_lakehouse", "silver")
    
    # Find and load the LIVE MTA data
    live_pattern = os.path.join(silver_dir, "mta_alerts_*.csv")
    live_files = glob.glob(live_pattern)
    live_mta = pd.DataFrame()
    if live_files:
        latest_live = max(live_files, key=os.path.getctime)
        live_mta = pd.read_csv(latest_live)

    # Find and load the HISTORICAL MTA data
    hist_pattern = os.path.join(silver_dir, "historical_mta_data_*.csv")
    hist_files = glob.glob(hist_pattern)
    hist_mta = pd.DataFrame()
    if hist_files:
        latest_hist = max(hist_files, key=os.path.getctime)
        hist_mta = pd.read_csv(latest_hist)

    # Stack them on top of each other!
    if not live_mta.empty and not hist_mta.empty:
        unified_mta = pd.concat([hist_mta, live_mta], ignore_index=True)
    elif not hist_mta.empty:
        unified_mta = hist_mta
    else:
        unified_mta = live_mta

    if unified_mta.empty:
        print("No MTA data found at all in the silver directory.")
        return None
        
    return unified_mta

@task
def read_latest_silver_weather_data():
    silver_dir = os.path.join(base_dir, "..", "data_lakehouse", "silver")
    
    # Find and load the LIVE weather data
    live_pattern = os.path.join(silver_dir, "weather_data_*.csv")
    live_files = glob.glob(live_pattern)
    live_weather = pd.DataFrame()
    if live_files:
        latest_live = max(live_files, key=os.path.getctime)
        live_weather = pd.read_csv(latest_live)

    # Find and load the HISTORICAL weather data
    hist_pattern = os.path.join(silver_dir, "historical_weather_*.csv")
    hist_files = glob.glob(hist_pattern)
    hist_weather = pd.DataFrame()
    if hist_files:
        latest_hist = max(hist_files, key=os.path.getctime)
        hist_weather = pd.read_csv(latest_hist)

    # Stack them on top of each other!
    if not live_weather.empty and not hist_weather.empty:
        unified_weather = pd.concat([hist_weather, live_weather], ignore_index=True)
    elif not hist_weather.empty:
        unified_weather = hist_weather
    else:
        unified_weather = live_weather

    if unified_weather.empty:
        print("No weather data found at all in the silver directory.")
        return None
        
    return unified_weather
        
@task
def combine_tables(weather_data, mta_data):
    # THE DATA CONTRACT
    required_columns = ['entity_id', 'routeId', 'start', 'end', 'header_text', 'description_text']

    for col in required_columns:
        if col not in mta_data.columns:
            mta_data[col] = pd.NA
    mta_data_aligned = mta_data[required_columns]
    
    # STRICT UTC ENFORCEMENT
    mta_data_aligned['start'] = pd.to_datetime(mta_data_aligned['start'], errors='coerce', utc=True)
    mta_data_aligned['end'] = pd.to_datetime(mta_data_aligned['end'], errors='coerce', utc=True)
    
    weather_data['startTime'] = pd.to_datetime(weather_data['startTime'], errors='coerce', utc=True)
    if 'endTime' in weather_data.columns:
        weather_data['endTime'] = pd.to_datetime(weather_data['endTime'], errors='coerce', utc=True)

    # PREPARE FOR AS-OF MERGE (Must drop null times and sort)
    mta_data_aligned = mta_data_aligned.dropna(subset=['start']).sort_values('start')
    weather_data = weather_data.dropna(subset=['startTime']).sort_values('startTime')

    # Merge AsOf: Finds the closest weather forecast within a 2-hour window!
    gold_df = pd.merge_asof(
        mta_data_aligned, 
        weather_data, 
        left_on='start', 
        right_on='startTime',
        direction='nearest',
        tolerance=pd.Timedelta(hours=2)
    )

    # Drop any alerts that failed to find a weather match within 2 hours
    gold_df = gold_df.dropna(subset=['startTime'])

    # Explicitly rename the weather timestamps so they are obvious in the database
    gold_df = gold_df.rename(columns={
        'startTime': 'weather_start',
        'endTime': 'weather_end'
    })

    return gold_df

@task
def summarize(gold_df):
    # Ensure there is data to summarize
    if gold_df.empty:
        return pd.DataFrame()

    # Extract just the YYYY-MM-DD component for daily grouping
    gold_df['alert_date'] = gold_df['weather_start'].dt.date

    summary_df = gold_df.groupby(['alert_date', 'routeId']).agg({
        'entity_id': 'nunique', # total unique alerts impacting service
        'temperature': 'mean',
        'shortForecast': lambda x: x.mode()[0] if not x.mode().empty else None
    }).reset_index()

    return summary_df

@task  
def save(gold_df, summary_df):
    if gold_df.empty:
        print("No overlapping weather/transit data found in this run. Skipping save.")
        return

    db_path = os.path.join(base_dir, "..", "data_lakehouse", "nyc_transit_weather.db")

    with closing(connect(db_path)) as conn:
        try:
            # Added routeId to the composite key check
            existing_keys = pd.read_sql(
                "SELECT DISTINCT entity_id, routeId, weather_start FROM gold_transit_weather_fact",
                con=conn
            )
            existing_keys['weather_start'] = pd.to_datetime(existing_keys['weather_start'], utc=True)
        except Exception:
            # Added routeId to the fallback dataframe
            existing_keys = pd.DataFrame(columns=['entity_id', 'routeId', 'weather_start'])
        
        # Merge to find which records are brand new
        merged_df = pd.merge(gold_df, existing_keys, indicator=True, how='left')
        new_granular_data = merged_df[merged_df['_merge'] == 'left_only']

        if not new_granular_data.empty:
            new_granular_data.drop(columns=['_merge']).to_sql('gold_transit_weather_fact', conn, if_exists='append', index=False)
            print(f"Added {len(new_granular_data)} new rows to gold_transit_weather_fact.")
        else:
            print("No new unique records to insert into the fact table.")

        if not summary_df.empty:
            summary_df.to_sql('gold_daily_transit_weather_summary', conn, if_exists='replace', index=False)
            print(f"Updated summary data in gold_daily_transit_weather_summary.")
        
        cursor = conn.cursor()

        cursor.execute("DROP VIEW IF EXISTS view_weather_impact;")

        view_sql = """
        CREATE VIEW view_weather_impact AS 
            SELECT 
                routeId, 
                weather_start, 
                CASE 
                    WHEN shortForecast LIKE '%rain%' OR shortForecast LIKE '%showers%' OR shortForecast LIKE '%hurricane%' THEN 'Rain'
                    WHEN shortForecast LIKE '%Snow%' OR shortForecast LIKE '%blizzard%' THEN 'Snow'
                    WHEN shortForecast LIKE '%sunny%' OR shortForecast LIKE '%clear%' THEN 'Clear'
                    ELSE 'Cloudy/Other'
                END AS weather_category,
                COUNT(DISTINCT entity_id) AS total_alerts
            FROM gold_transit_weather_fact
            WHERE (
                LOWER(header_text) LIKE '%weather%'
                OR LOWER(header_text) LIKE '%heavy rain%'
                OR LOWER(description_text) LIKE '%light rain%'
                OR LOWER(description_text) LIKE '%drizzle%'
                OR LOWER(header_text) LIKE '%snow%'
                OR LOWER(header_text) LIKE '%flood%'
                OR LOWER(header_text) LIKE '%wind%'
                OR LOWER(header_text) LIKE '%storm%'
                OR LOWER(header_text) LIKE '%icy%'
                OR LOWER(header_text) LIKE '%icing%'
                OR LOWER(header_text) LIKE '%hurricane%'
                OR LOWER(header_text) LIKE '%blizzard%'
                OR LOWER(header_text) LIKE '%fog%'
                OR LOWER(header_text) LIKE '%heat%'
                

                OR LOWER(description_text) LIKE '%weather%'
                OR LOWER(description_text) LIKE '%light rain%'
                OR LOWER(description_text) LIKE '%drizzle%'
                OR LOWER(description_text) LIKE '%heavy rain%'
                OR LOWER(description_text) LIKE '%snow%'
                OR LOWER(description_text) LIKE '%flood%'
                OR LOWER(description_text) LIKE '%wind%'
                OR LOWER(description_text) LIKE '%storm%'
                OR LOWER(description_text) LIKE '%icy%'
                OR LOWER(description_text) LIKE '%icing%'
                OR LOWER(description_text) LIKE '%hurricane%'
                OR LOWER(description_text) LIKE '%blizzard%'
                OR LOWER(description_text) LIKE '%fog%'
                OR LOWER(description_text) LIKE '%heat%'
            )

            GROUP BY 
                routeId, 
                weather_start,
                weather_category;
        """

        cursor.execute(view_sql)
        print("📊 Reporting View 'view_weather_impact' successfully rebuilt.")

        filename = f"view_weather_impact.csv"
        gold_dir = os.path.join(base_dir, "..", "data_lakehouse", "gold")
        output_file_path = os.path.join(gold_dir, filename)

        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        df = pd.read_sql("SELECT * FROM view_weather_impact", conn)
        
        if df.empty:
            print("Empty table, nothing to export")
        else:
            df.to_csv(output_file_path, index=False) 
            print("📁 CSV Export successful.")

@task(name="Sync View to Google Sheets")
def sync_view_to_google_sheets():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, "..", "data_lakehouse", "nyc_transit_weather.db")
    
    # Pull the fresh semantic view data out of SQLite
    print("🔄 Extracting reporting view from SQLite...")
    with closing(connect(db_path)) as conn:
        df = pd.read_sql("SELECT * FROM view_weather_impact", conn)
    
    if df.empty:
        print("⚠️ Reporting view is empty. Skipping upload.")
        return

    # Authenticate using either Cloud Secret or Local Key file
    print("🔐 Authenticating with Google Cloud...")
    google_cred_env = os.getenv("GOOGLE_CREDENTIALS")
    
    if google_cred_env:
        # Running in GitHub Actions Cloud
        creds_dict = json.loads(google_cred_env)
        gc = gspread.service_account_from_dict(creds_dict)
    else:
        # Running locally on your Mac
        local_key_path = os.path.join(base_dir, "..", "google_keys.json")
        if not os.path.exists(local_key_path):
            raise FileNotFoundError(f"Could not find local key file at {local_key_path}")
        gc = gspread.service_account(filename=local_key_path)

    # Open the Spreadsheet and completely overwrite it
    # Note: Make sure the string matches your Google Sheet name exactly!
    spreadsheet_name = "NYC_Transit_Weather_Impact" 
    print(f"📊 Connecting to Google Sheet: '{spreadsheet_name}'...")
    
    sh = gc.open(spreadsheet_name)
    worksheet = sh.get_worksheet(0) # Selects the first tab
    
    # Format DataFrame to a list of lists that gspread accepts
    # Replace NaN values with strings so json serialization doesn't crash
    df_clean = df.fillna("")
    data_to_upload = [df_clean.columns.values.tolist()] + df_clean.values.tolist()
    
    print("🚀 Overwriting Google Sheet with fresh hourly analytics data...")
    worksheet.clear()
    worksheet.update(values=data_to_upload, range_name="A1")
    print("✅ Google Sheet sync successful! Tableau data engine is primed.")

@flow
def gold_pipeline():
    # Extract unified Silver Data
    mta_data = read_latest_silver_mta_data()
    weather_data = read_latest_silver_weather_data() 

    if mta_data is not None and weather_data is not None:
        # Transform (Equi-Join Big Data)
        gold_df = combine_tables(weather_data, mta_data)
        
        # Aggregate
        summary_df = summarize(gold_df)
        
        # Load
        save(gold_df, summary_df)
        sync_view_to_google_sheets()
    else:
        print("Pipeline Stopped. Core files missing from Silver layer.")

if __name__ == "__main__":
    gold_pipeline()