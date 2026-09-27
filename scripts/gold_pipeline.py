import os
from prefect import task, flow
import glob
import pandas as pd
from sqlalchemy import text
import gspread
import json

from db import get_engine
from weather_cause import extract_alert_reason

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
        live_mta = pd.concat([pd.read_csv(f) for f in live_files], ignore_index=True)
        print("✅ Reading All Live MTA data")
    else:
        print("❌ No Live MTA data")

    # Find and load the HISTORICAL MTA data
    hist_pattern = os.path.join(silver_dir, "historical_mta_data_*.csv")
    hist_files = glob.glob(hist_pattern)
    hist_mta = pd.DataFrame()
    if hist_files:
        latest_hist = max(hist_files, key=os.path.getctime)
        hist_mta = pd.read_csv(latest_hist)
        print("✅ Reading Latest Historical MTA data")
    else:
        print("❌ No Historical MTA data")

    # Stack them on top of each other!
    if not live_mta.empty and not hist_mta.empty:
        unified_mta = pd.concat([hist_mta, live_mta], ignore_index=True)
        print("✅ Merging historical and live MTA data")
    elif not hist_mta.empty:
        unified_mta = hist_mta
        print("❌ Only historical Data")
    else:
        unified_mta = live_mta
        print("❌ Only Live Data")

    if unified_mta.empty:
        print("No MTA data found at all in the silver directory.")
        return None
    print("✅ Historical and Live MTA Data Merged")
    return unified_mta

@task
def read_latest_silver_weather_data():
    silver_dir = os.path.join(base_dir, "..", "data_lakehouse", "silver")
    
    # Find and load the LIVE weather data
    live_pattern = os.path.join(silver_dir, "weather_data_*.csv")
    live_files = glob.glob(live_pattern)
    live_weather = pd.DataFrame()
    if live_files:
        live_weather = pd.concat([pd.read_csv(f) for f in live_files], ignore_index=True)
        print("✅ Reading All Live Weather data")
    else:
        print("❌ No Live Weather data")

    # Find and load the HISTORICAL weather data
    hist_pattern = os.path.join(silver_dir, "historical_weather_*.csv")
    hist_files = glob.glob(hist_pattern)
    hist_weather = pd.DataFrame()
    if hist_files:
        latest_hist = max(hist_files, key=os.path.getctime)
        hist_weather = pd.read_csv(latest_hist)
        print("✅ Reading Latest Historical weather data")
    else:
        print("❌ No Historical weather data")

    # Stack them on top of each other!
    if not live_weather.empty and not hist_weather.empty:
        unified_weather = pd.concat([hist_weather, live_weather], ignore_index=True)
        print("✅ Merging historical and live weather data")
    elif not hist_weather.empty:
        unified_weather = hist_weather
        print("❌ Only historical weather data")
    else:
        unified_weather = live_weather
        print("❌ Only live weather data")

    if unified_weather.empty:
        print("No weather data found at all in the silver directory.")
        return None
    
    print("✅ Historical and Live weather Data Merged")
    return unified_weather
        
@task
def combine_tables(weather_data, mta_data):
    required_columns = ['entity_id', 'routeId', 'start', 'end', 'header_text', 'description_text', 'alert_reason']

    # Align and Clean
    for col in required_columns:
        if col not in mta_data.columns:
            mta_data[col] = pd.NA
    mta_data_aligned = mta_data[required_columns].copy()

    # Recompute alert_reason directly from the alert text rather than trusting
    # whatever is already in the silver CSV - some older snapshots predate
    # weather-filtering entirely and would otherwise let non-weather alerts
    # (broken windows, signal problems, planned service changes, etc.) leak
    # into the Gold layer just because they landed near some weather reading.
    mta_data_aligned['alert_reason'] = mta_data_aligned.apply(
        lambda row: extract_alert_reason(row['header_text'], row['description_text']), axis=1
    )
    mta_data_aligned = mta_data_aligned[mta_data_aligned['alert_reason'].notna()]
    print(f"✅ Filtered to {len(mta_data_aligned)} weather-related alerts")

    # Drop duplicates
    mta_data_aligned = mta_data_aligned.drop_duplicates(subset=['entity_id', 'start', 'routeId'])
    print("✅ Duplicates Dropped")
    
    # Convert and Sort
    mta_data_aligned['start'] = pd.to_datetime(mta_data_aligned['start'], format='mixed', errors='coerce', utc=True)
    mta_data_aligned = mta_data_aligned.dropna(subset=['start']).sort_values('start')
    
    weather_data['startTime'] = pd.to_datetime(weather_data['startTime'], format='mixed', errors='coerce', utc=True)
    weather_data = weather_data.dropna(subset=['startTime']).sort_values('startTime')

    # Time-Series Merge
    # By using 'nearest', we ensure the alert is mapped to the most relevant weather block
    gold_df = pd.merge_asof(
        mta_data_aligned, 
        weather_data, 
        left_on='start', 
        right_on='startTime',
        direction='nearest',
        tolerance=pd.Timedelta(hours=2)
    )

    # Filter and Rename
    gold_df = gold_df.dropna(subset=['startTime']).rename(columns={
        'startTime': 'weather_start',
        'endTime': 'weather_end'
    })

    print("✅ gold dataframe")
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
        'shortForecast': lambda x: x.mode()[0] if not x.mode().empty else None,
        'alert_reason': lambda x: x.mode()[0] if not x.mode().empty else None
    }).reset_index()

    summary_df = summary_df.rename(columns={
        'entity_id': 'total_alerts',
        'shortForecast': 'typical_conditions_that_day',
        'alert_reason': 'primary_weather_cause'
    })

    print("✅ data summarized")
    return summary_df

@task
def save(gold_df, summary_df):
    if gold_df.empty:
        print("No overlapping weather/transit data found in this run. Skipping save.")
        return

    # 1. Prepare the gold_df with a stable Composite Key
    # We round to 15 minutes to ignore micro-jitter in API timestamps
    gold_df = gold_df.copy()
    gold_df['weather_start'] = pd.to_datetime(gold_df['weather_start'], utc=True).dt.round('15min')

    engine = get_engine()
    with engine.begin() as conn:
        # Schema evolution: older rows were written before alert_reason existed.
        # Add it if missing so the pipeline keeps working against an existing db.
        # (Not all MySQL versions support "ADD COLUMN IF NOT EXISTS", so check first.)
        try:
            existing_cols = pd.read_sql(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'gold_transit_weather_fact'",
                con=conn
            )
            if not existing_cols.empty and 'alert_reason' not in existing_cols['COLUMN_NAME'].values:
                conn.execute(text(
                    "ALTER TABLE gold_transit_weather_fact ADD COLUMN alert_reason VARCHAR(32)"
                ))
        except Exception:
            pass  # table doesn't exist yet; to_sql will create it with alert_reason included

        # Re-classify every existing row against the current extraction logic, not
        # just ones with a NULL alert_reason. Keyword logic keeps improving (e.g. the
        # "flood protection" construction-project false positive, or the nor'easter
        # fix) and rows written under an older version of that logic would otherwise
        # keep a stale cause forever. Rows that no longer match anything are removed
        # entirely, consistent with how already-confirmed non-weather rows were
        # purged from this table.
        try:
            existing_rows = pd.read_sql(
                "SELECT entity_id, routeId, weather_start, header_text, description_text, alert_reason "
                "FROM gold_transit_weather_fact",
                con=conn
            )
        except Exception:
            existing_rows = pd.DataFrame()

        if not existing_rows.empty:
            existing_rows['new_reason'] = existing_rows.apply(
                lambda row: extract_alert_reason(row['header_text'], row['description_text']), axis=1
            )
            old_reason = existing_rows['alert_reason']
            new_reason = existing_rows['new_reason']
            unchanged = (old_reason == new_reason) | (old_reason.isna() & new_reason.isna())
            changed = existing_rows[~unchanged]

            to_delete = changed[changed['new_reason'].isna()]
            to_update = changed[changed['new_reason'].notna()]

            if not to_delete.empty:
                delete_sql = text("""
                    DELETE FROM gold_transit_weather_fact
                    WHERE entity_id = :entity_id AND routeId = :routeId
                          AND weather_start = :weather_start AND header_text = :header_text
                """)
                for row in to_delete.to_dict(orient="records"):
                    conn.execute(delete_sql, row)
                print(f"🔧 Removed {len(to_delete)} rows that no longer match any weather cause.")

            if not to_update.empty:
                update_sql = text("""
                    UPDATE gold_transit_weather_fact
                    SET alert_reason = :new_reason
                    WHERE entity_id = :entity_id AND routeId = :routeId
                          AND weather_start = :weather_start AND header_text = :header_text
                """)
                for row in to_update.to_dict(orient="records"):
                    conn.execute(update_sql, row)
                print(f"🔧 Re-classified alert_reason for {len(to_update)} rows.")

        # 2. Fetch existing keys from the database for comparison
        try:
            existing_keys = pd.read_sql(
                "SELECT entity_id, routeId, weather_start, header_text FROM gold_transit_weather_fact",
                con=conn
            )
            existing_keys['weather_start'] = pd.to_datetime(existing_keys['weather_start'], utc=True)
        except Exception:
            existing_keys = pd.DataFrame(columns=['entity_id', 'routeId', 'weather_start', 'header_text'])

        # 3. Perform Left Anti-Join (Merge with indicator)
        # This identifies rows in gold_df that do NOT have a match in existing_keys
        merged = pd.merge(
            gold_df,
            existing_keys,
            on=['entity_id', 'routeId', 'weather_start', 'header_text'],
            how='left',
            indicator=True
        )

        # 4. Filter to get only new records (keep the original gold_df columns only)
        new_only = merged.loc[merged['_merge'] == 'left_only', gold_df.columns]

        if not new_only.empty:
            # Insert only the new, granular data
            new_only.to_sql('gold_transit_weather_fact', conn, if_exists='append', index=False)
            print(f"✅ Added {len(new_only)} new unique rows to Fact Table.")
        else:
            print("ℹ️ No new records to append. Database is up to date.")

        if not summary_df.empty:
            summary_df.to_sql('gold_daily_transit_weather_summary', conn, if_exists='replace', index=False)
            print(f"Updated summary data in gold_daily_transit_weather_summary.")

        conn.execute(text("DROP VIEW IF EXISTS view_weather_impact;"))

        # weather_cause is the reason MTA itself gave for the alert (extracted from the
        # alert text) - this is what actually explains a delay, including the days after
        # a storm has passed when service is still disrupted by cleanup/recovery.
        # conditions_at_alert_time / temperature are the point-in-time forecast for extra
        # context, but are NOT used to determine cause - they'd mislabel a Hurricane Sandy
        # suspension as "Cloudy" once the storm itself has moved on.
        view_sql = """
        CREATE VIEW view_weather_impact AS
            SELECT
                routeId,
                weather_start,
                weather_end,
                alert_reason AS weather_cause,
                shortForecast AS conditions_at_alert_time,
                temperature,
                header_text,
                description_text
            FROM gold_transit_weather_fact
            WHERE alert_reason IS NOT NULL
            ORDER BY weather_start, routeId;
        """

        conn.execute(text(view_sql))
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

    # Pull the fresh semantic view data out of MySQL
    print("🔄 Extracting reporting view from MySQL...")
    engine = get_engine()
    with engine.connect() as conn:
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
    spreadsheet_name = "NYC_Transit_Weather_Impact" 
    print(f"📊 Connecting to Google Sheet: '{spreadsheet_name}'...")
    
    sh = gc.open(spreadsheet_name)
    worksheet = sh.get_worksheet(0) # Selects the first tab
    
    # Format DataFrame to a list of lists that gspread accepts
    # Stringify datetime columns (Timestamp objects aren't JSON serializable),
    # then replace NaN values with strings so json serialization doesn't crash
    df_clean = df.copy()
    for col in df_clean.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df_clean[col] = df_clean[col].astype(str)
    df_clean = df_clean.fillna("")
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
        try:
            sync_view_to_google_sheets()
        except Exception as e:
            print(f"⚠️ Google Sheets sync failed, continuing without it: {e}")
    else:
        print("Pipeline Stopped. Core files missing from Silver layer.")

if __name__ == "__main__":
    gold_pipeline()