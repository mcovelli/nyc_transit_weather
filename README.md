# 🚋 NYC Transit & Weather

**Automated Transit & Weather Data Pipeline**  
Data pipeline using live data from the National Weather Service (NWS) and the Metropolitan Transit Authority (MTA) to analyze how weather affects MTA subway service.

---

## System Architecture & Data Flow

- Follows the Medallion Architecture
  - 🥉 Bronze (raw data)
  - 🥈 Silver (cleaned data)
  - 🥇 Gold (curated data)
  - Data is written dynamically into the Bronze layer (data_lakehouse/bronze)

*Note: Bronze layer data is completely raw, unmodified and historically isolated via timestamped file patterns to act as an immutable local data lake.*

- **The Gold layer lives in a local MySQL database** (`nyc_transit_weather`), not in the repo. The whole pipeline now runs locally twice a day (see [Schedule data updates](#schedule-data-updates-twice-a-day)) so it can collect a full year of data without ever pushing that data to GitHub. `data_lakehouse/` and `.env` are gitignored — everything under them stays on your machine only.

This repo is designed to be cloned and run standalone: everyone who clones it gets their own local MySQL database and their own scheduled pipeline — nothing is shared between clones, and no cloud credentials are required except the optional Google Sheets sync.

### What's in the Gold MySQL database

| Table / View | What it's for |
| --- | --- |
| `gold_transit_weather_fact` | One row per MTA alert that was actually attributed to weather. `alert_reason` is the cause MTA itself gave (extracted from the alert's header/description text, e.g. `snow`, `hurricane`, `wind`) — this is what explains a delay, including the days after a storm has passed but cleanup is still disrupting service. `temperature`/`shortForecast` are the point-in-time forecast when the alert was posted (context, not cause). |
| `gold_daily_transit_weather_summary` | One row per (day, route): total alerts, average temperature, `typical_conditions_that_day`, and `primary_weather_cause` — the most common stated cause that day. |
| `view_weather_impact` | A read-friendly projection over the fact table (`weather_cause`, `conditions_at_alert_time`, the original alert text), filtered to only rows with an attributed cause. This is what gets exported to CSV/Google Sheets. |

Only `description_text`/`header_text` matching a known weather term (`scripts/mta_constants.py`) ever gets a `weather_cause` — every other MTA alert (broken windows, signal problems, planned service changes, etc.) is filtered out before it reaches the Gold layer, so this table stays a clean record of weather-impacted service.

---

## 📋 Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
  - [Step 1: Install Homebrew](#step-1-install-homebrew)
  - [Step 2: Install System Dependencies](#step-2-install-system-dependencies)
  - [Step 3: Clone and Set Up NYC Transit and Weather](#step-3-clone-and-set-up-nyc-transit-and-weather)
  - [Production Constraints & Lessons Learned](#production-constraints--lessons-learned)

---

## Requirements

| Dependency | Purpose | Install Method |
| --- | --- | --- |
| **Python 3.13** | Runtime. | `brew install python@3.13` |
| **Prefect** | Workflow Scheduling. | `pip install prefect` |
| **Requests** | HTTP requests to NWS and MTA APIs to retrieve data. | `pip install requests` |
| **FastAPI** | sub-dependency to Prefect with version constrants. | `pip install "fastapi>=0.111.0,<0.112.0"` |
| **GTFS-Realtime** | Parses raw, binary Google Protocol Buffer (Protobuf) streams from the MTA feed into native Python objects. | `pip install gtfs-realtime-bindings` |
| **Pandas** | Dataframes to clean, organize and analyze data | `pip install pandas` |
| **MySQL** | Local database that stores the Gold layer. | `brew install mysql && brew services start mysql` |
| **PyMySQL** | Python driver used to connect to the local MySQL database. | `pip install PyMySQL` |

---

## Installation

### Step 1: Install Homebrew

If you don't already have Homebrew:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

After installation, follow the on-screen instructions to add Homebrew to your PATH.

### Step 2: Install System Dependencies

```bash
brew install python@3.13
```  

**Verify installation:**

```bash
python3 --version    # Should show Python 3.13
```

---

### Step 3: Clone and Set Up NYC Transit and Weather

```bash
# Clone the repository
git clone https://github.com/mcovelli/nyc_transit_weather.git
cd nyc_transit_weather

# Create a Python virtual environment
python3.13 -m venv venv

# Activate the virtual environment
source venv/bin/activate
```

## Install Python dependencies

```bash
pip install -r requirements.txt
```

## Set up the local MySQL database

The Gold layer (the fact table, daily summary, and reporting view) lives in a local MySQL database instead of the repo, so it can grow for as long as you want without ever being pushed to GitHub.

```bash
brew install mysql
brew services start mysql
mysql -u root -e "CREATE DATABASE IF NOT EXISTS nyc_transit_weather;"
```

Create a `.env` file in the repo root (already gitignored) with the connection details the pipeline scripts (`scripts/db.py`) will read:

```bash
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=
MYSQL_DATABASE=nyc_transit_weather
```

Adjust `MYSQL_USER`/`MYSQL_PASSWORD` if you're not using a passwordless local root account. View the data any time with the `mysql` CLI, or point a GUI client (TablePlus, MySQL Workbench, DBeaver, etc.) at `127.0.0.1:3306`.

A `.env.example` is included as a starting point:

```bash
cp .env.example .env
```

## Google Sheets sync (optional)

The Gold pipeline can also push the `view_weather_impact` report to a Google Sheet (e.g. for Tableau). This step is **optional** — if it isn't configured, the pipeline logs a warning and continues; MySQL is the real source of truth. To enable it:

1. Create a Google Cloud service account with Sheets API access and download its JSON key.
2. Save it as `google_keys.json` in the repo root (already gitignored — never commit it).
3. Share your target Google Sheet ("NYC_Transit_Weather_Impact") with the service account's email address.

---

## Generate data on demand

- Run `main_2.py` if you want to generate new data through the entire pipeline one time.

```bash
cd ~/nyc_transit_weather
source venv/bin/activate
python scripts/main_2.py
```

## Schedule data updates (twice a day)

Extract (bronze) and transform (silver) run for both MTA and weather, followed by the Gold load into MySQL, as a single pipeline twice a day — **6:00 AM and 6:00 PM, local time** — via a macOS `launchd` agent. This keeps collecting data even across logout/reboot, for as long as you want to leave it running (e.g. a full year), without you needing to keep a terminal open.

The install script figures out your cloned repo's path automatically, so it works the same regardless of where or as whom you cloned it:

```bash
cd ~/nyc_transit_weather
./scripts/install_launchd.sh
```

Output from each run is appended to `pipeline_cron.log` in the repo root (gitignored). To change the schedule, edit the `Hour`/`Minute` values in `scripts/com.nyctransitweather.pipeline.plist` and re-run `install_launchd.sh`.

***Note: `start.sh` / `stop.sh` still exist for foreground, ad-hoc testing of the old continuous-loop scheduler (`scripts/main.py`) — they are not what drives the twice-daily collection.***

## Stop scheduled data updates

```bash
launchctl unload ~/Library/LaunchAgents/com.nyctransitweather.pipeline.plist
```

---

## Production Constraints & Lessons Learned

- Library conflicts between Python, Prefect and Fast API required version pinning:
  - Python downgraded to 3.13
  - FastAPI pinned between versions 0.111.0 and 0.112.0

- Relative paths behave differently depending on what  directory the terminal is sitting.
  - Dynamic paths using os.path allows for absolute file paths so the script can run outside of the scripts directory.

- GTFS-realtime data needs to be fetched using .content and converted to JSON using FeedMessage()

- MTA data updates frequently, we use NO_CACHE on those tasks to get fresh data everytime.

- The bronze layer is used as a historical record for all updates of raw, untouched data
  - We find the most recent bronze data file to process in the silver layer

- The old hourly GitHub Actions workflow (`.github/workflows/pipeline.yml`) has been removed: it committed `data_lakehouse/` (including the SQLite db) back to the repo every run, which doesn't scale for a year of collection and can't reach a MySQL database that only exists on your machine. All collection is now local, on the `launchd` schedule above.

---
