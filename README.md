*Note: needs fixing to join live data*

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

## Set up Start and Stop Shell scripts on Mac

```bash
cd ~/nyc_transit_weather
chmod +x start.sh stop.sh
```

---

## Generate data on demand:'

- Run main_2.py if you want to generate new data through the entire pipeline one time.

```bash
cd ~/nyc_transit_weather
python main_2.py
```

## Schedule data updates

- mta bronze/silver: every 10 minutes
- weather bronze/silver: every hour
- gold pipeline: every 15 minutes

```bash
cd ~/nyc_transit_weather
./start.sh
```

***Note: this will run in the background until you stop it.***

## Stop data updates

```bash
cd ~/nyc_transit_weather
./stop.sh
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

---
