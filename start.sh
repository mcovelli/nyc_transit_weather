#!/bin/bash

# Navigate to the project root directory
cd /Users/mike/nyc_transit_weather

# Activate the virtual environment
source venv/bin/activate

# Launch the master orchestrator in the background and log outputs to a file
python3 -u scripts/main.py > orchestrator.log 2>&1 &

# Save the unique background Process ID (PID) into a text file
echo $! > orchestrator.pid

current_time=$(date +"%r")
echo "Master orchestration engine launched in the background at $current_time! (PID: $(cat orchestrator.pid))"
