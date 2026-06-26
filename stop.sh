#!/bin/bash

# Check if the PID file exists
if [ -f orchestrator.pid ]; then
    # Read the PID and kill the process
    kill $(cat orchestrator.pid)
    # Remove the file since the process is no longer running
    rm orchestrator.pid

    current_time=$(date +"%r")
    echo "Master orchestration engine stopped successfully. 🛑 $current_time"
else
    echo "No running orchestrator found (orchestrator.pid missing). 🔍"
fi