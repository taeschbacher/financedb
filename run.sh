#!/bin/bash

# Get the directory of this script (ensures it works even if run from another location)
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

# Define relative paths
VENV_PATH="venv/bin/activate"
MAIN_SCRIPT="src/financedb/main.py"

# Check if VENV_PATH exists
if [[ ! -f "$BASE_DIR/$VENV_PATH" ]]; then
    echo "Error: Virtual environment activation script not found at $BASE_DIR/$VENV_PATH"
    exit 1
fi

# Check if MAIN_SCRIPT exists
if [[ ! -f "$BASE_DIR/$MAIN_SCRIPT" ]]; then
    echo "Error: Python script not found at $BASE_DIR/$MAIN_SCRIPT"
    exit 1
fi

# Activate virtual environment
source "$BASE_DIR/$VENV_PATH"

# Run the Python script
python "$BASE_DIR/$MAIN_SCRIPT"

# Deactivate virtual environment
deactivate
