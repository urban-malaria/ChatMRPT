#!/bin/bash
echo "=========================="
echo "Legacy Code Cleanup Script"
echo "=========================="

# Activate the virtual environment
echo "Activating virtual environment..."
if [ -d "chatmrpt_venv" ]; then
    source chatmrpt_venv/Scripts/activate 2>/dev/null || source chatmrpt_venv/bin/activate 2>/dev/null
    if [ $? -ne 0 ]; then
        echo "Failed to activate chatmrpt_venv."
    else
        echo "Successfully activated chatmrpt_venv."
    fi
else
    echo "Looking for virtual environment..."
    for venv in *venv*; do
        if [ -d "$venv" ]; then
            echo "Found possible virtual environment: $venv"
            source "$venv/Scripts/activate" 2>/dev/null || source "$venv/bin/activate" 2>/dev/null
            if [ $? -eq 0 ]; then
                echo "Successfully activated $venv"
                break
            fi
        fi
    done
fi

echo ""
echo "Setting up environment..."

echo "Running cleanup script..."
python cleanup_legacy_code.py

echo ""
echo "Cleanup complete. Please restart your Flask application."
echo "=========================="

read -p "Press Enter to continue..." 