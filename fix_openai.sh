#!/bin/bash
echo "=========================="
echo "OpenAI API Fix Script"
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

# Check if OpenAI API key is set
if [ -z "$OPENAI_API_KEY" ]; then
    echo "WARNING: OPENAI_API_KEY environment variable not set."
    echo "You should set it for the application to function properly."
    echo ""
fi

echo "Running fix script..."
python fix_openai.py

echo ""
echo "Fix complete. Please restart your Flask application."
echo "=========================="

read -p "Press Enter to continue..." 