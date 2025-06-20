#!/bin/bash
# Build script for Render deployment

echo "Starting ChatMRPT build process..."

# Install system dependencies (handled by apt-packages file on Render)
echo "System dependencies will be installed via apt-packages file"

# Create necessary directories
echo "Creating required directories..."
mkdir -p instance/uploads
mkdir -p instance/reports
mkdir -p instance/logs
mkdir -p sessions
mkdir -p data

# Set up settlement data directory structure
echo "Setting up settlement data directory structure..."
mkdir -p kano_settlement_data/Kano_clustered_footprint

# Create a placeholder file to indicate settlement data needs to be added
touch kano_settlement_data/SETTLEMENT_DATA_REQUIRED.txt
echo "Settlement data files need to be uploaded to this directory:" > kano_settlement_data/SETTLEMENT_DATA_REQUIRED.txt
echo "- kano_grids_clustered.dbf" >> kano_settlement_data/SETTLEMENT_DATA_REQUIRED.txt
echo "- kano_grids_clustered.shp" >> kano_settlement_data/SETTLEMENT_DATA_REQUIRED.txt
echo "- kano_grids_clustered.shx" >> kano_settlement_data/SETTLEMENT_DATA_REQUIRED.txt
echo "- kano_grids_clustered.prj" >> kano_settlement_data/SETTLEMENT_DATA_REQUIRED.txt

# Set permissions
chmod -R 755 instance/
chmod -R 755 sessions/
chmod -R 755 data/
chmod -R 755 kano_settlement_data/

echo "Build setup complete!"
echo "Note: Settlement data files need to be uploaded separately due to size (436MB)"