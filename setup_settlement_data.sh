#!/bin/bash
# Setup script for settlement data on production server
# Run this after pulling from GitHub to set up the settlement data

echo "Setting up ChatMRPT settlement data..."

# Create directory structure
mkdir -p kano_settlement_data/Kano_clustered_footprint

# Option 1: Copy from a shared location on the server
# Uncomment and modify the path below if settlement data is stored elsewhere on server
# cp -r /path/to/shared/settlement_data/* kano_settlement_data/

# Option 2: Download from organization's internal storage
# Uncomment and modify the URL below if using internal file server
# wget -O settlement_data.tar.gz "https://internal-server/chatmrpt/settlement_data.tar.gz"
# tar -xzf settlement_data.tar.gz
# rm settlement_data.tar.gz

# Option 3: rsync from another server
# Uncomment and modify below if syncing from another server
# rsync -avz user@source-server:/path/to/kano_settlement_data/ ./kano_settlement_data/

echo "Settlement data setup complete!"
echo "Files in settlement directory:"
ls -lh kano_settlement_data/Kano_clustered_footprint/