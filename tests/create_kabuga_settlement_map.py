#!/usr/bin/env python3
"""
Create settlement map for Kabuga ward using test data and settlement visualization tools.
"""

import os
import sys
import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import zipfile
import tempfile
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_test_data():
    """Load the test data from CSV"""
    try:
        csv_path = "data/testdata/kano_test_data.csv"
        df = pd.read_csv(csv_path)
        logger.info(f"✅ Loaded {len(df)} wards from test data")
        return df
    except Exception as e:
        logger.error(f"Error loading test data: {e}")
        return None

def load_shapefile():
    """Load the shapefile from the zip file"""
    try:
        zip_path = "data/testdata/Kano_shapefile (1).zip"
        
        # Extract to temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Find the shapefile
            shp_files = [f for f in os.listdir(temp_dir) if f.endswith('.shp')]
            if not shp_files:
                logger.error("No shapefile found in the zip")
                return None
            
            shp_path = os.path.join(temp_dir, shp_files[0])
            gdf = gpd.read_file(shp_path)
            logger.info(f"✅ Loaded shapefile with {len(gdf)} features")
            
            # Copy to avoid file access issues after temp dir cleanup
            return gdf.copy()
            
    except Exception as e:
        logger.error(f"Error loading shapefile: {e}")
        return None

def create_settlement_map_for_kabuga():
    """Create settlement map specifically for Kabuga ward"""
    
    # Load data
    df = load_test_data()
    gdf = load_shapefile()
    
    if df is None or gdf is None:
        logger.error("Failed to load data")
        return None
    
    # Find Kabuga ward data
    kabuga_data = df[df['WardName'] == 'Kabuga']
    if kabuga_data.empty:
        logger.error("Kabuga ward not found in data")
        return None
    
    kabuga_row = kabuga_data.iloc[0]
    settlement_type_value = kabuga_row['settlement type ']
    
    logger.info(f"📊 Kabuga ward settlement type value: {settlement_type_value}")
    
    # Match ward with shapefile (assuming WardName column exists)
    if 'WardName' in gdf.columns:
        kabuga_geom = gdf[gdf['WardName'] == 'Kabuga']
    else:
        # Try different possible column names
        possible_cols = ['WARD_NAME', 'Ward_Name', 'ward_name', 'NAME', 'name']
        kabuga_geom = None
        for col in possible_cols:
            if col in gdf.columns:
                kabuga_geom = gdf[gdf[col] == 'Kabuga']
                if not kabuga_geom.empty:
                    break
    
    if kabuga_geom is None or kabuga_geom.empty:
        logger.warning("Kabuga ward geometry not found, using center coordinates")
        # Use approximate coordinates for Kano area
        center_lat, center_lon = 11.9804, 8.5201
    else:
        # Get centroid of Kabuga ward
        centroid = kabuga_geom.geometry.centroid.iloc[0]
        center_lat, center_lon = centroid.y, centroid.x
        logger.info(f"📍 Kabuga ward center: {center_lat:.4f}, {center_lon:.4f}")
    
    # Interpret settlement type value
    # Values seem to be between 0-1, where closer to 1 might indicate more formal settlement
    if settlement_type_value >= 0.7:
        settlement_category = "Mostly Formal"
        color = "#00FF00"  # Green
    elif settlement_type_value >= 0.4:
        settlement_category = "Mixed Settlement"
        color = "#FFA500"  # Orange
    else:
        settlement_category = "Mostly Informal"
        color = "#FF0000"  # Red
    
    # Create the map
    fig = go.Figure()
    
    # Add Kabuga ward boundary if available
    if kabuga_geom is not None and not kabuga_geom.empty:
        # Convert geometry to plotly format
        geom = kabuga_geom.geometry.iloc[0]
        if geom.geom_type == 'Polygon':
            x, y = geom.exterior.coords.xy
            fig.add_trace(go.Scattermapbox(
                lon=list(x),
                lat=list(y),
                mode='lines',
                fill='toself',
                fillcolor=color,
                line=dict(width=3, color=color),
                opacity=0.6,
                name=f'Kabuga Ward ({settlement_category})',
                hovertemplate=f'<b>Kabuga Ward</b><br>' +
                             f'Settlement Type Score: {settlement_type_value:.3f}<br>' +
                             f'Category: {settlement_category}<br>' +
                             f'Population: {kabuga_row["total population "]:,}<br>' +
                             f'Test Positivity Rate: {kabuga_row["test positivity rate "]:.3f}<br>' +
                             f'Housing Quality: {kabuga_row["housing quality "]:.3f}<br>' +
                             '<extra></extra>',
            ))
    
    # Add center point marker
    fig.add_trace(go.Scattermapbox(
        lon=[center_lon],
        lat=[center_lat],
        mode='markers',
        marker=dict(size=15, color=color, symbol='circle'),
        name='Kabuga Center',
        hovertemplate=f'<b>Kabuga Ward Center</b><br>' +
                     f'Settlement Type: {settlement_category}<br>' +
                     f'Score: {settlement_type_value:.3f}<br>' +
                     '<extra></extra>',
    ))
    
    # Configure map layout
    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=13
        ),
        title=dict(
            text=f"Settlement Map: Kabuga Ward<br>" +
                 f"<sub>Settlement Category: {settlement_category} (Score: {settlement_type_value:.3f})</sub>",
            x=0.5
        ),
        height=600,
        margin=dict(l=0, r=0, t=60, b=0),
        showlegend=True
    )
    
    # Save the map
    output_dir = "instance/uploads"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"kabuga_settlement_map_{timestamp}.html"
    filepath = os.path.join(output_dir, filename)
    
    fig.write_html(filepath)
    
    logger.info(f"✅ Settlement map created: {filepath}")
    
    # Display summary
    print("\n" + "="*60)
    print("🗺️  KABUGA WARD SETTLEMENT MAP SUMMARY")
    print("="*60)
    print(f"📍 Ward Name: Kabuga")
    print(f"📊 Settlement Type Score: {settlement_type_value:.3f}")
    print(f"🏘️  Settlement Category: {settlement_category}")
    print(f"👥 Population: {kabuga_row['total population ']:,}")
    print(f"🏠 Housing Quality: {kabuga_row['housing quality ']:.3f}")
    print(f"🦟 Test Positivity Rate: {kabuga_row['test positivity rate ']:.3f}")
    print(f"🌿 Enhanced Vegetation Index: {kabuga_row['enhance vegetation index ']:.3f}")
    print(f"💧 Distance to Waterbodies: {kabuga_row['distance to waterbodies ']} km")
    print(f"🌧️  Rainfall: {kabuga_row['rainfall ']:.3f}")
    print(f"🗂️  Map File: {filepath}")
    print("="*60)
    
    return {
        'status': 'success',
        'filepath': filepath,
        'ward_data': kabuga_row.to_dict(),
        'settlement_category': settlement_category,
        'map_center': {'lat': center_lat, 'lon': center_lon}
    }

if __name__ == "__main__":
    result = create_settlement_map_for_kabuga()
    if result and result['status'] == 'success':
        print(f"\n✅ Settlement map successfully created!")
        print(f"🌐 Open the file to view: {result['filepath']}")
    else:
        print("❌ Failed to create settlement map") 