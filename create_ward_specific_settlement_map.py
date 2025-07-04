#!/usr/bin/env python3
"""
Create ward-specific settlement maps for any ward in the Kano test data.
This script provides comprehensive settlement visualization with detailed analytics.
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
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WardSettlementMapGenerator:
    """Generate ward-specific settlement maps with comprehensive analytics"""
    
    def __init__(self):
        self.df = None
        self.gdf = None
        self.load_data()
    
    def load_data(self):
        """Load the test data and shapefile"""
        try:
            # Load CSV data
            csv_path = "data/testdata/kano_test_data.csv"
            self.df = pd.read_csv(csv_path)
            logger.info(f"✅ Loaded {len(self.df)} wards from test data")
            
            # Load shapefile
            zip_path = "data/testdata/Kano_shapefile (1).zip"
            with tempfile.TemporaryDirectory() as temp_dir:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                shp_files = [f for f in os.listdir(temp_dir) if f.endswith('.shp')]
                if shp_files:
                    shp_path = os.path.join(temp_dir, shp_files[0])
                    self.gdf = gpd.read_file(shp_path).copy()
                    logger.info(f"✅ Loaded shapefile with {len(self.gdf)} features")
                else:
                    logger.warning("⚠️ No shapefile found, will use approximate coordinates")
                    self.gdf = None
                    
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            self.df = None
            self.gdf = None
    
    def get_available_wards(self):
        """Get list of available wards"""
        if self.df is None:
            return []
        return sorted(self.df['WardName'].unique())
    
    def get_ward_data(self, ward_name):
        """Get data for a specific ward"""
        if self.df is None:
            return None
        
        ward_data = self.df[self.df['WardName'] == ward_name]
        if ward_data.empty:
            return None
        
        return ward_data.iloc[0]
    
    def get_settlement_category(self, settlement_score):
        """Categorize settlement based on score"""
        if settlement_score >= 0.8:
            return "Highly Formal", "#00AA00", "🏢"
        elif settlement_score >= 0.6:
            return "Mostly Formal", "#00FF00", "🏘️"
        elif settlement_score >= 0.4:
            return "Mixed Settlement", "#FFA500", "🏚️"
        elif settlement_score >= 0.2:
            return "Mostly Informal", "#FF6600", "🏴"
        else:
            return "Highly Informal", "#FF0000", "🚨"
    
    def get_ward_geometry(self, ward_name):
        """Get geometry for a specific ward"""
        if self.gdf is None:
            return None
        
        # Try different possible column names for ward matching
        possible_cols = ['WardName', 'WARD_NAME', 'Ward_Name', 'ward_name', 'NAME', 'name']
        
        for col in possible_cols:
            if col in self.gdf.columns:
                ward_geom = self.gdf[self.gdf[col] == ward_name]
                if not ward_geom.empty:
                    return ward_geom.iloc[0]
        
        return None
    
    def create_ward_map(self, ward_name, include_neighbors=True, zoom_level=13):
        """Create comprehensive settlement map for a specific ward"""
        
        # Get ward data
        ward_data = self.get_ward_data(ward_name)
        if ward_data is None:
            logger.error(f"Ward '{ward_name}' not found")
            return None
        
        # Get settlement analysis
        settlement_score = ward_data['settlement type ']
        category, color, icon = self.get_settlement_category(settlement_score)
        
        # Get ward geometry
        ward_geom = self.get_ward_geometry(ward_name)
        
        # Determine center coordinates
        if ward_geom is not None and hasattr(ward_geom, 'geometry'):
            centroid = ward_geom.geometry.centroid
            center_lat, center_lon = centroid.y, centroid.x
        else:
            # Use approximate coordinates for Kano area
            center_lat, center_lon = 11.9804, 8.5201
        
        # Create the map
        fig = go.Figure()
        
        # Add ward boundary if available
        if ward_geom is not None and hasattr(ward_geom, 'geometry'):
            geom = ward_geom.geometry
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
                    name=f'{ward_name} Ward',
                    hovertemplate=self._build_hover_info(ward_name, ward_data, category),
                ))
        
        # Add center point marker
        fig.add_trace(go.Scattermapbox(
            lon=[center_lon],
            lat=[center_lat],
            mode='markers+text',
            marker=dict(size=20, color=color, symbol='circle'),
            text=[f'{icon} {ward_name}'],
            textposition="top center",
            textfont=dict(size=14, color="black"),
            name=f'{ward_name} Center',
            hovertemplate=self._build_hover_info(ward_name, ward_data, category),
        ))
        
        # Add neighboring wards if requested
        if include_neighbors and self.gdf is not None:
            self._add_neighbor_wards(fig, ward_name, center_lat, center_lon)
        
        # Configure map layout
        fig.update_layout(
            mapbox=dict(
                style="open-street-map",
                center=dict(lat=center_lat, lon=center_lon),
                zoom=zoom_level
            ),
            title=dict(
                text=f"Settlement Analysis: {ward_name} Ward<br>" +
                     f"<sub>{icon} {category} | Score: {settlement_score:.3f} | Population: {ward_data['total population ']:,}</sub>",
                x=0.5,
                font=dict(size=16)
            ),
            height=700,
            margin=dict(l=0, r=0, t=80, b=0),
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255,255,255,0.8)"
            )
        )
        
        return fig, ward_data, category
    
    def _add_neighbor_wards(self, fig, target_ward, center_lat, center_lon, radius_km=10):
        """Add neighboring wards to the map"""
        try:
            # Calculate distances to all wards (simplified approach)
            neighbors = []
            for _, row in self.df.iterrows():
                if row['WardName'] != target_ward:
                    # Simple distance estimation (not accurate for large distances)
                    ward_geom = self.get_ward_geometry(row['WardName'])
                    if ward_geom is not None and hasattr(ward_geom, 'geometry'):
                        neighbor_centroid = ward_geom.geometry.centroid
                        # Simple distance calculation
                        distance = ((neighbor_centroid.y - center_lat)**2 + 
                                  (neighbor_centroid.x - center_lon)**2)**0.5
                        if distance < 0.1:  # Approximate neighboring radius
                            neighbors.append((row['WardName'], row['settlement type '], 
                                           neighbor_centroid.y, neighbor_centroid.x))
            
            # Add neighbor markers
            for neighbor_name, neighbor_score, lat, lon in neighbors[:10]:  # Limit to 10 neighbors
                _, neighbor_color, neighbor_icon = self.get_settlement_category(neighbor_score)
                
                fig.add_trace(go.Scattermapbox(
                    lon=[lon],
                    lat=[lat],
                    mode='markers+text',
                    marker=dict(size=12, color=neighbor_color, symbol='circle', opacity=0.7),
                    text=[f'{neighbor_icon}'],
                    textposition="middle center",
                    textfont=dict(size=10),
                    name=f'Neighbor: {neighbor_name}',
                    hovertemplate=f'<b>{neighbor_name}</b><br>' +
                                 f'Settlement Score: {neighbor_score:.3f}<br>' +
                                 '<extra></extra>',
                ))
        except Exception as e:
            logger.warning(f"Could not add neighbor wards: {e}")
    
    def _build_hover_info(self, ward_name, ward_data, category):
        """Build detailed hover information"""
        return (
            f'<b>{ward_name} Ward</b><br>' +
            f'🏘️ Settlement: {category}<br>' +
            f'📊 Score: {ward_data["settlement type "]:.3f}<br>' +
            f'👥 Population: {ward_data["total population "]:,}<br>' +
            f'🦟 Test Positivity: {ward_data["test positivity rate "]:.3f}<br>' +
            f'🏠 Housing Quality: {ward_data["housing quality "]:.3f}<br>' +
            f'🌿 Vegetation Index: {ward_data["enhance vegetation index "]:.3f}<br>' +
            f'💧 Distance to Water: {ward_data["distance to waterbodies "]:.3f} km<br>' +
            f'🌧️ Rainfall: {ward_data["rainfall"]:.3f}<br>' +
            f'🛏️ Nets per Capita: {ward_data["nets per capita"]:.3f}<br>' +
            '<extra></extra>'
        )
    
    def save_map(self, fig, ward_name, category, ward_data):
        """Save the map and return summary"""
        # Create output directory
        output_dir = "instance/uploads"
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{ward_name.lower().replace(' ', '_')}_settlement_map_{timestamp}.html"
        filepath = os.path.join(output_dir, filename)
        
        # Save map
        fig.write_html(filepath)
        logger.info(f"✅ Settlement map saved: {filepath}")
        
        return filepath, self._generate_summary(ward_name, category, ward_data, filepath)
    
    def _generate_summary(self, ward_name, category, ward_data, filepath):
        """Generate comprehensive summary"""
        settlement_score = ward_data['settlement type ']
        
        # Risk assessment
        risk_factors = []
        if ward_data['test positivity rate '] > 0.6:
            risk_factors.append("High malaria test positivity")
        if ward_data['housing quality '] < 3:
            risk_factors.append("Poor housing quality")
        if ward_data['distance to waterbodies '] < 2:
            risk_factors.append("Close to water bodies")
        if ward_data['nets per capita'] < 2:
            risk_factors.append("Low ITN coverage")
        
        risk_level = "HIGH" if len(risk_factors) >= 3 else "MEDIUM" if len(risk_factors) >= 2 else "LOW"
        
        summary = f"""
🗺️  WARD-SPECIFIC SETTLEMENT MAP ANALYSIS
{'='*60}
📍 Ward: {ward_name}
🏘️  Settlement Category: {category}
📊 Settlement Score: {settlement_score:.3f}
⚠️  Risk Level: {risk_level}

📈 KEY INDICATORS:
👥 Population: {ward_data['total population ']:,}
🏠 Housing Quality: {ward_data['housing quality ']:.3f}/10
🦟 Test Positivity Rate: {ward_data['test positivity rate ']:.3f}
🛏️ Nets per Capita: {ward_data['nets per capita']:.3f}
🌿 Vegetation Index: {ward_data['enhance vegetation index ']:.3f}
💧 Distance to Water: {ward_data['distance to waterbodies ']:.3f} km
🌧️ Rainfall: {ward_data['rainfall']:.3f}

🚨 RISK FACTORS:
{('• ' + chr(10) + '• '.join(risk_factors)) if risk_factors else '• None identified'}

🗂️  Map File: {filepath}
{'='*60}
"""
        return summary

def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(description='Generate ward-specific settlement maps')
    parser.add_argument('ward_name', nargs='?', help='Name of the ward to analyze')
    parser.add_argument('--list', action='store_true', help='List available wards')
    parser.add_argument('--no-neighbors', action='store_true', help='Don\'t include neighboring wards')
    parser.add_argument('--zoom', type=int, default=13, help='Map zoom level (default: 13)')
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = WardSettlementMapGenerator()
    
    if generator.df is None:
        print("❌ Error: Could not load data")
        return
    
    # List available wards
    if args.list:
        wards = generator.get_available_wards()
        print(f"\n📋 Available Wards ({len(wards)}):")
        print("=" * 40)
        for i, ward in enumerate(wards, 1):
            print(f"{i:2d}. {ward}")
        return
    
    # Get ward name
    if not args.ward_name:
        print("\n🗺️  Ward Settlement Map Generator")
        print("=" * 40)
        print("Available wards:")
        wards = generator.get_available_wards()
        for i, ward in enumerate(wards[:10], 1):
            print(f"{i:2d}. {ward}")
        if len(wards) > 10:
            print(f"... and {len(wards) - 10} more (use --list to see all)")
        
        ward_name = input("\nEnter ward name: ").strip()
        if not ward_name:
            return
    else:
        ward_name = args.ward_name
    
    # Generate map
    result = generator.create_ward_map(
        ward_name, 
        include_neighbors=not args.no_neighbors,
        zoom_level=args.zoom
    )
    
    if result is None:
        print(f"❌ Error: Could not generate map for '{ward_name}'")
        available_wards = generator.get_available_wards()
        close_matches = [w for w in available_wards if ward_name.lower() in w.lower()]
        if close_matches:
            print(f"💡 Did you mean: {', '.join(close_matches[:3])}")
        return
    
    fig, ward_data, category = result
    
    # Save map and show summary
    filepath, summary = generator.save_map(fig, ward_name, category, ward_data)
    print(summary)

if __name__ == "__main__":
    main() 