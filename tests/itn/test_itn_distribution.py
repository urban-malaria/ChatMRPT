#!/usr/bin/env python3
"""Test ITN distribution pipeline with Kano session data."""

import sys
import os
import pandas as pd
import geopandas as gpd

# Add the app directory to the path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app.analysis.itn_pipeline import calculate_itn_distribution, detect_state, load_population_data
from app.services.data_handler import DataHandler

def load_test_data():
    """Load test data from the Kano session."""
    # Session ID from the Kano analysis
    session_id = "6e90b139-5d30-40fd-91ad-4af66fec5f00"
    
    # Create a mock data handler with the session data
    data_handler = DataHandler(session_id)
    
    # Load unified dataset
    unified_path = f"instance/uploads/{session_id}/unified_dataset.csv"
    if os.path.exists(unified_path):
        data_handler.unified_dataset = pd.read_csv(unified_path)
        print(f"Loaded unified dataset with {len(data_handler.unified_dataset)} wards")
    else:
        print(f"Error: Unified dataset not found at {unified_path}")
        return None
    
    # Load shapefile data
    shapefile_path = f"instance/uploads/{session_id}/shapefile/raw.shp"
    if os.path.exists(shapefile_path):
        data_handler.shapefile_data = gpd.read_file(shapefile_path)
        print(f"Loaded shapefile with {len(data_handler.shapefile_data)} features")
    else:
        print(f"Error: Shapefile not found at {shapefile_path}")
        return None
    
    # Load vulnerability rankings (composite)
    composite_path = f"instance/uploads/{session_id}/analysis_vulnerability_rankings.csv"
    if os.path.exists(composite_path):
        data_handler.vulnerability_rankings = pd.read_csv(composite_path)
        print(f"Loaded composite rankings with {len(data_handler.vulnerability_rankings)} wards")
    
    # Load vulnerability rankings (PCA)
    pca_path = f"instance/uploads/{session_id}/analysis_vulnerability_rankings_pca.csv"
    if os.path.exists(pca_path):
        data_handler.vulnerability_rankings_pca = pd.read_csv(pca_path)
        print(f"Loaded PCA rankings with {len(data_handler.vulnerability_rankings_pca)} wards")
    
    return data_handler

def test_state_detection(data_handler):
    """Test state detection."""
    print("\n=== Testing State Detection ===")
    detected_state = detect_state(data_handler)
    print(f"Detected state: {detected_state}")
    
    # Check if it correctly detects Kano
    assert detected_state == "Kano", f"Expected 'Kano' but got '{detected_state}'"
    print("✓ State detection passed")
    
    return detected_state

def test_population_loading(state):
    """Test population data loading."""
    print("\n=== Testing Population Data Loading ===")
    pop_data = load_population_data(state)
    
    if pop_data is None:
        print("✗ Failed to load population data")
        return None
    
    print(f"Loaded population data with {len(pop_data)} ward-LGA combinations")
    print(f"Total population: {pop_data['Population'].sum():,.0f}")
    
    # Check for duplicate ward names
    duplicate_wards = pop_data[pop_data.duplicated(subset=['WardName'], keep=False)]
    if len(duplicate_wards) > 0:
        print(f"Found {len(duplicate_wards)} duplicate ward entries across LGAs")
        dup_names = duplicate_wards['WardName'].unique()
        print(f"Sample duplicate ward names: {list(dup_names)[:5]}")
    
    print("✓ Population data loading passed")
    return pop_data

def test_ward_matching(data_handler, pop_data):
    """Test ward name matching between datasets."""
    print("\n=== Testing Ward Name Matching ===")
    
    # Get ward names from unified dataset
    unified_wards = data_handler.unified_dataset['WardName'].tolist()
    print(f"Unified dataset has {len(unified_wards)} wards")
    
    # Check for ward names with appended codes
    wards_with_codes = [w for w in unified_wards if '(' in w and ')' in w]
    print(f"Wards with appended codes: {len(wards_with_codes)}")
    if wards_with_codes:
        print(f"Sample wards with codes: {wards_with_codes[:5]}")
    
    # Extract original ward names
    import re
    original_names = [re.sub(r'\s*\([A-Z]{2}\d+\)$', '', w) for w in unified_wards]
    unique_original = set(original_names)
    print(f"Unique original ward names: {len(unique_original)}")
    
    # Check matching with population data
    pop_wards = set(pop_data['WardName'].str.lower())
    unified_wards_lower = set([w.lower() for w in original_names])
    
    matched = unified_wards_lower.intersection(pop_wards)
    unmatched_unified = unified_wards_lower - pop_wards
    unmatched_pop = pop_wards - unified_wards_lower
    
    print(f"Matched wards: {len(matched)}")
    print(f"Unmatched from unified: {len(unmatched_unified)}")
    print(f"Unmatched from population: {len(unmatched_pop)}")
    
    if unmatched_unified:
        print(f"Sample unmatched from unified: {list(unmatched_unified)[:5]}")
    
    print("✓ Ward matching analysis complete")
    
    return len(matched), len(unified_wards)

def test_itn_distribution(data_handler, session_id, method='composite'):
    """Test ITN distribution calculation."""
    print(f"\n=== Testing ITN Distribution ({method} method) ===")
    
    # Test with different net quantities
    test_configs = [
        {'nets': 10000, 'household_size': 5.0, 'urban_threshold': 30.0},
        {'nets': 50000, 'household_size': 5.0, 'urban_threshold': 30.0},
        {'nets': 100000, 'household_size': 5.0, 'urban_threshold': 30.0}
    ]
    
    for config in test_configs:
        print(f"\nTesting with {config['nets']:,} nets:")
        
        result = calculate_itn_distribution(
            data_handler,
            session_id,
            total_nets=config['nets'],
            avg_household_size=config['household_size'],
            urban_threshold=config['urban_threshold'],
            method=method
        )
        
        if result['status'] == 'error':
            print(f"✗ Error: {result['message']}")
            continue
        
        stats = result['stats']
        print(f"  Total nets: {stats['total_nets']:,}")
        print(f"  Allocated: {stats['allocated']:,}")
        print(f"  Remaining: {stats['remaining']:,}")
        print(f"  Coverage: {stats['coverage_percent']}%")
        print(f"  Prioritized wards (rural): {stats['prioritized_wards']}")
        print(f"  Reprioritized wards (urban): {stats['reprioritized_wards']}")
        print(f"  Total population: {stats['total_population']:,}")
        print(f"  Covered population: {stats['covered_population']:,}")
        
        # Check if map was generated
        if 'map_path' in result and os.path.exists(result['map_path']):
            print(f"  ✓ Map generated: {result['map_path']}")
        
        # Sample allocation details
        prioritized = result['prioritized']
        if len(prioritized) > 0:
            print(f"\n  Top 5 prioritized wards:")
            top_5 = prioritized.nlargest(5, 'nets_allocated')[['WardName', 'Population', 'nets_allocated', 'overall_rank']]
            print(top_5.to_string(index=False))
    
    print("\n✓ ITN distribution tests complete")

def main():
    """Run all tests."""
    print("Starting ITN Distribution Tests for Kano Data")
    print("=" * 60)
    
    # Load test data
    data_handler = load_test_data()
    if not data_handler:
        print("Failed to load test data")
        return
    
    # Test state detection
    state = test_state_detection(data_handler)
    
    # Test population data loading
    pop_data = test_population_loading(state)
    if pop_data is None:
        print("Cannot continue without population data")
        return
    
    # Test ward matching
    matched_count, total_wards = test_ward_matching(data_handler, pop_data)
    print(f"\nMatching rate: {(matched_count/total_wards)*100:.1f}%")
    
    # Test ITN distribution with both methods
    test_itn_distribution(data_handler, method='composite')
    test_itn_distribution(data_handler, method='pca')
    
    print("\n" + "=" * 60)
    print("All tests completed!")

if __name__ == "__main__":
    main()