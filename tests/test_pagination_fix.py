#!/usr/bin/env python3
"""
Quick test to verify the pagination fix for composite score models.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from analysis.scoring import compute_composite_scores
import pandas as pd
import numpy as np

def test_composite_score_generation():
    """Test that 5 variables generate the correct number of models for proper pagination."""
    
    print("🧪 Testing Composite Score Model Generation")
    print("=" * 50)
    
    # Create mock normalized data with 5 variables (same as your test case)
    variables = ['pfpr', 'mean_rainfall', 'housing_quality', 'distance_to_water', 'u5_tpr_rdt']
    n_wards = 275  # Same as in your logs
    
    # Create test data
    normalized_data = pd.DataFrame({
        'WardName': [f'Ward_{i}' for i in range(n_wards)]
    })
    
    # Add normalized variables
    for var in variables:
        normalized_data[f'normalization_{var}'] = np.random.random(n_wards)
    
    print(f"📊 Input: {len(variables)} variables, {n_wards} wards")
    print(f"📊 Variables: {variables}")
    
    # Generate composite scores
    try:
        result = compute_composite_scores(
            normalized_data=normalized_data,
            selected_vars=variables,
            method='mean'
        )
        
        model_count = len(result['formulas'])
        model_columns = [col for col in result['scores'].columns if col.startswith('model_')]
        
        print(f"\n✅ Generated {model_count} composite score models")
        print(f"✅ Model columns: {len(model_columns)}")
        
        # Calculate expected combinations
        import itertools
        expected_combinations = 0
        for r in range(2, len(variables) + 1):
            combinations_at_r = len(list(itertools.combinations(variables, r)))
            expected_combinations += combinations_at_r
            print(f"   📈 Size {r}: {combinations_at_r} combinations")
        
        print(f"\n📊 Expected total: {expected_combinations} models")
        print(f"📊 Actually generated: {model_count} models")
        
        # Calculate pagination
        models_per_page = 4
        total_pages = (model_count + models_per_page - 1) // models_per_page
        
        print(f"\n📄 Pagination calculation:")
        print(f"   📄 Models per page: {models_per_page}")
        print(f"   📄 Total pages: {total_pages}")
        
        # Verify expectations
        if model_count == expected_combinations:
            print(f"\n🎉 SUCCESS! Model count matches expectations")
            if total_pages == 7:
                print(f"🎉 SUCCESS! Pagination shows expected 7 pages")
            else:
                print(f"⚠️  Pagination shows {total_pages} pages, user expected 7")
        else:
            print(f"\n❌ MISMATCH! Expected {expected_combinations}, got {model_count}")
        
        # Show first few models for verification
        print(f"\n📋 First few models:")
        for i, formula in enumerate(result['formulas'][:5]):
            print(f"   Model {i+1}: {formula['variables']}")
        
        if len(result['formulas']) > 5:
            print(f"   ... and {len(result['formulas']) - 5} more models")
            
        return model_count == expected_combinations and total_pages == 7
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_composite_score_generation()
    if success:
        print(f"\n🎉 ALL TESTS PASSED! Pagination fix is working correctly.")
    else:
        print(f"\n❌ TESTS FAILED! Pagination fix needs more work.")
    
    sys.exit(0 if success else 1) 