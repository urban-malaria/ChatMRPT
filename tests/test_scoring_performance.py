#!/usr/bin/env python3
"""
Test composite scoring performance fix
"""
import time
import pandas as pd
import numpy as np

print("=== TESTING COMPOSITE SCORING PERFORMANCE ===")

# Test 1: Import and basic setup
try:
    from app.analysis.scoring import compute_composite_scores
    from app.analysis.metadata import AnalysisMetadata
    print("✅ Imports successful")
except Exception as e:
    print(f"❌ Import error: {e}")
    exit(1)

# Test 2: Create test data
print("\n2. Creating test data...")
try:
    # Create sample normalized data
    ward_names = [f"Ward_{i}" for i in range(1, 51)]  # 50 wards
    test_data = pd.DataFrame({'WardName': ward_names})
    
    # Add 5 normalized variables (like LLM would select)
    variables = ['pfpr', 'rainfall', 'temperature', 'poverty_rate', 'population_density']
    for var in variables:
        col_name = f"normalization_{var}"
        test_data[col_name] = np.random.random(50)  # Random normalized values 0-1
    
    print(f"✅ Test data created: {len(test_data)} wards, {len(variables)} variables")
    print(f"   Variables: {variables}")
except Exception as e:
    print(f"❌ Test data error: {e}")
    exit(1)

# Test 3: Performance test with timer
print("\n3. Testing composite scoring performance...")
try:
    metadata = AnalysisMetadata()
    
    start_time = time.time()
    result = compute_composite_scores(
        normalized_data=test_data,
        selected_vars=variables,  # Use the 5 LLM-selected variables
        method='mean',
        metadata=metadata
    )
    end_time = time.time()
    
    execution_time = end_time - start_time
    
    print(f"✅ Composite scoring completed in {execution_time:.2f} seconds")
    print(f"   Models generated: {len(result['formulas'])}")
    print(f"   Wards scored: {len(result['scores'])}")
    
    # Check if it's fast enough (should be under 5 seconds)
    if execution_time < 5.0:
        print(f"✅ PERFORMANCE GOOD: {execution_time:.2f}s < 5.0s threshold")
    else:
        print(f"⚠️  PERFORMANCE SLOW: {execution_time:.2f}s >= 5.0s threshold")
        
except Exception as e:
    print(f"❌ Scoring error: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Check model formulas
print("\n4. Checking model formulas...")
try:
    if 'formulas' in result:
        print(f"✅ Generated {len(result['formulas'])} models:")
        for i, formula in enumerate(result['formulas'][:5]):  # Show first 5
            vars_used = ', '.join(formula['variables'])
            print(f"   {formula['model']}: {vars_used}")
        
        if len(result['formulas']) > 5:
            print(f"   ... and {len(result['formulas']) - 5} more models")
            
        # Check reasonable number
        if len(result['formulas']) <= 20:
            print(f"✅ REASONABLE MODEL COUNT: {len(result['formulas'])} <= 20")
        else:
            print(f"⚠️  TOO MANY MODELS: {len(result['formulas'])} > 20")
    else:
        print("❌ No formulas found in result")
        
except Exception as e:
    print(f"❌ Formula check error: {e}")

print("\n=== TEST SUMMARY ===")
try:
    if execution_time < 5.0 and len(result['formulas']) <= 20:
        print("🎉 SUCCESS: Composite scoring is now FAST and EFFICIENT!")
        print(f"   ⚡ Time: {execution_time:.2f}s")
        print(f"   📊 Models: {len(result['formulas'])}")
        print(f"   🏘️  Wards: {len(result['scores'])}")
    else:
        print("❌ Issues remain - needs further optimization")
except:
    print("❌ Test failed") 