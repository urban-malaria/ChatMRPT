#!/usr/bin/env python3
"""
Comprehensive Analysis Comparison Test
Identifies missing functionality between legacy and current implementation
"""

print("=== ANALYSIS FUNCTIONALITY COMPARISON ===")

# Test 1: Basic Analysis Pipeline
print("\n1. Testing Analysis Pipeline Components:")
try:
    from app.analysis.pipeline import run_full_analysis_pipeline
    from app.core.llm_manager import select_optimal_variables_with_llm, LLMManager
    from app.models.data_handler import DataHandler
    from app.analysis import AnalysisMetadata
    print("✅ Core analysis components imported")
except Exception as e:
    print(f"❌ Core analysis import error: {e}")

# Test 2: LLM Variable Selection (3-5 variables)
print("\n2. Testing LLM Variable Selection:")
try:
    llm_manager = LLMManager()
    available_vars = ['pfpr', 'temperature', 'rainfall', 'poverty_rate', 'population_density', 'distance_to_health']
    
    selected_vars, explanations = select_optimal_variables_with_llm(
        llm_manager=llm_manager,
        available_vars=available_vars,
        csv_data=None,
        min_vars=3,
        max_vars=5
    )
    
    print(f"✅ LLM selection: {len(selected_vars)} variables")
    print(f"   Variables: {selected_vars}")
    print(f"   Has explanations: {len(explanations) > 0}")
    
    if 3 <= len(selected_vars) <= 5:
        print("✅ Variable count in correct range (3-5)")
    else:
        print(f"⚠️  Variable count {len(selected_vars)} not in expected range 3-5")
        
except Exception as e:
    print(f"⚠️  LLM selection fallback: {e}")

# Test 3: Missing Legacy Functions
print("\n3. Checking Missing Legacy Functions:")

missing_functions = []

# Check explanation functions
try:
    from app.analysis.pipeline import get_explanation_for_visualization
    print("✅ get_explanation_for_visualization exists")
except ImportError:
    missing_functions.append("get_explanation_for_visualization")
    print("❌ get_explanation_for_visualization missing")

try:
    from app.analysis.pipeline import get_explanation_for_ward
    print("✅ get_explanation_for_ward exists")
except ImportError:
    missing_functions.append("get_explanation_for_ward")
    print("❌ get_explanation_for_ward missing")

try:
    from app.analysis.pipeline import get_explanation_for_analysis_result
    print("✅ get_explanation_for_analysis_result exists")
except ImportError:
    missing_functions.append("get_explanation_for_analysis_result")
    print("❌ get_explanation_for_analysis_result missing")

# Check report generation functions
try:
    from app.analysis.pipeline import generate_analysis_report
    print("✅ generate_analysis_report exists")
except ImportError:
    missing_functions.append("generate_analysis_report")
    print("❌ generate_analysis_report missing")

try:
    from app.analysis.pipeline import check_data_quality
    print("✅ check_data_quality exists")
except ImportError:
    missing_functions.append("check_data_quality")
    print("❌ check_data_quality missing")

# Test 4: Result Format Compatibility
print("\n4. Testing Result Format:")
try:
    import tempfile
    temp_dir = tempfile.mkdtemp()
    data_handler = DataHandler(temp_dir)
    
    # Check if run_full_analysis returns the expected format
    analysis_method = data_handler.run_full_analysis
    print(f"✅ run_full_analysis method exists")
    print(f"   Parameters: {analysis_method.__code__.co_varnames}")
    
    # Check if it has LLM integration
    if 'llm_manager' in analysis_method.__code__.co_varnames:
        print("✅ LLM integration available")
    else:
        print("❌ LLM integration missing")
        
except Exception as e:
    print(f"❌ DataHandler error: {e}")

# Test 5: Data Quality Assessment
print("\n5. Testing Data Quality Functions:")
try:
    from app.analysis.utils import check_data_quality
    print("✅ Data quality check in utils")
except ImportError:
    try:
        from app.data.validation import run_quality_assessment
        print("✅ Quality assessment in data validation")
    except ImportError:
        missing_functions.append("comprehensive_data_quality_check")
        print("❌ Data quality assessment missing")

# Test 6: Visualization Integration
print("\n6. Testing Visualization Integration:")
try:
    from app.visualization import create_vulnerability_map, create_analysis_charts
    print("✅ Visualization functions available")
except ImportError:
    print("⚠️  Visualization integration may be incomplete")

# Summary
print(f"\n=== SUMMARY ===")
print(f"Missing Functions: {len(missing_functions)}")
for func in missing_functions:
    print(f"  - {func}")

if missing_functions:
    print(f"\n❌ {len(missing_functions)} critical functions need implementation")
else:
    print(f"\n✅ All core functions available")

print("\n=== REQUIRED IMPLEMENTATIONS ===")
print("1. Explanation functions for ward analysis")
print("2. Comprehensive report generation")
print("3. Data quality assessment integration") 
print("4. Result format standardization")
print("5. Visualization pipeline integration") 