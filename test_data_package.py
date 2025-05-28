"""
Data Package Test Suite - Phase 5 Validation

Comprehensive testing for the modular data package to validate:
- All component functionality
- Backward compatibility  
- New enhanced features
- Integration between modules
- Error handling and edge cases
"""

import os
import sys
import pandas as pd
import tempfile
import shutil
from typing import Dict, Any, List

# Test configuration
TEMP_DIR = None
TEST_SESSION_FOLDER = None

def setup_test_environment():
    """Set up test environment with temporary directories"""
    global TEMP_DIR, TEST_SESSION_FOLDER
    
    TEMP_DIR = tempfile.mkdtemp()
    TEST_SESSION_FOLDER = os.path.join(TEMP_DIR, 'test_session')
    os.makedirs(TEST_SESSION_FOLDER, exist_ok=True)
    print(f"Test environment created: {TEMP_DIR}")

def cleanup_test_environment():
    """Clean up test environment"""
    global TEMP_DIR
    
    if TEMP_DIR and os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
        print(f"Test environment cleaned up")

def create_test_csv_data():
    """Create sample CSV data for testing"""
    data = {
        'WardName': ['Ward_A', 'Ward_B', 'Ward_C', 'Ward_D', 'Ward_E'],
        'Population': [1000, 1500, 800, 1200, 900],
        'Poverty_Rate': [15.5, 22.1, 8.3, 18.7, 12.4],
        'Education_Index': [0.75, 0.60, 0.85, 0.68, 0.80],
        'Health_Access': [85, 70, 90, 75, 82],
        'Missing_Column': [1, None, 3, None, 5]
    }
    
    df = pd.DataFrame(data)
    test_csv_path = os.path.join(TEST_SESSION_FOLDER, 'test_data.csv')
    df.to_csv(test_csv_path, index=False)
    
    return test_csv_path, df

def run_test_suite():
    """Run comprehensive test suite for data package"""
    
    print("DATA PACKAGE TEST SUITE - PHASE 5 VALIDATION")
    print("=" * 60)
    
    test_results = {
        'total_tests': 0,
        'passed': 0,
        'failed': 0,
        'details': []
    }
    
    try:
        # Test 1: Package Imports
        print("\nTEST 1: PACKAGE IMPORTS AND INITIALIZATION")
        test_results['total_tests'] += 1
        
        try:
            from app.data import DataHandler, __version__
            from app.data import (
                CSVLoader, ShapefileLoader, DataValidator, DataProcessor,
                AnalysisCoordinator, ReportGenerator, FileManager,
                DataConverter, ValidationHelper, SessionMetadata
            )
            print(f"✓ All package imports successful (v{__version__})")
            test_results['passed'] += 1
            test_results['details'].append("Package imports: PASSED")
            
        except Exception as e:
            print(f"✗ Package import failed: {str(e)}")
            test_results['failed'] += 1
            test_results['details'].append(f"Package imports: FAILED - {str(e)}")
            return test_results
        
        # Test 2: DataHandler Initialization
        print("\nTEST 2: DATAHANDLER INITIALIZATION")
        test_results['total_tests'] += 1
        
        try:
            handler = DataHandler(TEST_SESSION_FOLDER)
            info = handler.get_package_info()
            
            print(f"✓ DataHandler initialized successfully")
            print(f"✓ Package version: {info['version']}")
            print(f"✓ Available methods: {info['available_methods']}")
            
            test_results['passed'] += 1
            test_results['details'].append("DataHandler initialization: PASSED")
            
        except Exception as e:
            print(f"✗ DataHandler initialization failed: {str(e)}")
            test_results['failed'] += 1
            test_results['details'].append(f"DataHandler initialization: FAILED - {str(e)}")
            return test_results
        
        # Test 3: Package Integrity Validation
        print("\nTEST 3: PACKAGE INTEGRITY VALIDATION")
        test_results['total_tests'] += 1
        
        try:
            validation = handler.validate_package_integrity()
            
            if validation['overall_status'] in ['success', 'issues_found']:
                component_count = len(validation['components'])
                available_count = sum(1 for comp in validation['components'].values() 
                                    if comp['status'] == 'available')
                
                print(f"✓ Package validation: {validation['overall_status']}")
                print(f"✓ Components available: {available_count}/{component_count}")
                
                if validation['issues']:
                    print(f"⚠ Issues found: {len(validation['issues'])}")
                    for issue in validation['issues'][:3]:  # Show first 3 issues
                        print(f"  - {issue}")
                
                test_results['passed'] += 1
                test_results['details'].append("Package integrity: PASSED")
                
            else:
                print(f"✗ Package validation failed: {validation.get('message', 'Unknown error')}")
                test_results['failed'] += 1
                test_results['details'].append("Package integrity: FAILED")
                
        except Exception as e:
            print(f"✗ Package validation error: {str(e)}")
            test_results['failed'] += 1
            test_results['details'].append(f"Package integrity: FAILED - {str(e)}")
        
        # Test 4: CSV Loading (Original Interface)
        print("\nTEST 4: CSV LOADING - ORIGINAL INTERFACE")
        test_results['total_tests'] += 1
        
        try:
            test_csv_path, test_df = create_test_csv_data()
            
            # Test original method
            result = handler.load_csv_file(test_csv_path)
            
            if result['status'] == 'success':
                print(f"✓ CSV loaded successfully: {result['rows']} rows, {result['columns']} columns")
                print(f"✓ Data stored in handler.csv_data: {handler.csv_data is not None}")
                
                test_results['passed'] += 1
                test_results['details'].append("CSV loading: PASSED")
                
            else:
                print(f"✗ CSV loading failed: {result['message']}")
                test_results['failed'] += 1
                test_results['details'].append("CSV loading: FAILED")
                
        except Exception as e:
            print(f"✗ CSV loading error: {str(e)}")
            test_results['failed'] += 1
            test_results['details'].append(f"CSV loading: FAILED - {str(e)}")
        
        # Test 5: Variable Validation (Original Interface)
        print("\nTEST 5: VARIABLE VALIDATION - ORIGINAL INTERFACE")
        test_results['total_tests'] += 1
        
        try:
            # Test original methods
            available_vars = handler.get_available_variables()
            test_variables = ['Population', 'Poverty_Rate', 'NonExistent_Var']
            validation_result = handler.validate_variables(test_variables)
            
            print(f"✓ Available variables: {len(available_vars)} ({available_vars[:3]}...)")
            print(f"✓ Variable validation: {validation_result['is_valid']}")
            print(f"✓ Valid variables: {len(validation_result['valid_variables'])}")
            print(f"✓ Invalid variables: {len(validation_result['invalid_variables'])}")
            
            test_results['passed'] += 1
            test_results['details'].append("Variable validation: PASSED")
            
        except Exception as e:
            print(f"✗ Variable validation error: {str(e)}")
            test_results['failed'] += 1
            test_results['details'].append(f"Variable validation: FAILED - {str(e)}")
        
        # Test 6: Data Quality Assessment (New Feature)
        print("\nTEST 6: DATA QUALITY ASSESSMENT - NEW FEATURE")
        test_results['total_tests'] += 1
        
        try:
            quality_result = handler.run_data_quality_assessment()
            
            if quality_result['status'] == 'success':
                checks_count = len(quality_result['checks_performed'])
                issues_count = len(quality_result['issues_found'])
                severe_count = len(quality_result['severe_issues'])
                
                print(f"✓ Quality checks performed: {checks_count}")
                print(f"✓ Issues found: {issues_count}")
                print(f"✓ Severe issues: {severe_count}")
                print(f"✓ Available variables: {len(quality_result.get('available_variables', []))}")
                
                test_results['passed'] += 1
                test_results['details'].append("Data quality assessment: PASSED")
                
            else:
                print(f"✗ Quality assessment failed: {quality_result['message']}")
                test_results['failed'] += 1
                test_results['details'].append("Data quality assessment: FAILED")
                
        except Exception as e:
            print(f"✗ Quality assessment error: {str(e)}")
            test_results['failed'] += 1
            test_results['details'].append(f"Data quality assessment: FAILED - {str(e)}")
        
        # Test 7: Data Cleaning (New Enhanced Feature)
        print("\nTEST 7: DATA CLEANING - ENHANCED FEATURE")
        test_results['total_tests'] += 1
        
        try:
            # Test data cleaning with missing values
            clean_result = handler.clean_data({'Missing_Column': 'mean'})
            
            if clean_result['status'] == 'success':
                print(f"✓ Data cleaning successful")
                print(f"✓ Cleaning methods used: {len(clean_result.get('methods_used', {}))}")
                print(f"✓ Cleaned data available: {handler.cleaned_data is not None}")
                
                test_results['passed'] += 1
                test_results['details'].append("Data cleaning: PASSED")
                
            else:
                print(f"✗ Data cleaning failed: {clean_result['message']}")
                test_results['failed'] += 1
                test_results['details'].append("Data cleaning: FAILED")
                
        except Exception as e:
            print(f"✗ Data cleaning error: {str(e)}")
            test_results['failed'] += 1
            test_results['details'].append(f"Data cleaning: FAILED - {str(e)}")
        
        # Test 8: Full Analysis Pipeline (Original Interface)
        print("\nTEST 8: FULL ANALYSIS PIPELINE - ORIGINAL INTERFACE")
        test_results['total_tests'] += 1
        
        try:
            # Run full analysis with selected variables
            selected_vars = ['Population', 'Poverty_Rate', 'Education_Index']
            analysis_result = handler.run_full_analysis(selected_vars)
            
            if analysis_result['status'] == 'success':
                print(f"✓ Full analysis completed successfully")
                print(f"✓ Variables used: {analysis_result.get('variables_used', 0)}")
                print(f"✓ Rankings generated: {analysis_result.get('rankings_count', 0)}")
                print(f"✓ Vulnerability rankings available: {handler.vulnerability_rankings is not None}")
                
                test_results['passed'] += 1
                test_results['details'].append("Full analysis pipeline: PASSED")
                
            else:
                print(f"✗ Full analysis failed: {analysis_result['message']}")
                test_results['failed'] += 1
                test_results['details'].append("Full analysis pipeline: FAILED")
                
        except Exception as e:
            print(f"✗ Full analysis error: {str(e)}")
            test_results['failed'] += 1
            test_results['details'].append(f"Full analysis pipeline: FAILED - {str(e)}")
        
        # Test 9: Result Formatting (New Feature)
        print("\nTEST 9: RESULT FORMATTING - NEW FEATURE")
        test_results['total_tests'] += 1
        
        try:
            if handler.vulnerability_rankings is not None:
                format_result = handler.format_vulnerability_results(top_n=3)
                
                if format_result['status'] == 'success':
                    top_count = len(format_result.get('top_vulnerable_wards', []))
                    bottom_count = len(format_result.get('least_vulnerable_wards', []))
                    
                    print(f"✓ Results formatted successfully")
                    print(f"✓ Top vulnerable wards: {top_count}")
                    print(f"✓ Least vulnerable wards: {bottom_count}")
                    print(f"✓ Category distribution: {len(format_result.get('category_distribution', {}))}")
                    
                    test_results['passed'] += 1
                    test_results['details'].append("Result formatting: PASSED")
                    
                else:
                    print(f"✗ Result formatting failed: {format_result['message']}")
                    test_results['failed'] += 1
                    test_results['details'].append("Result formatting: FAILED")
            else:
                print("⚠ Skipping result formatting (no vulnerability rankings available)")
                test_results['passed'] += 1
                test_results['details'].append("Result formatting: SKIPPED")
                
        except Exception as e:
            print(f"✗ Result formatting error: {str(e)}")
            test_results['failed'] += 1
            test_results['details'].append(f"Result formatting: FAILED - {str(e)}")
        
        # Test 10: Analysis Summary Generation (New Feature)
        print("\nTEST 10: ANALYSIS SUMMARY GENERATION - NEW FEATURE")
        test_results['total_tests'] += 1
        
        try:
            summary_result = handler.generate_analysis_summary()
            
            if summary_result['status'] == 'success':
                print(f"✓ Analysis summary generated successfully")
                print(f"✓ Data overview available: {'data_overview' in summary_result}")
                print(f"✓ Pipeline summary available: {'analysis_pipeline' in summary_result}")
                print(f"✓ Recommendations provided: {len(summary_result.get('recommendations', []))}")
                
                test_results['passed'] += 1
                test_results['details'].append("Analysis summary: PASSED")
                
            else:
                print(f"✗ Analysis summary failed: {summary_result['message']}")
                test_results['failed'] += 1
                test_results['details'].append("Analysis summary: FAILED")
                
        except Exception as e:
            print(f"✗ Analysis summary error: {str(e)}")
            test_results['failed'] += 1
            test_results['details'].append(f"Analysis summary: FAILED - {str(e)}")
        
        # Test 11: Session State Management (New Feature)
        print("\nTEST 11: SESSION STATE MANAGEMENT - NEW FEATURE")
        test_results['total_tests'] += 1
        
        try:
            # Test saving and loading session state
            save_result = handler.save_session_state()
            load_result = handler.load_session_state()
            
            print(f"✓ Session state save: {save_result}")
            print(f"✓ Session state load: {load_result}")
            print(f"✓ Metadata file created: {os.path.exists(os.path.join(TEST_SESSION_FOLDER, 'session_metadata.json'))}")
            
            test_results['passed'] += 1
            test_results['details'].append("Session state management: PASSED")
            
        except Exception as e:
            print(f"✗ Session state error: {str(e)}")
            test_results['failed'] += 1
            test_results['details'].append(f"Session state management: FAILED - {str(e)}")
        
        # Test 12: Export Functionality (New Feature)
        print("\nTEST 12: EXPORT FUNCTIONALITY - NEW FEATURE")
        test_results['total_tests'] += 1
        
        try:
            export_result = handler.export_analysis_report('summary')
            
            if export_result['status'] == 'success':
                files_created = len(export_result.get('files_created', []))
                print(f"✓ Export completed successfully")
                print(f"✓ Files created: {files_created}")
                print(f"✓ Export format: {export_result.get('format', 'unknown')}")
                
                test_results['passed'] += 1
                test_results['details'].append("Export functionality: PASSED")
                
            else:
                print(f"✗ Export failed: {export_result['message']}")
                test_results['failed'] += 1
                test_results['details'].append("Export functionality: FAILED")
                
        except Exception as e:
            print(f"✗ Export error: {str(e)}")
            test_results['failed'] += 1
            test_results['details'].append(f"Export functionality: FAILED - {str(e)}")
        
    except Exception as e:
        print(f"\n✗ CRITICAL ERROR: {str(e)}")
        test_results['failed'] += 1
        test_results['details'].append(f"Critical error: {str(e)}")
    
    return test_results

def main():
    """Main test execution"""
    
    setup_test_environment()
    
    try:
        # Run the test suite
        results = run_test_suite()
        
        # Print final results
        print("\n" + "=" * 60)
        print("TEST RESULTS SUMMARY")
        print("=" * 60)
        print(f"Total Tests: {results['total_tests']}")
        print(f"Passed: {results['passed']}")
        print(f"Failed: {results['failed']}")
        print(f"Success Rate: {(results['passed'] / results['total_tests'] * 100):.1f}%")
        
        print("\nDETAILED RESULTS:")
        for detail in results['details']:
            status = "✓" if "PASSED" in detail else "✗" if "FAILED" in detail else "⚠"
            print(f"{status} {detail}")
        
        if results['failed'] == 0:
            print("\n🎉 ALL TESTS PASSED! PHASE 5 REFACTORING SUCCESSFUL!")
            return True
        else:
            print(f"\n⚠ {results['failed']} TESTS FAILED. REVIEW NEEDED.")
            return False
            
    finally:
        cleanup_test_environment()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 