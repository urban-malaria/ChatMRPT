#!/usr/bin/env python3
"""
Comprehensive Test Suite for app.interaction Package

This test suite validates the Phase 4 refactoring of the InteractionLogger
from a monolithic class into a modular package architecture.

Test Coverage:
- Core functionality (database, sessions)
- Event logging (all event types)
- Storage and retrieval (queries, exports)
- Utilities and validation
- Backward compatibility
- Package integrity

Version: 1.0.0
"""

import os
import sys
import tempfile
import shutil
import datetime
import json
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_interaction_package():
    """
    Comprehensive test suite for the interaction package
    """
    print("INTERACTION PACKAGE TEST SUITE")
    print("=" * 60)
    
    test_results = {
        'total_tests': 0,
        'passed': 0,
        'failed': 0,
        'errors': []
    }
    
    # Test 1: Package Import
    print("\nTEST 1: PACKAGE IMPORTS")
    try:
        # Test importing the main package
        from app.interaction import InteractionLogger, __version__
        print(f"✓ Main package import successful (v{__version__})")
        
        # Test importing individual modules
        from app.interaction.core import DatabaseManager, SessionManager
        from app.interaction.events import EventLogger
        from app.interaction.storage import StorageManager
        from app.interaction.utils import DataValidator, InteractionTimer
        print("✓ All module imports successful")
        
        test_results['passed'] += 1
    except Exception as e:
        print(f"✗ Package import failed: {e}")
        test_results['failed'] += 1
        test_results['errors'].append(f"Import error: {e}")
    
    test_results['total_tests'] += 1
    
    # Test 2: Database Initialization
    print("\nTEST 2: DATABASE INITIALIZATION")
    try:
        # Create temporary database for testing
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as temp_db:
            temp_db_path = temp_db.name
        
        # Test DatabaseManager initialization
        from app.interaction.core import DatabaseManager
        db_manager = DatabaseManager(temp_db_path)
        print("✓ Database manager created successfully")
        
        # Test database connection
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # Verify tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        expected_tables = [
            'sessions', 'messages', 'file_uploads', 'analysis_events',
            'errors', 'analysis_steps', 'algorithm_decisions', 'calculations',
            'anomalies', 'variable_relationships', 'ward_rankings',
            'visualization_metadata', 'llm_interactions', 'explanations'
        ]
        
        missing_tables = set(expected_tables) - set(tables)
        if missing_tables:
            raise Exception(f"Missing tables: {missing_tables}")
        
        print(f"✓ All {len(expected_tables)} database tables created successfully")
        conn.close()
        
        test_results['passed'] += 1
    except Exception as e:
        print(f"✗ Database initialization failed: {e}")
        test_results['failed'] += 1
        test_results['errors'].append(f"Database error: {e}")
    
    test_results['total_tests'] += 1
    
    # Test 3: Core Functionality
    print("\nTEST 3: CORE FUNCTIONALITY")
    try:
        from app.interaction.core import SessionManager, log_message, log_error
        
        # Test session management
        session_manager = SessionManager(db_manager)
        test_session_id = "test_session_123"
        
        # Test session start
        result = session_manager.log_session_start(test_session_id, "test_browser", "127.0.0.1")
        if not result:
            raise Exception("Session start failed")
        print("✓ Session management working")
        
        # Test message logging
        message_id = log_message(db_manager, test_session_id, "user", "Hello, test message!")
        if not message_id:
            raise Exception("Message logging failed")
        print("✓ Message logging working")
        
        # Test error logging
        error_id = log_error(db_manager, test_session_id, "test_error", "Test error message")
        if not error_id:
            raise Exception("Error logging failed")
        print("✓ Error logging working")
        
        test_results['passed'] += 1
    except Exception as e:
        print(f"✗ Core functionality failed: {e}")
        test_results['failed'] += 1
        test_results['errors'].append(f"Core error: {e}")
    
    test_results['total_tests'] += 1
    
    # Test 4: Event Logging
    print("\nTEST 4: EVENT LOGGING")
    try:
        from app.interaction.events import EventLogger
        
        event_logger = EventLogger(db_manager)
        
        # Test various event types
        events_to_test = [
            ('file_upload', lambda: event_logger.log_file_upload(test_session_id, "csv", "test.csv", 1024)),
            ('analysis_event', lambda: event_logger.log_analysis_event(test_session_id, "visualization", {"type": "test"})),
            ('analysis_step', lambda: event_logger.log_analysis_step(test_session_id, "test_step", {"input": "test"})),
            ('visualization_metadata', lambda: event_logger.log_visualization_metadata(test_session_id, "variable_map", ["var1"])),
            ('llm_interaction', lambda: event_logger.log_llm_interaction(test_session_id, "explanation", "Test prompt")),
            ('explanation', lambda: event_logger.log_explanation(test_session_id, "ward", "test_ward", "what", "Test question", "Test explanation"))
        ]
        
        for event_name, event_func in events_to_test:
            result = event_func()
            if not result:
                raise Exception(f"{event_name} logging failed")
        
        print(f"✓ All {len(events_to_test)} event types logged successfully")
        test_results['passed'] += 1
    except Exception as e:
        print(f"✗ Event logging failed: {e}")
        test_results['failed'] += 1
        test_results['errors'].append(f"Event error: {e}")
    
    test_results['total_tests'] += 1
    
    # Test 5: Storage and Retrieval
    print("\nTEST 5: STORAGE AND RETRIEVAL")
    try:
        from app.interaction.storage import StorageManager
        
        storage_manager = StorageManager(db_manager)
        
        # Test session history retrieval
        history = storage_manager.get_session_history(test_session_id)
        if not isinstance(history, list):
            raise Exception("Session history retrieval failed")
        print(f"✓ Session history retrieved ({len(history)} messages)")
        
        # Test analysis metadata retrieval
        metadata = storage_manager.get_analysis_metadata(test_session_id)
        if not isinstance(metadata, dict):
            raise Exception("Analysis metadata retrieval failed")
        print("✓ Analysis metadata retrieved")
        
        # Test explanations retrieval
        explanations = storage_manager.get_explanations(test_session_id)
        if not isinstance(explanations, list):
            raise Exception("Explanations retrieval failed")
        print(f"✓ Explanations retrieved ({len(explanations)} explanations)")
        
        test_results['passed'] += 1
    except Exception as e:
        print(f"✗ Storage and retrieval failed: {e}")
        test_results['failed'] += 1
        test_results['errors'].append(f"Storage error: {e}")
    
    test_results['total_tests'] += 1
    
    # Test 6: Utilities and Validation
    print("\nTEST 6: UTILITIES AND VALIDATION")
    try:
        from app.interaction.utils import (
            validate_session_id, safe_json_parse, DataValidator, 
            InteractionTimer, create_export_filename
        )
        
        # Test session ID validation
        assert validate_session_id("valid_session") == True
        assert validate_session_id("") == False
        assert validate_session_id(None) == False
        print("✓ Session ID validation working")
        
        # Test JSON utilities
        test_dict = {"key": "value"}
        json_str = safe_json_parse('{"key": "value"}')
        assert json_str == test_dict
        print("✓ JSON utilities working")
        
        # Test data validation
        is_valid, error = DataValidator.validate_message_data("test_session", "user", "test content")
        assert is_valid == True
        print("✓ Data validation working")
        
        # Test timer
        with InteractionTimer("test_operation") as timer:
            pass  # Simulate some work
        assert timer.get_duration() is not None
        print("✓ Interaction timer working")
        
        # Test filename creation
        filename = create_export_filename("test", "session123", "json")
        assert "test" in filename and "session123" in filename
        print("✓ Filename utilities working")
        
        test_results['passed'] += 1
    except Exception as e:
        print(f"✗ Utilities and validation failed: {e}")
        test_results['failed'] += 1
        test_results['errors'].append(f"Utilities error: {e}")
    
    test_results['total_tests'] += 1
    
    # Test 7: Backward Compatibility
    print("\nTEST 7: BACKWARD COMPATIBILITY")
    try:
        from app.interaction import InteractionLogger
        
        # Test original interface
        logger = InteractionLogger(temp_db_path)
        
        # Test that all original methods still exist and work
        original_methods = [
            'log_session_start', 'log_message', 'log_error', 'log_file_upload',
            'log_analysis_event', 'get_session_history', 'export_to_json'
        ]
        
        for method_name in original_methods:
            assert hasattr(logger, method_name), f"Missing method: {method_name}"
        
        # Test a few key methods
        result = logger.log_session_start("compat_test_session", "browser_info")
        assert result == True
        
        message_id = logger.log_message("compat_test_session", "user", "Compatibility test message")
        assert message_id is not None
        
        history = logger.get_session_history("compat_test_session")
        assert isinstance(history, list)
        
        print(f"✓ All {len(original_methods)} original methods available and working")
        test_results['passed'] += 1
    except Exception as e:
        print(f"✗ Backward compatibility failed: {e}")
        test_results['failed'] += 1
        test_results['errors'].append(f"Compatibility error: {e}")
    
    test_results['total_tests'] += 1
    
    # Test 8: Package Validation
    print("\nTEST 8: PACKAGE VALIDATION")
    try:
        from app.interaction import validate_package, get_package_info
        
        # Test package info
        info = get_package_info()
        assert isinstance(info, dict)
        assert info['version'] == '1.0.0'
        assert len(info['modules']) == 4
        print("✓ Package info retrieval working")
        
        # Test package validation
        validation_result = validate_package()
        assert validation_result['status'] == 'success'
        print("✓ Package validation successful")
        
        test_results['passed'] += 1
    except Exception as e:
        print(f"✗ Package validation failed: {e}")
        test_results['failed'] += 1
        test_results['errors'].append(f"Package validation error: {e}")
    
    test_results['total_tests'] += 1
    
    # Cleanup
    try:
        os.unlink(temp_db_path)
    except:
        pass
    
    # Test Results Summary
    print("\n" + "=" * 60)
    print("TEST RESULTS SUMMARY")
    print("=" * 60)
    
    success_rate = (test_results['passed'] / test_results['total_tests']) * 100
    
    print(f"Total Tests: {test_results['total_tests']}")
    print(f"Passed: {test_results['passed']}")
    print(f"Failed: {test_results['failed']}")
    print(f"Success Rate: {success_rate:.1f}%")
    
    if test_results['failed'] > 0:
        print(f"\nERRORS ENCOUNTERED:")
        for i, error in enumerate(test_results['errors'], 1):
            print(f"{i}. {error}")
    
    if success_rate == 100:
        print(f"\nALL TESTS PASSED! PHASE 4 REFACTORING SUCCESSFUL!")
        return True
    else:
        print(f"\nSOME TESTS FAILED - NEEDS ATTENTION")
        return False


if __name__ == "__main__":
    try:
        success = test_interaction_package()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\nCRITICAL TEST FAILURE: {e}")
        sys.exit(1) 