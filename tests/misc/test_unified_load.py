#!/usr/bin/env python3
"""Test unified dataset loading fix."""

import sys
import os

# Add the app directory to the path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app.services.dataset_builder import load_unified_dataset
from app.data import DataHandler

def test_direct_load():
    """Test loading unified dataset directly."""
    print("Testing direct unified dataset load...")
    session_id = "6e90b139-5d30-40fd-91ad-4af66fec5f00"
    
    gdf = load_unified_dataset(session_id)
    if gdf is not None:
        print(f"✅ Direct load SUCCESS: {len(gdf)} rows, {len(gdf.columns)} columns")
        print(f"   Columns sample: {list(gdf.columns)[:10]}")
        return True
    else:
        print("❌ Direct load FAILED")
        return False

def test_data_handler_load():
    """Test loading through DataHandler."""
    print("\nTesting DataHandler unified dataset load...")
    session_id = "6e90b139-5d30-40fd-91ad-4af66fec5f00"
    session_folder = f"instance/uploads/{session_id}"
    
    # Create data handler
    data_handler = DataHandler(session_folder)
    
    # Check if unified dataset was auto-loaded
    if data_handler.unified_dataset is not None:
        print(f"✅ Auto-load SUCCESS: {len(data_handler.unified_dataset)} rows")
    else:
        print("⚠️  Auto-load failed, trying get_unified_dataset...")
        
        # Try get method
        unified = data_handler.get_unified_dataset()
        if unified is not None:
            print(f"✅ Get method SUCCESS: {len(unified)} rows")
            return True
        else:
            print("❌ Get method FAILED")
            return False
    
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("Testing Unified Dataset Loading Fix")
    print("=" * 60)
    
    # Test both methods
    direct_ok = test_direct_load()
    handler_ok = test_data_handler_load()
    
    print("\n" + "=" * 60)
    if direct_ok and handler_ok:
        print("✅ ALL TESTS PASSED - Unified dataset loading is fixed!")
    else:
        print("❌ TESTS FAILED - Check the logs above")
    print("=" * 60) 