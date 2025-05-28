#!/usr/bin/env python3
"""
Debug script to test pagination data flow from backend to frontend
"""

import sys
import os
sys.path.append('.')

from app.data import DataHandler
from app.visualization.maps import create_composite_map
from app.services.message_service import MessageService
from app.services.visualization.chart_service import VisualizationService
import pandas as pd

def test_pagination_data_flow():
    """Test the complete pagination data flow"""
    print("🔍 DEBUGGING PAGINATION DATA FLOW")
    print("=" * 50)
    
    # Create mock data handler with realistic composite scores
    class MockDataHandler:
        def __init__(self):
            self.session_folder = 'test_session'
            self.shapefile_data = None
            
            # Create realistic composite scores with 26 models (should give 7 pages)
            models = {}
            formulas = []
            
            for i in range(1, 27):  # 26 models
                models[f'model_{i}'] = [0.1 * i] * 10  # Mock scores
                formulas.append({
                    'model': f'model_{i}',
                    'variables': [f'var_{i%5 + 1}', f'var_{(i+1)%5 + 1}']
                })
            
            models['WardName'] = [f'Ward_{i}' for i in range(1, 11)]
            
            self.composite_scores = {
                'scores': pd.DataFrame(models),
                'formulas': formulas
            }
            
            self.vulnerability_rankings = pd.DataFrame({
                'WardName': [f'Ward_{i}' for i in range(1, 11)],
                'median_score': [0.1 * i for i in range(1, 11)],
                'overall_rank': list(range(1, 11)),
                'vulnerability_category': ['High'] * 10
            })
    
    # Test 1: Direct visualization generation
    print("📊 Test 1: Direct create_composite_map call")
    handler = MockDataHandler()
    
    # Mock shapefile data
    handler.shapefile_data = pd.DataFrame({
        'WardName': [f'Ward_{i}' for i in range(1, 11)],
        'geometry': [None] * 10  # Mock geometry
    })
    
    result = create_composite_map(handler, model_index=1)
    
    print(f"   Status: {result.get('status')}")
    print(f"   Current Page: {result.get('current_page')}")
    print(f"   Total Pages: {result.get('total_pages')}")
    print(f"   Models per page: 4 (hardcoded)")
    print(f"   Total models: {len([col for col in handler.composite_scores['scores'].columns if col.startswith('model_')])}")
    print(f"   Expected pages: {(26 + 4 - 1) // 4}")
    
    # Test 2: Visualization service response
    print("\n📡 Test 2: VisualizationService response structure")
    viz_service = VisualizationService()
    
    try:
        viz_result = viz_service.generate_visualization(
            'composite_map', 
            handler, 
            {'page': 1}, 
            'test_session'
        )
        
        print(f"   Status: {viz_result.get('status')}")
        print(f"   Current Page: {viz_result.get('current_page')}")
        print(f"   Total Pages: {viz_result.get('total_pages')}")
        print(f"   Image Path: {viz_result.get('image_path', 'N/A')}")
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
    
    # Test 3: Message service response structure
    print("\n💬 Test 3: MessageService response structure")
    message_service = MessageService()
    
    # Mock session state
    session_state = {
        'analysis_complete': True,
        'csv_loaded': True,
        'shapefile_loaded': True
    }
    
    # Mock NLU result for composite map request
    nlu_result = {
        'intent': 'request_visualization',
        'entities': {
            'visualization_type': 'composite map',
            'variables': [],
            'other_entities': None
        }
    }
    
    try:
        msg_result = message_service._handle_show_visualization(
            nlu_result, session_state, handler, 'test_session'
        )
        
        print(f"   Status: {msg_result.get('status')}")
        print(f"   Action: {msg_result.get('action')}")
        print(f"   Current Page: {msg_result.get('current_page')}")
        print(f"   Total Pages: {msg_result.get('total_pages')}")
        print(f"   Visualization Path: {msg_result.get('visualization', 'N/A')}")
        print(f"   Metadata Keys: {list(msg_result.get('metadata', {}).keys())}")
        
        # Check if metadata contains pagination
        metadata = msg_result.get('metadata', {})
        if metadata:
            print(f"   Metadata Current Page: {metadata.get('current_page')}")
            print(f"   Metadata Total Pages: {metadata.get('total_pages')}")
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("\n🎯 SUMMARY:")
    print("   - Backend should generate 26 models")
    print("   - With 4 models per page = 7 total pages")
    print("   - Frontend should receive current_page and total_pages")
    print("   - Check if frontend is using the correct data source")

if __name__ == "__main__":
    test_pagination_data_flow() 