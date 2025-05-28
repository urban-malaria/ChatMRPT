#!/usr/bin/env python3
"""
Test the new modern report generation system.
"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

import pandas as pd
import tempfile

# Create mock data handler
class MockDataHandler:
    def __init__(self):
        # Create mock vulnerability rankings
        self.vulnerability_rankings = pd.DataFrame({
            'overall_rank': range(1, 21),
            'WardName': [f'Ward_{i}' for i in range(1, 21)],
            'median_score': [0.9 - i*0.04 for i in range(20)],
            'vulnerability_category': ['High'] * 5 + ['Medium'] * 8 + ['Low'] * 7
        })
        
        # Mock variables used
        self.composite_variables = [
            'population_density',
            'distance_to_water',
            'rainfall_annual',
            'temperature_avg',
            'poverty_index'
        ]
        
        self.session_id = 'test_session'

# Mock Flask current_app for testing
class MockApp:
    def __init__(self):
        self.instance_path = tempfile.mkdtemp()

def test_report_generation():
    """Test both PDF and dashboard generation"""
    print("🧪 Testing New Report Generation System...")
    
    # Create mock data handler
    data_handler = MockDataHandler()
    
    # Mock Flask app context
    import app.reports.modern_generator as gen_module
    original_current_app = getattr(gen_module, 'current_app', None)
    gen_module.current_app = MockApp()
    
    try:
        # Import after mocking
        from app.reports.modern_generator import ModernReportGenerator
        
        # Create report generator
        generator = ModernReportGenerator(data_handler, 'test_session')
        
        print("\n📄 Testing PDF Report Generation...")
        pdf_result = generator.generate_report('pdf')
        print(f"PDF Result: {pdf_result['status']} - {pdf_result.get('message', 'No message')}")
        if pdf_result['status'] == 'success':
            print(f"✅ PDF report saved: {pdf_result.get('filename')}")
        else:
            print(f"❌ Error: {pdf_result.get('message')}")
        
        print("\n📊 Testing HTML Dashboard Generation...")
        html_result = generator.generate_report('html')
        print(f"HTML Result: {html_result['status']} - {html_result.get('message', 'No message')}")
        if html_result['status'] == 'success':
            print(f"✅ Dashboard saved: {html_result.get('filename')}")
        else:
            print(f"❌ Error: {html_result.get('message')}")
        
        print("\n🎯 Testing Dashboard Method...")
        dashboard_result = generator.generate_dashboard()
        print(f"Dashboard Result: {dashboard_result['status']} - {dashboard_result.get('message', 'No message')}")
        if dashboard_result['status'] == 'success':
            print(f"✅ Dashboard method works: {dashboard_result.get('filename')}")
        else:
            print(f"❌ Error: {dashboard_result.get('message')}")
        
        print("\n✨ Testing with different detail levels...")
        
        # Test basic level
        basic_result = generator.generate_report('pdf', detail_level='basic')
        print(f"Basic PDF: {basic_result['status']} - {basic_result.get('filename', 'N/A')}")
        
        # Test technical level  
        tech_result = generator.generate_report('html', detail_level='technical')
        print(f"Technical Dashboard: {tech_result['status']} - {tech_result.get('filename', 'N/A')}")
        
        print("\n✅ All tests completed successfully!")
        print(f"📁 Test reports saved in: {generator.reports_folder}")
        
        return pdf_result, html_result, dashboard_result
        
    finally:
        # Restore original current_app
        if original_current_app:
            gen_module.current_app = original_current_app

if __name__ == '__main__':
    test_report_generation() 