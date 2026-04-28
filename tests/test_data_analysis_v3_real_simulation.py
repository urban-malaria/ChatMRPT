"""
Real User Simulation Test for Data Analysis V3
Tests actual agent responses against staging ALB
Generates HTML report with complete interactions
"""

import pytest
import requests
import time
import os
import json
from datetime import datetime
from typing import Dict, List, Tuple

# Staging ALB endpoint
STAGING_URL = "http://chatmrpt-staging-alb-752380251.us-east-2.elb.amazonaws.com"

class TestDataAnalysisV3RealSimulation:
    """Test suite simulating real user interactions with Data Analysis V3."""
    
    @pytest.fixture(scope="class")
    def session_data(self):
        """Create a unique session for testing."""
        return {
            'session_id': f'pytest_{int(time.time())}',
            'upload_time': None,
            'interactions': []
        }
    
    @pytest.fixture(scope="class", autouse=True)
    def upload_test_data(self, session_data):
        """Upload Adamawa TPR data once for all tests."""
        print(f"\n{'='*60}")
        print("UPLOADING TEST DATA TO STAGING")
        print(f"{'='*60}")
        
        # Upload the Adamawa data
        data_file = 'www/adamawa_tpr_cleaned.csv'
        if not os.path.exists(data_file):
            pytest.skip(f"Test data file not found: {data_file}")
        
        with open(data_file, 'rb') as f:
            files = {'file': ('adamawa_test.csv', f, 'text/csv')}
            data = {'session_id': session_data['session_id']}
            
            response = requests.post(
                f"{STAGING_URL}/api/data-analysis/upload",
                files=files,
                data=data,
                timeout=30
            )
            
            assert response.status_code == 200, f"Upload failed: {response.text}"
            session_data['upload_time'] = datetime.now().isoformat()
            
            print(f"✅ Data uploaded successfully")
            print(f"   Session ID: {session_data['session_id']}")
            
            # Wait for data to be processed
            time.sleep(3)
    
    def _send_message(self, session_data: Dict, message: str) -> Tuple[bool, str, float]:
        """
        Send a message to the agent and get response.
        
        Returns:
            Tuple of (success, response_text, response_time_seconds)
        """
        start_time = time.time()
        
        try:
            response = requests.post(
                f"{STAGING_URL}/api/v1/data-analysis/chat",
                json={
                    'message': message,
                    'session_id': session_data['session_id']
                },
                timeout=60
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                success = result.get('success', False)
                message_text = result.get('message', 'No response')
                
                # Store interaction
                session_data['interactions'].append({
                    'query': message,
                    'response': message_text,
                    'success': success,
                    'response_time': response_time,
                    'timestamp': datetime.now().isoformat()
                })
                
                return success, message_text, response_time
            else:
                return False, f"HTTP {response.status_code}: {response.text}", response_time
                
        except Exception as e:
            response_time = time.time() - start_time
            return False, f"Error: {str(e)}", response_time
    
    def test_01_initial_exploration(self, session_data):
        """Test initial data exploration query."""
        print(f"\n{'='*60}")
        print("TEST 1: Initial Data Exploration")
        print(f"{'='*60}")
        
        success, response, time_taken = self._send_message(
            session_data,
            "What's in the uploaded data? Give me a quick overview."
        )
        
        print(f"Response time: {time_taken:.2f}s")
        print(f"Success: {success}")
        print(f"Response preview: {response[:200]}...")
        
        # Assertions
        assert success, "Initial exploration failed"
        assert len(response) > 100, "Response too short"
        assert "rows" in response.lower() or "columns" in response.lower(), "No data description found"
        
        print("✅ Initial exploration test passed")
    
    def test_02_top_10_facilities(self, session_data):
        """Test the critical top 10 query that was previously broken."""
        print(f"\n{'='*60}")
        print("TEST 2: Top 10 Facilities Query")
        print(f"{'='*60}")
        
        success, response, time_taken = self._send_message(
            session_data,
            "Show me the top 10 health facilities by total testing volume. List all 10."
        )
        
        print(f"Response time: {time_taken:.2f}s")
        print(f"Success: {success}")
        
        # Count numbered items in response
        import re
        numbered_items = re.findall(r'^\s*\d+\.', response, re.MULTILINE)
        print(f"Number of items found: {len(numbered_items)}")
        
        # Check for hallucinations
        has_hallucination = "Facility A" in response or "Facility B" in response
        print(f"Hallucination detected: {has_hallucination}")
        
        # Print first 10 numbered items for verification
        lines = response.split('\n')
        numbered_lines = [line for line in lines if re.match(r'^\s*\d+\.', line)]
        print("\nTop facilities found:")
        for line in numbered_lines[:10]:
            print(f"  {line}")
        
        # Assertions
        assert success, "Top 10 query failed"
        assert len(numbered_items) >= 8, f"Expected at least 8 items, got {len(numbered_items)}"
        assert not has_hallucination, "Hallucinated facility names detected"
        
        print("✅ Top 10 facilities test passed")
    
    def test_03_statistical_summary(self, session_data):
        """Test statistical summary generation."""
        print(f"\n{'='*60}")
        print("TEST 3: Statistical Summary")
        print(f"{'='*60}")
        
        success, response, time_taken = self._send_message(
            session_data,
            "Generate a statistical summary of the testing data. Include averages and totals."
        )
        
        print(f"Response time: {time_taken:.2f}s")
        print(f"Success: {success}")
        
        # Check for statistical terms
        has_statistics = any(term in response.lower() for term in ['average', 'total', 'mean', 'sum'])
        print(f"Contains statistics: {has_statistics}")
        
        # Assertions
        assert success, "Statistical summary failed"
        assert has_statistics, "No statistical information in response"
        
        print("✅ Statistical summary test passed")
    
    def test_04_trend_analysis(self, session_data):
        """Test trend analysis over time."""
        print(f"\n{'='*60}")
        print("TEST 4: Trend Analysis")
        print(f"{'='*60}")
        
        success, response, time_taken = self._send_message(
            session_data,
            "Analyze testing trends over time. Are there any patterns?"
        )
        
        print(f"Response time: {time_taken:.2f}s")
        print(f"Success: {success}")
        
        # Check for trend-related terms
        has_trends = any(term in response.lower() for term in ['trend', 'pattern', 'increase', 'decrease', 'change'])
        print(f"Contains trend analysis: {has_trends}")
        
        # Assertions
        assert success, "Trend analysis failed"
        
        print("✅ Trend analysis test passed")
    
    def test_05_comparison_query(self, session_data):
        """Test comparison between different groups."""
        print(f"\n{'='*60}")
        print("TEST 5: Comparison Query")
        print(f"{'='*60}")
        
        success, response, time_taken = self._send_message(
            session_data,
            "Compare testing volumes between different wards. Which ones are doing better?"
        )
        
        print(f"Response time: {time_taken:.2f}s")
        print(f"Success: {success}")
        
        # Check for comparison terms
        has_comparison = any(term in response.lower() for term in ['compare', 'higher', 'lower', 'better', 'worse', 'more', 'less'])
        print(f"Contains comparison: {has_comparison}")
        
        # Assertions
        assert success, "Comparison query failed"
        
        print("✅ Comparison query test passed")
    
    def test_06_visualization_request(self, session_data):
        """Test visualization generation."""
        print(f"\n{'='*60}")
        print("TEST 6: Visualization Request")
        print(f"{'='*60}")
        
        success, response, time_taken = self._send_message(
            session_data,
            "Create a visualization showing the distribution of testing across facilities."
        )
        
        print(f"Response time: {time_taken:.2f}s")
        print(f"Success: {success}")
        
        # Check for visualization references
        has_viz = any(term in response.lower() for term in ['chart', 'graph', 'visualization', 'plot', 'shown', 'display'])
        print(f"References visualization: {has_viz}")
        
        # Assertions
        assert success, "Visualization request failed"
        
        print("✅ Visualization test passed")
    
    @pytest.fixture(scope="class", autouse=True)
    def generate_html_report(self, session_data, request):
        """Generate HTML report after all tests complete."""
        yield  # Run tests first
        
        # Generate report after tests
        print(f"\n{'='*60}")
        print("GENERATING HTML REPORT")
        print(f"{'='*60}")
        
        html_content = self._create_html_report(session_data)
        
        # Save report
        report_path = f"tests/reports/data_analysis_v3_test_{session_data['session_id']}.html"
        os.makedirs("tests/reports", exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ HTML report saved to: {report_path}")
        print(f"   Open this file in a browser to view the complete interaction history")
    
    def _create_html_report(self, session_data: Dict) -> str:
        """Create comprehensive HTML report of all interactions."""
        
        # Calculate statistics
        total_queries = len(session_data['interactions'])
        successful_queries = sum(1 for i in session_data['interactions'] if i['success'])
        avg_response_time = sum(i['response_time'] for i in session_data['interactions']) / total_queries if total_queries > 0 else 0
        
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Analysis V3 Test Report - {session_data['session_id']}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .header h1 {{
            margin: 0 0 10px 0;
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .stat-label {{
            color: #666;
            font-size: 14px;
            margin-bottom: 5px;
        }}
        .stat-value {{
            font-size: 24px;
            font-weight: bold;
            color: #333;
        }}
        .interaction {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .query {{
            background: #e3f2fd;
            padding: 15px;
            border-radius: 6px;
            margin-bottom: 15px;
            border-left: 4px solid #2196f3;
        }}
        .response {{
            background: #f5f5f5;
            padding: 15px;
            border-radius: 6px;
            white-space: pre-wrap;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 14px;
            max-height: 600px;
            overflow-y: auto;
        }}
        .metadata {{
            display: flex;
            justify-content: space-between;
            margin-top: 10px;
            padding-top: 10px;
            border-top: 1px solid #e0e0e0;
            color: #666;
            font-size: 14px;
        }}
        .success {{
            color: #4caf50;
            font-weight: bold;
        }}
        .failed {{
            color: #f44336;
            font-weight: bold;
        }}
        .timestamp {{
            color: #999;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Analysis V3 Test Report</h1>
        <p>Session ID: {session_data['session_id']}</p>
        <p>Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Endpoint: {STAGING_URL}</p>
    </div>
    
    <div class="stats">
        <div class="stat-card">
            <div class="stat-label">Total Queries</div>
            <div class="stat-value">{total_queries}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Successful</div>
            <div class="stat-value">{successful_queries}/{total_queries}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Success Rate</div>
            <div class="stat-value">{(successful_queries/total_queries*100):.1f}%</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Avg Response Time</div>
            <div class="stat-value">{avg_response_time:.2f}s</div>
        </div>
    </div>
    
    <h2>Interaction History</h2>
"""
        
        # Add each interaction
        for i, interaction in enumerate(session_data['interactions'], 1):
            status_class = "success" if interaction['success'] else "failed"
            status_text = "✅ Success" if interaction['success'] else "❌ Failed"
            
            html += f"""
    <div class="interaction">
        <h3>Query {i}</h3>
        <div class="query">
            <strong>User:</strong> {interaction['query']}
        </div>
        <div class="response">
            <strong>Agent Response:</strong>
{interaction['response']}
        </div>
        <div class="metadata">
            <span class="{status_class}">{status_text}</spa[=;;n>
            <span>Response Time: {interaction['response_time']:.2f}s</span>
            <span class="timestamp">{interaction['timestamp']}</span>
        </div>
    </div>
"""
        
        html += """
</body>
</html>
"""
        return html


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])