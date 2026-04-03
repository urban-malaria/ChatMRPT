#!/usr/bin/env python3
"""Test the actual explain endpoint"""

import os
import sys
import tempfile
import json

# Add parent to path
sys.path.insert(0, '/home/ec2-user/ChatMRPT')

# Set environment
os.environ['ENABLE_VISION_EXPLANATIONS'] = 'true'
os.environ['FLASK_ENV'] = 'development'

# Import after setting env
from app.visualization.explainer import UniversalVisualizationExplainer

def test_explain():
    """Test the explainer with a real HTML file."""

    print("1. Creating test HTML visualization...")

    test_html = """
    <!DOCTYPE html>
    <html>
    <head><title>TPR Distribution Map</title></head>
    <body style="background:linear-gradient(to right, red, yellow, green); width:800px; height:600px; padding:20px;">
        <h1>Test Positivity Rate Map</h1>
        <div style="background:red; padding:10px; margin:10px;">Ward A: 78% TPR (High Risk)</div>
        <div style="background:yellow; padding:10px; margin:10px;">Ward B: 32% TPR (Medium Risk)</div>
        <div style="background:green; padding:10px; margin:10px;">Ward C: 5% TPR (Low Risk)</div>
        <p>This map shows malaria test positivity rates across different wards.</p>
    </body>
    </html>
    """

    # Create temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
        f.write(test_html)
        test_path = f.name

    print(f"   Created test HTML at: {test_path}")

    try:
        print("\n2. Initializing explainer...")
        explainer = UniversalVisualizationExplainer()

        print("\n3. Explaining visualization...")
        explanation = explainer.explain_visualization(
            viz_path=test_path,
            viz_type='tpr_map',
            session_id='test_session_123'
        )

        print("\n4. Result:")
        print(f"   Explanation length: {len(explanation)} chars")
        print(f"   First 300 chars:\n{explanation[:300]}...")

        # Check if it's a fallback
        if "This visualization shows malaria risk analysis" in explanation:
            print("\n   ⚠️ WARNING: This is still a FALLBACK explanation!")
            print("   Vision API was NOT used")
        else:
            print("\n   ✅ SUCCESS: This appears to be a DYNAMIC explanation!")
            print("   Vision API was successfully used")

        # Check cache
        print("\n5. Testing cache...")
        explanation2 = explainer.explain_visualization(
            viz_path=test_path,
            viz_type='tpr_map',
            session_id='test_session_123'
        )

        if explanation == explanation2:
            print("   ✓ Cache is working (same explanation returned)")

    except Exception as e:
        import traceback
        print(f"\n✗ Error: {e}")
        traceback.print_exc()

    finally:
        # Clean up
        if os.path.exists(test_path):
            os.unlink(test_path)
            print("\n6. Cleaned up test file")

if __name__ == "__main__":
    test_explain()