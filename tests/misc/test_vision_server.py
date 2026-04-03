#!/usr/bin/env python3
"""
Test vision explanation system on AWS server
"""

import os
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def test_vision():
    """Test vision explanations directly."""

    # Set environment variable
    os.environ['ENABLE_VISION_EXPLANATIONS'] = 'true'

    # Import after setting env
    from app.visualization.explainer import UniversalVisualizationExplainer

    print("\n1️⃣ Testing vision system...")
    explainer = UniversalVisualizationExplainer()

    # Test with a simple viz_type
    viz_type = 'tpr_map'
    session_id = 'test_session'

    # Check if vision would be enabled
    important_viz_types = [
        'tpr_map', 'vulnerability_map', 'pca_map', 'itn_map',
        'composite_score_map', 'urban_extent_map', 'box_plot'
    ]

    is_important = viz_type and any(viz_type.startswith(vtype) for vtype in important_viz_types)
    env_setting = os.environ.get('ENABLE_VISION_EXPLANATIONS', 'true').lower()
    enable_vision = is_important or env_setting in ['true', '1', 'yes']

    print(f"   viz_type: {viz_type}")
    print(f"   is_important: {is_important}")
    print(f"   env_setting: {env_setting}")
    print(f"   enable_vision: {enable_vision}")

    # Check conversion methods
    print("\n2️⃣ Checking image conversion tools...")

    # Check html2image
    try:
        from html2image import Html2Image
        print("   ✅ html2image is available")
    except ImportError:
        print("   ❌ html2image NOT available")

    # Check playwright
    try:
        from playwright.sync_api import sync_playwright
        print("   ✅ playwright is available")
    except ImportError:
        print("   ❌ playwright NOT available")

    # Check selenium
    try:
        from selenium import webdriver
        print("   ✅ selenium is available")
    except ImportError:
        print("   ❌ selenium NOT available")

    # Check wkhtmltoimage
    import subprocess
    try:
        result = subprocess.run(['which', 'wkhtmltoimage'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"   ✅ wkhtmltoimage available at: {result.stdout.strip()}")
        else:
            print("   ❌ wkhtmltoimage NOT available")
    except:
        print("   ❌ wkhtmltoimage check failed")

    # Test creating a simple HTML file
    print("\n3️⃣ Testing with actual HTML visualization...")

    test_html = """
    <!DOCTYPE html>
    <html>
    <head><title>Test TPR Map</title></head>
    <body style="background: linear-gradient(to right, red, yellow, green); width: 600px; height: 400px;">
        <h1>Test TPR Map</h1>
        <p>Ward A: 45% positivity</p>
        <p>Ward B: 32% positivity</p>
    </body>
    </html>
    """

    # Save to uploads directory
    test_path = '/home/ec2-user/ChatMRPT/instance/uploads/test_viz.html'
    os.makedirs(os.path.dirname(test_path), exist_ok=True)

    with open(test_path, 'w') as f:
        f.write(test_html)

    print(f"   Created test HTML at: {test_path}")

    # Try to explain it
    try:
        explanation = explainer.explain_visualization(
            viz_path=test_path,
            viz_type='tpr_map',
            session_id='test_session'
        )

        print("\n4️⃣ Explanation result:")
        print(f"   Length: {len(explanation)} characters")
        print(f"   First 200 chars: {explanation[:200]}...")

        # Check if it's a fallback
        if "This visualization shows malaria risk analysis results" in explanation:
            print("   ⚠️ This is a FALLBACK explanation!")
        else:
            print("   ✅ This appears to be a dynamic explanation")

    except Exception as e:
        print(f"\n❌ Error during explanation: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Clean up
        if os.path.exists(test_path):
            os.remove(test_path)
            print("\n5️⃣ Cleaned up test file")

if __name__ == "__main__":
    test_vision()