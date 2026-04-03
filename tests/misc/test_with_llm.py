#!/usr/bin/env python3
"""Test with actual LLM manager"""

import os
import sys
import tempfile

# Add parent to path
sys.path.insert(0, '/home/ec2-user/ChatMRPT')

# Set environment
os.environ['ENABLE_VISION_EXPLANATIONS'] = 'true'
os.environ['FLASK_ENV'] = 'development'

# Import after setting env
from app.visualization.explainer import UniversalVisualizationExplainer
from app.services.llm_manager import LLMManager

def test_with_llm():
    """Test the explainer with LLM manager."""

    print("1. Setting up OpenAI API key...")
    # Get API key from .env
    with open('/home/ec2-user/ChatMRPT/.env', 'r') as f:
        for line in f:
            if line.startswith('OPENAI_API_KEY='):
                api_key = line.strip().split('=')[1]
                os.environ['OPENAI_API_KEY'] = api_key
                print("   ✓ API key loaded")
                break

    print("\n2. Creating LLM Manager...")
    llm_manager = LLMManager(api_key=os.environ.get('OPENAI_API_KEY'))
    print(f"   LLM Manager created: {llm_manager}")
    print(f"   Has generate_with_image: {hasattr(llm_manager, 'generate_with_image')}")

    print("\n3. Creating test HTML visualization...")
    test_html = """
    <!DOCTYPE html>
    <html>
    <head><title>TPR Distribution Map</title></head>
    <body style="background:linear-gradient(to right, red, yellow, green); width:800px; height:600px; padding:20px;">
        <h1>Malaria Test Positivity Rate Map - Kano State</h1>
        <div style="background:#ff0000; padding:10px; margin:10px; color:white;">Dala Ward: 78% TPR (Critical)</div>
        <div style="background:#ffaa00; padding:10px; margin:10px;">Fagge Ward: 45% TPR (High)</div>
        <div style="background:#ffff00; padding:10px; margin:10px;">Kumbotso Ward: 32% TPR (Moderate)</div>
        <div style="background:#00ff00; padding:10px; margin:10px;">Gwale Ward: 5% TPR (Low)</div>
        <p>Data from December 2024 surveillance reports</p>
    </body>
    </html>
    """

    # Create temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
        f.write(test_html)
        test_path = f.name

    print(f"   Created test HTML at: {test_path}")

    try:
        print("\n4. Initializing explainer WITH LLM manager...")
        explainer = UniversalVisualizationExplainer(llm_manager=llm_manager)

        print("\n5. Explaining visualization...")
        explanation = explainer.explain_visualization(
            viz_path=test_path,
            viz_type='tpr_map',
            session_id='test_session_456'
        )

        print("\n6. Result:")
        print(f"   Explanation length: {len(explanation)} chars")
        print("\n   Full explanation:")
        print("="*60)
        print(explanation)
        print("="*60)

        # Check if it's a fallback
        if "This visualization shows malaria risk analysis" in explanation or "Tpr Map Generated" in explanation:
            print("\n   ⚠️ WARNING: This is still a FALLBACK explanation!")
        else:
            print("\n   ✅ SUCCESS: This is a DYNAMIC AI explanation!")

    except Exception as e:
        import traceback
        print(f"\n✗ Error: {e}")
        traceback.print_exc()

    finally:
        # Clean up
        if os.path.exists(test_path):
            os.unlink(test_path)
            print("\n7. Cleaned up test file")

if __name__ == "__main__":
    test_with_llm()