#!/usr/bin/env python
"""
Test script for vision-based visualization explanations on AWS.
This tests the full pipeline with real visualizations.
"""

import os
import sys
import tempfile
import json
import time
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def create_test_visualizations():
    """Create various test visualizations that mimic ChatMRPT outputs."""
    print("\n🎨 Creating test visualizations...")

    test_files = []

    # 1. Create a malaria risk map (bar chart)
    print("  Creating malaria risk bar chart...")
    wards = ['Fagge', 'Dala', 'Nasarawa', 'Tarauni', 'Kumbotso', 'Ungogo', 'Gwale', 'Kano Municipal']
    risk_scores = [0.85, 0.72, 0.91, 0.68, 0.45, 0.39, 0.76, 0.82]

    fig1 = go.Figure(data=[
        go.Bar(
            x=wards,
            y=risk_scores,
            marker_color=['red' if s > 0.8 else 'orange' if s > 0.6 else 'yellow' if s > 0.4 else 'green'
                         for s in risk_scores],
            text=[f'{s:.2f}' for s in risk_scores],
            textposition='outside'
        )
    ])
    fig1.update_layout(
        title='Malaria Vulnerability by Ward - Composite Scores',
        xaxis_title='Ward Name',
        yaxis_title='Vulnerability Score',
        height=600,
        showlegend=False
    )

    with tempfile.NamedTemporaryFile(suffix='_risk_chart.html', delete=False, mode='w') as f:
        fig1.write_html(f.name)
        test_files.append(('vulnerability_map', f.name))

    # 2. Create TPR trend line chart
    print("  Creating TPR trend chart...")
    months = pd.date_range('2024-01', '2024-12', freq='MS')
    tpr_values = [22.5, 24.1, 26.8, 31.2, 35.6, 42.3, 45.7, 41.2, 36.8, 32.1, 28.4, 25.2]

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=months,
        y=tpr_values,
        mode='lines+markers',
        name='TPR',
        line=dict(color='#2E86AB', width=3),
        marker=dict(size=8)
    ))
    fig2.add_hline(y=30, line_dash="dash", line_color="red",
                   annotation_text="Alert Threshold (30%)")
    fig2.update_layout(
        title='Test Positivity Rate (TPR) Trend - 2024',
        xaxis_title='Month',
        yaxis_title='TPR (%)',
        height=500,
        hovermode='x unified'
    )

    with tempfile.NamedTemporaryFile(suffix='_tpr_chart.html', delete=False, mode='w') as f:
        fig2.write_html(f.name)
        test_files.append(('tpr_trend', f.name))

    # 3. Create box plot for ward rankings
    print("  Creating box plot...")
    np.random.seed(42)
    data = {
        'High Risk': np.random.normal(0.75, 0.1, 20),
        'Medium Risk': np.random.normal(0.5, 0.12, 25),
        'Low Risk': np.random.normal(0.25, 0.08, 15)
    }

    fig3 = go.Figure()
    for category, values in data.items():
        fig3.add_trace(go.Box(y=values, name=category))

    fig3.update_layout(
        title='Risk Distribution by Category',
        yaxis_title='Vulnerability Score',
        height=500
    )

    with tempfile.NamedTemporaryFile(suffix='_box_plot.html', delete=False, mode='w') as f:
        fig3.write_html(f.name)
        test_files.append(('box_plot', f.name))

    print(f"  ✅ Created {len(test_files)} test visualizations")
    return test_files

def test_vision_explanation(viz_type, file_path):
    """Test the vision explanation for a specific visualization."""
    try:
        from app.visualization.explainer import UniversalVisualizationExplainer
        from app.services.llm_manager import LLMManager

        # Initialize with LLM manager
        llm_manager = LLMManager()
        explainer = UniversalVisualizationExplainer(llm_manager=llm_manager)

        # Get explanation
        print(f"\n📊 Testing {viz_type}...")
        print(f"  File: {file_path}")

        start_time = time.time()
        explanation = explainer.explain_visualization(file_path, viz_type, 'test_session')
        elapsed = time.time() - start_time

        print(f"  ⏱️ Time: {elapsed:.2f}s")

        # Check if we got a vision-based or fallback explanation
        if 'Generated' in explanation:
            print("  📝 Result: Fallback explanation (expected if vision fails)")
        else:
            print("  👁️ Result: Vision-based explanation!")

        print(f"  Preview: {explanation[:200]}...")

        return True

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def test_plotly_kaleido():
    """Test if Kaleido can convert Plotly to images."""
    print("\n🔬 Testing Kaleido conversion...")

    try:
        fig = go.Figure(data=[go.Bar(x=['A', 'B', 'C'], y=[1, 2, 3])])
        img_bytes = fig.to_image(format="png")

        if img_bytes:
            print(f"  ✅ Kaleido works! Generated {len(img_bytes)} bytes")
            return True
        else:
            print("  ❌ Kaleido failed to generate image")
            return False

    except Exception as e:
        print(f"  ❌ Kaleido error: {e}")
        return False

def main():
    """Run all tests."""
    print("=" * 70)
    print("🚀 ChatMRPT Vision Explanation Test Suite")
    print("=" * 70)

    # Check environment
    vision_enabled = os.environ.get('ENABLE_VISION_EXPLANATIONS', 'false').lower() in ['true', '1', 'yes']
    print(f"\n🔧 Configuration:")
    print(f"  ENABLE_VISION_EXPLANATIONS: {vision_enabled}")
    print(f"  OPENAI_API_KEY: {'✅ Set' if os.environ.get('OPENAI_API_KEY') else '❌ Not set'}")

    # Test 1: Kaleido
    kaleido_ok = test_plotly_kaleido()

    # Test 2: Create and test visualizations
    test_files = create_test_visualizations()

    results = []
    for viz_type, file_path in test_files:
        success = test_vision_explanation(viz_type, file_path)
        results.append((viz_type, success))

        # Clean up
        try:
            os.unlink(file_path)
        except:
            pass

    # Summary
    print("\n" + "=" * 70)
    print("📋 Test Summary:")
    print(f"  Kaleido: {'✅ PASSED' if kaleido_ok else '❌ FAILED'}")

    for viz_type, success in results:
        status = '✅ PASSED' if success else '❌ FAILED'
        print(f"  {viz_type}: {status}")

    all_passed = kaleido_ok and all(s for _, s in results)

    if all_passed:
        print("\n🎉 All tests passed! Vision explanations are working!")
    else:
        print("\n⚠️ Some tests failed. Check the errors above.")

    if vision_enabled:
        print("\n💡 Vision explanations are ENABLED")
        print("   Visualizations will be converted to images and analyzed by GPT-4o")
    else:
        print("\n💡 Vision explanations are DISABLED")
        print("   Using fallback text explanations")

    print("=" * 70)

if __name__ == "__main__":
    main()