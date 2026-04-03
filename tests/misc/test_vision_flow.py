import sys
import os
sys.path.insert(0, '/home/ec2-user/ChatMRPT')
os.environ['ENABLE_VISION_EXPLANATIONS'] = 'true'

# Load API key
with open('.env', 'r') as f:
    for line in f:
        if line.startswith('OPENAI_API_KEY='):
            os.environ['OPENAI_API_KEY'] = line.strip().split('=')[1]
            break

print("Testing Vision Flow...")
print("=" * 60)

# 1. Test the services
from app import create_app
app = create_app()

with app.app_context():
    print("\n1. Testing Service Container:")
    llm = app.services.llm_manager
    print(f"   LLM type: {type(llm).__name__}")
    print(f"   Has generate_with_image: {hasattr(llm, 'generate_with_image')}")
    
    # 2. Test the explainer
    print("\n2. Testing UniversalVisualizationExplainer:")
    from app.visualization.explainer import UniversalVisualizationExplainer
    
    explainer = UniversalVisualizationExplainer(llm_manager=llm)
    print(f"   Explainer created")
    print(f"   LLM manager type in explainer: {type(explainer.llm_manager).__name__}")
    
    # 3. Test with actual file
    print("\n3. Testing with actual TPR map file:")
    test_file = "instance/uploads/945c9128-ca4d-4116-a927-3e85e5357dd1/tpr_distribution_map.html"
    
    if os.path.exists(test_file):
        print(f"   File exists: {test_file}")
        print(f"   File size: {os.path.getsize(test_file)} bytes")
        
        # Call explain_visualization
        print("\n   Calling explain_visualization...")
        try:
            explanation = explainer.explain_visualization(
                viz_path=test_file,
                viz_type='tpr_map',
                session_id='test'
            )
            print(f"   Got explanation, length: {len(explanation)}")
            print(f"   First 300 chars: {explanation[:300]}")
            
            if "This visualization shows malaria risk analysis" in explanation:
                print("\n   ❌ FALLBACK MESSAGE - Vision not working!")
            else:
                print("\n   ✅ Real AI explanation!")
        except Exception as e:
            print(f"   ERROR: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"   File not found: {test_file}")

print("\n" + "=" * 60)
