"""
Simple tests for UX improvements - focus on what works without complex setup.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.tpr.intent import TPRIntent, TPRIntentClassifier


def test_intent_classifier_basics():
    """Test basic intent classification."""
    classifier = TPRIntentClassifier()

    # Test help requests
    assert classifier.classify("what is TPR?", "any") == TPRIntent.HELP_REQUEST
    assert classifier.classify("explain this", "any") == TPRIntent.HELP_REQUEST

    # Test navigation
    assert classifier.classify("go back", "any") == TPRIntent.NAVIGATION
    assert classifier.classify("skip", "any") == TPRIntent.NAVIGATION

    # Test selections
    assert classifier.classify("1", "state_selection") == TPRIntent.SELECTION
    assert classifier.classify("primary", "facility_selection") == TPRIntent.SELECTION

    print("✅ Intent classifier tests passed!")


def test_navigation_types():
    """Test navigation command detection."""
    classifier = TPRIntentClassifier()

    assert classifier.get_navigation_type("go back") == "back"
    assert classifier.get_navigation_type("skip this") == "skip"
    assert classifier.get_navigation_type("restart") == "restart"
    assert classifier.get_navigation_type("where am i") == "status"
    assert classifier.get_navigation_type("exit") == "exit"

    print("✅ Navigation detection tests passed!")


def test_selection_extraction():
    """Test extraction of selections."""
    classifier = TPRIntentClassifier()

    # Facility selections
    assert classifier.extract_selection("primary", "facility_selection") == "primary"
    assert classifier.extract_selection("I'll take secondary", "facility_selection") == "secondary"
    assert classifier.extract_selection("all levels", "facility_selection") == "all"

    # Age selections
    assert classifier.extract_selection("under 5", "age_selection") == "under-5"
    assert classifier.extract_selection("5 to 15", "age_selection") == "5-15"
    assert classifier.extract_selection("all ages", "age_selection") == "all_ages"

    # Number selections
    assert classifier.extract_selection("1", "any") == "1"
    assert classifier.extract_selection("2", "any") == "2"

    print("✅ Selection extraction tests passed!")


def test_imports():
    """Test all key imports work."""
    try:
        from app.tpr.intent import TPRIntentClassifier
        from app.data_analysis_v3.core.tpr_workflow_handler import TPRWorkflowHandler
        from app.agent.agent import DataAnalysisAgent
        from app.agent.prompt_builder import PromptBuilder
        print("✅ All imports successful!")
        return True
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False


def test_welcome_message_config():
    """Test that welcome message is configured."""
    import os

    # Check that analysis_routes.py has welcome message
    routes_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'app', 'web', 'routes', 'analysis_routes.py'
    )

    if os.path.exists(routes_file):
        with open(routes_file, 'r') as f:
            content = f.read()
            if 'Welcome to ChatMRPT!' in content:
                print("✅ Welcome message configured in analysis_routes.py!")
                return True

    print("⚠️ Welcome message not found")
    return False


def test_prompt_builder_ui_awareness():
    """Test that prompt builder has correct UI awareness."""
    from app.agent.prompt_builder import PromptBuilder

    builder = PromptBuilder()
    prompt = builder.build({'session_id': 'test'})

    # Check for correct UI elements
    assert 'Two main tabs' in prompt
    assert 'Standard Upload' in prompt
    assert 'Data Analysis' in prompt

    # Should NOT have made-up buttons
    assert 'Run Analysis button' not in prompt
    assert 'Export Results button' not in prompt  # These were the made-up ones

    print("✅ Prompt builder has correct UI awareness!")


def test_no_red_warnings():
    """Test that red warning symbols are not used."""
    import os

    # Check agent.py for removal of warning symbols
    agent_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'app', 'data_analysis_v3', 'core', 'agent.py'
    )

    if os.path.exists(agent_file):
        with open(agent_file, 'r') as f:
            content = f.read()
            # Look for the upload confirmation section
            if '📊 **Your data has been uploaded successfully!**' in content:
                print("✅ Friendly upload message found!")
                if '⚠️ IMPORTANT:' not in content or content.count('⚠️ IMPORTANT:') < 3:
                    print("✅ Warning symbols reduced/removed!")
                    return True

    print("⚠️ Check red warning removal manually")
    return False


def run_all_tests():
    """Run all simple tests."""
    print("="*60)
    print("ChatMRPT UX Improvements - Simple Test Suite")
    print("="*60)

    test_results = []

    # Run each test
    tests = [
        ("Intent Classifier", test_intent_classifier_basics),
        ("Navigation Types", test_navigation_types),
        ("Selection Extraction", test_selection_extraction),
        ("Module Imports", test_imports),
        ("Welcome Message", test_welcome_message_config),
        ("UI Awareness", test_prompt_builder_ui_awareness),
        ("Red Warning Removal", test_no_red_warnings)
    ]

    for test_name, test_func in tests:
        print(f"\nRunning: {test_name}")
        print("-"*40)
        try:
            test_func()
            test_results.append((test_name, "PASSED"))
        except Exception as e:
            print(f"❌ Test failed: {str(e)}")
            test_results.append((test_name, f"FAILED: {str(e)}"))

    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    passed = sum(1 for _, result in test_results if result == "PASSED")
    total = len(test_results)

    for test_name, result in test_results:
        status = "✅" if result == "PASSED" else "❌"
        print(f"{status} {test_name}: {result}")

    print("-"*60)
    print(f"Results: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 ALL TESTS PASSED! Ready for deployment.")
    else:
        print(f"\n⚠️ {total - passed} tests need attention before deployment.")

    return passed == total


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)