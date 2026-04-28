#!/usr/bin/env python3
"""
AWS Routing Test Script
Tests semantic routing directly on the AWS instance
"""

import asyncio
import sys
import os

# Add project path
sys.path.insert(0, '/home/ec2-user/ChatMRPT')

# Set environment variables
os.environ['FLASK_ENV'] = 'development'
os.environ['OLLAMA_HOST'] = 'http://localhost:11434'

async def test_semantic_routing():
    """Test the semantic routing with various queries."""

    print("=" * 80)
    print("SEMANTIC ROUTING TEST - AWS")
    print("=" * 80)

    # Import after environment setup
    try:
        from app.api.analysis.chat_routing import route_with_mistral
        print("✅ Successfully imported route_with_mistral")
    except ImportError as e:
        print(f"❌ Failed to import: {e}")
        return

    # Test cases organized by category
    test_suites = {
        "GREETINGS (Expected: can_answer)": [
            "hi",
            "hello",
            "good morning",
            "hey there",
            "greetings"
        ],

        "ANALYSIS WITHOUT 'THE' (Expected: needs_tools with data)": [
            "run malaria risk analysis",
            "perform analysis",
            "analyze data",
            "start risk assessment",
            "execute malaria analysis"
        ],

        "ANALYSIS WITH 'THE' (Expected: needs_tools with data)": [
            "run the malaria risk analysis",
            "perform the analysis",
            "analyze the data",
            "start the assessment",
            "execute the analysis"
        ],

        "VISUALIZATIONS (Expected: needs_tools with data)": [
            "create vulnerability map",
            "show me variable distributions",
            "plot data spread",
            "generate histogram",
            "make a box plot",
            "display risk map"
        ],

        "EXPLANATIONS (Expected: can_answer)": [
            "what is malaria",
            "explain composite score",
            "how does PCA work",
            "what does vulnerability mean",
            "why is risk analysis important"
        ],

        "ITN PLANNING (Expected: needs_tools with data)": [
            "plan bed net distribution",
            "distribute mosquito nets",
            "allocate ITNs to wards",
            "plan intervention",
            "where should we put nets"
        ]
    }

    # Test Context 1: No Data
    print("\n" + "=" * 60)
    print("CONTEXT: NO DATA UPLOADED")
    print("=" * 60)

    no_data_context = {
        'has_uploaded_files': False,
        'csv_loaded': False,
        'analysis_complete': False,
        'session_id': 'test_no_data'
    }

    for suite_name, test_cases in test_suites.items():
        print(f"\n📋 {suite_name}")
        print("-" * 40)

        for message in test_cases[:2]:  # Test first 2 from each suite
            try:
                result = await route_with_mistral(message, no_data_context)
                print(f"  '{message}' → {result}")
            except Exception as e:
                print(f"  '{message}' → ERROR: {str(e)[:50]}")

    # Test Context 2: With Data
    print("\n" + "=" * 60)
    print("CONTEXT: DATA UPLOADED & ANALYSIS COMPLETE")
    print("=" * 60)

    with_data_context = {
        'has_uploaded_files': True,
        'csv_loaded': True,
        'shapefile_loaded': True,
        'analysis_complete': True,
        'session_id': 'test_with_data'
    }

    # Create mock results directory
    os.makedirs(f'/home/ec2-user/ChatMRPT/instance/uploads/{with_data_context["session_id"]}', exist_ok=True)

    # Test specific important cases
    critical_tests = [
        ("run malaria risk analysis", "needs_tools", "Without 'the'"),
        ("run the malaria risk analysis", "needs_tools", "With 'the'"),
        ("show me variable distributions", "needs_tools", "Distributions"),
        ("variable spread across wards", "needs_tools", "Alternative phrasing"),
        ("what does composite score mean", "can_answer", "Explanation"),
        ("explain my results", "can_answer", "Results explanation"),
        ("plan bed net distribution", "needs_tools", "ITN planning"),
        ("hello", "can_answer", "Greeting"),
        ("create a map", "needs_tools", "Generic visualization"),
        ("why is Bunkure high risk", "can_answer", "Ward explanation")
    ]

    print("\n📊 CRITICAL TEST CASES")
    print("-" * 60)
    print(f"{'Query':<40} {'Expected':<15} {'Result':<15} {'Status'}")
    print("-" * 60)

    passed = 0
    failed = 0

    for message, expected, description in critical_tests:
        try:
            result = await route_with_mistral(message, with_data_context)
            status = "✅" if result == expected else "❌"
            if result == expected:
                passed += 1
            else:
                failed += 1
            print(f"{message[:39]:<40} {expected:<15} {result:<15} {status}")
        except Exception as e:
            failed += 1
            error_msg = str(e)[:30]
            print(f"{message[:39]:<40} {expected:<15} ERROR: {error_msg:<8} ❌")

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📊 Success Rate: {(passed/(passed+failed)*100):.1f}%" if (passed+failed) > 0 else "N/A")

    # Cleanup
    try:
        os.rmdir(f'/home/ec2-user/ChatMRPT/instance/uploads/{with_data_context["session_id"]}')
    except:
        pass

    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    print("Starting semantic routing test on AWS...")
    print("Note: This test requires Ollama/Mistral to be running")
    print("-" * 60)

    # Check if Ollama is accessible
    import subprocess
    try:
        result = subprocess.run(['curl', '-s', 'http://localhost:11434/api/tags'],
                              capture_output=True, timeout=2)
        if result.returncode == 0:
            print("✅ Ollama is accessible")
        else:
            print("⚠️  Ollama may not be running on port 11434")
    except:
        print("⚠️  Could not check Ollama status")

    # Run the async test
    try:
        asyncio.run(test_semantic_routing())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()