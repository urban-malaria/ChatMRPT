#!/usr/bin/env python3
"""
Comprehensive test of arena models' knowledge of complete ChatMRPT workflows
"""

import requests
import json
import sys
import os

# Get the enhanced prompt
sys.path.append('/home/ec2-user/ChatMRPT')
os.environ['FLASK_ENV'] = 'production'
os.environ['OPENAI_API_KEY'] = 'dummy'

from app.arena.prompts import get_arena_system_prompt

# Get the prompt
prompt = get_arena_system_prompt()
print(f"System prompt loaded: {len(prompt)} characters")
print("="*70)

# Test complete user journeys
test_scenarios = [
    {
        "scenario": "Complete beginner wanting to analyze malaria risk",
        "question": "I'm new to ChatMRPT and have ward-level malaria data from Kano State. Walk me through the complete process.",
        "expected": ["paperclip", "standard upload", "csv", "shapefile", "wardname", "statecode", "analyze", "composite", "pca", "map", "ranking"],
        "critical": ["paperclip", "standard upload", "both files"]
    },
    {
        "scenario": "User with TPR data wanting full analysis",
        "question": "I have test positivity data from health facilities. How do I calculate TPR and then do risk analysis?",
        "expected": ["data analysis tab", "option 2", "tpr", "state", "facility", "age", "transition", "risk analysis"],
        "critical": ["data analysis", "option 2", "transition"]
    },
    {
        "scenario": "User wanting to explore data first",
        "question": "I want to explore my CSV data, understand patterns, and create visualizations before doing formal analysis. How?",
        "expected": ["data analysis tab", "option 1", "explore", "analyze", "pattern", "visualization"],
        "critical": ["data analysis", "option 1"]
    },
    {
        "scenario": "User needing sample data",
        "question": "I don't have data yet but want to try ChatMRPT. Can I use sample data?",
        "expected": ["sidebar", "samples", "kano", "sample", "history"],
        "critical": ["sidebar", "samples", "kano"]
    },
    {
        "scenario": "User with analysis results wanting ITN planning",
        "question": "I've completed my risk analysis and see red areas on the map. How do I plan ITN distribution?",
        "expected": ["red", "high", "priority", "itn", "distribution", "target", "orange"],
        "critical": ["red", "priority", "itn"]
    },
    {
        "scenario": "User troubleshooting missing WardName error",
        "question": "I'm getting 'Missing WardName' error but my file has ward names. What's wrong?",
        "expected": ["exact", "spelling", "wardname", "capital", "ward_name", "space", "rename"],
        "critical": ["wardname", "capital", "spelling"]
    },
    {
        "scenario": "User wanting to export results",
        "question": "How do I download and share my analysis results with stakeholders?",
        "expected": ["download", "csv", "excel", "word", "report", "export"],
        "critical": ["download", "word", "report"]
    }
]

ollama_url = "http://172.31.45.157:11434/v1/chat/completions"

print("COMPREHENSIVE WORKFLOW TEST")
print("Testing Arena Models' Understanding of Complete User Journeys")
print("="*70)

results = {"perfect": 0, "good": 0, "poor": 0}

for i, test in enumerate(test_scenarios, 1):
    print(f"\n\nScenario {i}: {test['scenario']}")
    print("="*60)
    print(f"User asks: \"{test['question']}\"")
    print("-" * 60)
    
    payload = {
        "model": "mistral:7b",
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": test['question']}
        ],
        "temperature": 0.7,
        "max_tokens": 600
    }
    
    try:
        response = requests.post(ollama_url, json=payload, timeout=30)
        if response.status_code == 200:
            answer = response.json()['choices'][0]['message']['content']
            answer_lower = answer.lower()
            
            # Check for keywords
            found_expected = [kw for kw in test['expected'] if kw.lower() in answer_lower]
            found_critical = [kw for kw in test['critical'] if kw.lower() in answer_lower]
            
            # Show response
            print("Response:")
            print(answer[:500] + "..." if len(answer) > 500 else answer)
            print("\nEvaluation:")
            
            # Evaluate quality
            if len(found_critical) == len(test['critical']) and len(found_expected) >= len(test['expected']) * 0.7:
                print(f"✅ PERFECT - All critical points covered")
                print(f"   Found {len(found_expected)}/{len(test['expected'])} expected keywords")
                print(f"   Critical: {', '.join(found_critical)}")
                results["perfect"] += 1
            elif len(found_critical) >= len(test['critical']) * 0.6:
                print(f"⚠️ GOOD - Most critical points covered")
                print(f"   Found {len(found_expected)}/{len(test['expected'])} expected keywords")
                print(f"   Missing critical: {', '.join([c for c in test['critical'] if c not in found_critical])}")
                results["good"] += 1
            else:
                print(f"❌ POOR - Missing critical information")
                print(f"   Found only {len(found_critical)}/{len(test['critical'])} critical points")
                print(f"   Missing: {', '.join([c for c in test['critical'] if c not in found_critical])}")
                results["poor"] += 1
                
    except Exception as e:
        print(f"Error: {e}")
        results["poor"] += 1

print("\n\n" + "="*70)
print("FINAL ASSESSMENT")
print("="*70)
print(f"✅ Perfect responses: {results['perfect']}/{len(test_scenarios)}")
print(f"⚠️ Good responses: {results['good']}/{len(test_scenarios)}")
print(f"❌ Poor responses: {results['poor']}/{len(test_scenarios)}")
print(f"\nOverall Score: {((results['perfect']*100 + results['good']*70)/len(test_scenarios)):.1f}%")

if results['perfect'] >= 6:
    print("\n🎆 EXCELLENT! Arena models have comprehensive ChatMRPT knowledge.")
    print("Newcomers can confidently use the system without external help.")
elif results['perfect'] + results['good'] >= 5:
    print("\n✅ GOOD! Arena models provide helpful guidance.")
    print("Most users will get accurate help.")
else:
    print("\n⚠️ NEEDS IMPROVEMENT. Some critical workflows not well explained.")