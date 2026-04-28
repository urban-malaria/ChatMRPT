#!/usr/bin/env python3
"""
Test if previously poor-performing areas are now improved
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

# Focus on previously problematic areas
test_cases = [
    {
        "category": "Sample Data Access",
        "question": "I don't have any data yet but want to try ChatMRPT. How can I use sample data?",
        "expected": ["sidebar", "left", "expand", "arrow", "samples", "kano", "state"],
        "wrong": ["upload modal", "upload tab"]
    },
    {
        "category": "Sample Data - Direct Question",
        "question": "Where do I find the sample datasets?",
        "expected": ["sidebar", "left", "expand", "samples", "tab"],
        "wrong": ["paperclip", "upload modal"]
    },
    {
        "category": "Download Results",
        "question": "How do I download my analysis results and create reports for stakeholders?",
        "expected": ["paperclip", "download processed data", "tab", "grouped", "tpr", "risk", "word", "report"],
        "wrong": ["automatic download"]
    },
    {
        "category": "Download Location",
        "question": "Where can I find and download my TPR Excel file after analysis?",
        "expected": ["paperclip", "download", "processed", "tab", "tpr", "excel", "category"],
        "wrong": ["email", "automatic"]
    },
    {
        "category": "Complete Beginner Path",
        "question": "I'm completely new. What are my options to get started with ChatMRPT?",
        "expected": ["three", "options", "upload", "sample", "sidebar", "ask", "questions"],
        "wrong": ["must upload", "only upload"]
    },
    {
        "category": "Sidebar Functionality",
        "question": "What's in the sidebar and how do I access it?",
        "expected": ["collapsed", "arrow", "expand", "history", "samples", "kano", "recent"],
        "wrong": ["upload button", "analysis button"]
    }
]

ollama_url = "http://172.31.45.157:11434/v1/chat/completions"

print("TESTING IMPROVED AREAS")
print("Focus: Previously poor-performing responses")
print("="*70)

results = {"excellent": 0, "good": 0, "poor": 0}

for i, test in enumerate(test_cases, 1):
    print(f"\nTest {i}: {test['category']}")
    print("-" * 60)
    print(f"Question: {test['question']}")
    
    payload = {
        "model": "mistral:7b",
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": test['question']}
        ],
        "temperature": 0.7,
        "max_tokens": 400
    }
    
    try:
        response = requests.post(ollama_url, json=payload, timeout=30)
        if response.status_code == 200:
            answer = response.json()['choices'][0]['message']['content']
            answer_lower = answer.lower()
            
            # Check for keywords
            found_expected = [kw for kw in test['expected'] if kw.lower() in answer_lower]
            found_wrong = [kw for kw in test['wrong'] if kw.lower() in answer_lower]
            
            print(f"\nResponse preview: {answer[:300]}...")
            
            # Evaluate
            if len(found_expected) >= len(test['expected']) * 0.7 and not found_wrong:
                print(f"\n✅ EXCELLENT - Correctly explained")
                print(f"   Found {len(found_expected)}/{len(test['expected'])} key points")
                if found_expected:
                    print(f"   Key points: {', '.join(found_expected[:5])}")
                results["excellent"] += 1
            elif len(found_expected) >= len(test['expected']) * 0.5 and not found_wrong:
                print(f"\n⚠️ GOOD - Mostly correct")
                print(f"   Found {len(found_expected)}/{len(test['expected'])} key points")
                missing = [kw for kw in test['expected'] if kw not in found_expected]
                print(f"   Missing: {', '.join(missing[:3])}")
                results["good"] += 1
            else:
                print(f"\n❌ POOR - Still needs improvement")
                if found_wrong:
                    print(f"   Wrong information: {', '.join(found_wrong)}")
                print(f"   Found only {len(found_expected)}/{len(test['expected'])} key points")
                if not found_expected:
                    print(f"   Expected to mention: {', '.join(test['expected'][:3])}...")
                results["poor"] += 1
                
    except Exception as e:
        print(f"Error: {e}")
        results["poor"] += 1

print("\n\n" + "="*70)
print("IMPROVEMENT ASSESSMENT")
print("="*70)
print(f"✅ Excellent: {results['excellent']}/{len(test_cases)}")
print(f"⚠️ Good: {results['good']}/{len(test_cases)}")
print(f"❌ Poor: {results['poor']}/{len(test_cases)}")

score = (results['excellent'] * 100 + results['good'] * 70) / len(test_cases)
print(f"\nImprovement Score: {score:.1f}%")

if results['excellent'] >= 5:
    print("\n🎆 EXCELLENT! Previously problematic areas are now well-explained!")
    print("The improvements have successfully addressed the issues.")
elif results['excellent'] + results['good'] >= 5:
    print("\n✅ GOOD! Significant improvement in problem areas.")
    print("Most issues have been resolved.")
else:
    print("\n⚠️ Still needs work. Some problem areas remain.")

print("\nKey Improvements Made:")
print("1. Sample data location clearly explained (sidebar, not upload modal)")
print("2. Download process detailed with tab navigation")
print("3. Multiple entry paths for beginners documented")
print("4. Sidebar functionality and access method clarified")