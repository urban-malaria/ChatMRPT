#!/usr/bin/env python3
"""
Test if arena models accurately answer risk analysis questions
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

# Test questions about risk analysis workflows
test_cases = [
    {
        "question": "How do I run a malaria risk analysis?",
        "expected": ["standard upload", "csv", "shapefile", "analyze my data", "composite", "pca"],
        "wrong": ["automatic", "sidebar"]
    },
    {
        "question": "What's the difference between Composite and PCA analysis?",
        "expected": ["composite", "simple", "weighted", "pca", "pattern", "component"],
        "wrong": ["tpr"]
    },
    {
        "question": "What files do I need for risk analysis?",
        "expected": ["csv", "shapefile", "wardname", "statecode", "both"],
        "wrong": ["only csv", "just csv"]
    },
    {
        "question": "Can I go from TPR to risk analysis?",
        "expected": ["yes", "option 2", "transition", "lead to risk", "after tpr"],
        "wrong": ["no", "cannot", "separate"]
    },
    {
        "question": "What do the colors on the risk map mean?",
        "expected": ["red", "high", "yellow", "medium", "green", "low"],
        "wrong": ["blue high", "purple"]
    },
    {
        "question": "How do I plan ITN distribution?",
        "expected": ["high-risk", "red", "orange", "priority", "target"],
        "wrong": ["automatic distribution", "equal distribution"]
    },
    {
        "question": "What columns are required in my CSV?",
        "expected": ["wardname", "statecode", "state"],
        "wrong": ["optional", "not required"]
    },
    {
        "question": "What happens after I upload through Standard Upload?",
        "expected": ["type", "analyze", "run analysis", "choose", "composite", "pca"],
        "wrong": ["automatic analysis", "immediately shows"]
    },
    {
        "question": "Can I download the results?",
        "expected": ["yes", "download", "csv", "excel", "word", "report"],
        "wrong": ["cannot download", "no export"]
    },
    {
        "question": "What indicators are recommended for risk analysis?",
        "expected": ["u5_tpr_rdt", "population", "rainfall", "housing", "test positivity"],
        "wrong": ["age", "gender"]
    }
]

ollama_url = "http://172.31.45.157:11434/v1/chat/completions"

print("Testing Arena Model - Risk Analysis Knowledge")
print("="*70)

results = {"accurate": 0, "inaccurate": 0, "unclear": 0}

for i, test in enumerate(test_cases, 1):
    print(f"\nTest {i}: {test['question']}")
    print("-" * 60)
    
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
            
            # Check for expected keywords
            found_expected = [kw for kw in test['expected'] if kw.lower() in answer_lower]
            found_wrong = [kw for kw in test['wrong'] if kw.lower() in answer_lower]
            
            print(f"Response preview: {answer[:250]}...")
            
            if len(found_expected) >= 2 and not found_wrong:
                print(f"✅ ACCURATE - Found: {', '.join(found_expected)}")
                results["accurate"] += 1
            elif found_wrong:
                print(f"❌ INACCURATE - Contains wrong info: {', '.join(found_wrong)}")
                results["inaccurate"] += 1
            else:
                print(f"⚠️ UNCLEAR - Found only: {', '.join(found_expected) if found_expected else 'none'}")
                print(f"   Expected at least 2 of: {', '.join(test['expected'][:3])}...")
                results["unclear"] += 1
                
    except Exception as e:
        print(f"Error: {e}")
        results["unclear"] += 1

print("\n" + "="*70)
print("RISK ANALYSIS TEST SUMMARY")
print("="*70)
print(f"✅ Accurate: {results['accurate']}/{len(test_cases)}")
print(f"❌ Inaccurate: {results['inaccurate']}/{len(test_cases)}")
print(f"⚠️ Unclear: {results['unclear']}/{len(test_cases)}")
print(f"\nAccuracy Rate: {(results['accurate']/len(test_cases))*100:.1f}%")
print("\nKey Points to Verify:")
print("- Standard Upload requires BOTH CSV and Shapefile")
print("- Analysis is triggered by typing 'analyze my data'")
print("- TPR (Option 2) can transition to risk analysis")
print("- Results can be downloaded in multiple formats")
print("- Risk maps use Red=High, Yellow=Medium, Green=Low")