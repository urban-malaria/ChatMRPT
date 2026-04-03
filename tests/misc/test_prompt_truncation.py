#!/usr/bin/env python3
"""
Test if Ollama is truncating the system prompt
"""

import requests
import json

ollama_url = "http://172.31.45.157:11434/v1/chat/completions"

# Test 1: Short prompt that should work
print("Test 1: Short direct prompt")
print("="*60)

short_prompt = """You are ChatMRPT, a Chat-based Malaria Risk Prioritization Tool. 
ChatMRPT is an AI-powered platform that helps Nigerian health officials analyze malaria risk data.
It allows users to upload ward-level data and geographic boundaries to identify high-risk areas for malaria interventions.
ChatMRPT performs statistical analysis, generates maps, and helps plan ITN (insecticide-treated net) distribution."""

payload = {
    "model": "mistral:7b",
    "messages": [
        {"role": "system", "content": short_prompt},
        {"role": "user", "content": "What is ChatMRPT?"}
    ],
    "temperature": 0.7,
    "max_tokens": 200
}

response = requests.post(ollama_url, json=payload, timeout=30)
if response.status_code == 200:
    answer = response.json()['choices'][0]['message']['content']
    print(f"Response: {answer[:300]}")
    if 'malaria' in answer.lower() and 'risk' in answer.lower():
        print("✅ Correct - mentions malaria risk")
    else:
        print("❌ Wrong - doesn't mention malaria risk")

# Test 2: Medium prompt (5k chars)
print("\n\nTest 2: Medium prompt (5000 chars)")
print("="*60)

medium_prompt = short_prompt + "\n\n" + "Additional context: " * 500  # Make it ~5k chars
medium_prompt = medium_prompt[:5000]

payload['messages'][0]['content'] = medium_prompt
print(f"Prompt size: {len(medium_prompt)} chars")

response = requests.post(ollama_url, json=payload, timeout=30)
if response.status_code == 200:
    answer = response.json()['choices'][0]['message']['content']
    print(f"Response: {answer[:300]}")
    if 'malaria' in answer.lower() and 'risk' in answer.lower():
        print("✅ Correct - mentions malaria risk")
    else:
        print("❌ Wrong - doesn't mention malaria risk")

# Test 3: Large prompt (30k chars - like our actual prompt)
print("\n\nTest 3: Large prompt (30000 chars)")
print("="*60)

import sys
import os
sys.path.append('/home/ec2-user/ChatMRPT')
os.environ['FLASK_ENV'] = 'production'
os.environ['OPENAI_API_KEY'] = 'dummy'

from app.arena.prompts import get_arena_system_prompt
large_prompt = get_arena_system_prompt()

print(f"Prompt size: {len(large_prompt)} chars")
payload['messages'][0]['content'] = large_prompt

response = requests.post(ollama_url, json=payload, timeout=30)
if response.status_code == 200:
    answer = response.json()['choices'][0]['message']['content']
    print(f"Response: {answer[:300]}")
    if 'malaria' in answer.lower() and 'risk' in answer.lower():
        print("✅ Correct - mentions malaria risk")
    else:
        print("❌ Wrong - doesn't mention malaria risk")

print("\n\nCONCLUSION:")
print("="*60)
print("The issue is likely that Ollama/Mistral is truncating or ignoring")
print("the system prompt when it's too large (>16k chars).")
print("\nPossible solutions:")
print("1. Shorten the system prompt to essential information only")
print("2. Move detailed documentation to a separate context injection")
print("3. Use a different model that supports longer contexts")
print("4. Dynamically select relevant documentation based on query")