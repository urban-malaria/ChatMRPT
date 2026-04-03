#!/usr/bin/env python3
"""
Test if Ollama models receive and use the ChatMRPT system prompt
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

# Get the prompt that should be sent
prompt = get_arena_system_prompt()
print(f"System prompt loaded: {len(prompt)} characters")
print(f"Contains ChatMRPT documentation: {'ChatMRPT Complete System Documentation' in prompt}")
print("="*70)

# Test with a simple question about ChatMRPT
test_question = "What is ChatMRPT?"

# Call Ollama with the same setup as arena_routes.py
ollama_url = "http://172.31.45.157:11434/v1/chat/completions"

# Test with Mistral model
print("\nTesting with mistral model...")
payload = {
    "model": "mistral:7b",
    "messages": [
        {"role": "system", "content": prompt},
        {"role": "user", "content": test_question}
    ],
    "temperature": 0.7,
    "max_tokens": 800
}

print(f"Sending request to {ollama_url}")
print(f"Payload structure:")
print(f"  - System prompt: {len(payload['messages'][0]['content'])} chars")
print(f"  - User message: '{payload['messages'][1]['content']}'")
print(f"  - Max tokens: {payload['max_tokens']}")

try:
    response = requests.post(ollama_url, json=payload, timeout=30)
    if response.status_code == 200:
        result = response.json()
        answer = result['choices'][0]['message']['content']
        print("\n" + "="*70)
        print("RESPONSE:")
        print(answer[:500])
        print("\n" + "="*70)
        
        # Check if response is accurate
        keywords = ['malaria', 'risk', 'prioritization', 'tool', 'nigeria']
        accurate = any(kw.lower() in answer.lower() for kw in keywords)
        
        if accurate:
            print("✅ Response appears accurate about ChatMRPT")
        else:
            print("❌ Response does NOT accurately describe ChatMRPT")
            print("   Expected keywords: malaria, risk, prioritization, tool, Nigeria")
    else:
        print(f"Error: HTTP {response.status_code}")
        print(response.text)
except Exception as e:
    print(f"Error calling Ollama: {e}")

print("\nDiagnostics:")
print(f"1. Prompt length sent: {len(prompt)} characters")
if len(prompt) > 32000:
    print("   ⚠️ WARNING: Prompt exceeds 32k chars - may be truncated by model")
if len(prompt) > 16000:
    print("   ⚠️ WARNING: Prompt exceeds 16k chars - may affect smaller models")
print(f"2. Model endpoint: {ollama_url}")
print(f"3. Model: mistral:7b")