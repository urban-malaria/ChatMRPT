"""
Flexible LLM adapter that supports multiple backends.
Supports OpenAI, vLLM (local GPU), Ollama, and cloud providers.
Enhanced for Arena mode with multiple model support.
"""

import os
import json
import logging
import requests
import asyncio
import aiohttp
from typing import Optional, Dict, Any, List, Union
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


# No fallback map; models must be configured explicitly.


class LLMAdapter:
    """
    Unified LLM interface that can use different backends.
    Currently supports:
    - Ollama (for local CPU inference)
    - vLLM (for GPU-accelerated inference)
    - OpenAI (fallback for cloud)
    """
    
    def __init__(self, backend: str = None, model: str = None, **kwargs):
        """
        Initialize LLM adapter with specified backend.
        
        Args:
            backend: 'openai', 'vllm', 'ollama', 'mistral', 'groq'
            model: Specific model name (e.g., 'llama-3.1-8b', 'mistral-7b')
            **kwargs: Backend-specific configuration
        """
        # Auto-detect backend based on environment or explicit setting
        if backend is None:
            if os.environ.get('USE_VLLM', 'false').lower() == 'true':
                backend = 'vllm'
                logger.info("Using vLLM backend for local models")
            elif os.environ.get('USE_OLLAMA', 'false').lower() == 'true':
                backend = 'ollama'
                logger.info("Using Ollama backend")
            else:
                backend = 'openai'  # Default to OpenAI for compatibility
                logger.info("Using OpenAI backend")
        
        self.backend = backend
        self.config = kwargs
        
        # Model-specific configuration for Arena mode
        model_configs = {
            'gpt-4o': {'backend': 'openai', 'model_name': 'gpt-4o'},
            'llama-3.1-8b': {'backend': 'vllm', 'model_name': 'meta-llama/Llama-3.1-8B-Instruct'},
            'mistral-7b': {'backend': 'vllm', 'model_name': 'mistralai/Mistral-7B-Instruct-v0.3'},
            'qwen-2.5-7b': {'backend': 'vllm', 'model_name': 'Qwen/Qwen2.5-7B-Instruct'},
            'phi-3-mini': {'backend': 'vllm', 'model_name': 'microsoft/Phi-3-mini-4k-instruct'},
        }
        
        # Override backend if specific model is requested
        if model and model in model_configs:
            self.backend = model_configs[model]['backend']
            model_name = model_configs[model]['model_name']
        else:
            model_name = model
        
        # Initialize backend-specific settings
        if backend == 'ollama':
            self.base_url = kwargs.get('base_url', os.environ.get('OLLAMA_BASE_URL', 'http://localhost:11434'))
            self.model = model_name or kwargs.get('model', 'phi3:mini')
            self.api_url = f"{self.base_url}/api"
            
        elif backend == 'vllm':
            self.base_url = kwargs.get('base_url', os.environ.get('VLLM_BASE_URL', 'http://localhost:8000'))
            self.model = model_name or kwargs.get('model', 'meta-llama/Llama-3.1-8B-Instruct')
            self.api_url = f"{self.base_url}/v1"  # vLLM uses OpenAI-compatible API
            
        elif backend == 'openai':
            self.api_key = kwargs.get('api_key', os.environ.get('OPENAI_API_KEY'))
            self.model = model_name or kwargs.get('model', 'gpt-4o')
            self.base_url = 'https://api.openai.com/v1'
            
        elif backend == 'mistral':
            self.api_key = kwargs.get('api_key', os.environ.get('MISTRAL_API_KEY'))
            self.model = model_name or 'mistral-large-latest'
            self.base_url = 'https://api.mistral.ai/v1'
            
        elif backend == 'groq':
            self.api_key = kwargs.get('api_key', os.environ.get('GROQ_API_KEY'))
            self.model = model_name or 'llama3-70b-8192'
            self.base_url = 'https://api.groq.com/openai/v1'
            
        logger.info(f"LLM Adapter initialized: backend={backend}, model={self.model}")
    
    def generate(self, prompt: str, max_tokens: int = 500, temperature: float = 0.7, 
                 context: Optional[Any] = None, system_message: Optional[str] = None, **kwargs) -> str:
        """
        Generate response using configured backend.
        
        Args:
            prompt: User query
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            context: Optional context (DataFrame, dict, etc.)
            **kwargs: Additional backend-specific parameters
            
        Returns:
            Generated text response
        """
        # Build prompt with context if provided
        full_prompt = self._build_prompt_with_context(prompt, context, system_message=system_message)
        
        try:
            if self.backend == 'ollama':
                return self._generate_ollama(full_prompt, max_tokens, temperature, **kwargs)
            elif self.backend == 'vllm':
                return self._generate_vllm(full_prompt, max_tokens, temperature, **kwargs)
            elif self.backend == 'openai':
                return self._generate_openai(full_prompt, max_tokens, temperature, system_message=system_message, **kwargs)
            elif self.backend in ('groq', 'mistral'):
                return self._generate_openai_compatible(full_prompt, max_tokens, temperature, system_message=system_message, **kwargs)
            else:
                raise ValueError(f"Unsupported backend: {self.backend}")
                
        except Exception as e:
            logger.error(f"Error generating response with {self.backend}: {e}")
            return f"Error generating response: {str(e)}"
    
    def _clean_chain_of_thought(self, text: str) -> str:
        """
        Remove chain of thought, reasoning, and thinking patterns from model output.
        Handles various formats used by different models (especially Qwen).
        """
        import re

        # Remove <thinking>...</thinking> tags and content
        text = re.sub(r'<thinking>.*?</thinking>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Remove <think>...</think> tags and content
        text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Remove <reasoning>...</reasoning> tags and content
        text = re.sub(r'<reasoning>.*?</reasoning>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Remove **Reasoning:** or **Chain of thought:** sections
        text = re.sub(r'\*\*(?:Reasoning|Chain of thought|Thinking|Internal reasoning):?\*\*.*?(?=\n\n|\*\*|$)', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Remove lines starting with "Reasoning:" or "Thinking:"
        text = re.sub(r'^(?:Reasoning|Thinking|Chain of thought):.*?$', '', text, flags=re.MULTILINE | re.IGNORECASE)

        # Remove hidden reasoning between curly braces like {{reasoning}}
        text = re.sub(r'\{\{.*?\}\}', '', text, flags=re.DOTALL)

        # Remove any remaining reasoning steps like "Step 1:", "Step 2:" if they appear in a reasoning block
        if 'step 1:' in text.lower() and 'step 2:' in text.lower():
            # Only remove if it looks like a reasoning chain
            lines = text.split('\n')
            filtered_lines = []
            in_reasoning = False
            for line in lines:
                if re.match(r'^step \d+:', line.strip(), re.IGNORECASE):
                    in_reasoning = True
                elif line.strip() and not re.match(r'^step \d+:', line.strip(), re.IGNORECASE):
                    in_reasoning = False
                if not in_reasoning:
                    filtered_lines.append(line)
            text = '\n'.join(filtered_lines)

        # Clean up extra whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = text.strip()

        return text

    def _build_prompt_with_context(self, prompt: str, context: Optional[Any], system_message: Optional[str] = None) -> str:
        """Build prompt with context using ChatML format for Qwen3."""
        # Use ChatML format
        full_prompt = "<|im_start|>system\n"
        if system_message:
            full_prompt += system_message.strip()
        else:
            full_prompt += "You are ChatMRPT, a helpful assistant for malaria risk analysis. "
            full_prompt += "Provide concise, direct responses without showing internal reasoning."
        
        # Add context if provided
        if context:
            full_prompt += "\n# Context:\n"
            if isinstance(context, pd.DataFrame):
                full_prompt += f"- Data shape: {context.shape[0]} rows × {context.shape[1]} columns\n"
                full_prompt += f"- Columns: {', '.join(context.columns[:10])}\n"
                if len(context) > 0:
                    full_prompt += f"- Sample: {context.head(2).to_string()}\n"
            elif isinstance(context, dict):
                full_prompt += f"{json.dumps(context, indent=2, default=str)[:500]}\n"
            else:
                full_prompt += f"{str(context)[:500]}\n"
        
        full_prompt += "<|im_end|>\n"
        full_prompt += f"<|im_start|>user\n{prompt}<|im_end|>\n"
        full_prompt += "<|im_start|>assistant\n"
        
        return full_prompt
    
    def _generate_ollama(self, prompt: str, max_tokens: int, temperature: float, **kwargs) -> str:
        """Generate using Ollama backend."""
        try:
            response = requests.post(
                f"{self.api_url}/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "num_predict": max_tokens,
                        "stop": ["<|im_end|>", "<|im_start|>", "</think>"]
                    }
                },
                timeout=30  # Shorter timeout for smaller models
            )
            
            if response.status_code == 200:
                result = response.json()
                text = result.get('response', '')
                # Clean up any thinking/reasoning tags (Qwen uses various formats)
                text = self._clean_chain_of_thought(text)
                return text
            else:
                return f"Ollama error: {response.status_code}"
                
        except requests.exceptions.Timeout:
            return "Request timed out. The model may be loading. Please try again."
        except Exception as e:
            return f"Ollama error: {str(e)}"
    
    def generate_stream(self, prompt: str, max_tokens: int = 500, temperature: float = 0.7, **kwargs):
        """Generate streaming response using configured backend."""
        if self.backend == 'vllm':
            yield from self._generate_vllm_stream(prompt, max_tokens, temperature, **kwargs)
        else:
            # Fallback to non-streaming for other backends
            response = self.generate(prompt, max_tokens, temperature, **kwargs)
            # Simulate streaming by yielding words
            words = response.split()
            for i, word in enumerate(words):
                yield word + (' ' if i < len(words) - 1 else '')
    
    def _generate_vllm_stream(self, prompt: str, max_tokens: int, temperature: float, **kwargs):
        """Stream response from vLLM using SSE."""
        try:
            # Parse simple format to messages (same as _generate_vllm)
            messages = []
            
            if "System:" in prompt or "User:" in prompt:
                # Parse the simple format
                current_content = []
                current_role = None
                
                for line in prompt.split('\n'):
                    if line.startswith('System:'):
                        if current_role and current_content:
                            messages.append({"role": current_role, "content": '\n'.join(current_content).strip()})
                        current_role = "system"
                        current_content = [line[7:].strip()]
                    elif line.startswith('User:'):
                        if current_role and current_content:
                            messages.append({"role": current_role, "content": '\n'.join(current_content).strip()})
                        current_role = "user"
                        current_content = [line[5:].strip()]
                    elif line.startswith('Assistant:'):
                        if current_role and current_content:
                            messages.append({"role": current_role, "content": '\n'.join(current_content).strip()})
                        current_role = "assistant"
                        current_content = [line[10:].strip()]
                    elif current_content is not None:
                        current_content.append(line)
                
                # Add last message if exists
                if current_role and current_content:
                    content = '\n'.join(current_content).strip()
                    if content:  # Only add if there's content
                        messages.append({"role": current_role, "content": content})
            else:
                messages = [
                    {"role": "system", "content": "You are ChatMRPT, a helpful assistant for malaria risk analysis."},
                    {"role": "user", "content": prompt}
                ]
            
            # Use streaming chat API
            response = requests.post(
                f"{self.api_url}/chat/completions",
                json={
                    "model": self.model,
                    "messages": messages,
                    "stream": True,  # Enable streaming
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "top_p": 0.8,
                    "top_k": 20,
                    "presence_penalty": 1.0,
                    "chat_template_kwargs": {
                        "enable_thinking": False
                    }
                },
                stream=True,  # Stream the response
                timeout=60
            )
            
            if response.status_code == 200:
                for line in response.iter_lines():
                    if line:
                        line = line.decode('utf-8')
                        if line.startswith('data: '):
                            data = line[6:]  # Remove 'data: ' prefix
                            if data == '[DONE]':
                                break
                            try:
                                import json
                                chunk = json.loads(data)
                                if 'choices' in chunk and len(chunk['choices']) > 0:
                                    delta = chunk['choices'][0].get('delta', {})
                                    content = delta.get('content', '')
                                    if content:
                                        # Clean thinking tags if any
                                        if '<think>' not in content and '</think>' not in content:
                                            yield content
                            except json.JSONDecodeError:
                                continue
            else:
                logger.error(f"Streaming failed with status {response.status_code}")
                yield f"Error: Unable to generate response (status {response.status_code})"
                
        except Exception as e:
            logger.error(f"Streaming error: {str(e)}")
            yield f"Error: {str(e)}"
    
    def _generate_vllm(self, prompt: str, max_tokens: int, temperature: float, **kwargs) -> str:
        """Generate using vLLM backend with chat API for Llama 3.1."""
        try:
            # Parse simple format to messages
            messages = []
            
            # Check if prompt has explicit role markers
            if "System:" in prompt or "User:" in prompt:
                # Parse the simple format
                current_content = []
                current_role = None
                
                for line in prompt.split('\n'):
                    if line.startswith('System:'):
                        if current_role and current_content:
                            messages.append({"role": current_role, "content": '\n'.join(current_content).strip()})
                        current_role = "system"
                        current_content = [line[7:].strip()]
                    elif line.startswith('User:'):
                        if current_role and current_content:
                            messages.append({"role": current_role, "content": '\n'.join(current_content).strip()})
                        current_role = "user"
                        current_content = [line[5:].strip()]
                    elif line.startswith('Assistant:'):
                        if current_role and current_content:
                            messages.append({"role": current_role, "content": '\n'.join(current_content).strip()})
                        current_role = "assistant"
                        current_content = [line[10:].strip()]
                    elif current_content is not None:
                        current_content.append(line)
                
                # Add last message if exists
                if current_role and current_content:
                    content = '\n'.join(current_content).strip()
                    if content:  # Only add if there's content
                        messages.append({"role": current_role, "content": content})
            else:
                # Simple user message
                messages = [
                    {"role": "system", "content": "You are ChatMRPT, a helpful assistant for malaria risk analysis."},
                    {"role": "user", "content": prompt}
                ]
            
            # Use chat completions API with proper thinking control
            response = requests.post(
                f"{self.api_url}/chat/completions",
                json={
                    "model": self.model,
                    "messages": messages,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "top_p": 0.8,
                    "top_k": 20,
                    "presence_penalty": 1.0,
                    "chat_template_kwargs": {
                        "enable_thinking": False  # Disable thinking mode
                    }
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                text = result['choices'][0]['message']['content']
                
                # Clean up any remaining thinking tags (fallback)
                import re
                text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
                text = text.replace('<think>', '').replace('</think>', '')
                text = text.strip()
                
                return text
            else:
                # If chat API fails, fallback to completions API
                logger.warning(f"Chat API failed with {response.status_code}, falling back to completions API")
                return self._generate_vllm_completions(prompt, max_tokens, temperature, **kwargs)
                
        except Exception as e:
            logger.error(f"vLLM chat error: {str(e)}, falling back to completions")
            return self._generate_vllm_completions(prompt, max_tokens, temperature, **kwargs)
    
    def _generate_vllm_completions(self, prompt: str, max_tokens: int, temperature: float, **kwargs) -> str:
        """Fallback to completions API for vLLM."""
        try:
            stop_tokens = kwargs.get('stop', ["User:", "System:"])
            
            response = requests.post(
                f"{self.api_url}/completions",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "stop": stop_tokens,
                    "top_p": 0.8,
                    "top_k": 20,
                    "presence_penalty": 1.0
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                text = result['choices'][0]['text']
                
                # Aggressive cleanup of thinking content
                import re
                # Remove everything before </think> if it exists
                if '</think>' in text:
                    text = text.split('</think>')[-1]
                # Remove <think> tags and content
                text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
                text = text.replace('<think>', '').replace('</think>', '')
                # Remove any "thinking" content patterns
                text = re.sub(r'^.*?(?:Okay|Alright|Hmm|Let me|I need to|First).*?\..*?(?=\n\n|\Z)', '', text, flags=re.DOTALL)
                text = text.strip()
                
                return text if text else "Hello! How can I assist you with malaria risk analysis today?"
            else:
                return f"vLLM error: {response.status_code}"
                
        except Exception as e:
            return f"vLLM error: {str(e)}"
    
    def _generate_openai(self, prompt: str, max_tokens: int, temperature: float, system_message: Optional[str] = None, **kwargs) -> str:
        """Generate using OpenAI backend with graceful model fallbacks."""
        try:
            from openai import OpenAI
        except Exception as e:
            return f"OpenAI error: failed to import client: {str(e)}"

        if not self.api_key:
            return "OpenAI error: missing OPENAI_API_KEY"

        def _try_model(model_id: str) -> str:
            # Honor optional OPENAI_PROJECT; do not override OPENAI_BASE_URL here
            project = os.environ.get('OPENAI_PROJECT')
            base_url_env = os.environ.get('OPENAI_BASE_URL')
            try:
                client = OpenAI(api_key=self.api_key, project=project) if project else OpenAI(api_key=self.api_key)
            except TypeError:
                # Older SDK versions may not accept 'project' kwarg
                client = OpenAI(api_key=self.api_key)
            logger.info(f"OpenAI call: model={model_id}, project={'set' if project else 'unset'}, base_url_env={'set' if base_url_env else 'unset'}")
            messages = []
            if system_message:
                messages.append({"role": "system", "content": system_message})
                messages.append({"role": "user", "content": prompt})
            else:
                # Fallback: no system message provided
                messages.append({"role": "user", "content": prompt})

            resp = client.chat.completions.create(
                model=model_id,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature
            )
            return resp.choices[0].message.content or ""

        # Build candidate model list: primary then fallbacks
        fallback_env = os.environ.get('OPENAI_MODEL_FALLBACKS', '')
        env_fallbacks = [m.strip() for m in fallback_env.split(',') if m.strip()] if fallback_env else []
        default_fallbacks = ['chatgpt-4o-latest', 'gpt-4o-mini', 'gpt-4o-2024-08-06']

        seen = set()
        candidates = []
        for m in [self.model] + env_fallbacks + default_fallbacks:
            if m and m not in seen:
                seen.add(m)
                candidates.append(m)

        last_error = None
        for model_id in candidates:
            try:
                return _try_model(model_id)
            except Exception as e:
                msg = str(e)
                last_error = msg
                # If the model doesn't exist for this account, try next candidate
                if '404' in msg or 'not found' in msg.lower() or 'does not exist' in msg.lower():
                    logger.warning(f"OpenAI model '{model_id}' unavailable (404/not found). Trying fallback if available...")
                    continue
                # For other errors (auth, rate limits, etc.) don't silently hide root cause
                logger.error(f"OpenAI call failed for model '{model_id}': {msg}")
                return f"OpenAI error: {msg}"

        return f"OpenAI error: all candidate models failed. Last error: {last_error or 'unknown'}"

    def _generate_openai_compatible(self, prompt: str, max_tokens: int, temperature: float,
                                    system_message: Optional[str] = None, **kwargs) -> str:
        """Generate using OpenAI-compatible APIs (Groq, Mistral, etc.)."""
        if not self.api_key:
            return f"{self.backend.title()} error: missing API key (GROQ_API_KEY or MISTRAL_API_KEY)"

        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }

            messages = []
            if system_message:
                messages.append({"role": "system", "content": system_message})
                messages.append({"role": "user", "content": prompt})
            else:
                messages.append({"role": "user", "content": prompt})

            payload = {
                "model": self.model,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature
            }

            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                text = result['choices'][0]['message']['content'] or ""
                # Clean chain-of-thought tags (Qwen uses <think>)
                text = self._clean_chain_of_thought(text)
                return text
            else:
                error_msg = response.text[:200] if response.text else f"HTTP {response.status_code}"
                logger.error(f"{self.backend.title()} API error: {error_msg}")
                return f"{self.backend.title()} error: {error_msg}"

        except requests.exceptions.Timeout:
            return f"{self.backend.title()} error: request timed out"
        except Exception as e:
            logger.error(f"{self.backend.title()} error: {str(e)}")
            return f"{self.backend.title()} error: {str(e)}"

    def analyze_tpr_data(self, tpr_data: pd.DataFrame, query: str) -> Dict[str, Any]:
        """
        Analyze TPR data with full context.
        
        Args:
            tpr_data: TPR DataFrame
            query: Analysis query
            
        Returns:
            Analysis results
        """
        # Calculate basic TPR statistics
        stats = {
            'total_wards': tpr_data['Ward'].nunique() if 'Ward' in tpr_data.columns else 0,
            'total_facilities': tpr_data['Health Facility'].nunique() if 'Health Facility' in tpr_data.columns else 0,
            'date_range': f"{tpr_data.index.min()} to {tpr_data.index.max()}" if not tpr_data.empty else "N/A",
            'average_tpr': tpr_data['TPR'].mean() if 'TPR' in tpr_data.columns else 0
        }
        
        # Generate analysis using LLM
        analysis_prompt = f"""
Analyze this TPR data and answer: {query}

Key Statistics:
- Total Wards: {stats['total_wards']}
- Total Facilities: {stats['total_facilities']}
- Date Range: {stats['date_range']}
- Average TPR: {stats['average_tpr']:.2f}%

Provide specific, actionable insights.
"""
        
        response = self.generate(
            prompt=analysis_prompt,
            context=tpr_data.head(50),  # Include sample data
            max_tokens=500,
            temperature=0.3  # Lower temperature for factual analysis
        )
        
        return {
            'success': True,
            'analysis': response,
            'statistics': stats,
            'backend': self.backend,
            'model': self.model
        }
    
    def switch_backend(self, new_backend: str, **kwargs):
        """
        Switch to a different backend dynamically.
        Useful when GPU becomes available.
        
        Args:
            new_backend: 'ollama', 'vllm', or 'openai'
            **kwargs: Backend-specific configuration
        """
        logger.info(f"Switching LLM backend from {self.backend} to {new_backend}")
        self.__init__(backend=new_backend, **kwargs)
    
    def health_check(self) -> Dict[str, Any]:
        """Check if the backend is responsive."""
        try:
            # For vLLM, send a properly formatted test prompt
            if self.backend == 'vllm':
                # Use raw API call for health check to avoid complex formatting
                import requests
                # First try to get models to verify connectivity
                try:
                    models_response = requests.get(f"{self.api_url}/models", timeout=2)
                    if models_response.status_code == 200:
                        return {
                            'healthy': True,
                            'backend': self.backend,
                            'model': self.model,
                            'response_sample': 'vLLM is responsive'
                        }
                except:
                    pass
                
                # Fallback to completions test with Llama format
                response = requests.post(
                    f"{self.api_url}/completions",
                    json={
                        "model": self.model,
                        "prompt": "User: Hello\nAssistant:",
                        "max_tokens": 5,
                        "temperature": 0.1
                    },
                    timeout=5
                )
                if response.status_code == 200:
                    return {
                        'healthy': True,
                        'backend': self.backend,
                        'model': self.model,
                        'response_sample': 'vLLM is responsive'
                    }
                else:
                    return {
                        'healthy': False,
                        'backend': self.backend,
                        'error': f"vLLM returned status {response.status_code}"
                    }
            else:
                # For other backends, use simple test
                response = self.generate("Hello", max_tokens=10, temperature=0.1)
                return {
                    'healthy': len(response) > 0,
                    'backend': self.backend,
                    'model': self.model,
                    'response_sample': response[:50]
                }
        except Exception as e:
            return {
                'healthy': False,
                'backend': self.backend,
                'error': str(e)
            }


# Factory function for easy initialization
def create_llm_adapter() -> LLMAdapter:
    """
    Create LLM adapter with auto-detected backend.
    
    Returns:
        Configured LLMAdapter instance
    """
    # Check environment for backend preference
    if os.environ.get('USE_VLLM', 'false').lower() == 'true':
        logger.info("Creating LLM adapter with vLLM backend (GPU)")
        return LLMAdapter(backend='vllm')
    elif os.environ.get('USE_OLLAMA', 'false').lower() == 'true':
        logger.info("Creating LLM adapter with Ollama backend (CPU)")
        return LLMAdapter(backend='ollama')
    else:
        logger.info("Creating LLM adapter with OpenAI backend (Cloud)")
        return LLMAdapter(backend='openai')
