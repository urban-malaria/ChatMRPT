"""
Flexible LLM adapter that supports multiple backends.
Easily switch between Ollama (development/CPU) and vLLM (production/GPU).
"""

import os
import json
import logging
import requests
from typing import Optional, Dict, Any, List
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class LLMAdapter:
    """
    Unified LLM interface that can use different backends.
    Currently supports:
    - Ollama (for local CPU inference)
    - vLLM (for GPU-accelerated inference)
    - OpenAI (fallback for cloud)
    """
    
    def __init__(self, backend: str = None, **kwargs):
        """
        Initialize LLM adapter with specified backend.
        
        Args:
            backend: 'ollama', 'vllm', or 'openai'
            **kwargs: Backend-specific configuration
        """
        # Auto-detect backend based on environment
        if backend is None:
            if os.environ.get('USE_VLLM', 'false').lower() == 'true':
                backend = 'vllm'
                logger.info("✅ Using vLLM backend (Qwen3-8B) - NO OpenAI")
            elif os.environ.get('USE_OLLAMA', 'false').lower() == 'true':
                backend = 'ollama'
                logger.info("✅ Using Ollama backend - NO OpenAI")
            else:
                # DISABLED: OpenAI fallback removed for privacy
                logger.error("❌ No local LLM backend configured! Set USE_VLLM=true")
                raise ValueError("OpenAI is disabled. Please configure vLLM (USE_VLLM=true) or Ollama (USE_OLLAMA=true)")
        
        self.backend = backend
        self.config = kwargs
        
        # Initialize backend-specific settings
        if backend == 'ollama':
            self.base_url = kwargs.get('base_url', os.environ.get('OLLAMA_BASE_URL', 'http://localhost:11434'))
            self.model = kwargs.get('model', os.environ.get('OLLAMA_MODEL', 'phi3:mini'))
            self.api_url = f"{self.base_url}/api"
            
        elif backend == 'vllm':
            self.base_url = kwargs.get('base_url', os.environ.get('VLLM_BASE_URL', 'http://localhost:8000'))
            self.model = kwargs.get('model', os.environ.get('VLLM_MODEL', 'Qwen/Qwen3-235B-A22B'))  # Best Qwen 3 MoE model
            self.api_url = f"{self.base_url}/v1"  # vLLM uses OpenAI-compatible API
            
        elif backend == 'openai':
            # DISABLED: OpenAI is not used for privacy reasons
            pass  # OpenAI enabled
            # self.api_key = kwargs.get('api_key', os.environ.get('OPENAI_API_KEY'))
            # self.model = kwargs.get('model', os.environ.get('OPENAI_MODEL_NAME', 'gpt-4'))
            # self.base_url = 'https://api.openai.com/v1'
            
        logger.info(f"LLM Adapter initialized with backend: {backend}, model: {self.model}")
    
    def generate(self, prompt: str, max_tokens: int = 500, temperature: float = 0.7, 
                 context: Optional[Any] = None, **kwargs) -> str:
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
        full_prompt = self._build_prompt_with_context(prompt, context)
        
        try:
            if self.backend == 'ollama':
                return self._generate_ollama(full_prompt, max_tokens, temperature, **kwargs)
            elif self.backend == 'vllm':
                return self._generate_vllm(full_prompt, max_tokens, temperature, **kwargs)
            elif self.backend == 'openai':
                return self._generate_openai(full_prompt, max_tokens, temperature, **kwargs)
            else:
                raise ValueError(f"Unsupported backend: {self.backend}")
                
        except Exception as e:
            logger.error(f"Error generating response with {self.backend}: {e}")
            return f"Error generating response: {str(e)}"
    
    def _build_prompt_with_context(self, prompt: str, context: Optional[Any]) -> str:
        """Build prompt with context using ChatML format for Qwen3."""
        # Use ChatML format for Qwen3
        # Add /no_think to disable thinking mode
        full_prompt = "<|im_start|>system\n"
        full_prompt += "You are ChatMRPT, a helpful assistant for malaria risk analysis. "
        full_prompt += "Provide concise, practical insights. Do not repeat the question or show internal thinking.\n"
        full_prompt += "/no_think\n"  # Disable thinking mode
        
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
                # Clean up any thinking tags
                if '<think>' in text:
                    text = text.split('</think>')[-1].strip()
                return text
            else:
                return f"Ollama error: {response.status_code}"
                
        except requests.exceptions.Timeout:
            return "Request timed out. The model may be loading. Please try again."
        except Exception as e:
            return f"Ollama error: {str(e)}"
    
    def _generate_vllm(self, prompt: str, max_tokens: int, temperature: float, **kwargs) -> str:
        """Generate using vLLM backend (OpenAI-compatible API)."""
        try:
            # Get stop tokens, default to Qwen3 tokens if not provided
            stop_tokens = kwargs.get('stop', ["<|im_end|>", "</think>", "<think>"])
            
            response = requests.post(
                f"{self.api_url}/completions",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "stop": stop_tokens,
                    "top_p": 0.8,  # Recommended for Qwen3
                    "top_k": 20,   # Recommended for Qwen3
                    "presence_penalty": 1.0  # Reduce repetitions
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                return result['choices'][0]['text']
            else:
                return f"vLLM error: {response.status_code}"
                
        except Exception as e:
            return f"vLLM error: {str(e)}"
    
    def _generate_openai(self, prompt: str, max_tokens: int, temperature: float, **kwargs) -> str:
        """Generate using OpenAI backend - DISABLED FOR PRIVACY."""
        pass  # OpenAI enabled
        # try:
        #     import openai
        #     openai.api_key = self.api_key
        #     
        #     response = openai.ChatCompletion.create(
        #         model=self.model,
        #         messages=[{"role": "user", "content": prompt}],
        #         max_tokens=max_tokens,
        #         temperature=temperature
        #     )
        #     
        #     return response.choices[0].message.content
        #     
        # except Exception as e:
        #     return f"OpenAI error: {str(e)}"
    
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
            # Simple test generation
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