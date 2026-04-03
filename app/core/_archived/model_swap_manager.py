"""
Model Swap Manager for Dynamic Arena System
Manages loading/unloading of models for optimal VRAM usage
"""
import subprocess
import time
import logging
import psutil
import os
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ModelConfig:
    """Configuration for a model"""
    name: str
    path: str
    port: int
    vram_gb: float
    max_tokens: int = 2048
    dtype: str = "float16"
    gpu_util: float = 0.8

class ModelSwapManager:
    """
    Manages dynamic loading and unloading of vLLM models for Arena system
    """
    
    # Optimized model pairings for 24GB VRAM - Using available models
    MODEL_CONFIGS = {
        'phi-3-mini': ModelConfig(
            name='phi-3-mini',
            path='/home/ec2-user/models/phi3-mini',
            port=8000,
            vram_gb=7.12,
            gpu_util=0.8
        ),
        'biomistral-7b': ModelConfig(
            name='biomistral-7b',
            path='/home/ec2-user/models/models--BioMistral--BioMistral-7B',
            port=8001,
            vram_gb=14.0,
            gpu_util=0.55
        ),
        'qwen3-8b': ModelConfig(
            name='qwen3-8b',
            path='/home/ec2-user/models/models--Qwen--Qwen3-8B',
            port=8000,
            vram_gb=16.0,
            gpu_util=0.65
        ),
        'openhermes-2.5': ModelConfig(
            name='openhermes-2.5',
            path='/home/ec2-user/models/models--teknium--OpenHermes-2.5-Mistral-7B',
            port=8001,
            vram_gb=14.0,
            gpu_util=0.55
        ),
        'llama-3.1-8b': ModelConfig(
            name='llama-3.1-8b',
            path='/home/ec2-user/models/models--meta-llama--Meta-Llama-3.1-8B-Instruct',
            port=8000,
            vram_gb=16.0,
            gpu_util=0.65
        )
    }
    
    # Arena view pairings - Optimized for available models
    VIEW_PAIRINGS = [
        ('phi-3-mini', 'biomistral-7b'),      # View 1: 21GB total (7+14)
        ('qwen3-8b', 'phi-3-mini'),            # View 2: 23GB total (16+7) - swap biomistral
        ('openhermes-2.5', 'biomistral-7b')   # View 3: 28GB total (14+14) - need careful swap
    ]
    
    def __init__(self):
        self.loaded_models = {}  # port -> model_name
        self.loading_state = {}  # model_name -> 'loading'|'loaded'|'unloaded'
        self.current_view = 0
        
    def get_loaded_models(self) -> Dict[int, str]:
        """Get currently loaded models"""
        return self.loaded_models.copy()
    
    def kill_vllm_on_port(self, port: int) -> bool:
        """Kill vLLM process on specified port"""
        try:
            # Find process using the port
            result = subprocess.run(
                f"lsof -ti :{port}", 
                shell=True, 
                capture_output=True, 
                text=True
            )
            
            if result.stdout.strip():
                pid = result.stdout.strip()
                subprocess.run(f"kill -9 {pid}", shell=True)
                logger.info(f"Killed vLLM process on port {port} (PID: {pid})")
                time.sleep(2)  # Wait for process to fully terminate
                return True
                
            logger.info(f"No process found on port {port}")
            return False
            
        except Exception as e:
            logger.error(f"Error killing process on port {port}: {e}")
            return False
    
    def start_model(self, model_name: str) -> bool:
        """Start a vLLM model"""
        if model_name not in self.MODEL_CONFIGS:
            logger.error(f"Unknown model: {model_name}")
            return False
            
        config = self.MODEL_CONFIGS[model_name]
        
        # Mark as loading
        self.loading_state[model_name] = 'loading'
        
        # Build vLLM command
        cmd = f"""
        VLLM_USE_V1=0 nohup python3 -m vllm.entrypoints.openai.api_server \
          --model {config.path} \
          --port {config.port} \
          --host 0.0.0.0 \
          --max-model-len {config.max_tokens} \
          --dtype {config.dtype} \
          --gpu-memory-utilization {config.gpu_util} \
          --disable-log-requests \
          --trust-remote-code > /home/ec2-user/vllm_{model_name}_{config.port}.log 2>&1 &
        """
        
        try:
            subprocess.run(cmd, shell=True)
            logger.info(f"Starting {model_name} on port {config.port}...")
            
            # Wait for model to load (check every 5 seconds, up to 90 seconds)
            for i in range(18):
                time.sleep(5)
                if self.check_model_ready(config.port):
                    self.loaded_models[config.port] = model_name
                    self.loading_state[model_name] = 'loaded'
                    logger.info(f"✓ {model_name} ready on port {config.port}")
                    return True
                    
            logger.error(f"Timeout waiting for {model_name} to start")
            self.loading_state[model_name] = 'unloaded'
            return False
            
        except Exception as e:
            logger.error(f"Error starting {model_name}: {e}")
            self.loading_state[model_name] = 'unloaded'
            return False
    
    def check_model_ready(self, port: int) -> bool:
        """Check if model is ready on port"""
        try:
            import requests
            response = requests.get(f"http://localhost:{port}/v1/models", timeout=2)
            return response.status_code == 200
        except:
            return False
    
    def swap_models_for_view(self, view_index: int) -> Tuple[str, str]:
        """
        Swap models for a specific view
        Returns tuple of (model_a, model_b) that are ready
        """
        if view_index >= len(self.VIEW_PAIRINGS):
            view_index = view_index % len(self.VIEW_PAIRINGS)
            
        model_a, model_b = self.VIEW_PAIRINGS[view_index]
        required_models = {model_a, model_b}
        loaded = set(self.loaded_models.values())
        
        # Check what needs to be loaded
        to_load = required_models - loaded
        to_unload = loaded - required_models
        
        logger.info(f"View {view_index}: Need {required_models}, Have {loaded}")
        
        # Unload unnecessary models first to free VRAM
        for model in to_unload:
            for port, loaded_model in self.loaded_models.items():
                if loaded_model == model:
                    logger.info(f"Unloading {model} from port {port}")
                    self.kill_vllm_on_port(port)
                    del self.loaded_models[port]
                    self.loading_state[model] = 'unloaded'
                    time.sleep(3)  # Wait for VRAM to free
                    
        # Load required models
        for model in to_load:
            config = self.MODEL_CONFIGS[model]
            
            # Check if port is free
            if config.port in self.loaded_models:
                # Need to unload what's on this port first
                old_model = self.loaded_models[config.port]
                logger.info(f"Port {config.port} occupied by {old_model}, unloading...")
                self.kill_vllm_on_port(config.port)
                del self.loaded_models[config.port]
                self.loading_state[old_model] = 'unloaded'
                time.sleep(3)
                
            # Start the model
            if self.start_model(model):
                logger.info(f"✓ Loaded {model}")
            else:
                logger.error(f"✗ Failed to load {model}")
                
        self.current_view = view_index
        return model_a, model_b
    
    def preload_next_view(self, current_view: int):
        """
        Preload models for next view in background
        This is called while user is reading current responses
        """
        next_view = (current_view + 1) % len(self.VIEW_PAIRINGS)
        next_models = self.VIEW_PAIRINGS[next_view]
        
        logger.info(f"Preloading for next view {next_view}: {next_models}")
        
        # Check available VRAM before preloading
        # This is a simplified check - in production use nvidia-smi
        current_vram_usage = sum(
            self.MODEL_CONFIGS[model].vram_gb 
            for model in self.loaded_models.values()
        )
        
        available_vram = 24.0 - current_vram_usage  # 24GB total
        
        for model in next_models:
            if model not in self.loaded_models.values():
                required_vram = self.MODEL_CONFIGS[model].vram_gb
                if required_vram <= available_vram:
                    logger.info(f"Preloading {model} (fits in {available_vram:.1f}GB available)")
                    # Start loading in background
                    # In production, use threading or asyncio
                else:
                    logger.info(f"Cannot preload {model} (needs {required_vram}GB, have {available_vram}GB)")
    
    def get_model_status(self) -> Dict:
        """Get status of all models"""
        return {
            'loaded_models': self.loaded_models,
            'loading_state': self.loading_state,
            'current_view': self.current_view,
            'vram_usage': sum(
                self.MODEL_CONFIGS[model].vram_gb 
                for model in self.loaded_models.values()
            )
        }