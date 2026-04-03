"""
Production-ready tool registry caching system.

Caches tool discovery results to avoid expensive scanning on every startup.
Critical for deployment performance and reliability.
"""

import os
import json
import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ToolRegistryCache:
    """
    Production-ready caching system for tool registry.
    
    Caches tool discovery results and schemas to avoid expensive
    scanning operations on every application startup.
    """
    
    def __init__(self, cache_dir: str = "instance/tool_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_file = self.cache_dir / "tool_registry.json"
        self.metadata_file = self.cache_dir / "cache_metadata.json"
        
        # Cache TTL - tools change rarely, cache for 24 hours
        self.cache_ttl = timedelta(hours=24)
        
    def _get_tools_directory_hash(self) -> str:
        """Generate hash of tools directory for cache invalidation."""
        tools_dir = Path(__file__).parent.parent / "tools"
        
        # Get modification times of all Python files
        file_mtimes = []
        for py_file in tools_dir.rglob("*.py"):
            if py_file.is_file():
                file_mtimes.append(f"{py_file.name}:{py_file.stat().st_mtime}")
        
        # Create hash from file modification times
        hash_content = "|".join(sorted(file_mtimes))
        return hashlib.md5(hash_content.encode()).hexdigest()
    
    def is_cache_valid(self) -> bool:
        """Check if the cached tool registry is still valid."""
        try:
            if not self.cache_file.exists() or not self.metadata_file.exists():
                logger.info("🔄 Tool cache not found - will rebuild")
                return False
                
            # Load cache metadata
            with open(self.metadata_file, 'r') as f:
                metadata = json.load(f)
            
            # Check TTL
            cache_time = datetime.fromisoformat(metadata['timestamp'])
            if datetime.now() - cache_time > self.cache_ttl:
                logger.info("🕐 Tool cache expired - will rebuild")
                return False
            
            # Check directory hash
            current_hash = self._get_tools_directory_hash()
            if metadata.get('directory_hash') != current_hash:
                logger.info("🔄 Tools directory changed - will rebuild cache")
                return False
                
            logger.info("✅ Tool cache is valid and up-to-date")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Error checking cache validity: {e}")
            return False
    
    def load_cached_registry(self) -> Optional[Dict[str, Any]]:
        """Load cached tool registry data."""
        try:
            if not self.is_cache_valid():
                return None
                
            with open(self.cache_file, 'r') as f:
                cached_data = json.load(f)
                
            logger.info(f"📦 Loaded {len(cached_data.get('tool_names', []))} tools from cache")
            return cached_data
            
        except Exception as e:
            logger.error(f"❌ Error loading cached registry: {e}")
            return None
    
    def save_registry_cache(self, tool_names: List[str], tool_schemas: Dict[str, Any], 
                          discovery_stats: Dict[str, Any]) -> None:
        """Save tool registry data to cache."""
        try:
            # Prepare cache data
            cache_data = {
                'tool_names': tool_names,
                'tool_schemas': tool_schemas,
                'discovery_stats': discovery_stats,
                'cache_version': '1.0'
            }
            
            # Save cache data
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            # Save metadata
            metadata = {
                'timestamp': datetime.now().isoformat(),
                'directory_hash': self._get_tools_directory_hash(),
                'tool_count': len(tool_names),
                'cache_size_bytes': self.cache_file.stat().st_size
            }
            
            with open(self.metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
                
            logger.info(f"💾 Cached {len(tool_names)} tools to {self.cache_file}")
            
        except Exception as e:
            logger.error(f"❌ Error saving registry cache: {e}")
    
    def clear_cache(self) -> None:
        """Clear the tool registry cache."""
        try:
            if self.cache_file.exists():
                self.cache_file.unlink()
            if self.metadata_file.exists():
                self.metadata_file.unlink()
            logger.info("🗑️ Tool cache cleared")
        except Exception as e:
            logger.error(f"❌ Error clearing cache: {e}")


# Global cache instance
_tool_cache = None

def get_tool_cache() -> ToolRegistryCache:
    """Get the global tool cache instance."""
    global _tool_cache
    if _tool_cache is None:
        _tool_cache = ToolRegistryCache()
    return _tool_cache