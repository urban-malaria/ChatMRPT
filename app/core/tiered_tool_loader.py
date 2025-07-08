"""
Tiered Tool Loading System for ChatMRPT

This module implements a production-ready tiered loading strategy that loads tools
in groups based on usage patterns, avoiding the performance issues of both
eager loading (slow startup) and lazy loading (production bottlenecks).

Strategy:
- Core tools (90% of users): Loaded immediately on startup
- Visualization tools: Loaded as bundle when first viz tool is needed
- Statistical tools: Loaded as bundle when first stats tool is needed
- Specialized tools: Loaded individually when needed
"""

import logging
import importlib
import time
from typing import Dict, List, Type, Set, Optional, Any
from dataclasses import dataclass
from enum import Enum
import threading

# Direct import to avoid heavy app.tools.__init__.py (13+ second delay)
import sys
import importlib.util
import os

# Load base.py directly without going through __init__.py
_base_spec = importlib.util.spec_from_file_location(
    "app.tools.base", 
    os.path.join(os.path.dirname(__file__), "..", "tools", "base.py")
)
_base_module = importlib.util.module_from_spec(_base_spec)
sys.modules["app.tools.base"] = _base_module
_base_spec.loader.exec_module(_base_module)

BaseTool = _base_module.BaseTool
ToolCategory = _base_module.ToolCategory

logger = logging.getLogger(__name__)


class ToolGroup(Enum):
    """Tool loading groups based on usage patterns"""
    CORE = "core"
    VISUALIZATION = "visualization"
    STATISTICAL = "statistical"
    SPECIALIZED = "specialized"


@dataclass
class ToolGroupDefinition:
    """Definition of a tool group with its modules and tools"""
    name: str
    modules: List[str]
    tools: List[str]
    description: str
    dependencies: List[str] = None  # Heavy dependencies this group loads


class TieredToolLoader:
    """
    Manages tiered loading of tools based on usage patterns.
    
    This class implements the production-proven pattern of loading tools
    in strategic groups rather than all-at-once or one-by-one.
    """
    
    def __init__(self):
        self._loaded_groups: Set[ToolGroup] = set()
        self._tools: Dict[str, Type[BaseTool]] = {}
        self._group_definitions = self._define_tool_groups()
        self._loading_lock = threading.Lock()
        self._load_times: Dict[ToolGroup, float] = {}
        
        # Don't load any tools at startup - load them when user first visits the app
        logger.info("🚀 Tiered loader initialized - tools will load on first app visit")
    
    def _define_tool_groups(self) -> Dict[ToolGroup, ToolGroupDefinition]:
        """Define tool groups based on usage patterns analysis"""
        return {
            ToolGroup.CORE: ToolGroupDefinition(
                name="Core Tools",
                modules=[
                    'app.tools.knowledge_tools',          # greetings, explanations, methodology (truly lightweight)
                    'app.tools.variable_distribution',    # variable distribution maps (eager loaded)
                ],
                tools=[
                    'simplegreeting',
                    'explainconcept',
                    'explainanalysismethodology',
                    'variable_distribution'
                ],
                description="Essential tools for immediate startup including variable visualization and methodology explanations",
                dependencies=['plotly', 'geopandas']  # Needed for variable distribution
            ),
            
            ToolGroup.VISUALIZATION: ToolGroupDefinition(
                name="Visualization Tools",
                modules=[
                    'app.tools.visualization_maps_tools',
                    'app.tools.visualization_charts_tools',
                    'app.tools.settlement_visualization_tools',
                ],
                tools=[
                    'createvulnerabilitymap', 'createpcamap', 'createurbanextentmap',
                    'createhistogram', 'createscatterplot', 'createcorrelationheatmap',
                    'createsettlementanalysismap', 'createinterventionmap'
                ],
                description="All visualization tools sharing plotly/matplotlib",
                dependencies=['plotly', 'matplotlib', 'folium']
            ),
            
            ToolGroup.STATISTICAL: ToolGroupDefinition(
                name="Statistical Analysis Tools",
                modules=[
                    'app.tools.ward_data_tools',          # ward data queries (moved from CORE)
                    'app.tools.smart_knowledge_tools',    # personalized recommendations (moved from CORE)
                    'app.tools.risk_analysis_tools',      # ward risk tools (moved from CORE)
                    'app.tools.statistical_analysis_tools',
                    'app.tools.spatial_autocorrelation_tools',
                ],
                tools=[
                    'getwardinformation', 'getwardvariable', 'comparewards', 'searchwards',  # Ward data
                    'getpersonalizedrecommendations', 'interpretyourresults',  # Smart knowledge
                    'getwardriskscore', 'gettopriskwards', 'filterwardsbyrisklevel', 'getriskstatistics',  # Risk analysis
                    'getdescriptivestatistics', 'getcorrelationanalysis',
                    'performregressionanalysis', 'performanova', 'performttest'
                ],
                description="Statistical, ward data, and risk analysis tools using scipy/sklearn/geopandas",
                dependencies=['scipy', 'sklearn', 'statsmodels', 'geopandas']
            ),
            
            ToolGroup.SPECIALIZED: ToolGroupDefinition(
                name="Specialized Tools",
                modules=[
                    'app.tools.complete_analysis_tools',  # Heavy analysis tools (moved from CORE)
                    'app.tools.settlement_validation_tools',
                    'app.tools.settlement_intervention_tools',
                    'app.tools.scenario_simulation_tools',
                    'app.tools.intervention_targeting_tools',
                    'app.tools.data_preparation_tools',
                    'app.tools.advanced_mapping_tools',
                ],
                tools=[
                    'runcompleteanalysis', 'runcompositeanalysis', 'runpcaanalysis',  # Complete analysis tools
                    'runcustomcompositeanalysis', 'runcustompcaanalysis',  # Custom variable analysis tools
                    'generatecomprehensiveanalysissummary',  # Analysis summary tool
                    'settlement_validation_tools', 'settlement_intervention_tools',
                    'scenario_simulation_tools', 'intervention_targeting_tools'
                ],
                description="Low-frequency specialized tools and heavy analysis workflows",
                dependencies=['geopandas', 'shapely', 'rasterio']
            )
        }
    
    def _load_core_tools(self):
        """Load core tools immediately on startup"""
        logger.info("🚀 Loading core tools for fast startup...")
        start_time = time.time()
        
        try:
            self._load_tool_group(ToolGroup.CORE)
            load_time = time.time() - start_time
            self._load_times[ToolGroup.CORE] = load_time
            logger.info(f"✅ Core tools loaded in {load_time:.2f}s ({len(self._tools)} tools)")
        except Exception as e:
            logger.error(f"❌ Failed to load core tools: {e}")
            raise
    
    def _load_tool_group(self, group: ToolGroup):
        """Load all tools in a specific group"""
        if group in self._loaded_groups:
            return
        
        with self._loading_lock:
            # Double-check pattern
            if group in self._loaded_groups:
                return
            
            group_def = self._group_definitions[group]
            logger.info(f"📦 Loading {group_def.name} ({len(group_def.modules)} modules)...")
            
            start_time = time.time()
            loaded_tools = 0
            
            for module_path in group_def.modules:
                try:
                    module = importlib.import_module(module_path)
                    
                    # Find all BaseTool subclasses in the module
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        
                        if (isinstance(attr, type) and 
                            issubclass(attr, BaseTool) and 
                            attr != BaseTool):
                            
                            tool_name = attr.get_tool_name()
                            self._tools[tool_name] = attr
                            loaded_tools += 1
                            
                except Exception as e:
                    logger.warning(f"⚠️ Failed to load module {module_path}: {e}")
                    continue
            
            load_time = time.time() - start_time
            self._load_times[group] = load_time
            self._loaded_groups.add(group)
            
            logger.info(f"✅ {group_def.name} loaded: {loaded_tools} tools in {load_time:.2f}s")
    
    def get_tool(self, tool_name: str) -> Optional[Type[BaseTool]]:
        """
        Get a tool, loading its group if necessary.
        
        This is the main entry point that implements the tiered loading strategy.
        """
        # Check if already loaded
        if tool_name in self._tools:
            return self._tools[tool_name]
        
        # Determine which group this tool belongs to
        target_group = self._find_tool_group(tool_name)
        
        if target_group:
            logger.info(f"🔄 Loading {target_group.value} group for tool '{tool_name}'")
            self._load_tool_group(target_group)
            
            # Check if tool is now available
            if tool_name in self._tools:
                return self._tools[tool_name]
        
        logger.warning(f"❌ Tool '{tool_name}' not found in any group")
        return None
    
    def _find_tool_group(self, tool_name: str) -> Optional[ToolGroup]:
        """Find which group a tool belongs to"""
        for group, group_def in self._group_definitions.items():
            if tool_name in group_def.tools:
                return group
        return None
    
    def get_available_tools(self) -> Dict[str, Type[BaseTool]]:
        """Get all currently loaded tools"""
        return self._tools.copy()
    
    def get_tool_names(self) -> List[str]:
        """Get names of all currently loaded tools"""
        return list(self._tools.keys())
    
    def get_all_available_tool_names(self) -> List[str]:
        """Get names of all tools that can be loaded (loaded or unloaded)"""
        all_tools = set(self._tools.keys())
        
        # Add tools from all group definitions
        for group_def in self._group_definitions.values():
            all_tools.update(group_def.tools)
        
        return list(all_tools)
    
    def get_basic_tool_schemas(self) -> List[Dict[str, Any]]:
        """Get basic schemas for currently loaded tools without heavy registry initialization"""
        schemas = []
        
        for tool_name, tool_class in self._tools.items():
            try:
                # Get basic schema from the tool class
                schema = {
                    "name": tool_name,
                    "description": tool_class.get_description(),
                    "parameters": tool_class.schema() if hasattr(tool_class, 'schema') else {
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                }
                schemas.append(schema)
            except Exception as e:
                logger.warning(f"Could not get schema for {tool_name}: {e}")
                # Add minimal schema
                schemas.append({
                    "name": tool_name,
                    "description": f"Execute {tool_name}",
                    "parameters": {"type": "object", "properties": {}}
                })
        
        return schemas
    
    def preload_group(self, group: ToolGroup):
        """Preload a specific tool group"""
        if group not in self._loaded_groups:
            self._load_tool_group(group)
    
    def preload_visualization_tools(self):
        """Preload visualization tools (commonly used together)"""
        self.preload_group(ToolGroup.VISUALIZATION)
    
    def preload_statistical_tools(self):
        """Preload statistical tools (commonly used together)"""
        self.preload_group(ToolGroup.STATISTICAL)
    
    def preload_knowledge_tools_for_app_visit(self):
        """Preload knowledge tools when user first visits the app"""
        if ToolGroup.CORE not in self._loaded_groups:
            logger.info("🌐 User visited app - loading knowledge tools in background...")
            self.preload_group(ToolGroup.CORE)
    
    def get_loading_stats(self) -> Dict[str, Any]:
        """Get statistics about tool loading"""
        stats = {
            'loaded_groups': [group.value for group in self._loaded_groups],
            'total_tools': len(self._tools),
            'load_times': {group.value: time_taken for group, time_taken in self._load_times.items()},
            'groups_available': len(self._group_definitions)
        }
        
        # Add per-group tool counts
        for group in self._loaded_groups:
            group_def = self._group_definitions[group]
            loaded_count = sum(1 for tool in group_def.tools if tool in self._tools)
            stats[f'{group.value}_tools'] = loaded_count
        
        return stats
    
    def is_tool_available(self, tool_name: str) -> bool:
        """Check if a tool is available (loaded or can be loaded)"""
        if tool_name in self._tools:
            return True
        
        # Check if it's in any group definition
        return self._find_tool_group(tool_name) is not None
    
    def execute_tool(self, tool_name: str, session_id: str, **parameters):
        """
        Execute a tool with the given parameters.
        
        This method:
        1. Loads the tool if not already loaded (triggers group loading)
        2. Instantiates the tool with the parameters
        3. Executes the tool with the session_id
        4. Returns the result as dictionary format expected by request_interpreter
        """
        # Get the tool class (this will trigger group loading if needed)
        tool_class = self.get_tool(tool_name)
        
        if not tool_class:
            return {
                "status": "error",
                "message": f"Tool '{tool_name}' not found or failed to load",
                "tool_name": tool_name
            }
        
        try:
            # Instantiate the tool with parameters
            tool_instance = tool_class(**parameters)
            
            # Execute the tool
            result = tool_instance.execute(session_id)
            
            # Convert ToolExecutionResult to dictionary format expected by request_interpreter
            if hasattr(result, 'success'):
                # Extract web_path and chart_type from data dict if they exist
                result_data = result.data or {}
                web_path = result_data.get('web_path') or getattr(result, 'web_path', None)
                chart_type = result_data.get('chart_type') or getattr(result, 'chart_type', None)
                
                return {
                    'status': 'success' if result.success else 'error',
                    'message': result.message,
                    'tool_name': tool_name,
                    'data': result_data,
                    'execution_time': getattr(result, 'execution_time', None),
                    'web_path': web_path,
                    'chart_type': chart_type,
                    'error_details': getattr(result, 'error_details', None)
                }
            
            # Fallback for any other result format
            return result
            
        except Exception as e:
            logger.error(f"Error executing tool {tool_name}: {e}")
            return {
                "status": "error",
                "message": f"Tool execution failed: {str(e)}",
                "tool_name": tool_name,
                "error_details": f"Exception: {type(e).__name__}"
            }
    
    def get_group_info(self, group: ToolGroup) -> Dict[str, Any]:
        """Get information about a specific tool group"""
        group_def = self._group_definitions[group]
        return {
            'name': group_def.name,
            'description': group_def.description,
            'modules': group_def.modules,
            'tools': group_def.tools,
            'dependencies': group_def.dependencies or [],
            'loaded': group in self._loaded_groups,
            'load_time': self._load_times.get(group, 0),
            'tool_count': len(group_def.tools)
        }
    
    def clear_cache(self):
        """Clear loaded tools (for testing/debugging)"""
        with self._loading_lock:
            self._tools.clear()
            self._loaded_groups.clear()
            self._load_times.clear()
            # Reload core tools
            self._load_core_tools()


# Global instance
_tiered_loader: Optional[TieredToolLoader] = None


def get_tiered_tool_loader() -> TieredToolLoader:
    """Get or create the global tiered tool loader instance"""
    global _tiered_loader
    if _tiered_loader is None:
        _tiered_loader = TieredToolLoader()
    return _tiered_loader


def reset_tiered_loader():
    """Reset the global tiered loader (for testing)"""
    global _tiered_loader
    _tiered_loader = None