"""
Tool Functions for ChatMRPT LLM Integration

Comprehensive tool system for natural language interaction with ChatMRPT.
Tools are organized by category and provide clean interfaces between
user requests and system functionality.

Updated to use Pydantic-based tools with the new ToolRegistry system.
"""

# ToolRegistry will be initialized at runtime to avoid circular imports

# DEPRECATED: These imports will be replaced by new tool architecture
# See TOOL_ARCHITECTURE.md for details
# from .data_tools import (
#     run_composite_analysis,
#     run_pca_analysis,
#     get_composite_rankings,
#     get_pca_rankings,
#     create_composite_score_maps,
#     create_vulnerability_map,
#     create_decision_tree,
#     create_urban_extent_map,
#     filter_wards_by_risk,
#     filter_wards_by_criteria,
#     get_session_data_summary
# )

# from .statistical_tools import (
#     summary_stats,
#     correlation,
#     chi_square,
#     t_test,
#     anova
# )

# from .visual_tools import (
#     histogram,
#     boxplot,
#     bar_chart,
#     line_chart,
#     scatter_plot,
#     heatmap,
#     pie_chart,
#     map_plot
# )

# from .knowledge_tools import (
#     simple_greeting,
#     explain_concept
# )

# from .system_tools import (
#     check_data_availability,
#     get_session_status,
#     get_available_variables,
#     get_ward_information,
#     get_ward_variable_value
# )

# from .data_analysis_tools import (
#     analyze_uploaded_data_and_recommend,
#     generate_comprehensive_analysis_summary
# )

# visual_explanation_tools removed during streamlining

# from .spatial_tools import (
#     spatial_autocorrelation_analysis,
#     spatial_similarity_analysis,
#     spatial_dependency_test
# )

# from .group_analysis_tools import (
#     settlement_type_analysis,
#     cross_variable_analysis,
#     geographic_aggregation_analysis,
#     environmental_risk_grouping
# )

# from .methodology_tools import (
#     explain_pca_methodology,
#     explain_composite_score_methodology,
#     compare_methodologies,
#     get_variable_importance_analysis
# )

# KEEPING SETTLEMENT TOOLS - These are working correctly
from .settlement_validation_tools import *
from .settlement_visualization_tools import *

# NEW ARCHITECTURE - Phase 1 Tools
# Removed tools: risk_analysis_tools, ward_data_tools, statistical_analysis_tools
# These have been replaced with conversational data access

from .visualization_maps_tools import (
    CreateVulnerabilityMap,
    CreatePCAMap,
    CreateUrbanExtentMap,
    CreateDecisionTree,
    CreateCompositeScoreMaps,
    CreateBoxPlot,
    CreateInterventionMap
)

# Phase 3: VISUALIZATION_CHARTS tools
from .visualization_charts_tools import (
    # Statistical Distribution Charts
    CreateHistogram,
    CreateViolinPlot,
    CreateDensityPlot,
    
    # Correlation & Relationship Charts
    CreateScatterPlot,
    CreateCorrelationHeatmap,
    CreatePairPlot,
    CreateRegressionPlot,
    
    # Comparative Charts
    CreateBarChart,
    CreateGroupedBarChart,
    CreateStackedBarChart,
    
    # Ranking & Performance Charts
    CreateLollipopChart,
    
    # Categorical Analysis
    CreatePieChart,
    CreateDonutChart,
    
    # Advanced Statistical Charts
    CreateQQPlot,
    CreateResidualPlot,
    CreateBoxPlotGrid,
    
    # Geographic/Spatial Charts
    CreateBubbleMap,
    CreateCoordinatePlot
)

# Phase 4: INTERVENTION_TARGETING tools - REMOVED
# intervention_targeting_tools removed during streamlining

# Phase 5: SCENARIO_SIMULATION tools - REMOVED
# scenario_simulation_tools removed during streamlining

# Phase 4: SMART_KNOWLEDGE tools - REMOVED
# smart_knowledge_tools removed during streamlining

# Data Preparation Tools
# from .data_preparation_tools import (
#     CreateUnifiedDataset,
#     CheckDataReadiness
# )

# Advanced Mapping Tools
# from .advanced_mapping_tools import (
#     CreateMultiLayerRiskMap,
#     CreateEnvironmentalDriverMap
# )

from .settlement_intervention_tools import (
    CreateSettlementAnalysisMap,
    CreateInterventionTargetingMap
)

# from .spatial_autocorrelation_tools import (
#     CreateSpatialAutocorrelationMap
# )

# Complete Analysis Tools - Coordinated Dual-Method Workflow
from .complete_analysis_tools import (
    RunCompleteAnalysis,
    RunCompositeAnalysis,
    RunPCAAnalysis,
    GenerateComprehensiveAnalysisSummary
)

# Data Query Tools for Conversational Access - REMOVED
# Now handled directly by request interpreter with conversational data access

# Variable Distribution Tools
from .variable_distribution import (
    VariableDistribution
)

# Methodology Explanation Tools
from .methodology_explanation_tools import (
    ExplainAnalysisMethodology
)

# Enhanced Environmental Risk Analysis Tools
# from .environmental_risk_tools import (
#     get_flood_prone_wards,
#     analyze_water_proximity_correlation,
#     get_ward_elevation_profile,
#     get_high_vegetation_wards,
#     analyze_low_lying_areas_risk
# )

# Intervention Targeting Tools
# from .intervention_targeting_tools import (
#     identify_itn_priority_wards,
#     identify_irs_eligible_wards,
#     identify_coverage_gaps,
#     recommend_chw_deployment
# )

# scenario_simulation_tools removed during streamlining

# Strategic Decision Support Tools
# from .strategic_decision_tools import (
#     recommend_priority_targeting_strategy,
#     analyze_lga_risk_distribution,
#     generate_monitoring_priorities,
#     identify_deprioritization_candidates
# )

# memory_tools removed during streamlining

# Initialize Pydantic tool registry at runtime
_pydantic_registry = None

def _initialize_pydantic_registry():
    """Initialize Pydantic registry lazily to avoid circular imports."""
    global _pydantic_registry
    if _pydantic_registry is None:
        try:
            from ..core.tool_registry import ToolRegistry
            _pydantic_registry = ToolRegistry()
            
            # Simple tool discovery without problematic timeouts
            try:
                tools_discovered = _pydantic_registry.discover_tools()
                print(f"✅ Discovered {tools_discovered} Pydantic tools")
            except Exception as discovery_error:
                print(f"⚠️ Tool discovery error: {discovery_error}")
                # Continue with partial registry rather than failing completely
                
        except Exception as e:
            print(f"⚠️ Error initializing tool registry: {e}")
            # Create minimal registry to prevent crashes
            class MinimalRegistry:
                def get_tool_names(self): return []
                def get_tool_schemas(self): return {}
                def execute_tool(self, *args, **kwargs): return {'status': 'error', 'message': 'Registry unavailable'}
            _pydantic_registry = MinimalRegistry()
    return _pydantic_registry

# Hybrid tool registry that combines Pydantic tools with legacy tools
TOOL_REGISTRY = {}

# Legacy tools to add (will be skipped if Pydantic version exists)
# DEPRECATED: Most legacy tools commented out during architecture refactoring
legacy_tools = {
    # ALL LEGACY TOOLS REMOVED - CLEAN SLATE FOR NEW ARCHITECTURE
    # (System tools moved to deprecated folder)
    
    # NO LEGACY TOOLS ACTIVE
    
    # ALL OTHER LEGACY TOOLS REMOVED - See TOOL_ARCHITECTURE.md
    
    # ORIGINAL LEGACY TOOLS (ALL COMMENTED OUT):
    # 'run_composite_analysis': run_composite_analysis,
    # 'run_pca_analysis': run_pca_analysis,
    # ... (removed for brevity - see git history)
}

# Initialize the hybrid registry
def _build_tool_registry():
    """Build the combined tool registry with Pydantic and legacy tools."""
    global TOOL_REGISTRY
    
    # Initialize Pydantic registry
    registry = _initialize_pydantic_registry()
    
    # Add Pydantic tools with wrappers
    for tool_name in registry.get_tool_names():
        def create_wrapper(name):
            def wrapper(session_id: str, **kwargs):
                try:
                    result = registry.execute_tool(name, session_id, **kwargs)
                    # Convert ToolExecutionResult to legacy dict format
                    if hasattr(result, 'status'):
                        response = {
                            'status': 'success' if result.success else 'error',
                            'message': result.message,
                            **result.data
                        }
                        if result.error_details:
                            response['error_details'] = result.error_details
                        if result.execution_time:
                            response['execution_time'] = result.execution_time
                        return response
                    return result
                except Exception as e:
                    return {
                        'status': 'error',
                        'message': f'Tool execution failed: {str(e)}',
                        'error_details': str(e)
                    }
            return wrapper
        
        TOOL_REGISTRY[tool_name] = create_wrapper(tool_name)
    
    # Add legacy tools that don't have Pydantic equivalents
    for tool_name, tool_func in legacy_tools.items():
        if tool_name not in TOOL_REGISTRY:
            TOOL_REGISTRY[tool_name] = tool_func

# Build the registry on first access
_registry_built = False

def _ensure_registry_built():
    """Ensure the tool registry is built."""
    global _registry_built
    if not _registry_built:
        # TEMPORARILY DISABLE FAST STARTUP - Always build full registry
        print("🔧 Building full tool registry for reliability...")
        _build_tool_registry()
        _registry_built = True

def _build_minimal_registry():
    """Build a minimal registry for fast startup."""
    global TOOL_REGISTRY
    
    # Include essential tools for basic functionality AND analysis tools
    essential_tools = [
        'run_complete_analysis',
        'run_composite_analysis', 
        'run_pca_analysis',
        'createvulnerabilitymap',
        'createpcamap',
        'createboxplot',
        'createscatterplot',
        'executedataquery',
        'variable_distribution'
    ]
    
    # Create deferred wrapper that will load tools on first use
    def create_deferred_wrapper(tool_name):
        def deferred_wrapper(session_id: str, **kwargs):
            global _registry_built
            
            # On first use, build full registry and execute
            if not _registry_built:
                print(f"🔄 Loading tool '{tool_name}' - building full registry...")
                _registry_built = False  # Reset to allow full build
                _build_tool_registry()
                _registry_built = True
            
            # Now execute the actual tool
            if tool_name in TOOL_REGISTRY:
                return TOOL_REGISTRY[tool_name](session_id, **kwargs)
            else:
                return {
                    'status': 'error', 
                    'message': f'Tool {tool_name} not available after full registry build'
                }
        return deferred_wrapper
    
    TOOL_REGISTRY = {name: create_deferred_wrapper(name) for name in essential_tools}

def get_tool_function(tool_name: str):
    """Get a tool function by name for dynamic resolution."""
    _ensure_registry_built()
    return TOOL_REGISTRY.get(tool_name)

def get_all_tools():
    """Get all available tools for LLM use."""
    _ensure_registry_built()
    return TOOL_REGISTRY

def get_pydantic_registry():
    """Get the Pydantic tool registry for schema generation."""
    return _initialize_pydantic_registry()

def get_tool_schemas():
    """Get OpenAI-compatible schemas for all tools."""
    registry = _initialize_pydantic_registry()
    return registry.get_tool_schemas()

def get_tools_by_category():
    """Get tools organized by category."""
    return {
        'data_analysis': [
            'run_composite_analysis', 'run_pca_analysis', 'get_composite_rankings',
            'get_pca_rankings', 'create_composite_score_maps', 'create_vulnerability_map',
            'create_decision_tree', 'create_urban_extent_map', 'filter_wards_by_risk',
            'filter_wards_by_criteria', 'get_session_data_summary', 'analyze_uploaded_data_and_recommend',
            'generate_comprehensive_analysis_summary', 'explain_analysis_methodology'
        ],
        'statistical': [
            'summary_stats', 'correlation', 'chi_square', 't_test', 'anova'
        ],
        'visualization': [
            'histogram', 'boxplot', 'box_plot', 'bar_chart', 'line_chart', 'scatter_plot',
            'heatmap', 'pie_chart', 'map_plot'
        ],
        'knowledge': [
            'simple_greeting', 'explain_concept'
        ],
        # 'visual_explanation': removed during streamlining
        'spatial_analysis': [
            'spatial_autocorrelation_analysis', 'spatial_similarity_analysis', 'spatial_dependency_test'
        ],
        'group_analysis': [
            'settlement_type_analysis', 'cross_variable_analysis', 'geographic_aggregation_analysis', 'environmental_risk_grouping'
        ],
        'methodology': [
            'explain_pca_methodology', 'explain_composite_score_methodology', 'compare_methodologies', 'get_variable_importance_analysis'
        ],
        # 'settlement_validation': removed during streamlining
        'environmental_risk': [
            'get_flood_prone_wards', 'analyze_water_proximity_correlation',
            'get_ward_elevation_profile', 'get_high_vegetation_wards',
            'analyze_low_lying_areas_risk'
        ],
        'intervention_targeting': [
            'identify_itn_priority_wards', 'identify_irs_eligible_wards',
            'identify_coverage_gaps', 'recommend_chw_deployment'
        ],
        # 'scenario_simulation': removed during streamlining
        'strategic_decision': [
            'recommend_priority_targeting_strategy', 'analyze_lga_risk_distribution',
            'generate_monitoring_priorities', 'identify_deprioritization_candidates'
        ],
        # 'memory': removed during streamlining
        'system': [
            'check_data_availability', 'get_session_status', 'get_available_variables',
            'get_ward_information', 'get_ward_variable_value', 'analyze_uploaded_data_and_recommend'
        ]
    }

__all__ = [
    'TOOL_REGISTRY', 'get_tool_function', 'get_all_tools', 'get_tools_by_category',
    'get_pydantic_registry', 'get_tool_schemas'
] 