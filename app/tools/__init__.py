"""
Tool Functions for ChatMRPT LLM Integration

Comprehensive tool system for natural language interaction with ChatMRPT.
Tools are organized by category and provide clean interfaces between
user requests and system functionality.
"""

from .data_tools import (
    run_composite_analysis,
    run_pca_analysis,
    get_composite_rankings,
    get_pca_rankings,
    create_composite_score_maps,
    create_vulnerability_map,
    create_decision_tree,
    create_urban_extent_map,
    filter_wards_by_risk,
    filter_wards_by_criteria,
    get_session_data_summary
)

from .statistical_tools import (
    summary_stats,
    correlation,
    chi_square,
    t_test,
    anova,
    distribution_test,
    group_summary,
    descriptive_stats,
    regression_analysis
)

from .visual_tools import (
    histogram,
    boxplot,
    bar_chart,
    line_chart,
    scatter_plot,
    heatmap,
    pie_chart,
    map_plot,
    box_plot,
    box_plot_flexible,
    scatter_plot_flexible
)

from .knowledge_tools import (
    simple_greeting,
    explain_concept,
    explain_methodology,
    explain_variable,
    interpret_results
)

from .system_tools import (
    check_data_availability,
    get_session_status,
    get_available_variables,
    get_ward_information
)

from .data_analysis_tools import (
    analyze_uploaded_data_and_recommend,
    generate_comprehensive_analysis_summary
)

# Comprehensive tool registry for LLM dynamic resolution
TOOL_REGISTRY = {
    # Core Data Analysis Tools
    'run_composite_analysis': run_composite_analysis,
    'run_pca_analysis': run_pca_analysis,
    'get_composite_rankings': get_composite_rankings,
    'get_pca_rankings': get_pca_rankings,
    'create_composite_score_maps': create_composite_score_maps,

    'create_vulnerability_map': create_vulnerability_map,
    'create_decision_tree': create_decision_tree,
    'create_urban_extent_map': create_urban_extent_map,
    'filter_wards_by_risk': filter_wards_by_risk,
    'filter_wards_by_criteria': filter_wards_by_criteria,
    'get_session_data_summary': get_session_data_summary,
    
    # Statistical Analysis Tools
    'summary_stats': summary_stats,
    'correlation': correlation,
    'chi_square': chi_square,
    't_test': t_test,
    'anova': anova,
    'distribution_test': distribution_test,
    'group_summary': group_summary,
    'descriptive_stats': descriptive_stats,
    'regression_analysis': regression_analysis,
    
    # Visualization Tools
    'histogram': histogram,
    'boxplot': box_plot_flexible,  # Use flexible version
    'box_plot': box_plot_flexible,  # Use flexible version
    'bar_chart': bar_chart,
    'line_chart': line_chart,
    'scatter_plot': scatter_plot_flexible,  # Use flexible version
    'heatmap': heatmap,
    'pie_chart': pie_chart,
    'map_plot': map_plot,
    
    # Knowledge Tools
    'simple_greeting': simple_greeting,
    'explain_concept': explain_concept,
    'explain_methodology': explain_methodology,
    'explain_variable': explain_variable,
    'interpret_results': interpret_results,
    
    # System Tools
    'check_data_availability': check_data_availability,
    'get_session_status': get_session_status,
    'get_available_variables': get_available_variables,
    'get_ward_information': get_ward_information,
    
    # Data Analysis Tools
    'analyze_uploaded_data_and_recommend': analyze_uploaded_data_and_recommend,
    'generate_comprehensive_analysis_summary': generate_comprehensive_analysis_summary
}

def get_tool_function(tool_name: str):
    """Get a tool function by name for dynamic resolution."""
    return TOOL_REGISTRY.get(tool_name)

def get_all_tools():
    """Get all available tools for LLM use."""
    return TOOL_REGISTRY

def get_tools_by_category():
    """Get tools organized by category."""
    return {
        'data_analysis': [
            'run_composite_analysis', 'run_pca_analysis', 'get_composite_rankings',
            'get_pca_rankings', 'create_composite_score_maps', 'create_vulnerability_map',
            'create_decision_tree', 'create_urban_extent_map', 'filter_wards_by_risk',
            'filter_wards_by_criteria', 'get_session_data_summary', 'analyze_uploaded_data_and_recommend',
            'generate_comprehensive_analysis_summary'
        ],
        'statistical': [
            'summary_stats', 'correlation', 'chi_square', 't_test', 'anova',
            'distribution_test', 'group_summary', 'descriptive_stats', 'regression_analysis'
        ],
        'visualization': [
            'histogram', 'boxplot', 'box_plot', 'bar_chart', 'line_chart', 'scatter_plot',
            'heatmap', 'pie_chart', 'map_plot', 'box_plot_flexible', 'scatter_plot_flexible'
        ],
        'knowledge': [
            'simple_greeting', 'explain_concept', 'explain_methodology', 'explain_variable', 'interpret_results'
        ],
        'system': [
            'check_data_availability', 'get_session_status', 'get_available_variables',
            'get_ward_information', 'analyze_uploaded_data_and_recommend'
        ]
    }

__all__ = [
    'TOOL_REGISTRY', 'get_tool_function', 'get_all_tools', 'get_tools_by_category'
] 