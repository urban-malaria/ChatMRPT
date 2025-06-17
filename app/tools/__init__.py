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
    get_ward_information,
    get_ward_variable_value
)

from .data_analysis_tools import (
    analyze_uploaded_data_and_recommend,
    generate_comprehensive_analysis_summary
)

from .visual_explanation_tools import (
    explain_last_visualization,
    explain_specific_visualization,
    get_visualization_recommendations,
    track_visualization_creation,
    get_session_visualizations
)

from .spatial_tools import (
    spatial_autocorrelation_analysis,
    spatial_similarity_analysis,
    spatial_dependency_test
)

from .group_analysis_tools import (
    settlement_type_analysis,
    cross_variable_analysis,
    geographic_aggregation_analysis,
    environmental_risk_grouping
)

from .methodology_tools import (
    explain_pca_methodology,
    explain_composite_score_methodology,
    compare_methodologies,
    get_variable_importance_analysis
)

# Import Settlement Validation and Analysis Tools
from .settlement_validation_tools import (
    create_building_classification_map,
    create_settlement_validation_map,
    get_building_statistics
)

from .settlement_tools import (
    integrate_settlement_data_unified,
    get_settlement_enhanced_analysis_summary,
    create_settlement_enhanced_vulnerability_explanation
)

# Import Chat-Accessible Settlement Visualization Tools
from .settlement_visualization_tools import (
    create_settlement_map,
    show_settlement_statistics,
    create_ward_specific_settlement_map,
    integrate_settlement_data_with_analysis,
    check_settlement_dependencies
)

# Legacy settlement tools from standardized analysis
from ..services.tools.standardized_analysis_tools import (
    get_settlement_validation_summary
)

# Enhanced Environmental Risk Analysis Tools
from .environmental_risk_tools import (
    get_flood_prone_wards,
    analyze_water_proximity_correlation,
    get_ward_elevation_profile,
    get_high_vegetation_wards,
    analyze_low_lying_areas_risk
)

# Intervention Targeting Tools
from .intervention_targeting_tools import (
    identify_itn_priority_wards,
    identify_irs_eligible_wards,
    identify_coverage_gaps,
    recommend_chw_deployment
)

# Scenario Simulation Tools
from .scenario_simulation_tools import (
    simulate_coverage_increase_impact,
    simulate_variable_exclusion,
    simulate_tpr_assumption_change,
    simulate_compactness_threshold_scenario
)

# Strategic Decision Support Tools
from .strategic_decision_tools import (
    recommend_priority_targeting_strategy,
    analyze_lga_risk_distribution,
    generate_monitoring_priorities,
    identify_deprioritization_candidates
)

# Memory Tools for Conversational Continuity
from .memory_tools import (
    get_conversation_history,
    find_previous_discussion,
    get_analysis_context,
    save_analysis_result,
    get_previous_analysis_results,
    compare_with_previous_analysis
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
    'get_ward_variable_value': get_ward_variable_value,
    
    # Data Analysis Tools
    'analyze_uploaded_data_and_recommend': analyze_uploaded_data_and_recommend,
    'generate_comprehensive_analysis_summary': generate_comprehensive_analysis_summary,
    
    # Visual Explanation Tools
    'explain_last_visualization': explain_last_visualization,
    'explain_specific_visualization': explain_specific_visualization,
    'get_visualization_recommendations': get_visualization_recommendations,
    'track_visualization_creation': track_visualization_creation,
    'get_session_visualizations': get_session_visualizations,
    
    # Spatial Analysis Tools
    'spatial_autocorrelation_analysis': spatial_autocorrelation_analysis,
    'spatial_similarity_analysis': spatial_similarity_analysis,
    'spatial_dependency_test': spatial_dependency_test,
    
    # Group Analysis Tools
    'settlement_type_analysis': settlement_type_analysis,
    'cross_variable_analysis': cross_variable_analysis,
    'geographic_aggregation_analysis': geographic_aggregation_analysis,
    'environmental_risk_grouping': environmental_risk_grouping,
    
    # Methodology Explanation Tools
    'explain_pca_methodology': explain_pca_methodology,
    'explain_composite_score_methodology': explain_composite_score_methodology,
    'compare_methodologies': compare_methodologies,
    'get_variable_importance_analysis': get_variable_importance_analysis,
    
    # Phase 2: Settlement Validation Tools
    'create_settlement_validation_map': create_settlement_validation_map,
    'get_settlement_validation_summary': get_settlement_validation_summary,
    
    # Environmental Risk Analysis Tools
    'get_flood_prone_wards': get_flood_prone_wards,
    'analyze_water_proximity_correlation': analyze_water_proximity_correlation,
    'get_ward_elevation_profile': get_ward_elevation_profile,
    'get_high_vegetation_wards': get_high_vegetation_wards,
    'analyze_low_lying_areas_risk': analyze_low_lying_areas_risk,
    
    # Intervention Targeting Tools
    'identify_itn_priority_wards': identify_itn_priority_wards,
    'identify_irs_eligible_wards': identify_irs_eligible_wards,
    'identify_coverage_gaps': identify_coverage_gaps,
    'recommend_chw_deployment': recommend_chw_deployment,
    
    # Scenario Simulation Tools
    'simulate_coverage_increase_impact': simulate_coverage_increase_impact,
    'simulate_variable_exclusion': simulate_variable_exclusion,
    'simulate_tpr_assumption_change': simulate_tpr_assumption_change,
    'simulate_compactness_threshold_scenario': simulate_compactness_threshold_scenario,
    
    # Strategic Decision Support Tools
    'recommend_priority_targeting_strategy': recommend_priority_targeting_strategy,
    'analyze_lga_risk_distribution': analyze_lga_risk_distribution,
    'generate_monitoring_priorities': generate_monitoring_priorities,
    'identify_deprioritization_candidates': identify_deprioritization_candidates,
    
    # Memory Tools for Conversational Continuity
    'get_conversation_history': get_conversation_history,
    'find_previous_discussion': find_previous_discussion,
    'get_analysis_context': get_analysis_context,
    'save_analysis_result': save_analysis_result,
    'get_previous_analysis_results': get_previous_analysis_results,
    'compare_with_previous_analysis': compare_with_previous_analysis
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
        'visual_explanation': [
            'explain_last_visualization', 'explain_specific_visualization', 'get_visualization_recommendations',
            'track_visualization_creation', 'get_session_visualizations'
        ],
        'spatial_analysis': [
            'spatial_autocorrelation_analysis', 'spatial_similarity_analysis', 'spatial_dependency_test'
        ],
        'group_analysis': [
            'settlement_type_analysis', 'cross_variable_analysis', 'geographic_aggregation_analysis', 'environmental_risk_grouping'
        ],
        'methodology': [
            'explain_pca_methodology', 'explain_composite_score_methodology', 'compare_methodologies', 'get_variable_importance_analysis'
        ],
        'settlement_validation': [
            'create_settlement_validation_map', 'get_settlement_validation_summary'
        ],
        'environmental_risk': [
            'get_flood_prone_wards', 'analyze_water_proximity_correlation',
            'get_ward_elevation_profile', 'get_high_vegetation_wards',
            'analyze_low_lying_areas_risk'
        ],
        'intervention_targeting': [
            'identify_itn_priority_wards', 'identify_irs_eligible_wards',
            'identify_coverage_gaps', 'recommend_chw_deployment'
        ],
        'scenario_simulation': [
            'simulate_coverage_increase_impact', 'simulate_variable_exclusion',
            'simulate_tpr_assumption_change', 'simulate_compactness_threshold_scenario'
        ],
        'strategic_decision': [
            'recommend_priority_targeting_strategy', 'analyze_lga_risk_distribution',
            'generate_monitoring_priorities', 'identify_deprioritization_candidates'
        ],
        'memory': [
            'get_conversation_history', 'find_previous_discussion', 'get_analysis_context',
            'save_analysis_result', 'get_previous_analysis_results', 'compare_with_previous_analysis'
        ],
        'system': [
            'check_data_availability', 'get_session_status', 'get_available_variables',
            'get_ward_information', 'get_ward_variable_value', 'analyze_uploaded_data_and_recommend'
        ]
    }

__all__ = [
    'TOOL_REGISTRY', 'get_tool_function', 'get_all_tools', 'get_tools_by_category'
] 