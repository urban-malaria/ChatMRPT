"""
Tool Capabilities Definition for Semantic Routing

This module defines what each tool DOES (not keywords to match).
Used by the routing system to understand when tool execution is needed.

TWO-LAYER DATA ARCHITECTURE:
- query_data: Simple text-to-SQL for data queries (rankings, filtering, statistics)
- analyze_data: Python execution for complex analysis and explicit visualizations
"""

TOOL_CAPABILITIES = {
    # =========================================================================
    # TWO-LAYER DATA ARCHITECTURE
    # =========================================================================
    'execute_data_query': {
        'purpose': 'Run simple data queries to extract rankings, filtered data, statistics, and column information',
        'generates': 'Text-only results showing data, rankings, statistics, or column lists',
        'requires': 'Uploaded data to query',
        'execution_verbs': ['show', 'get', 'find', 'list', 'filter', 'rank', 'top', 'highest', 'lowest'],
        'example_queries': [
            'what are the top 10 highest risk wards',
            'show me wards with composite_score > 0.5',
            'what is the average TPR',
            'list all columns in my data',
            'how many wards are in each LGA',
            'what variables do I have',
            'describe my data columns'
        ]
    },

    'analyze_data_complex': {
        'purpose': 'Execute complex Python analysis and generate visualizations ONLY when explicitly requested',
        'generates': 'Analysis results and charts/plots when user explicitly asks for visualization',
        'requires': 'Uploaded data',
        'execution_verbs': ['create', 'plot', 'chart', 'graph', 'heatmap', 'visualize', 'cluster', 'regression'],
        'example_queries': [
            'create a correlation heatmap',
            'plot a histogram of TPR values',
            'run a regression analysis',
            'create a scatter plot of X vs Y',
            'perform K-means clustering',
            'visualize the distribution'
        ]
    },

    # =========================================================================
    # ANALYSIS TOOLS
    # =========================================================================
    'run_malaria_risk_analysis': {
        'purpose': 'Execute new malaria risk analysis calculations on uploaded data',
        'generates': 'New risk scores, vulnerability rankings, and analysis results',
        'requires': 'Uploaded CSV data with demographic/health indicators',
        'execution_verbs': ['run', 'execute', 'perform', 'start', 'calculate', 'analyze'],
        'example_queries': [
            'run the malaria risk analysis',
            'analyze my data',
            'perform risk assessment',
            'calculate vulnerability scores'
        ]
    },

    'explain_analysis_methodology': {
        'purpose': 'Generate explanation of analysis methods used',
        'generates': 'Detailed methodology explanation',
        'requires': 'Context about analysis type',
        'execution_verbs': ['explain', 'describe', 'detail'],
        'example_queries': [
            'explain the methodology',
            'how was this calculated',
            'describe analysis approach'
        ]
    },

    'generatecomprehensiveanalysissummary': {
        'purpose': 'Generate comprehensive summary of all analysis results',
        'generates': 'Complete analysis report with findings and recommendations',
        'requires': 'Completed analysis results',
        'execution_verbs': ['generate', 'create', 'summarize', 'compile'],
        'example_queries': [
            'generate comprehensive summary',
            'create analysis report',
            'summarize all findings'
        ]
    },

    # =========================================================================
    # MAP TOOLS (Specialized visualizations that remain as separate tools)
    # =========================================================================
    'create_vulnerability_map': {
        'purpose': 'Generate a new interactive HTML map showing vulnerability scores',
        'generates': 'Interactive choropleth map visualization',
        'requires': 'Completed analysis with risk scores',
        'execution_verbs': ['create', 'generate', 'plot', 'map', 'visualize'],
        'example_queries': [
            'create a vulnerability map',
            'show me the risk map',
            'plot vulnerability scores on map'
        ]
    },

    'create_pca_map': {
        'purpose': 'Generate PCA (Principal Component Analysis) visualization map',
        'generates': 'Map showing PCA components and loadings',
        'requires': 'Completed PCA analysis',
        'execution_verbs': ['create', 'generate', 'plot', 'visualize'],
        'example_queries': [
            'show PCA results on map',
            'create principal component map',
            'visualize PCA analysis'
        ]
    },

    'variable_distribution': {
        'purpose': 'Create spatial distribution maps for any variable showing how it varies across wards',
        'generates': 'Interactive map showing the spatial distribution of a specified variable',
        'requires': 'Uploaded data with the variable to map',
        'execution_verbs': ['plot', 'map', 'show', 'visualize', 'display', 'create'],
        'example_queries': [
            'plot the evi variable distribution',
            'show me the distribution of pfpr',
            'map the rainfall variable',
            'visualize housing_quality across wards',
            'display the spatial distribution of elevation'
        ]
    },

    'create_vulnerability_map_comparison': {
        'purpose': 'Create side-by-side comparison of vulnerability maps',
        'generates': 'Comparison map showing multiple methods',
        'requires': 'Multiple analysis results to compare',
        'execution_verbs': ['compare', 'create', 'show', 'contrast'],
        'example_queries': [
            'compare vulnerability maps',
            'show PCA vs composite maps',
            'create comparison visualization'
        ]
    },

    'createurbanextentmap': {
        'purpose': 'Create map showing urban vs rural areas',
        'generates': 'Urban extent classification map',
        'requires': 'Settlement or urbanization data',
        'execution_verbs': ['create', 'map', 'show', 'classify'],
        'example_queries': [
            'create urban extent map',
            'show urban vs rural areas',
            'map urbanization'
        ]
    },

    'createdecisiontree': {
        'purpose': 'Create decision tree visualization for risk factors',
        'generates': 'Decision tree diagram',
        'requires': 'Risk factors and thresholds',
        'execution_verbs': ['create', 'build', 'generate', 'visualize'],
        'example_queries': [
            'create decision tree',
            'show risk decision paths',
            'build classification tree'
        ]
    },

    # =========================================================================
    # SETTLEMENT TOOLS
    # =========================================================================
    'create_settlement_map': {
        'purpose': 'Create map showing settlement patterns and building footprints',
        'generates': 'Settlement classification map with building types',
        'requires': 'Settlement data or shapefile',
        'execution_verbs': ['create', 'generate', 'map', 'visualize'],
        'example_queries': [
            'show settlement patterns',
            'map building footprints',
            'visualize urban areas'
        ]
    },

    'show_settlement_statistics': {
        'purpose': 'Calculate and display settlement statistics',
        'generates': 'Statistical summary of settlement types and counts',
        'requires': 'Settlement data',
        'execution_verbs': ['show', 'calculate', 'display', 'get'],
        'example_queries': [
            'show settlement statistics',
            'get building counts',
            'display urban percentages'
        ]
    },

    # =========================================================================
    # PLANNING TOOLS
    # =========================================================================
    'plan_itn_distribution': {
        'purpose': 'Calculate optimal ITN (bed net) distribution plan',
        'generates': 'Distribution plan with net allocations per ward',
        'requires': 'Analysis results and net availability parameters',
        'execution_verbs': ['plan', 'calculate', 'distribute', 'allocate'],
        'example_queries': [
            'plan bed net distribution',
            'allocate ITNs to wards',
            'distribute 10000 nets optimally',
            'plan mosquito net campaign'
        ]
    }
}


def get_tool_capability(tool_name: str) -> dict:
    """Get capability description for a specific tool."""
    return TOOL_CAPABILITIES.get(tool_name, {})


def get_all_capabilities_summary() -> str:
    """Get a summary of all tool capabilities for routing context."""
    summary = []
    for tool_name, cap in TOOL_CAPABILITIES.items():
        summary.append(f"- {tool_name}: {cap['purpose']}")
    return "\n".join(summary)


def requires_tool_execution(message: str, context: dict) -> tuple[bool, str]:
    """
    Determine if a message requires tool execution based on semantic understanding.

    Returns:
        (requires_tool, reason)
    """
    message_lower = message.lower()

    # Check for execution verbs across all tools
    execution_verbs = set()
    for cap in TOOL_CAPABILITIES.values():
        execution_verbs.update(cap.get('execution_verbs', []))

    has_execution_verb = any(verb in message_lower for verb in execution_verbs)

    # Check if asking about existing results vs creating new ones
    explanation_patterns = ['what is', 'what does', 'explain', 'why is', 'how does', 'tell me about']
    is_explanation = any(pattern in message_lower for pattern in explanation_patterns)

    # If user has data and uses execution verb, likely needs tools
    if context.get('has_uploaded_files') and has_execution_verb and not is_explanation:
        return True, "Contains execution verb with uploaded data"

    # If asking for explanation of existing results, doesn't need tools
    if is_explanation and context.get('analysis_complete'):
        return False, "Asking for explanation of existing results"

    # Default based on context
    if context.get('has_uploaded_files') and not is_explanation:
        return True, "Has data and not asking for explanation"

    return False, "No execution needed"
