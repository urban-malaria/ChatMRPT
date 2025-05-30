"""
Guidance Generator for ChatMRPT

This module generates proactive user guidance, welcome messages, and contextual help
throughout the malaria risk analysis workflow.
"""

import logging
from typing import Dict, List, Optional
from .session_state import SessionState, WorkflowStage, DataState, AnalysisState, get_workflow_progress_percentage

logger = logging.getLogger(__name__)


class GuidanceGenerator:
    """Generates proactive user guidance and help messages"""
    
    def __init__(self):
        """Initialize guidance generator with stage-specific content"""
        self._initialize_guidance_content()
    
    def _initialize_guidance_content(self):
        """Initialize guidance content for different workflow stages"""
        self.stage_guidance = {
            WorkflowStage.INITIAL: {
                'welcome': """👋 **Welcome to ChatMRPT!** 
                
I'm your intelligent assistant for malaria risk analysis. I can help you:
- Analyze spatial data to identify high-risk areas
- Create interactive vulnerability maps  
- Generate comprehensive risk reports
- Provide data-driven insights for intervention planning

**Ready to get started?** I'll guide you through each step!""",
                
                'next_steps': [
                    "Upload your CSV data file with malaria risk variables",
                    "Upload your shapefile for geographic boundaries", 
                    "Ask me what file formats I accept",
                    "Learn about the analysis process"
                ],
                
                'help_topics': [
                    "What file formats do you accept?",
                    "What data do I need for analysis?",
                    "How does malaria risk analysis work?",
                    "Can you show me an example?"
                ],
                
                'capabilities': [
                    "Accept CSV and shapefile uploads",
                    "Validate data quality",
                    "Explain the analysis process",
                    "Answer questions about requirements"
                ]
            },
            
            WorkflowStage.DATA_UPLOAD: {
                'message': """📊 **Great progress!** You're uploading your data files.
                
I can see you've made progress with your data upload. Once I have both your CSV data and shapefile, 
I'll be able to validate the data quality and prepare for analysis.""",
                
                'next_steps': [
                    "Complete uploading both required files",
                    "Review the data summary once uploaded",
                    "Check for any data quality issues",
                    "Proceed to analysis when ready"
                ],
                
                'help_topics': [
                    "What should my CSV file contain?",
                    "What is a shapefile and why do I need it?",
                    "How do I check if my data is compatible?",
                    "What variables are best for malaria analysis?"
                ],
                
                'capabilities': [
                    "Accept additional file uploads",
                    "Show data summaries",
                    "Identify file format issues",
                    "Provide data preparation guidance"
                ]
            },
            
            WorkflowStage.DATA_VALIDATION: {
                'message': """✅ **Files uploaded successfully!** 
                
I now have both your CSV data and shapefile. I can validate data quality, check for ward name matches, 
and ensure everything is ready for analysis.""",
                
                'next_steps': [
                    "Review your data summary",
                    "Check for any data quality issues",  
                    "Start the malaria risk analysis",
                    "Customize analysis parameters if needed"
                ],
                
                'help_topics': [
                    "How do I interpret the data summary?",
                    "What if there are ward name mismatches?",
                    "How long does analysis take?",
                    "Can I customize which variables to use?"
                ],
                
                'capabilities': [
                    "Validate data quality",
                    "Check ward name matching",
                    "Show detailed data summaries", 
                    "Start malaria risk analysis",
                    "Identify and suggest fixes for data issues"
                ]
            },
            
            WorkflowStage.DATA_READY: {
                'message': """🎯 **Data validated and ready!**
                
Your data has been validated and is compatible for analysis. All ward names match between your CSV and shapefile, 
and I've identified the variables available for malaria risk assessment.""",
                
                'next_steps': [
                    "Start the comprehensive malaria risk analysis",
                    "Review which variables will be used",
                    "Customize analysis parameters",
                    "Learn about the analysis methodology"
                ],
                
                'help_topics': [
                    "What variables will be used in analysis?", 
                    "How is malaria risk calculated?",
                    "Can I exclude certain variables?",
                    "What analysis methods do you use?"
                ],
                
                'capabilities': [
                    "Run comprehensive malaria risk analysis",
                    "Show variable selection options",
                    "Explain analysis methodology",
                    "Provide estimated completion time"
                ]
            },
            
            WorkflowStage.ANALYSIS_RUNNING: {
                'message': """⚙️ **Analysis in progress...**
                
I'm currently analyzing your data to calculate malaria risk scores and vulnerability rankings. 
This process includes data cleaning, normalization, composite scoring, and ranking generation.""",
                
                'next_steps': [
                    "Wait for analysis completion",
                    "Monitor analysis progress",
                    "Prepare for results review",
                    "Plan visualization preferences"
                ],
                
                'help_topics': [
                    "How long will analysis take?",
                    "What steps are involved in analysis?",
                    "Can I see intermediate results?",
                    "What happens if analysis fails?"
                ],
                
                'capabilities': [
                    "Show analysis progress",
                    "Explain current analysis step",
                    "Provide time estimates",
                    "Handle analysis interruptions"
                ]
            },
            
            WorkflowStage.ANALYSIS_COMPLETE: {
                'message': """🎉 **Analysis complete!** 
                
Excellent! I've successfully analyzed your data and calculated malaria risk scores for all areas. 
You now have vulnerability rankings, composite scores, and detailed results ready for visualization.""",
                
                'next_steps': [
                    "Create a vulnerability map to visualize high-risk areas",
                    "View the vulnerability rankings",  
                    "Generate variable distribution maps",
                    "Create a comprehensive report",
                    "Download results in various formats"
                ],
                
                'help_topics': [
                    "How do I interpret the vulnerability rankings?",
                    "What types of maps can you create?",
                    "How are the risk scores calculated?",
                    "Can I customize the visualizations?"
                ],
                
                'capabilities': [
                    "Create vulnerability maps",
                    "Generate variable distribution maps", 
                    "Show vulnerability rankings",
                    "Create composite score visualizations",
                    "Generate comprehensive reports",
                    "Export results in multiple formats"
                ]
            },
            
            WorkflowStage.MAPS_AVAILABLE: {
                'message': """🗺️ **Maps and visualizations ready!**
                
You now have access to interactive maps and visualizations of your malaria risk analysis results. 
You can explore different visualization types, download results, and generate final reports.""",
                
                'next_steps': [
                    "Explore different map types",
                    "Download high-resolution maps",
                    "Generate a final comprehensive report", 
                    "Export data for external use",
                    "Create additional custom visualizations"
                ],
                
                'help_topics': [
                    "How do I interpret the maps?",
                    "What download formats are available?",
                    "Can I customize map colors and styling?",
                    "How do I share results with others?"
                ],
                
                'capabilities': [
                    "Create multiple map types",
                    "Customize map styling",
                    "Generate comprehensive reports",
                    "Export in multiple formats",
                    "Provide interpretation guidance",
                    "Create summary presentations"
                ]
            }
        }
    
    def get_welcome_message(self, state: SessionState) -> str:
        """
        Generate contextual welcome message based on current state
        
        Args:
            state: Current session state
            
        Returns:
            Contextual welcome message
        """
        stage_info = self.stage_guidance.get(state.workflow_stage, {})
        
        if state.workflow_stage == WorkflowStage.INITIAL:
            return stage_info.get('welcome', 'Welcome to ChatMRPT!')
        else:
            # For non-initial stages, show progress and current status
            progress = get_workflow_progress_percentage(state)
            base_message = stage_info.get('message', 'Welcome back to ChatMRPT!')
            
            progress_info = f"\n\n**Progress: {progress}% complete**"
            
            return base_message + progress_info
    
    def get_next_step_suggestions(self, state: SessionState) -> List[str]:
        """
        Get actionable next steps based on current state
        
        Args:
            state: Current session state
            
        Returns:
            List of suggested next steps
        """
        stage_info = self.stage_guidance.get(state.workflow_stage, {})
        base_suggestions = stage_info.get('next_steps', [])
        
        # Add contextual suggestions based on data state
        contextual_suggestions = []
        
        if state.workflow_stage == WorkflowStage.DATA_UPLOAD:
            if state.data_state == DataState.CSV_ONLY:
                contextual_suggestions = [
                    "Upload your shapefile to complete data setup",
                    f"Review your CSV data ({state.data_summary.ward_count} wards loaded)"
                ]
            elif state.data_state == DataState.SHAPEFILE_ONLY:
                contextual_suggestions = [
                    "Upload your CSV data file", 
                    f"Your shapefile contains {state.data_summary.shapefile_info.features if state.data_summary.shapefile_info else 'multiple'} geographic features"
                ]
        
        elif state.workflow_stage == WorkflowStage.ANALYSIS_COMPLETE:
            if state.analysis_results.vulnerability_rankings:
                contextual_suggestions = [
                    f"View rankings for {len(state.analysis_results.vulnerability_rankings)} areas",
                    "Create maps showing the top vulnerable areas"
                ]
        
        # Combine base and contextual suggestions
        all_suggestions = contextual_suggestions + base_suggestions
        return all_suggestions[:6]  # Limit to top 6 suggestions
    
    def get_help_topics(self, state: SessionState) -> List[str]:
        """
        Get relevant help topics for current stage
        
        Args:
            state: Current session state
            
        Returns:
            List of relevant help topics
        """
        stage_info = self.stage_guidance.get(state.workflow_stage, {})
        return stage_info.get('help_topics', [
            "Ask me anything about malaria risk analysis",
            "How does this tool work?",
            "What can I do next?"
        ])
    
    def get_current_capabilities(self, state: SessionState) -> List[str]:
        """
        Get what the user can currently do
        
        Args:
            state: Current session state
            
        Returns:
            List of current capabilities
        """
        stage_info = self.stage_guidance.get(state.workflow_stage, {})
        return stage_info.get('capabilities', ["Ask questions", "Get help"])
    
    def get_progress_update(self, state: SessionState) -> str:
        """
        Generate a progress update message
        
        Args:
            state: Current session state
            
        Returns:
            Progress update message
        """
        progress = get_workflow_progress_percentage(state)
        stage_name = state.workflow_stage.value.replace('_', ' ').title()
        
        # Create progress bar visualization
        progress_bar_length = 20
        filled_length = int(progress_bar_length * progress // 100)
        bar = '█' * filled_length + '░' * (progress_bar_length - filled_length)
        
        progress_message = f"""📈 **Analysis Progress: {progress}%**
        
**Current Stage:** {stage_name}
**Progress:** [{bar}] {progress}%

"""
        
        # Add stage-specific status
        if state.workflow_stage == WorkflowStage.DATA_UPLOAD:
            files_status = []
            if state.data_summary.csv_info:
                files_status.append(f"✅ CSV ({state.data_summary.ward_count} wards)")
            else:
                files_status.append("⏳ CSV (pending)")
                
            if state.data_summary.shapefile_info:
                files_status.append("✅ Shapefile")
            else:
                files_status.append("⏳ Shapefile (pending)")
                
            progress_message += "**Files:** " + " | ".join(files_status)
            
        elif state.workflow_stage == WorkflowStage.ANALYSIS_RUNNING:
            analysis_step = state.analysis_state.value.replace('_', ' ').title()
            progress_message += f"**Current Step:** {analysis_step}"
            
        elif state.workflow_stage == WorkflowStage.ANALYSIS_COMPLETE:
            results_count = len(state.analysis_results.vulnerability_rankings) if state.analysis_results.vulnerability_rankings else 0
            progress_message += f"**Results:** {results_count} areas analyzed and ranked"
        
        return progress_message
    
    def get_error_guidance(self, error_type: str, state: SessionState) -> str:
        """
        Generate guidance for handling errors
        
        Args:
            error_type: Type of error encountered
            state: Current session state
            
        Returns:
            Error-specific guidance message
        """
        error_guidance = {
            'file_upload': """📁 **File Upload Issue**
            
It looks like there was a problem uploading your file. Here's what you can try:

1. **Check file format** - I accept CSV files and ZIP files containing shapefiles
2. **Verify file size** - Large files may take longer to upload
3. **Try again** - Sometimes network issues can cause upload failures

Would you like me to explain the required file formats in detail?""",
            
            'data_validation': """⚠️ **Data Validation Issue**
            
I found some issues with your data that need to be resolved:

1. **Check ward names** - Ensure ward names match between CSV and shapefile
2. **Verify data format** - CSV should have proper headers and numeric data
3. **Review missing values** - Some variables may have too many missing values

I can help you identify and fix these specific issues. What would you like me to check first?""",
            
            'analysis_error': """🔧 **Analysis Error**
            
The analysis encountered an issue, but don't worry - I can help you resolve it:

1. **Check data quality** - There may be data issues preventing analysis
2. **Verify variables** - Some variables might not be suitable for analysis  
3. **Try again** - Sometimes restarting the analysis resolves temporary issues

Would you like me to run a data quality check to identify the specific issue?""",
            
            'visualization_error': """🗺️ **Visualization Error**
            
There was a problem creating your map or visualization:

1. **Check analysis results** - The analysis may not have completed properly
2. **Try different map type** - Some visualizations may work better than others
3. **Refresh and retry** - Sometimes refreshing resolves display issues

Which type of visualization were you trying to create? I can suggest alternatives."""
        }
        
        return error_guidance.get(error_type, """❓ **Unexpected Issue**
        
I encountered an unexpected issue, but I'm here to help! Here's what we can do:

1. **Try the action again** - Sometimes temporary issues resolve themselves
2. **Check your data** - Ensure your files are properly formatted
3. **Ask for help** - Tell me what you were trying to do and I'll guide you

What were you trying to accomplish? I'll help you find the best way forward.""")
    
    def get_feature_explanation(self, feature: str) -> str:
        """
        Explain specific features of the tool
        
        Args:
            feature: Feature to explain
            
        Returns:
            Feature explanation
        """
        explanations = {
            'malaria_analysis': """🦟 **Malaria Risk Analysis**
            
This comprehensive analysis evaluates multiple environmental and socio-economic factors to identify areas at highest risk for malaria transmission:

**Key Variables Analyzed:**
- Environmental factors (rainfall, temperature, elevation)
- Vector breeding sites (water bodies, vegetation)
- Socio-economic indicators (housing quality, population density)
- Geographic accessibility

**Analysis Process:**
1. **Data Cleaning** - Handle missing values and outliers
2. **Normalization** - Standardize variables to 0-1 scale
3. **Composite Scoring** - Combine variables using weighted models
4. **Ranking** - Rank areas from highest to lowest risk

**Output:**
- Vulnerability rankings for all geographic areas
- Interactive maps showing risk distribution
- Detailed reports with methodology and recommendations""",
            
            'vulnerability_mapping': """🗺️ **Vulnerability Mapping**
            
Create interactive maps that visualize malaria risk across your study area:

**Map Types Available:**
- **Vulnerability Map** - Shows overall risk ranking with color coding
- **Variable Maps** - Display individual risk factors
- **Composite Maps** - Show different risk calculation models
- **Urban Extent Maps** - Highlight urban vs rural risk patterns

**Features:**
- Interactive hover information for each area
- Color-coded risk levels (low to high)
- Downloadable in multiple formats
- Customizable styling and labels

**Interpretation:**
- Darker colors = Higher risk
- Areas ranked 1-10 are highest priority
- Maps help identify spatial patterns and clusters""",
            
            'data_requirements': """📊 **Data Requirements**
            
To perform malaria risk analysis, you need two types of files:

**1. CSV Data File:**
- Contains malaria risk variables for each geographic area
- Must include a 'WardName' column matching the shapefile
- Should include variables like rainfall, temperature, population, etc.
- Accepts missing values (will be handled during analysis)

**2. Shapefile (ZIP format):**
- Provides geographic boundaries for mapping
- Must contain polygons for each area in your CSV
- Should have a field matching the CSV 'WardName' column
- Standard shapefile components: .shp, .shx, .dbf, .prj files

**Optional Variables:**
Environmental: rainfall, temperature, elevation, NDVI, soil moisture
Socio-economic: population density, housing quality, urbanization
Health: distance to health facilities, previous malaria cases"""
        }
        
        return explanations.get(feature.lower().replace(' ', '_'), 
                              f"I'd be happy to explain {feature}! Could you be more specific about what aspect you'd like to know about?")
    
    def get_contextual_tips(self, state: SessionState) -> List[str]:
        """
        Get contextual tips based on current state
        
        Args:
            state: Current session state
            
        Returns:
            List of helpful tips
        """
        tips = []
        
        if state.workflow_stage == WorkflowStage.INITIAL:
            tips = [
                "💡 Tip: Have both your CSV data and shapefile ready before starting",
                "📋 Tip: Ensure your CSV has a 'WardName' column that matches your shapefile",
                "🔍 Tip: I can analyze any malaria-related variables in your data"
            ]
            
        elif state.workflow_stage == WorkflowStage.DATA_UPLOAD:
            tips = [
                "✅ Tip: I'll automatically validate your data once both files are uploaded",
                "📏 Tip: Missing values are okay - I'll handle them during analysis",
                "🗺️ Tip: Your shapefile should cover the same areas as your CSV data"
            ]
            
        elif state.workflow_stage == WorkflowStage.ANALYSIS_COMPLETE:
            tips = [
                "🎯 Tip: Focus on the top 10 ranked areas for priority interventions",
                "📊 Tip: Variable maps help understand what drives high risk in each area",
                "📑 Tip: Generate a report to share results with stakeholders"
            ]
        
        return tips[:3]  # Limit to 3 tips 