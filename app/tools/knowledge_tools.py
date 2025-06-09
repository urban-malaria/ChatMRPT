"""
Knowledge and Explanation Tools for ChatMRPT

These tools provide expert malaria epidemiology knowledge and explanations
using LLM capabilities with domain expertise and session context.
"""

import logging
from typing import Dict, Any, Optional
from flask import current_app

logger = logging.getLogger(__name__)


def simple_greeting(session_id: str, greeting_type: str = "hello") -> Dict[str, Any]:
    """Provide a simple, friendly greeting response."""
    try:
        greetings = {
            "hello": "Hello there! I'm ChatMRPT, your malaria risk assessment assistant. How can I help you today?",
            "hi": "Hi! I'm ChatMRPT, here to help you with malaria risk analysis. What would you like to explore?",
            "who_are_you": "Hello! I'm ChatMRPT, a malaria risk prioritization tool designed to help with urban microstratification analysis. How can I assist you?",
            "what_can_you_do": "I can help you analyze malaria risk data, create vulnerability maps, rank wards by risk, and generate visualizations for targeted intervention planning. What would you like to start with?"
        }
        
        message = greetings.get(greeting_type, greetings["hello"])
        
        return {
            'status': 'success',
            'message': 'Simple greeting generated',
            'greeting': message,
            'type': 'simple_greeting'
        }
        
    except Exception as e:
        logger.error(f"Error generating simple greeting: {e}")
        return {'status': 'error', 'message': f'Error generating greeting: {str(e)}'}


def _get_session_context(session_id: str) -> Optional[Dict[str, Any]]:
    """Get session context for personalized explanations."""
    try:
        from ..data.unified_dataset_builder import load_unified_dataset
        unified_gdf = load_unified_dataset(session_id)
        
        if unified_gdf is not None:
            context = {
                'total_wards': len(unified_gdf),
                'available_variables': list(unified_gdf.columns)[:20],
                'has_composite_analysis': any('composite' in col.lower() for col in unified_gdf.columns),
                'has_pca_analysis': any('pca' in col.lower() for col in unified_gdf.columns),
                'numeric_variables': list(unified_gdf.select_dtypes(include=['number']).columns)[:10]
            }
            
            # Add health variable example
            health_vars = [col for col in unified_gdf.columns if any(term in col.lower() for term in ['tpr', 'malaria', 'prevalence'])]
            if health_vars:
                var = health_vars[0]
                if unified_gdf[var].dtype in ['number']:
                    context['health_variable_example'] = {
                        'variable': var,
                        'mean': float(unified_gdf[var].mean()),
                        'range': [float(unified_gdf[var].min()), float(unified_gdf[var].max())]
                    }
            
            return context
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting session context: {e}")
        return None


def explain_concept(session_id: str, concept: str, include_context: bool = True) -> Dict[str, Any]:
    """Explain malaria epidemiology concepts with domain expertise."""
    try:
        llm_manager = current_app.services.llm_manager
        if not llm_manager:
            return {'status': 'error', 'message': 'LLM service not available'}
        
        # Get session context if requested
        session_context = None
        if include_context:
            session_context = _get_session_context(session_id)
        
        system_prompt = """
        You are a malaria epidemiologist and statistician embedded in the ChatMRPT system—a malaria risk assessment tool powered by GPT-4o.
        
        You specialize in conducting malaria risk assessments in endemic countries, with a focus on urban settings in sub-Saharan Africa, especially Nigeria. You understand the wide range of malaria risk factors and how they vary across local geographies.
        
        Your main role is to guide users through urban microstratification—a process used to rank lower-level administrative units (wards, districts, etc.) by malaria risk to enable targeted intervention planning.
        
        ChatMRPT supports two core analytical methods:
        • Composite Risk Scoring: summing normalized values across malaria risk factors
        • Principal Component Analysis (PCA): reducing dimensionality and ranking based on weighted combinations of variables
        
        Each analysis is conducted for a named geographic area—called a state in Nigeria. Prompt users to specify the name of the state they are working with and keep track of it for use in all subsequent responses.
        
        DATA UPLOAD HANDLING:
        Users will upload data in CSV, Excel, or shapefile formats. In spreadsheet files:
        • The first row should contain variable names.
        • If the first row is entirely numeric, it is not valid—discard it.
        • The variables represent malaria risk factors to be used in stratification.
        
        Upon upload of a CSV or Excel file:
        • Search the column headers to identify the one that likely contains ward names.
        • If a column name contains the text "wardname" (case-insensitive), assume that this column contains the ward names. Automatically keep track of this column and use it to respond to any questions that reference specific wards.
        • If no "wardname" column is found, look for likely alternatives (e.g., "ward", "district", "area") and ask the user to confirm which column contains the ward names before proceeding with the analysis.
        
        USER INTERACTION GUIDELINES:
        When users interact with you:
        • Use clear, accessible language
        • Match the user's level of expertise (e.g., simplify for program staff, use more technical language for data scientists)
        • Maintain a friendly, respectful tone
        • Provide step-by-step explanations of any analyses or visualizations
        • Reference the state name, ward names, and uploaded data context in all relevant replies
        • Maintain session memory to provide continuity across multiple user inputs
        
        TECHNICAL KNOWLEDGE:
        **Urban Microstratification**: A WHO-recommended spatial epidemiological approach that divides urban areas into smaller, homogeneous transmission zones based on malaria risk factors. It involves mapping epidemiological data at sub-district level, analyzing environmental risk factors (breeding sites, altitude, vegetation), incorporating socio-demographic variables, and using GIS and statistical analysis to create risk strata.
        
        **Risk Factors**: Include parasitemia prevalence, vector breeding sites, housing quality, population density, access to healthcare, environmental factors (NDVI, rainfall, temperature), and socio-economic indicators.
        
        Your goal is to serve as a knowledgeable, responsive assistant who translates complex statistical tools into actionable insights for malaria program decision-making.
        
        Provide technical, detailed explanations that demonstrate deep epidemiological expertise. Reference specific methodologies, WHO guidelines, and practical implementation details. Connect concepts to ChatMRPT's capabilities where relevant.
        
        When session data is available, provide specific examples using their dataset.
        """
        
        user_prompt = f"""
        Please explain the concept: "{concept}"
        
        If this is "ChatMRPT" or about this system:
        - Introduce yourself as a malaria epidemiologist embedded in ChatMRPT
        - Explain ChatMRPT as a malaria risk assessment tool for urban microstratification
        - Describe the two core methods (Composite Risk Scoring and PCA)
        - Explain how it helps prioritize wards for targeted interventions
        
        If this is "urban microstratification" or related:
        - Provide WHO definition and context
        - Explain the technical methodology
        - Describe implementation steps
        - Connect to ChatMRPT's capabilities
        
        For other concepts, provide comprehensive explanation covering:
        1. Technical definition with epidemiological context
        2. Relevance to malaria transmission and control
        3. How it's measured/analyzed in research
        4. Practical implications for public health programs
        5. Connection to urban settings and Nigeria where applicable
        """
        
        # Add context if available
        if session_context:
            user_prompt += f"""
            
        Session Context (use for personalized examples):
        - Dataset contains {session_context['total_wards']} wards
        - Analysis status: {'Composite analysis available' if session_context['has_composite_analysis'] else 'No composite analysis'}, {'PCA analysis available' if session_context['has_pca_analysis'] else 'No PCA analysis'}
        - Available variables include: {', '.join(session_context['available_variables'][:10])}
        """
            
            if 'health_variable_example' in session_context:
                health_example = session_context['health_variable_example']
                user_prompt += f"""
        - Health indicator example: {health_example['variable']} with mean value {health_example['mean']:.2f}
        """
        
        explanation = llm_manager.generate_response(
            prompt=user_prompt,
            system_message=system_prompt,
            temperature=0.7,
            max_tokens=1500,
            session_id=session_id
        )
        
        return {
            'status': 'success',
            'message': f'Explanation generated for concept: {concept}',
            'concept': concept,
            'explanation': explanation,
            'personalized': session_context is not None
        }
        
    except Exception as e:
        logger.error(f"Error explaining concept: {e}")
        return {'status': 'error', 'message': f'Error generating explanation: {str(e)}'}


def explain_methodology(session_id: str, methodology: str, technical_level: str = 'intermediate') -> Dict[str, Any]:
    """Explain analysis methodologies used in malaria research."""
    try:
        llm_manager = current_app.services.llm_manager
        if not llm_manager:
            return {'status': 'error', 'message': 'LLM service not available'}
        
        session_context = _get_session_context(session_id)
        
        system_prompt = f"""
        You are an expert in epidemiological analysis methods and malaria research.
        Explain analysis methodologies at a {technical_level} level.
        
        For basic: Focus on concepts and practical applications
        For intermediate: Include some technical details and assumptions
        For advanced: Include mathematical foundations and limitations
        """
        
        user_prompt = f"""
        Please explain the methodology: "{methodology}"
        
        Cover these aspects at a {technical_level} level:
        1. Purpose and objectives of this methodology
        2. How it works (steps/process)
        3. Assumptions and requirements
        4. Advantages and limitations
        5. When to use this methodology
        6. How to interpret results
        """
        
        if session_context:
            methodology_applied = (
                (methodology.lower() in ['composite', 'composite scoring'] and session_context['has_composite_analysis']) or
                (methodology.lower() in ['pca', 'principal component'] and session_context['has_pca_analysis'])
            )
            
            user_prompt += f"""
            
        Context from current session:
        - Working with {session_context['total_wards']} wards
        - {'This methodology has been applied to your data' if methodology_applied else 'This methodology could be applied to your data'}
        """
        
        explanation = llm_manager.generate_response(
            prompt=user_prompt,
            system_message=system_prompt,
            temperature=0.7,
            max_tokens=1200,
            session_id=session_id
        )
        
        return {
            'status': 'success',
            'message': f'Methodology explanation generated for: {methodology}',
            'methodology': methodology,
            'technical_level': technical_level,
            'explanation': explanation
        }
        
    except Exception as e:
        logger.error(f"Error explaining methodology: {e}")
        return {'status': 'error', 'message': f'Error generating methodology explanation: {str(e)}'}


def explain_variable(session_id: str, variable: str) -> Dict[str, Any]:
    """Explain specific variables and their role in malaria analysis."""
    try:
        llm_manager = current_app.services.llm_manager
        if not llm_manager:
            return {'status': 'error', 'message': 'LLM service not available'}
        
        # Get dataset to check if variable exists
        from ..data.unified_dataset_builder import load_unified_dataset
        unified_gdf = load_unified_dataset(session_id)
        
        variable_stats = None
        variable_exists = False
        
        if unified_gdf is not None and variable in unified_gdf.columns:
            variable_exists = True
            
            if unified_gdf[variable].dtype in ['number']:
                variable_stats = {
                    'type': 'numeric',
                    'mean': float(unified_gdf[variable].mean()),
                    'min': float(unified_gdf[variable].min()),
                    'max': float(unified_gdf[variable].max()),
                    'missing_count': int(unified_gdf[variable].isnull().sum()),
                    'total_count': len(unified_gdf)
                }
            else:
                value_counts = unified_gdf[variable].value_counts().head(5)
                variable_stats = {
                    'type': 'categorical',
                    'unique_values': int(unified_gdf[variable].nunique()),
                    'top_values': value_counts.to_dict(),
                    'missing_count': int(unified_gdf[variable].isnull().sum()),
                    'total_count': len(unified_gdf)
                }
        
        system_prompt = """
        You are an expert malaria epidemiologist. Explain variables used in malaria research
        with focus on their epidemiological significance, measurement methods, and interpretation.
        """
        
        user_prompt = f"""
        Please explain the variable: "{variable}"
        
        Provide explanation covering:
        1. What this variable measures/represents
        2. How it's typically collected or calculated
        3. Its relevance to malaria epidemiology
        4. Normal ranges or expected values (if applicable)
        5. How to interpret different values
        6. Limitations or considerations when using this variable
        """
        
        if variable_exists and variable_stats:
            user_prompt += f"""
            
        Data from current session:
        - Variable found in dataset with {variable_stats['total_count']} records
        """
            
            if variable_stats['type'] == 'numeric':
                user_prompt += f"""
        - Numeric variable: mean = {variable_stats['mean']:.2f}, range = [{variable_stats['min']:.2f}, {variable_stats['max']:.2f}]
        - Missing values: {variable_stats['missing_count']} ({(variable_stats['missing_count']/variable_stats['total_count']*100):.1f}%)
        
        Please comment on whether these values seem reasonable for this variable.
        """
            else:
                user_prompt += f"""
        - Categorical variable with {variable_stats['unique_values']} unique values
        - Top categories: {', '.join([f"{k}: {v}" for k, v in list(variable_stats['top_values'].items())[:3]])}
        - Missing values: {variable_stats['missing_count']} ({(variable_stats['missing_count']/variable_stats['total_count']*100):.1f}%)
        """
        elif not variable_exists:
            user_prompt += f"""
            
        Note: This variable was not found in the current dataset, so provide a general explanation
        of what this variable typically represents in malaria epidemiology.
        """
        
        explanation = llm_manager.generate_response(
            prompt=user_prompt,
            system_message=system_prompt,
            temperature=0.7,
            max_tokens=1500,
            session_id=session_id
        )
        
        return {
            'status': 'success',
            'message': f'Variable explanation generated for: {variable}',
            'variable': variable,
            'variable_exists_in_data': variable_exists,
            'variable_stats': variable_stats,
            'explanation': explanation
        }
        
    except Exception as e:
        logger.error(f"Error explaining variable: {e}")
        return {'status': 'error', 'message': f'Error generating variable explanation: {str(e)}'}


def interpret_results(session_id: str, analysis_type: str = 'composite', specific_question: str = None) -> Dict[str, Any]:
    """Interpret analysis results with expert guidance."""
    try:
        llm_manager = current_app.services.llm_manager
        if not llm_manager:
            return {'status': 'error', 'message': 'LLM service not available'}
        
        session_context = _get_session_context(session_id)
        if not session_context:
            return {'status': 'error', 'message': 'No session data available for interpretation'}
        
        system_prompt = """
        You are an expert malaria epidemiologist providing interpretation and guidance
        on analysis results. Focus on actionable insights and public health implications.
        """
        
        if analysis_type.lower() == 'composite':
            if not session_context['has_composite_analysis']:
                return {'status': 'error', 'message': 'No composite analysis results found in session data'}
            
            user_prompt = f"""
            Please interpret the composite scoring analysis results for this malaria risk assessment.
            
            Dataset context:
            - {session_context['total_wards']} wards analyzed
            - Composite analysis completed
            
            Provide interpretation covering:
            1. What the composite scores represent
            2. How to identify high-risk areas
            3. Implications for resource allocation
            4. Recommended follow-up actions
            5. Limitations and considerations
            """
            
        elif analysis_type.lower() == 'pca':
            if not session_context['has_pca_analysis']:
                return {'status': 'error', 'message': 'No PCA analysis results found in session data'}
            
            user_prompt = f"""
            Please interpret the Principal Component Analysis (PCA) results for vulnerability assessment.
            
            Dataset context:
            - {session_context['total_wards']} wards analyzed
            - PCA analysis completed
            
            Provide interpretation covering:
            1. What the principal components represent
            2. How to understand vulnerability patterns
            3. Which factors drive vulnerability
            4. Geographic/spatial implications
            5. How to use results for intervention planning
            """
            
        else:
            # General interpretation
            analyses = []
            if session_context['has_composite_analysis']:
                analyses.append('Composite scoring')
            if session_context['has_pca_analysis']:
                analyses.append('PCA')
            
            user_prompt = f"""
            Please provide guidance on interpreting malaria risk analysis results.
            
            Dataset context:
            - {session_context['total_wards']} wards in analysis
            - Available analyses: {', '.join(analyses) if analyses else 'None completed'}
            
            Provide interpretation covering:
            1. How to read and understand the results
            2. Key patterns to look for
            3. Public health implications
            4. Actionable recommendations
            5. Next steps for intervention planning
            """
        
        if specific_question:
            user_prompt += f"""
            
        Specific question to address: "{specific_question}"
        """
        
        interpretation = llm_manager.generate_response(
            prompt=user_prompt,
            system_message=system_prompt,
            temperature=0.7,
            max_tokens=1200,
            session_id=session_id
        )
        
        return {
            'status': 'success',
            'message': f'Results interpretation generated for {analysis_type} analysis',
            'analysis_type': analysis_type,
            'specific_question': specific_question,
            'interpretation': interpretation,
            'session_context': {
                'total_wards': session_context['total_wards'],
                'has_composite_analysis': session_context['has_composite_analysis'],
                'has_pca_analysis': session_context['has_pca_analysis']
            }
        }
        
    except Exception as e:
        logger.error(f"Error interpreting results: {e}")
        return {'status': 'error', 'message': f'Error generating result interpretation: {str(e)}'} 