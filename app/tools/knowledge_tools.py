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
        
        # Create adaptive system prompt based on concept and context
        if concept.lower() in ['chatmrpt', 'system capabilities', 'what can you do']:
            system_prompt = """
            You are a malaria epidemiologist embedded in ChatMRPT, an advanced malaria risk assessment system.
            
            Explain ChatMRPT's capabilities naturally and conversationally:
            • Urban microstratification for malaria control
            • Composite Risk Scoring and PCA analysis methods  
            • Ward-level vulnerability mapping and ranking
            • Data-driven intervention targeting
            
            Keep your ChatMRPT persona but be warm and helpful, not robotic.
            Reference the user's specific data when available.
            """
        elif any(keyword in concept.lower() for keyword in ['how to use', 'upload data', 'data accept', 'getting started', 'data format']):
            system_prompt = """
            You are a malaria epidemiologist embedded in ChatMRPT. The user is asking about practical usage - how to upload data and get started.
            
            Provide COMPREHENSIVE, step-by-step guidance covering:
            
            1. DATA REQUIREMENTS:
            • CSV file with ward-level data (rows = wards, columns = variables)
            • Shapefile (.zip) containing ward boundaries (geometry)
            • Variables needed: environmental, demographic, health, socioeconomic indicators
            
            2. STEP-BY-STEP UPLOAD PROCESS:
            • Click the upload button in the interface
            • Select your CSV and shapefile
            • System validates and processes the data
            • Automatic data integration and quality checks
            
            3. WHAT HAPPENS AFTER UPLOAD:
            • You can run composite risk scoring
            • You can run PCA analysis
            • Generate vulnerability maps and rankings
            • Ask questions about specific wards or variables
            
            4. EXAMPLE DATA STRUCTURE:
            • Ward names/IDs for linking
            • Population density, housing quality
            • Distance to health facilities
            • Environmental factors (elevation, water bodies, etc.)
            • Malaria indicators (if available)
            
            Be practical, specific, and maintain your epidemiologist persona.
            """
        else:
            system_prompt = """
            You are a malaria epidemiologist working with ChatMRPT, a malaria risk assessment system.
            
            Your expertise covers all aspects of malaria and public health:
            • Malaria biology, transmission, and control
            • Vector ecology and environmental factors  
            • Epidemiology and disease surveillance
            • Public health interventions and policy
            • Urban microstratification and spatial analysis
            
            INTELLIGENT RESPONSE GUIDELINES:
            
            📏 LENGTH & STRUCTURE:
            • For single concepts: 200-400 words, focused and clear
            • For broad topics: Up to 600 words but well-organized with headers
            • Use clear structure: intro → main content → practical implications
            • Break complex topics into digestible sections with **bold headers**
            
            🔗 TRANSITIONS & FLOW:
            • Start with context: "Understanding X is crucial because..."
            • Link concepts naturally: "This connects to..." or "Building on this..."
            • Use smooth transitions between sections
            • End with actionable insights or next steps
            
            🎯 PERSONALIZATION:
            • When user has data: Reference their specific context naturally
            • Connect theory to their practical situation
            • Use examples relevant to their dataset when available
            • Make it personal but not forced
            
            RESPONSE STYLE:
            • Expert but conversational - like a knowledgeable colleague
            • Educational and engaging, not textbook-like
            • Use accessible language while maintaining scientific accuracy
            • Focus on practical applications and real-world relevance
            
            Answer comprehensively but efficiently - provide maximum value in minimum words.
            """
        
        user_prompt = f'Please provide a comprehensive explanation of: "{concept}"'
        
        # Add context if available - let LLM weave it in naturally
        if session_context:
            context_details = []
            context_details.append(f"The user has uploaded data for {session_context['total_wards']} wards")
            
            if session_context['has_composite_analysis'] and session_context['has_pca_analysis']:
                context_details.append("They have run both composite and PCA risk analyses")
            elif session_context['has_composite_analysis']:
                context_details.append("They have run composite risk analysis")
            elif session_context['has_pca_analysis']:
                context_details.append("They have run PCA risk analysis")
            
            context_details.append(f"Their dataset includes variables like: {', '.join(session_context['available_variables'][:5])}")
            
            user_prompt += f"\n\nUser Context: {' | '.join(context_details)}. Naturally reference their data when relevant to make your response personal to their situation."
        
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


def show_help_options(session_id: str, error_context: str = None) -> dict:
    """
    Show comprehensive help options when user needs guidance.
    
    Args:
        session_id: The session ID for tracking and context
        error_context: Optional context about what went wrong
        
    Returns:
        dict: Help response with suggestions and guidance
    """
    try:
        from ..services import service_container
        
        llm_manager = service_container.get('llm_manager')
        data_service = service_container.get('data_service')
        
        # Get session context to provide relevant help
        session_context = _get_session_context(data_service, session_id)
        
        system_prompt = """You are ChatMRPT, a helpful malaria epidemiologist assistant.

The user needs help understanding what you can do or how to proceed. Your task is to:

1. Be warm and welcoming
2. Provide clear, actionable guidance
3. Suggest specific things they can try based on their current context
4. Use natural, conversational language
5. Make complex features sound simple and useful

CONTEXT-AWARE HELP:
- If they have data: Suggest specific analyses they can run
- If no data: Guide them on data upload and what formats work
- If analysis complete: Suggest visualizations or deeper insights
- Always provide 3-5 specific example questions they can ask

FORMAT:
Use a friendly introduction, then organize help into clear sections with examples.
Make it feel like a colleague explaining, not a manual."""

        # Build context-aware prompt
        context_details = []
        
        if session_context['total_wards'] > 0:
            context_details.append(f"User has uploaded data for {session_context['total_wards']} wards")
            
            if session_context['has_composite_analysis'] or session_context['has_pca_analysis']:
                context_details.append("They have completed some analyses")
                example_questions = [
                    "Show me the top 10 most vulnerable wards",
                    "Create a vulnerability map",
                    "What's the risk level for [specific ward name]?",
                    "Compare composite and PCA rankings",
                    "Explain what makes a ward high-risk"
                ]
            else:
                example_questions = [
                    "Run composite vulnerability analysis",
                    "Analyze my data using PCA",
                    "Show me summary statistics",
                    "What variables are in my dataset?",
                    "Create a scatter plot of population vs malaria cases"
                ]
        else:
            context_details.append("No data uploaded yet")
            example_questions = [
                "How do I upload my data?",
                "What file formats do you accept?",
                "Explain composite vulnerability scoring",
                "What is PCA analysis?",
                "Tell me about malaria risk factors"
            ]
        
        user_prompt = f"""Generate helpful guidance for the user.

Context: {' | '.join(context_details) if context_details else 'New session'}
{f"Error context: {error_context}" if error_context else ""}

Include these example questions naturally in your response:
{chr(10).join([f'• "{q}"' for q in example_questions])}

Make the help feel personal and actionable based on their current situation."""

        help_response = llm_manager.generate_response(
            prompt=user_prompt,
            system_message=system_prompt,
            temperature=0.8,
            max_tokens=800,
            session_id=session_id
        )
        
        return {
            'status': 'success',
            'message': 'Help guidance generated',
            'response': help_response.strip(),
            'help_type': 'contextual',
            'session_context': {
                'has_data': session_context['total_wards'] > 0,
                'has_analysis': session_context['has_composite_analysis'] or session_context['has_pca_analysis']
            },
            'suggested_actions': example_questions
        }
        
    except Exception as e:
        logger.error(f"Error generating help: {e}")
        # Fallback help response
        fallback_help = """I'm ChatMRPT, your malaria risk assessment assistant! Here's how I can help:

**If you have data to analyze:**
• Upload CSV, Excel, or Shapefile data through the upload interface
• Ask me to "run composite analysis" or "run PCA analysis"
• Request visualizations like "create a vulnerability map"
• Query specific wards: "What's the risk level for Dala ward?"

**For learning about malaria:**
• Ask about concepts: "Explain malaria transmission"
• Methodology questions: "How does composite scoring work?"
• Variable explanations: "What is pfpr?"

**Need analysis help?**
Try questions like:
• "Show me the top 20 most vulnerable wards"
• "Create a scatter plot of elevation vs malaria cases"
• "What are the main risk factors in my data?"

What would you like to explore?"""
        
        return {
            'status': 'success',
            'message': 'Help guidance provided',
            'response': fallback_help,
            'help_type': 'fallback'
        } 