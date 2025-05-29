"""
Core LLM Manager for ChatMRPT.

This module provides the essential LLM functionality needed by the modern
service architecture, extracted from the legacy ai_utils.py.
"""

import json
import time
import logging
import openai
from typing import Dict, Any, Optional, List
from flask import current_app

logger = logging.getLogger(__name__)


class LLMManager:
    """
    Core class for managing interactions with Language Models.
    
    This is a simplified version of the legacy LLMManager that provides
    the essential functionality needed by the modern service architecture.
    """
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4o", 
                 interaction_logger=None):
        """
        Initialize the LLM Manager.
        
        Args:
            api_key: OpenAI API key
            model: LLM model to use
            interaction_logger: Optional InteractionLogger instance
        """
        self.api_key = api_key or self._get_api_key_from_config()
        self.model = model
        self.interaction_logger = interaction_logger
        self.client = None
        
        # Initialize OpenAI client if API key is available
        if self.api_key:
            try:
                self.client = openai.OpenAI(api_key=self.api_key)
                logger.info(f"Initialized OpenAI client with model: {self.model}")
            except Exception as e:
                logger.error(f"Error initializing OpenAI client: {str(e)}")
                self.client = None
    
    def _get_api_key_from_config(self) -> Optional[str]:
        """Get API key from Flask app config."""
        try:
            return current_app.config.get('OPENAI_API_KEY')
        except:
            return None
    
    def generate_response(self, prompt: str, context: Optional[Any] = None, 
                         system_message: Optional[str] = None, temperature: float = 0.7, 
                         max_tokens: int = 1000, session_id: Optional[str] = None) -> str:
        """
        Generate a response from the LLM.
        
        Args:
            prompt: The prompt text
            context: Optional additional context
            system_message: Optional system message
            temperature: Temperature for generation
            max_tokens: Maximum tokens to generate
            session_id: Optional session ID for logging
            
        Returns:
            The generated response or error message
        """
        if not self.client:
            if not self.api_key:
                return "Error: No API key available for LLM"
            try:
                self.client = openai.OpenAI(api_key=self.api_key)
            except Exception as e:
                return f"Error initializing OpenAI client: {str(e)}"
        
        # Prepare messages array
        messages = []
        
        # Add system message
        if system_message:
            messages.append({"role": "system", "content": system_message})
        else:
            messages.append({
                "role": "system", 
                "content": "You are an assistant that provides clear, accurate, and helpful information about malaria risk assessment."
            })
        
        # Add context if provided
        if context:
            if isinstance(context, str):
                messages.append({
                    "role": "system",
                    "content": f"Additional context: {context}"
                })
            elif isinstance(context, dict):
                try:
                    context_str = json.dumps(context, indent=2, default=str)
                    messages.append({
                        "role": "system",
                        "content": f"Additional context (JSON):\n{context_str}"
                    })
                except Exception:
                    messages.append({
                        "role": "system",
                        "content": f"Additional context: {str(context)}"
                    })
        
        # Add user prompt
        messages.append({"role": "user", "content": prompt})
        
        # Track metrics
        start_time = time.time()
        
        try:
            # Call the API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            
            # Extract response
            llm_response = response.choices[0].message.content
            
            # Calculate metrics
            end_time = time.time()
            latency = end_time - start_time
            tokens_used = response.usage.total_tokens if hasattr(response, 'usage') else None
            
            # Log the interaction if logger is available
            if self.interaction_logger and session_id:
                prompt_type = "explanation" if "explain" in prompt.lower() else "general"
                self.interaction_logger.log_llm_interaction(
                    session_id=session_id,
                    prompt_type=prompt_type,
                    prompt=prompt,
                    prompt_context=context,
                    response=llm_response,
                    tokens_used=tokens_used,
                    latency=latency
                )
            
            return llm_response
            
        except Exception as e:
            error_message = f"Error calling LLM: {str(e)}"
            logger.error(error_message)
            
            # Log error if logger is available
            if self.interaction_logger and session_id:
                self.interaction_logger.log_error(
                    session_id=session_id,
                    error_type="llm_api_error",
                    error_message=str(e)
                )
            
            return f"I apologize, but I encountered an error while processing your request: {str(e)}"
    
    def generate_general_response(self, prompt: str, context: Optional[Any] = None, 
                                session_id: Optional[str] = None) -> str:
        """
        Generate a general response from the LLM - simplified wrapper for common use.
        
        Args:
            prompt: The prompt text
            context: Optional additional context
            session_id: Optional session ID for logging
            
        Returns:
            The generated response
        """
        return self.generate_response(
            prompt=prompt,
            context=context,
            session_id=session_id,
            temperature=0.7,
            max_tokens=1000
        )
    
    def generate_simple_response(self, prompt: str) -> str:
        """
        Generate a minimal response for testing connectivity.
        
        Args:
            prompt: Simple prompt for testing
            
        Returns:
            str: Response from the LLM or error message
        """
        try:
            if not self.client:
                if not self.api_key:
                    return "Error: No API key available"
                try:
                    self.client = openai.OpenAI(api_key=self.api_key)
                except Exception as e:
                    return f"Error initializing OpenAI client: {str(e)}"
            
            # Simple single message call
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a testing assistant."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.5,
                max_tokens=20,
            )
            
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error in simple response test: {str(e)}", exc_info=True)
            return f"Error: {str(e)}"
    
    def explain_visualization(self, session_id: str, viz_type: str, context: Optional[Any] = None, question: Optional[str] = None) -> str:
        """
        Generate an explanation for a visualization
        
        Args:
            session_id: Session identifier
            viz_type: Type of visualization ('variable_map', 'composite_map', etc.)
            context: Context information about the visualization
            question: Optional specific question about the visualization
            
        Returns:
            str: Explanation text
        """
        # Set up default question if none provided
        if not question:
            question = f"Please explain this {viz_type.replace('_', ' ')}."
        
        # Construct prompt for the LLM
        system_message = f"""
You are an expert in malaria epidemiology. Explain this visualization for a non-technical audience.
- Use bullet points.
- Limit each section to 1-2 sentences.
- Only include the most important details for understanding and decision-making.
- Do not repeat information across sections.
- Be concise and clear.
Sections:
1. Overview: What is this visualization and why does it matter?
2. How to Read: What do the colors/symbols mean?
3. Key Insights: What are the main takeaways?
4. Action: What should the user do or consider next?
        """
        
        # Generate the explanation
        explanation = self.generate_response(
            prompt=question,
            context=context,
            system_message=system_message,
            temperature=0.4,
            max_tokens=600,
            session_id=session_id
        )
        
        return explanation
    
    def extract_intent_and_entities(self, message: str, context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Extract intent and entities from user message using LLM.
        
        Args:
            message: User message
            context: Optional context information
            
        Returns:
            Dictionary with intent and entities
        """
        system_message = """You are an AI assistant for malaria risk assessment. Extract the user's intent and relevant entities from their message.
        
        Common intents include:
        - run_standard_analysis: User wants to run the standard analysis
        - run_custom_analysis: User wants to run analysis with specific variables  
        - request_visualization: User wants to create or view visualizations
        - explain_methodology: User wants explanation about methodology
        - explain_variable: User wants explanation about specific variables
        - explain_variable_selection: User wants explanation about why variables were selected
        - query_analysis_details: User wants details about the completed analysis
        - generate_report: User wants to generate a report
        - greet: User is greeting the system
        - general_knowledge_question: User is asking a general knowledge question unrelated to malaria analysis
        - clarification_needed: User's request is unclear
        
        Return a simple JSON with:
        {
          "intent": "one_of_the_intents_listed_above",
          "entities": {
            "variables": ["list", "of", "variable", "names"],
            "visualization_type": "type_if_mentioned",
            "other_entities": "value"
          }
        }"""
        
        prompt = f"Extract intent and entities from this message: '{message}'"
        
        try:
            response = self.generate_response(prompt, context, system_message, temperature=0.3)
            
            # Try to parse JSON response
            if response and '{' in response:
                # Extract JSON part
                start = response.find('{')
                end = response.rfind('}') + 1
                json_str = response[start:end]
                
                try:
                    result = json.loads(json_str)
                    return result
                except json.JSONDecodeError:
                    pass
            
            # Fallback: simple intent classification
            message_lower = message.lower()
            if any(word in message_lower for word in ['run', 'analysis', 'analyze']):
                return {"intent": "run_standard_analysis", "entities": {}}
            elif any(word in message_lower for word in ['visualiz', 'chart', 'map', 'plot']):
                return {"intent": "request_visualization", "entities": {}}
            elif any(word in message_lower for word in ['explain', 'why', 'how']):
                return {"intent": "explain_methodology", "entities": {}}
            elif any(word in message_lower for word in ['report', 'generate']):
                return {"intent": "generate_report", "entities": {}}
            elif any(word in message_lower for word in ['hello', 'hi', 'hey']):
                return {"intent": "greet", "entities": {}}
            # Check for general knowledge question patterns
            elif any(word in message_lower for word in ['what is', 'who is', 'where is', 'when did', 'how many', 'tell me about']):
                return {"intent": "general_knowledge_question", "entities": {"question": message}}
            elif message_lower.endswith('?'):
                return {"intent": "general_knowledge_question", "entities": {"question": message}}
            else:
                return {"intent": "clarification_needed", "entities": {}}
            
        except Exception as e:
            logger.error(f"Error extracting intent: {str(e)}")
            return {"intent": "clarification_needed", "entities": {}}

    def get_system_context(self):
        """
        Get system context for malaria risk assessment.
        
        Returns:
            dict: System context information
        """
        return {
            'purpose': 'Malaria Risk Prioritization Tool',
            'capabilities': [
                'Analyze malaria risk factors',
                'Generate vulnerability maps',
                'Compare areas for risk assessment',
                'Suggest intervention strategies',
                'Track control progress',
                'Predict outbreak risks',
                'Optimize resource allocation'
            ],
            'key_concepts': {
                'risk_factors': [
                    'Environmental conditions (rainfall, temperature, altitude)',
                    'Population density and movement patterns',
                    'Healthcare access and quality',
                    'Socioeconomic factors (poverty, education, housing)',
                    'Vector control coverage (bed nets, indoor spraying)',
                    'Climate patterns and seasonal variations',
                    'Previous malaria incidence',
                    'Proximity to water bodies and breeding sites',
                    'Housing quality and construction materials',
                    'Agricultural practices and land use'
                ],
                'interventions': [
                    'Vector control (ITNs, LLINs, IRS)',
                    'Case management (prompt diagnosis and treatment)',
                    'Preventive therapies (IPTp, SMC, MDA)',
                    'Community engagement and education',
                    'Health system strengthening',
                    'Surveillance and response systems',
                    'Environmental management',
                    'Cross-border collaboration',
                    'Research and innovation',
                    'Integrated approaches with other disease programs'
                ],
                'metrics': [
                    'Parasite prevalence rates',
                    'Test positivity rates',
                    'Incidence rates',
                    'Mortality rates',
                    'Healthcare utilization',
                    'Intervention coverage',
                    'Vector density',
                    'Entomological inoculation rate',
                    'Drug resistance markers',
                    'Seasonal patterns'
                ]
            },
            'analysis_methods': {
                'spatial_analysis': [
                    'Hotspot identification (local clusters)',
                    'Risk mapping (vulnerability distribution)',
                    'Cluster analysis (spatial patterns)',
                    'Buffer zone analysis (proximity effects)',
                    'Spatial regression (location-based relationships)',
                    'Boundary analysis (administrative divisions)',
                    'Distance calculations (healthcare access)',
                    'Geospatial modeling (prediction)',
                    'Network analysis (movement patterns)',
                    'Environmental correlation (habitat suitability)'
                ],
                'temporal_analysis': [
                    'Trend analysis (long-term patterns)',
                    'Seasonal patterns (annual cycles)',
                    'Outbreak prediction (early warning)',
                    'Progress tracking (intervention impact)',
                    'Time series decomposition',
                    'Periodicity detection',
                    'Change point analysis',
                    'Temporal clustering',
                    'Forecasting models',
                    'Intervention timing optimization'
                ],
                'multivariate_analysis': [
                    'Factor analysis (variable relationships)',
                    'Regression modeling (predictive factors)',
                    'Risk scoring (composite indices)',
                    'Vulnerability assessment (combined factors)',
                    'Principal component analysis',
                    'Cluster analysis',
                    'Decision tree models',
                    'Bayesian networks',
                    'Machine learning approaches',
                    'Ensemble methods'
                ]
            },
            'data_sources': [
                'Health facility records (cases, treatments)',
                'Population census (demographics)',
                'Environmental data (climate, topography)',
                'Climate records (rainfall, temperature)',
                'Socioeconomic indicators (wealth, education)',
                'Intervention coverage data (bed nets, spraying)',
                'Vector surveillance (mosquito populations)',
                'Sentinel site monitoring',
                'Household surveys',
                'Remote sensing imagery'
            ],
            'output_types': [
                'Risk maps (spatial distribution)',
                'Statistical reports (numerical findings)',
                'Trend visualizations (temporal patterns)',
                'Intervention recommendations (action plans)',
                'Resource allocation plans (optimization)',
                'Predictive models (forecasting)',
                'Vulnerability indices (composite scores)',
                'Comparison analyses (benchmarking)',
                'Progress tracking dashboards',
                'Scenario simulations'
            ],
            'who_guidelines': {
                'vector_control': 'Universal coverage with ITNs or IRS for populations at risk; larval control where appropriate',
                'case_management': 'Early diagnosis and prompt treatment with appropriate antimalarial medicines',
                'prevention': 'Intermittent preventive treatment for pregnant women, seasonal malaria chemoprevention for children in high transmission areas',
                'surveillance': 'Strengthen surveillance systems for tracking cases and intervention coverage',
                'elimination': 'Targeted approaches including active case detection and focal interventions in low transmission settings',
                'key_publications': [
                    'WHO Guidelines for Malaria (2021)',
                    'Global Technical Strategy for Malaria 2016-2030',
                    'Malaria Surveillance, Monitoring & Evaluation: A Reference Manual',
                    'Framework for Malaria Elimination'
                ]
            },
            'best_practices': {
                'data_collection': 'Standardized case reporting formats, regular data quality assessments',
                'intervention_targeting': 'Evidence-based prioritization of high-risk areas and populations',
                'resource_allocation': 'Optimizing limited resources based on burden and transmission intensity',
                'community_engagement': 'Involving local communities in planning and implementing interventions',
                'integrated_approach': 'Coordinating malaria control with other health programs for efficiency',
                'monitoring_evaluation': 'Regular assessment of intervention impact and program performance',
                'adaptability': 'Adjusting strategies based on changing epidemiology and new evidence'
            }
        }


def get_llm_manager(interaction_logger=None) -> LLMManager:
    """
    Factory function to create an LLM manager instance.
    
    Args:
        interaction_logger: Optional interaction logger
        
    Returns:
        LLM manager instance
    """
    try:
        api_key = current_app.config.get('OPENAI_API_KEY') if current_app else None
    except:
        api_key = None
    
    return LLMManager(api_key=api_key, interaction_logger=interaction_logger)


def convert_markdown_to_html(markdown_text: str) -> str:
    """
    Convert markdown text to HTML.
    
    Args:
        markdown_text: Markdown text to convert
        
    Returns:
        HTML string
    """
    if not markdown_text:
        return ""
    
    try:
        import markdown
        
        # Configure markdown with extensions
        md = markdown.Markdown(
            extensions=[
                'tables',
                'fenced_code', 
                'codehilite',
                'toc',
                'nl2br'
            ],
            extension_configs={
                'codehilite': {
                    'css_class': 'highlight'
                }
            }
        )
        
        html = md.convert(markdown_text)
        return html
        
    except ImportError:
        # Fallback: basic HTML conversion
        html = markdown_text
        html = html.replace('\n\n', '</p><p>')
        html = html.replace('\n', '<br>')
        html = html.replace('**', '<strong>').replace('**', '</strong>')
        html = html.replace('*', '<em>').replace('*', '</em>')
        html = f'<p>{html}</p>'
        return html
    except Exception as e:
        logger.error(f"Error converting markdown to HTML: {str(e)}")
        return markdown_text


def select_optimal_variables_with_llm(llm_manager, available_vars: List[str], csv_data, 
                                    relationships: Optional[Dict] = None,
                                    min_vars: int = 3, max_vars: int = 5) -> tuple:
    """
    Select optimal variables for analysis using LLM.
    
    Args:
        llm_manager: LLM manager instance
        available_vars: List of available variables
        csv_data: CSV data for analysis  
        relationships: Optional variable relationships
        min_vars: Minimum number of variables
        max_vars: Maximum number of variables
        
    Returns:
        Tuple of (selected_variables, explanations)
    """
    if not llm_manager or not available_vars:
        # Fallback: select first few numeric variables
        numeric_vars = [var for var in available_vars if var not in ['WardName', 'geometry']]
        selected = numeric_vars[:min_vars] if len(numeric_vars) >= min_vars else numeric_vars
        explanations = {var: f"Selected {var} for analysis (fallback)" for var in selected}
        return selected, explanations
    
    try:
        # Create context for LLM
        context = {
            'available_variables': available_vars,
            'min_variables': min_vars,
            'max_variables': max_vars,
            'data_shape': f"{len(csv_data)} rows" if csv_data is not None else "unknown"
        }
        
        prompt = f"""As a malaria epidemiologist, select {min_vars}-{max_vars} most important variables for malaria risk assessment from this list:

{', '.join(available_vars)}

Consider variables that best capture:
1. Disease burden indicators
2. Environmental risk factors  
3. Socioeconomic vulnerability
4. Healthcare accessibility

Return your selection as JSON:
{{
  "selected_variables": ["var1", "var2", "var3"],
  "explanations": {{
    "var1": "Brief explanation why this variable is important",
    "var2": "Brief explanation...",
    "var3": "Brief explanation..."
  }}
}}"""
        
        response = llm_manager.generate_response(prompt, context, temperature=0.3)
        
        # Try to parse JSON response
        if response and '{' in response:
            start = response.find('{')
            end = response.rfind('}') + 1
            json_str = response[start:end]
            
            try:
                result = json.loads(json_str)
                selected = result.get('selected_variables', [])
                explanations = result.get('explanations', {})
                
                # Validate selection
                if selected and all(var in available_vars for var in selected):
                    return selected, explanations
                
            except json.JSONDecodeError:
                pass
        
        # Fallback selection
        priority_vars = ['pfpr', 'u5_tpr', 'rainfall', 'temperature', 'poverty', 'population']
        selected = []
        for var in priority_vars:
            matching = [v for v in available_vars if var.lower() in v.lower()]
            if matching:
                selected.extend(matching[:1])
                if len(selected) >= max_vars:
                    break
        
        # Fill up to min_vars if needed
        if len(selected) < min_vars:
            remaining = [v for v in available_vars if v not in selected and v not in ['WardName', 'geometry']]
            selected.extend(remaining[:min_vars - len(selected)])
        
        explanations = {var: f"Selected {var} as important for malaria risk assessment" for var in selected}
        return selected[:max_vars], explanations
        
    except Exception as e:
        logger.error(f"Error in select_optimal_variables_with_llm: {str(e)}", exc_info=True)
        # Fallback selection
        numeric_vars = [var for var in available_vars if var not in ['WardName', 'geometry']]
        selected = numeric_vars[:min_vars] if len(numeric_vars) >= min_vars else numeric_vars
        explanations = {var: f"Selected {var} for analysis (error fallback)" for var in selected}
        return selected, explanations 