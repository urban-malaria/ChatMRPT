# app/ai_utils.py
import os
import json
import pandas as pd
import logging
import time
import re
from typing import Dict, List, Any, Optional, Union, Tuple
import datetime
import openai
from flask import current_app, session
import markdown # <<< ADD THIS IMPORT
from app.utilities import convert_to_json_serializable

# Set up logging
logger = logging.getLogger(__name__)

class LLMManager:
    """
    Central class for managing interactions with Language Models.
    Handles prompt construction, context assembly, and response processing
    for explanations and natural language understanding.
    """
    
    def __init__(self, api_key=None, model="gpt-4o", interaction_logger=None):
        """
        Initialize the LLM Manager
        
        Args:
            api_key: OpenAI API key (if None, will try to get from app config)
            model: LLM model to use
            interaction_logger: Optional InteractionLogger instance for tracking
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
    
    def _get_api_key_from_config(self):
        """Get API key from Flask app config"""
        try:
            return current_app.config.get('OPENAI_API_KEY')
        except:
            return None
    
    def generate_response(self, prompt, context=None, system_message=None, temperature=0.7, 
                         max_tokens=1000, session_id=None):
        """
        Generate a response from the LLM
        
        Args:
            prompt: The prompt text
            context: Optional additional context
            system_message: Optional system message
            temperature: Temperature for generation
            max_tokens: Maximum tokens to generate
            session_id: Optional session ID for logging
            
        Returns:
            str: The generated response or error message
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
        
        # Add system message if provided
        if system_message:
            messages.append({
                "role": "system", 
                "content": system_message
            })
        else:
            # Default system message
            messages.append({
                "role": "system", 
                "content": "You are an assistant that provides clear, accurate, and helpful information."
            })
        
        # Add context if provided
        if context:
            # If context is a string, add directly
            if isinstance(context, str):
                messages.append({
                    "role": "system",
                    "content": f"Additional context: {context}"
                })
            # If context is a dict, format it nicely
            elif isinstance(context, dict):
                context_str = json.dumps(context, indent=2)
                messages.append({
                    "role": "system",
                    "content": f"Additional context (JSON):\n{context_str}"
                })
        
        # Add user prompt
        messages.append({
            "role": "user",
            "content": prompt
        })
        
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
    
    def extract_intent_and_entities(self, message, context=None):
        """
        Extract intent and entities from user message using LLM
        
        Args:
            message: User message
            context: Optional context information
            
        Returns:
            dict: Extracted intent and entities
        """
        try:
            # Prepare system message with enhanced patterns
            system_message = """You are an AI assistant for malaria risk assessment. Extract the user's intent and relevant entities from their message.
            
            Common intents include:
            - run_standard_analysis: User wants to run the standard analysis
            - run_custom_analysis: User wants to run analysis with specific variables
            - request_visualization: User wants to create or view visualizations
            - explain_methodology: User wants explanation about methodology
            - explain_variable: User wants explanation about specific variables
            - explain_variable_category: User wants explanation about variable categories
            - explain_variable_selection: User wants explanation about why variables were selected
            - query_analysis_details: User wants details about the completed analysis
            - viz_followup_question: User is asking follow-up about a visualization
            - generate_report: User wants to generate a report
            - change_language: User wants to change language
            - greet: User is greeting the system
            - goodbye: User is saying goodbye
            - clarification_needed: User's request is unclear
            - request_elaboration: User wants more details
            - confirm_custom_analysis: User is confirming custom analysis
            - cancel_custom_analysis: User is canceling custom analysis
            
            # NEW MALARIA-SPECIFIC INTENTS:
            - query_malaria_transmission: User is asking about malaria transmission
            - query_malaria_prevention: User is asking about malaria prevention methods
            - query_malaria_treatment: User is asking about malaria treatment
            - query_malaria_risk_factors: User is asking about risk factors for malaria
            - query_malaria_statistics: User is asking for statistics about malaria
            - query_malaria_interventions: User is asking about intervention strategies
            - query_geographic_patterns: User is asking about geographic patterns of malaria
            - query_climate_effects: User is asking about climate and malaria
            - query_seasonal_trends: User is asking about seasonal trends of malaria
            - query_vulnerable_populations: User is asking which populations are most vulnerable
            
            Return a simple JSON with just two keys:
            {
              "intent": "one_of_the_intents_listed_above",
              "entities": {
                "key1": "value1",
                "key2": "value2"
              }
            }
            
            If you can't determine the intent, use "clarification_needed".
            For entities, only include keys that are present, such as:
            - variable_name: specific variable mentioned
            - variable_names: list of variables for custom analysis
            - variable_category: category of variables
            - visualization_type: general type (map, plot)
            - map_type: specific map type (variable, composite, etc)
            - plot_type: specific plot type
            - variable_for_viz: variable for visualization
            - threshold_value: threshold value for viz
            - methodology_type: type of methodology
            - report_format: format for report generation
            - language_code: language code for changing language
            - specific_region: name of a specific region or location
            - time_period: specific time period mentioned
            - population_group: specific population group mentioned
            - intervention_type: specific intervention type mentioned
            - malaria_species: specific species of malaria parasite mentioned
            """
            
            # Make sure context is a valid dictionary
            if context is None:
                context = {}
            elif not isinstance(context, dict):
                # Try to convert to dictionary if possible
                try:
                    context = dict(context)
                except:
                    context = {"original_context": str(context)}
            
            # Ensure context is serializable
            serializable_context = convert_to_json_serializable(context)
            
            # Generate response with intent extraction
            user_prompt = f"Extract the intent and entities from this message: \"{message}\""
            if serializable_context:
                user_prompt += f"\nContext: {json.dumps(serializable_context)}"
            
            # Set a short timeout for this response to prevent long waits
            response_text = self.generate_response(
                prompt=user_prompt,
                system_message=system_message,
                temperature=0.3,  # Lower temperature for consistent structured output
                max_tokens=300,   # Keep response short
                session_id=None   # Don't log this system interaction
            )
            
            # Parse the JSON response
            # First, try to find JSON pattern in the response text
            import re
            json_pattern = r'```(?:json)?\s*(\{[\s\S]*?\})\s*```'
            match = re.search(json_pattern, response_text)
            
            if match:
                json_str = match.group(1)
            else:
                # If no JSON code block, try to extract anything that looks like JSON
                json_pattern = r'\{[\s\S]*?\}'
                match = re.search(json_pattern, response_text)
                if match:
                    json_str = match.group(0)
                else:
                    json_str = response_text
            
            try:
                # Clean the string by removing any non-JSON content
                json_str = json_str.strip()
                result = json.loads(json_str)
                
                # Ensure we have the expected structure
                if not isinstance(result, dict):
                    raise ValueError("Response is not a dictionary")
                
                if "intent" not in result:
                    raise ValueError("Response does not contain 'intent' key")
                
                if "entities" not in result:
                    result["entities"] = {}
                
                return result
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response: {e}. Response: {response_text}")
                # Determine if it might be a greeting based on simple keyword matching
                user_message_lower = message.lower()
                if any(greeting in user_message_lower for greeting in ['hello', 'hi', 'hey', 'greetings']):
                    return {
                        "intent": "greet",
                        "entities": {}
                }
            
            return {
                    "intent": "clarification_needed",
                    "entities": {}
                }
        except Exception as e:
            logger.error(f"Error processing response: {e}")
            return {
                "intent": "clarification_needed",
                "entities": {}
            }
        
        except Exception as e:
            logger.error(f"Error extracting intent: {str(e)}", exc_info=True)
            # Try to detect basic intents without LLM
            user_message_lower = message.lower()
            
            # Simple rule-based intent detection for common cases
            if any(greeting in user_message_lower for greeting in ['hello', 'hi', 'hey', 'greetings']):
                return {
                    "intent": "greet",
                    "entities": {}
                }
            elif any(goodbye in user_message_lower for goodbye in ['bye', 'goodbye', 'see you']):
                return {
                    "intent": "goodbye",
                    "entities": {}
                }
            elif 'help' in user_message_lower:
                return {
                    "intent": "request_elaboration",
                    "entities": {}
                }
            # Rule-based detection for malaria-specific topics if LLM fails
            elif any(term in user_message_lower for term in ['transmission', 'spread', 'transmit']):
                return {
                    "intent": "query_malaria_transmission",
                    "entities": {}
                }
            elif any(term in user_message_lower for term in ['prevent', 'prevention', 'avoid']):
                return {
                    "intent": "query_malaria_prevention",
                    "entities": {}
                }
            elif any(term in user_message_lower for term in ['treat', 'treatment', 'cure', 'medication']):
                return {
                    "intent": "query_malaria_treatment",
                    "entities": {}
                }
            elif any(term in user_message_lower for term in ['risk', 'factor', 'cause']):
                return {
                    "intent": "query_malaria_risk_factors",
                    "entities": {}
                }
            else:
                return {
                    "intent": "clarification_needed",
                    "entities": {}
                }
    
    def explain_ward(self, session_id, ward_name, question=None, context=None):
        """
        Generate an explanation for a ward's ranking or characteristics
        
        Args:
            session_id: Session identifier
            ward_name: Name of the ward to explain
            question: Optional specific question about the ward
            context: Additional context information
            
        Returns:
            str: Explanation text
        """
        # Set up default question if none provided
        if not question:
            question = f"Please explain the vulnerability ranking of {ward_name}."
        
        # Get context from the interaction logger if available
        if self.interaction_logger and not context:
            try:
                context = self.interaction_logger.generate_explanation_context(
                    session_id, 'ward', ward_name, question
                )
            except Exception as e:
                logger.error(f"Error getting ward context: {str(e)}")
                # Continue with available context
        
        # Construct prompt for the LLM
        system_message = f"""
        You are an expert in malaria epidemiology and risk analysis explaining results from the Malaria Reprioritization Tool (MRPT).
        
        When explaining a ward's vulnerability ranking, include:
        1. The ward's rank and vulnerability category
        2. Key factors contributing to this ranking
        3. How this ward compares to others
        4. Any anomalies or special designations (like "not ideal")
        5. What this ranking means for malaria interventions
        
        If the ward has a "not ideal" designation, explain that this means it's a relatively high-risk ward that falls below the urban threshold,
        which may present logistical challenges for urban-focused interventions.
        
        Provide a clear, informative explanation based on the data provided in the context.
        """
        
        # Generate the explanation
        explanation = self.generate_response(
            prompt=question,
            context=context,
            system_message=system_message,
            temperature=0.4,  # Lower temperature for factual responses
            max_tokens=800,
            session_id=session_id
        )
        
        # Log the explanation if interaction logger is available
        if self.interaction_logger:
            self.interaction_logger.log_explanation(
                session_id=session_id,
                entity_type='ward',
                entity_name=ward_name,
                question_type='ward_ranking' if 'ranking' in question.lower() else 'ward_general',
                question=question,
                explanation=explanation,
                context_used=context
            )
        
        return explanation
    
    def explain_variable(self, session_id, variable_name, question=None, context=None):
        """
        Generate an explanation for a variable's relationship with malaria risk
        
        Args:
            session_id: Session identifier
            variable_name: Name of the variable to explain
            question: Optional specific question about the variable
            context: Additional context information
            
        Returns:
            str: Explanation text
        """
        # Set up default question if none provided
        if not question:
            question = f"Please explain how {variable_name} relates to malaria risk."
        
        # Get context from the interaction logger if available
        if self.interaction_logger and not context:
            try:
                context = self.interaction_logger.generate_explanation_context(
                    session_id, 'variable', variable_name, question
                )
            except Exception as e:
                logger.error(f"Error getting variable context: {str(e)}")
                # Continue with available context
        
        # Construct prompt for the LLM
        system_message = f"""
        You are an expert in malaria epidemiology and risk analysis explaining variables from the Malaria Reprioritization Tool (MRPT).
        
        When explaining a variable's relationship with malaria risk, include:
        1. What this variable measures
        2. Whether it has a direct or inverse relationship with malaria risk
        3. The epidemiological reasoning behind this relationship
        4. How this variable interacts with other factors
        5. Any limitations or caveats about this variable
        
        Provide a clear, scientifically accurate explanation based on malaria epidemiology and the data provided in the context.
        """
        
        # Generate the explanation
        explanation = self.generate_response(
            prompt=question,
            context=context,
            system_message=system_message,
            temperature=0.3,  # Lower temperature for scientific accuracy
            max_tokens=800,
            session_id=session_id
        )
        
        # Log the explanation if interaction logger is available
        if self.interaction_logger:
            self.interaction_logger.log_explanation(
                session_id=session_id,
                entity_type='variable',
                entity_name=variable_name,
                question_type='variable_relationship' if 'relate' in question.lower() else 'variable_general',
                question=question,
                explanation=explanation,
                context_used=context
            )
        
        return explanation
    
    # Add a method to the LLMManager class to generate structured responses
    def generate_structured_response(self, prompt, context=None, output_format="json"):
        """
        Generate a structured response (like JSON) from the LLM
        
        Args:
            prompt: Prompt for the LLM
            context: Optional context to provide to the LLM
            output_format: The desired output format (default: "json")
            
        Returns:
            dict or str: The structured response
        """
        try:
            import json
            import re
            
            # Enhance the prompt to request structured output
            structured_prompt = f"{prompt}\n\nProvide your response in {output_format.upper()} format."
            
            # Generate response
            response = self.generate_response(
                prompt=structured_prompt,
                context=context
            )
            
            # Extract and parse the structured part
            if output_format.lower() == "json":
                # Try to extract a JSON object from the response
                try:
                    # Look for patterns like ```json {...} ``` or just {...}
                    json_pattern = r'```(?:json)?\s*(\{[\s\S]*?\})\s*```'
                    match = re.search(json_pattern, response)
                    
                    if match:
                        json_str = match.group(1)
                    else:
                        # Try to find a JSON object directly
                        json_pattern = r'(\{[\s\S]*\})'
                        match = re.search(json_pattern, response)
                        if match:
                            json_str = match.group(1)
                        else:
                            # No JSON found, return the original response
                            return response
                    
                    # Parse the JSON string
                    return json.loads(json_str)
                except Exception as e:
                    self.logger.error(f"Error parsing JSON from LLM response: {str(e)}")
                    return response
            
            # Return the original response for other formats
            return response
        except Exception as e:
            self.logger.error(f"Error generating structured response: {str(e)}", exc_info=True)
            return {"error": str(e)}

    
    def explain_visualization(self, session_id, viz_type, context=None, question=None):
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
        
        # Get context from the interaction logger if available
        if self.interaction_logger and not context:
            try:
                context = self.interaction_logger.generate_explanation_context(
                    session_id, 'visualization', viz_type, question
                )
            except Exception as e:
                logger.error(f"Error getting visualization context: {str(e)}")
                # Continue with available context
        
        # --- SANITIZE CONTEXT FOR JSON SERIALIZATION ---
        if context:
            # Use the convert_to_json_serializable from app.utilities already imported at the top
            context = convert_to_json_serializable(context)
        
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
        
        # Log the explanation if interaction logger is available
        if self.interaction_logger:
            self.interaction_logger.log_explanation(
                session_id=session_id,
                entity_type='visualization',
                entity_name=viz_type,
                question_type='visualization_interpretation',
                question=question,
                explanation=explanation,
                context_used=context
            )
        
        return explanation
    
    def explain_methodology(self, session_id, methodology_type, question=None, context=None):
        """
        Generate an explanation for analysis methodology
        
        Args:
            session_id: Session identifier
            methodology_type: Type of methodology ('normalization', 'composite_scores', etc.)
            question: Optional specific question about the methodology
            context: Additional context information
            
        Returns:
            str: Explanation text
        """
        # Set up default question if none provided
        if not question:
            question = f"Please explain the {methodology_type} methodology used in the analysis."
        
        # Get context from the analysis metadata if possible and not provided
        if not context and self.interaction_logger:
            try:
                metadata = self.interaction_logger.get_analysis_metadata(session_id)
                
                # Find steps related to this methodology
                relevant_steps = []
                for step in metadata.get('steps', []):
                    if methodology_type.lower() in step.get('step_name', '').lower():
                        relevant_steps.append(step)
                
                if relevant_steps:
                    context = {
                        'methodology_type': methodology_type,
                        'relevant_steps': relevant_steps
                    }
            except Exception as e:
                logger.error(f"Error getting methodology context: {str(e)}")
                # Continue with available context
        
        # Construct prompt for the LLM
        system_message = f"""
        You are an expert in data analysis and malaria epidemiology explaining the methodologies used in the Malaria Reprioritization Tool (MRPT).
        
        When explaining a methodology, include:
        1. The purpose of this methodology step
        2. How it works in technical but accessible terms
        3. Why this approach was chosen over alternatives
        4. How it contributes to accurate risk assessment
        5. Any limitations or assumptions involved
        
        For different methodology types:
        - data_cleaning: Explains handling missing values through spatial imputation, mean imputation, mode imputation, etc.
        - normalization: Describes converting variables to 0-1 scale based on direct/inverse relationships with risk
        - composite_scores: Details how multiple variables are combined to create risk models
        - vulnerability_ranking: Explains how wards are ranked based on median scores and categorized
        - urban_extent: Describes classification based on urban percentage thresholds
        
        Provide a clear, technically accurate explanation based on the specific methodology and any context provided.
        """
        
        # Generate the explanation
        explanation = self.generate_response(
            prompt=question,
            context=context,
            system_message=system_message,
            temperature=0.3,  # Lower temperature for technical accuracy
            max_tokens=1000,
            session_id=session_id
        )
        
        # Log the explanation if interaction logger is available
        if self.interaction_logger:
            self.interaction_logger.log_explanation(
                session_id=session_id,
                entity_type='methodology',
                entity_name=methodology_type,
                question_type='methodology_explanation',
                question=question,
                explanation=explanation,
                context_used=context
            )
        
        return explanation
    
    def generate_general_response(self, session_id, message, context=None):
        """
        Generate a general response for user messages
        
        Args:
            session_id: Session identifier
            message: User message
            context: Additional context
            
        Returns:
            str: Response text
        """
        # Construct a system message that encourages helpful responses
        system_message = """
        You are an AI assistant for a Malaria Risk Analysis Tool. You help users understand malaria risk factors, 
        interpret analysis results, and use the tool effectively.
        
        Provide clear, concise, and helpful responses based on your knowledge of:
        - Malaria epidemiology and risk factors
        - Data analysis and visualization methods
        - Geographic analysis and spatial patterns
        - Public health intervention planning
        
        If the user asks about a specific ward, variable, visualization, or methodology, try to provide
        a detailed and accurate explanation based on the available context.
        
        If you don't have specific information about something mentioned by the user, acknowledge this
        and suggest how they might find that information using the tool.
        
        Your goal is to help users make informed decisions about malaria interventions based on data analysis.
        """
        
        # Get session info from interaction logger if available
        if self.interaction_logger and not context:
            try:
                # Check if analysis is complete
                session_info = {
                    'messages': self.interaction_logger.get_session_history(session_id)[-5:],  # Last 5 messages
                }
                
                # Get last visualization context if available
                viz_contexts = self.interaction_logger.get_visualization_context(session_id)
                if viz_contexts:
                    session_info['last_visualization'] = viz_contexts[0]
                
                context = session_info
            except Exception as e:
                logger.error(f"Error getting session context: {str(e)}")
                # Continue with available context
        
        # Generate the response
        response = self.generate_response(
            prompt=message,
            context=context,
            system_message=system_message,
            temperature=0.7,  # Higher temperature for conversational responses
            max_tokens=800,
            session_id=session_id
        )
        
        return response
    
    def get_system_context(self):
        """
        Get system context for malaria risk assessment
        
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
                    'Incidence rate (new cases per population)',
                    'Prevalence (existing cases per population)',
                    'Test positivity rate (TPR)',
                    'Mortality rate (deaths per population)',
                    'Vulnerability index (composite risk score)',
                    'Intervention coverage rates',
                    'Resource allocation efficiency',
                    'Case reporting rates',
                    'Treatment access and completion rates',
                    'Vector density and resistance status'
                ],
                'parasite_species': [
                    'Plasmodium falciparum (most severe, highest mortality)',
                    'Plasmodium vivax (can cause relapses)',
                    'Plasmodium ovale (similar to vivax)',
                    'Plasmodium malariae (can persist asymptomatically)',
                    'Plasmodium knowlesi (zoonotic, can cause severe disease)'
                ],
                'vulnerable_groups': [
                    'Children under 5 years',
                    'Pregnant women',
                    'People with HIV/AIDS',
                    'Migrants and mobile populations',
                    'Refugees and displaced persons',
                    'Remote rural communities',
                    'Urban poor in informal settlements',
                    'Travelers from non-endemic areas',
                    'Outdoor workers (farmers, miners, etc.)',
                    'People without access to healthcare'
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

    def get_context_for_analysis(self, analysis_type, context=None):
        """
        Get context for specific analysis type
        
        Args:
            analysis_type: Type of analysis
            context: Additional context information
            
        Returns:
            dict: Analysis-specific context
        """
        system_context = self.get_system_context()
        
        # Map analysis types to relevant context
        analysis_contexts = {
            'risk_assessment': {
                'focus': 'Evaluating malaria risk factors and vulnerability',
                'methods': system_context['analysis_methods']['multivariate_analysis'],
                'metrics': [m for m in system_context['key_concepts']['metrics'] 
                           if 'risk' in m.lower() or 'vulnerability' in m.lower()],
                'outputs': ['Risk maps', 'Vulnerability indices', 'Risk factor analysis']
            },
            'intervention_planning': {
                'focus': 'Planning and optimizing malaria interventions',
                'methods': system_context['analysis_methods']['spatial_analysis'],
                'metrics': [m for m in system_context['key_concepts']['metrics'] 
                           if 'coverage' in m.lower() or 'efficiency' in m.lower()],
                'outputs': ['Intervention maps', 'Resource allocation plans', 'Coverage analysis']
            },
            'progress_tracking': {
                'focus': 'Monitoring malaria control progress',
                'methods': system_context['analysis_methods']['temporal_analysis'],
                'metrics': [m for m in system_context['key_concepts']['metrics'] 
                           if 'rate' in m.lower() or 'prevalence' in m.lower()],
                'outputs': ['Trend visualizations', 'Progress reports', 'Impact assessment']
            }
        }
        
        # Get base context for analysis type
        base_context = analysis_contexts.get(analysis_type, {})
        
        # Merge with additional context
        if context:
            base_context.update(context)
        
        return {
            'analysis_type': analysis_type,
            'system_context': system_context,
            'analysis_context': base_context
        }

    def get_explanation_context(self, topic, context=None):
        """
        Get context for explaining malaria-related topics
        
        Args:
            topic: Topic to explain
            context: Additional context information
            
        Returns:
            dict: Explanation context
        """
        system_context = self.get_system_context()
        
        # Map topics to relevant context
        topic_contexts = {
            'risk_factors': {
                'description': 'Factors contributing to malaria risk',
                'key_points': system_context['key_concepts']['risk_factors'],
                'related_metrics': [m for m in system_context['key_concepts']['metrics'] 
                                  if 'risk' in m.lower()],
                'analysis_methods': system_context['analysis_methods']['multivariate_analysis']
            },
            'interventions': {
                'description': 'Malaria control interventions',
                'key_points': system_context['key_concepts']['interventions'],
                'related_metrics': [m for m in system_context['key_concepts']['metrics'] 
                                  if 'coverage' in m.lower()],
                'analysis_methods': system_context['analysis_methods']['spatial_analysis']
            },
            'vulnerability': {
                'description': 'Vulnerability assessment methodology',
                'key_points': [
                    'Risk factor identification',
                    'Vulnerability scoring',
                    'Spatial analysis',
                    'Resource allocation'
                ],
                'related_metrics': [m for m in system_context['key_concepts']['metrics'] 
                                  if 'vulnerability' in m.lower()],
                'analysis_methods': system_context['analysis_methods']['multivariate_analysis']
            }
        }
        
        # Get base context for topic
        base_context = topic_contexts.get(topic, {})
        
        # Merge with additional context
        if context:
            base_context.update(context)
        
        return {
            'topic': topic,
            'system_context': system_context,
            'topic_context': base_context
        }


class PromptTemplates:
    """
    Collection of prompt templates for different explanation types
    """
    
    @staticmethod
    def ward_explanation(ward_name, ward_data=None):
        """Generate a prompt template for ward explanations"""
        prompt = f"Please explain the vulnerability ranking and characteristics of {ward_name}."
        
        if ward_data:
            # Add details if available
            rank = ward_data.get('overall_rank')
            category = ward_data.get('vulnerability_category')
            
            if rank and category:
                prompt = f"Please explain why {ward_name} is ranked #{rank} and categorized as {category} vulnerability."
            
            # Add specific question if it's flagged
            if ward_data.get('is_not_ideal'):
                prompt = f"Please explain why {ward_name} is flagged as 'not ideal' and what this means for intervention planning."
        
        return prompt
    
    @staticmethod
    def variable_explanation(variable_name, relationship_type=None):
        """Generate a prompt template for variable explanations"""
        if relationship_type:
            return f"Please explain how {variable_name} relates to malaria risk with a {relationship_type} relationship."
        else:
            return f"Please explain how {variable_name} influences malaria transmission and risk."
    
    @staticmethod
    def visualization_explanation(viz_type, data_summary=None):
        """Generate a prompt template for visualization explanations"""
        base_prompt = f"Please explain this {viz_type.replace('_', ' ')} visualization."
        
        if data_summary:
            if viz_type == 'variable_map' and 'variable' in data_summary:
                return f"Please explain this map showing the distribution of {data_summary['variable']}."
            elif viz_type == 'composite_map' and 'model_count' in data_summary:
                return f"Please explain this composite map showing {data_summary['model_count']} different risk models."
            elif viz_type == 'vulnerability_plot':
                return "Please explain this vulnerability ranking plot and how to interpret the box and whisker elements."
            elif viz_type == 'urban_extent_map' and 'threshold' in data_summary:
                return f"Please explain this urban extent map with a {data_summary['threshold']}% threshold."
        
        return base_prompt
    
    @staticmethod
    def methodology_explanation(methodology_type):
        """Generate a prompt template for methodology explanations"""
        explanations = {
            'data_cleaning': "Please explain how missing values are handled in the data cleaning process.",
            'normalization': "Please explain how variables are normalized to account for direct and inverse relationships with malaria risk.",
            'composite_scores': "Please explain how composite risk scores are calculated using multiple variables.",
            'vulnerability_ranking': "Please explain how wards are ranked by vulnerability and categorized into risk levels.",
            'urban_extent': "Please explain how urban extent analysis classifies areas based on urban percentage thresholds."
        }
        
        return explanations.get(methodology_type, f"Please explain the {methodology_type} methodology used in the analysis.")


def get_llm_manager(interaction_logger=None):
    """Factory function to get LLM Manager instance with config from app"""
    api_key = current_app.config.get('OPENAI_API_KEY')
    return LLMManager(api_key=api_key, interaction_logger=interaction_logger)


def classify_question(question):
    """
    Classify the question type to determine appropriate explanation strategy
    
    Args:
        question: User question text
        
    Returns:
        dict: Classification with type and target
    """
    question_lower = question.lower()
    
    # Check for ward-specific questions
    ward_match = re.search(r'(?:ward|area|region|district)\s+([a-z0-9_]+)', question_lower)
    if ward_match or 'not ideal' in question_lower:
        return {
            'type': 'ward_explanation',
            'target': ward_match.group(1) if ward_match else None,
            'subtype': 'not_ideal' if 'not ideal' in question_lower else 'general'
        }
    
    # Check for variable-specific questions
    if any(word in question_lower for word in ['variable', 'factor', 'predictor', 'parameter']):
        var_matches = re.findall(r'(?:the|about|for|of)\s+([a-z0-9_]+)', question_lower)
        if var_matches:
            return {
                'type': 'variable_explanation',
                'target': var_matches[-1],  # Take the last match as it's likely the most specific
                'subtype': 'relationship' if 'relate' in question_lower or 'relationship' in question_lower else 'general'
            }
    
    # Check for visualization questions
    if any(word in question_lower for word in ['map', 'plot', 'chart', 'visualization', 'figure']):
        viz_type = None
        if 'composite' in question_lower:
            viz_type = 'composite_map'
        elif 'vulnerability' in question_lower:
            viz_type = 'vulnerability_plot' if 'plot' in question_lower else 'vulnerability_map'
        elif 'urban' in question_lower or 'extent' in question_lower:
            viz_type = 'urban_extent_map'
        elif 'variable' in question_lower:
            viz_type = 'variable_map'
        
        return {
            'type': 'visualization_explanation',
            'target': viz_type,
            'subtype': 'interpretation'
        }
    
    # Check for methodology questions
    if any(word in question_lower for word in ['methodology', 'method', 'process', 'procedure', 'analysis']):
        method_type = None
        if 'clean' in question_lower or 'missing' in question_lower:
            method_type = 'data_cleaning'
        elif 'normal' in question_lower:
            method_type = 'normalization'
        elif 'compos' in question_lower or 'score' in question_lower:
            method_type = 'composite_scores'
        elif 'rank' in question_lower or 'vulnerab' in question_lower:
            method_type = 'vulnerability_ranking'
        elif 'urban' in question_lower or 'extent' in question_lower:
            method_type = 'urban_extent'
        
        return {
            'type': 'methodology_explanation',
            'target': method_type,
            'subtype': 'process' if 'how' in question_lower else 'rationale'
        }
    
    # Default to general query
    return {
        'type': 'general_query',
        'target': None,
        'subtype': 'general'
    }


# Function to add to ai_utils.py
def select_optimal_variables_with_llm(llm_manager, available_vars, csv_data, relationships=None, min_vars=3, max_vars=5):
    """
    Use LLM to select the optimal variables for malaria risk analysis.
    
    Args:
        llm_manager: LLM manager instance
        available_vars: List of available variables
        csv_data: DataFrame with the raw data for context
        relationships: Optional dictionary mapping variables to relationships (direct/inverse)
        min_vars: Minimum number of variables to select (default: 3)
        max_vars: Maximum number of variables to select (default: 5)
        
    Returns:
        tuple: (selected_variables, explanations)
    """
    import logging
    import pandas as pd
    logger = logging.getLogger(__name__)
    
    try:
        if not available_vars or not llm_manager:
            logger.warning("Missing required parameters for variable selection with LLM")
            return [], {}
        
        # Create statistics about each variable for context
        var_stats = {}
        for var in available_vars:
            if var in csv_data.columns:
                # Get basic statistics
                stats = {}
                if pd.api.types.is_numeric_dtype(csv_data[var]):
                    series = csv_data[var].dropna()
                    stats = {
                        'min': float(series.min()) if not series.empty else None,
                        'max': float(series.max()) if not series.empty else None,
                        'mean': float(series.mean()) if not series.empty else None,
                        'std': float(series.std()) if not series.empty else None,
                        'missing': int(csv_data[var].isna().sum()),
                        'missing_percent': float(csv_data[var].isna().sum() / len(csv_data) * 100),
                        'type': 'numeric'
                    }
                else:
                    # For categorical variables
                    stats = {
                        'unique_values': int(csv_data[var].nunique()),
                        'most_common': str(csv_data[var].value_counts().index[0]) if not csv_data[var].value_counts().empty else None,
                        'missing': int(csv_data[var].isna().sum()),
                        'missing_percent': float(csv_data[var].isna().sum() / len(csv_data) * 100),
                        'type': 'categorical'
                    }
                
                # Add relationship information if available
                if relationships and var in relationships:
                    stats['relationship'] = relationships[var]
                
                var_stats[var] = stats
        
        # Create context for LLM
        context = {
            'available_variables': available_vars,
            'variable_statistics': var_stats,
            'min_variables': min_vars,
            'max_variables': max_vars,
            'selection_criteria': [
                "Choose variables that are most relevant to malaria risk factors",
                "Include variables with different aspects of risk (e.g., environmental, demographic)",
                "Prefer variables with lower missing data percentages",
                "Include Test Positivity Rate (TPR) if available",
                "Prefer variables that directly measure disease indicators"
            ]
        }
        
        # Create prompt for the LLM
        prompt = f"""
        Select the optimal {min_vars}-{max_vars} variables for calculating malaria risk composite scores.
        The selection should include the Test Positivity Rate (TPR) if it's available.
        
        For each selected variable, provide a brief explanation for why it was selected.
        
        Format your response as a JSON object with:
        1. "selected_variables": [list of selected variable names]
        2. "explanations": {{variable_name: explanation for selection}}
        """
        
        # Call the LLM to select variables
        response = llm_manager.generate_structured_response(
            prompt=prompt,
            context=context,
            output_format="json"
        )
        
        # Process the response - initialize defaults
        selected_vars = []
        explanations = {}
        
        # Extract data if present in proper format
        if isinstance(response, dict) and 'selected_variables' in response:
            selected_vars = response['selected_variables']
            explanations = response.get('explanations', {})
        
        # Validate the selected variables exist in the available variables
        valid_vars = [var for var in selected_vars if var in available_vars]
        
        # Ensure we have the minimum number of variables
        if len(valid_vars) < min_vars:
            # Add more variables if we don't have the minimum
            remaining_vars = [var for var in available_vars if var not in valid_vars]
            # Sort by missing percentage (ascending)
            remaining_vars_sorted = sorted(
                remaining_vars, 
                key=lambda var: var_stats.get(var, {}).get('missing_percent', 100)
            )
            # Add until we reach the minimum
            while len(valid_vars) < min_vars and remaining_vars_sorted:
                next_var = remaining_vars_sorted.pop(0)
                valid_vars.append(next_var)
                explanations[next_var] = f"Added to ensure minimum number of variables for analysis."
        
        # Ensure TPR is included if available
        tpr_vars = [var for var in available_vars if 'tpr' in var.lower()]
        if tpr_vars and not any(tpr_var in valid_vars for tpr_var in tpr_vars):
            # Add the first TPR variable and remove the last variable if necessary
            if len(valid_vars) >= max_vars:
                removed_var = valid_vars.pop()
                logger.info(f"Removed {removed_var} to make room for TPR variable")
            tpr_var = tpr_vars[0]
            valid_vars.insert(0, tpr_var)  # Add TPR as first variable
            explanations[tpr_var] = "Test Positivity Rate (TPR) is a direct measure of malaria prevalence and essential for risk assessment."
        
        # Log the selection
        logger.info(f"LLM selected {len(valid_vars)} variables for composite score: {valid_vars}")
        for var in valid_vars:
            if var in explanations:
                logger.info(f"  - {var}: {explanations[var]}")
            else:
                logger.info(f"  - {var}: Selected based on relevance to malaria risk")
                explanations[var] = "Selected based on relevance to malaria risk."
        
        return valid_vars, explanations
    except Exception as e:
        logger.error(f"Error in select_optimal_variables_with_llm: {str(e)}", exc_info=True)
        return [], {}


# Method to explain variable selection for the LLMManager class
def explain_variable_selection(self, variables, explanations, context=None, session_id=None):
    """
    Generate an explanation for why specific variables were selected for analysis
    
    Args:
        variables: List of selected variables
        explanations: Dictionary mapping variables to explanations
        context: Additional context for explanation
        session_id: Optional session ID for logging
        
    Returns:
        str: Explanation text
    """
    try:
        # Create prompt
        prompt = f"""
        Explain why the following variables were selected for the malaria risk analysis:
        {', '.join(variables)}
        
        For each variable, provide a clear explanation of its relationship to malaria risk 
        and why it's valuable for the analysis.
        """
        
        # Combine with the explanations
        explanation_context = {
            'selected_variables': variables,
            'variable_explanations': explanations,
            'additional_context': context or {}
        }
        
        # Generate response
        response = self.generate_response(
            prompt=prompt,
            context=explanation_context,
            session_id=session_id
        )
        
        return response
    except Exception as e:
        self.logger.error(f"Error explaining variable selection: {str(e)}", exc_info=True)
        return f"Unable to generate explanation due to an error. The system selected {len(variables)} variables for analysis based on their relevance to malaria risk patterns."
    

# Add to ai_utils.py to enhance the extract_intent_and_entities function

# This would be integrated into the existing extract_intent_and_entities function
# or used by it to better recognize variable selection explanation requests

def is_variable_selection_question(message):
    """
    Determine if a message is asking about variable selection
    
    Args:
        message: User message text
        
    Returns:
        bool: True if the message is asking about variable selection
    """
    import re
    
    # Convert to lowercase for case-insensitive matching
    message_lower = message.lower()
    
    # Patterns indicating questions about variable selection
    variable_selection_patterns = [
        r"why (?:did you|were these|those|the) variables (?:selected|chosen|picked)",
        r"which variables (?:did you|were) (?:select|choose|pick|use)",
        r"how (?:did you|were the) variables (?:selected|chosen|picked)",
        r"what (?:factors|variables) (?:did you|were) (?:use|used|selected|chosen)",
        r"explain (?:the|your) variable selection",
        r"why (?:these|those) (?:variables|factors)",
        r"why not (?:use|include) (?:other|different|more) variables"
    ]
    
    # Check if any pattern matches
    for pattern in variable_selection_patterns:
        if re.search(pattern, message_lower):
            return True
    
    # Also check for presence of certain keywords
    variable_keywords = ['variable', 'variables', 'factors', 'parameters', 'indicators']
    selection_keywords = ['select', 'choose', 'pick', 'include', 'selection', 'chosen']
    question_words = ['why', 'how', 'what', 'which', 'explain']
    
    # Count how many of each category appear in the message
    var_count = sum(1 for kw in variable_keywords if kw in message_lower)
    sel_count = sum(1 for kw in selection_keywords if kw in message_lower)
    q_count = sum(1 for kw in question_words if kw in message_lower)
    
    # If we have at least one from each category, likely a variable selection question
    if var_count > 0 and sel_count > 0 and q_count > 0:
        return True
    
    return False

def convert_markdown_to_html(markdown_text):
    """
    Convert markdown formatted text to HTML, properly handling all markdown elements.
    
    Args:
        markdown_text: String containing markdown formatting
        
    Returns:
        str: HTML formatted text
    """
    if not markdown_text:
        return ""
    
    # Remove HTML escape chars if present from previous processes
    markdown_text = markdown_text.replace("&lt;", "<").replace("&gt;", ">")
    
    try:
        # Use the markdown library to do the conversion
        # Enable all extensions for proper rendering
        html = markdown.markdown(
            markdown_text,
            extensions=[
                'markdown.extensions.extra',
                'markdown.extensions.nl2br',
                'markdown.extensions.smarty'
            ]
        )
        
        # Clean up any unwanted characters or patterns
        html = html.replace("###", "") # Remove any leftover markdown chars
        
        # Ensure paragraphs are properly formatted
        if not html.strip().startswith('<'):
            # If the HTML doesn't start with a tag, wrap it in a paragraph
            html = f"<p>{html}</p>"
            
        return html
        
    except Exception as e:
        logging.error(f"Error converting markdown to HTML: {str(e)}")
        # Fallback to basic formatting if the conversion fails
        text = markdown_text
        # Basic replacements - paragraphs
        text = "<p>" + text.replace("\n\n", "</p><p>") + "</p>"
        # Basic bold/italic
        text = text.replace("**", "<strong>").replace("**", "</strong>")
        text = text.replace("*", "<em>").replace("*", "</em>")
        # Remove any markdown heading symbols
        text = text.replace("### ", "").replace("## ", "").replace("# ", "")
        return text