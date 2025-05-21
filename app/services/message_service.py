"""
Message service for handling user chat messages and AI responses.
"""
import logging
import json
from functools import lru_cache

logger = logging.getLogger(__name__)

class MessageService:
    """
    Service for handling user messages and AI responses.
    
    This class separates message processing logic from HTTP request handling.
    """
    
    def __init__(self, llm_manager=None, interaction_logger=None, analysis_service=None):
        """
        Initialize the message service with dependencies.
        
        Args:
            llm_manager: LLM manager for AI operations
            interaction_logger: Logger for user interactions
            analysis_service: Service for running analyses
        """
        self.llm_manager = llm_manager
        self.interaction_logger = interaction_logger
        self.analysis_service = analysis_service
    
    def process_message(self, user_message, session_id, session_state, data_handler=None, pending_action=None, pending_variables=None):
        """
        Process a user message and generate an appropriate response.
        
        Args:
            user_message: User's text message
            session_id: User session ID
            session_state: Dictionary with current session state
            data_handler: Data handler with loaded data
            pending_action: Any pending action requiring confirmation
            pending_variables: Variables associated with pending action
            
        Returns:
            dict: Response data including AI response text and action
        """
        if not user_message:
            return {"status": "error", "message": "No message provided"}
        
        # Log the user message
        self._log_user_message(session_id, user_message)
        
        # If LLM manager is not available, return a friendly error
        if not self.llm_manager:
            return {
                "status": "error",
                "response": "I'm sorry, I'm having trouble connecting to my language model. Please try again in a moment.",
                "action": "error"
            }
        
        # Handle pending confirmation if exists
        if pending_action == 'confirm_custom_analysis' and pending_variables:
            return self._handle_custom_analysis_confirmation(
                user_message, 
                session_id, 
                data_handler, 
                pending_variables
            )
        
        # Process message with NLU
        nlu_result = self._process_user_intent(user_message, session_state, session_id)
        if not nlu_result:
            return self._generate_fallback_response(session_id, user_message, session_state)
        
        # Handle the specific intent
        return self._handle_intent(
            nlu_result, 
            session_state, 
            data_handler, 
            session_id
        )
    
    def _log_user_message(self, session_id, message):
        """Log the user message to interaction logger"""
        if self.interaction_logger and session_id:
            self.interaction_logger.log_message(session_id, 'user', message)
    
    def _process_user_intent(self, message, session_state, session_id):
        """Process user message to extract intent"""
        try:
            return self.llm_manager.extract_intent_and_entities(message, session_state)
        except Exception as e:
            logger.error(f"Error extracting intent: {str(e)}", exc_info=True)
            return None
    
    def _generate_fallback_response(self, session_id, message, context):
        """Generate a fallback response when NLU fails"""
        try:
            ai_response = self.llm_manager.generate_general_response(
                session_id, 
                message,
                context=context
            )
            
            # Format response as HTML
            from ..ai_utils import convert_markdown_to_html
            ai_response_html = convert_markdown_to_html(ai_response)
            
            # Log assistant response
            if self.interaction_logger and session_id:
                self.interaction_logger.log_message(session_id, 'assistant', ai_response_html)
            
            return {
                "status": "success",
                "response": ai_response_html
            }
        except Exception as e:
            logger.error(f"Error generating general response: {str(e)}", exc_info=True)
            # Provide a helpful fallback message
            fallback_html = """
            <p>I'm sorry, I'm having trouble processing your request at the moment. Here are some things you can do:</p>
            <ul>
                <li>Make sure you've uploaded both the CSV and shapefile</li>
                <li>Try asking a more specific question</li>
                <li>Check if you need to run an analysis first</li>
            </ul>
            <p>How would you like to proceed?</p>
            """
            return {"status": "success", "response": fallback_html}
    
    def _handle_custom_analysis_confirmation(self, message, session_id, data_handler, pending_variables):
        """Handle confirmation for custom analysis"""
        # Check if the message is a simple yes/no
        if message.lower() in ['yes', 'y', 'yeah', 'yep', 'ok', 'okay', 'sure', 'confirm']:
            confirmed = True
        elif message.lower() in ['no', 'n', 'nope', 'cancel', 'deny']:
            confirmed = False
        else:
            # For more complex responses, use the LLM to determine intent
            try:
                # Extract confirmation intent using LLM
                nlu_result = self.llm_manager.extract_intent_and_entities(
                    message,
                    {}  # Empty context for confirmation
                )
                
                # Determine if user confirmed or denied
                confirmed = nlu_result and nlu_result['intent'] in ['confirm_custom_analysis', 'yes', 'confirm']
            except Exception as e:
                logger.error(f"Error extracting intent for confirmation: {str(e)}", exc_info=True)
                confirmed = False  # Default to no on error
        
        if confirmed:
            logger.info(f"User confirmed custom analysis with variables: {pending_variables}")
            
            # Run the custom analysis through the analysis service
            if self.analysis_service:
                result = self.analysis_service.run_custom_analysis(
                    data_handler, 
                    pending_variables, 
                    message,  # Use message as context
                    session_id
                )
            else:
                # Fallback if analysis service not available
                result = self._legacy_run_custom_analysis(data_handler, pending_variables, message)
            
            # Check the result
            if result.get('status') == 'success':
                return self._generate_analysis_success_response(result, session_id)
            else:
                # Log the error
                if self.interaction_logger and session_id:
                    self.interaction_logger.log_error(
                        session_id,
                        'custom_analysis_error',
                        result.get('message', 'Unknown error')
                    )
                
                return {
                    "status": "error",
                    "response": f"Error running custom analysis: {result.get('message', 'Unknown error')}",
                    "action": "error"
                }
        else:  # User denied
            logger.info("User cancelled custom analysis")
            
            # Let the user know we cancelled
            ai_response = "I've cancelled the custom analysis. Is there something else you'd like to analyze or visualize instead?"
            
            # Format response
            from ..ai_utils import convert_markdown_to_html
            ai_response_html = convert_markdown_to_html(ai_response)
            
            # Log assistant response
            if self.interaction_logger and session_id:
                self.interaction_logger.log_message(session_id, 'assistant', ai_response_html)
            
            return {
                "status": "success", 
                "response": ai_response_html
            }
    
    def _generate_analysis_success_response(self, result, session_id):
        """Generate a response for successful analysis"""
        # Get report data for the response
        variables_used = result.get('variables_used', [])
        analysis_summary = result.get('summary', {})
        
        # Generate a nice response with LLM
        prompt = f"""
        Generate a concise response explaining that the custom analysis with the following variables was successful:
        {', '.join(variables_used)}
        
        Include key findings from the analysis:
        - {len(result.get('high_risk_wards', []))} wards classified as high vulnerability
        - {len(result.get('medium_risk_wards', []))} wards classified as medium vulnerability
        - {len(result.get('low_risk_wards', []))} wards classified as low vulnerability
        
        Suggest next steps for the user, such as:
        1. Exploring the vulnerability map to see the distribution of risk
        2. Checking specific wards of interest
        3. Analyzing individual variables to understand their impact
        """
        
        # Get the system's context to provide rich information
        context = {
            'variables_used': variables_used,
            'analysis_result': analysis_summary,
            'system_context': self.llm_manager.get_system_context(),
        }
        
        # Generate explanation
        ai_response = self.llm_manager.generate_response(
            prompt=prompt,
            context=context,
            session_id=session_id,
            temperature=0.7
        )
        
        # Format response
        from ..ai_utils import convert_markdown_to_html
        ai_response_html = convert_markdown_to_html(ai_response)
        
        # Log assistant response
        if self.interaction_logger and session_id:
            self.interaction_logger.log_message(session_id, 'assistant', ai_response_html)
        
        return {
            "status": "success", 
            "response": ai_response_html,
            "action": "analysis_complete"
        }
    
    def _handle_intent(self, nlu_result, session_state, data_handler, session_id):
        """Route to appropriate handler based on intent"""
        intent = nlu_result['intent']
        entities = nlu_result['entities']
        
        logger.info(f"Handling intent: {intent} with entities: {entities}")
        
        # Map intents to handler methods
        intent_handlers = {
            'run_custom_analysis': self._handle_run_custom_analysis,
            'request_visualization': self._handle_request_visualization,
            'explain_methodology': self._handle_explain_methodology,
            'explain_variable': self._handle_explain_variable,
            'query_malaria_transmission': self._handle_malaria_information,
            'query_malaria_prevention': self._handle_malaria_information,
            'query_malaria_treatment': self._handle_malaria_information,
            'query_malaria_risk_factors': self._handle_malaria_information,
            'greet': self._handle_greeting,
        }
        
        # Use the appropriate handler or default to general response
        handler = intent_handlers.get(intent, self._handle_general_response)
        return handler(entities, session_state, data_handler, session_id)
    
    def _handle_run_custom_analysis(self, entities, session_state, data_handler, session_id):
        """Handle request to run custom analysis"""
        if not all([session_state.get('csv_loaded', False), session_state.get('shapefile_loaded', False)]):
            return {"status": "error", "response": "Please upload both data files first.", "action": "error"}
        
        variables = entities.get('variable_names', [])
        
        if not variables or len(variables) < 2:
            # Generate response explaining the issue
            available_vars = []
            if data_handler:
                available_vars = data_handler.get_available_variables()[:10]  # Get first 10 for examples
                
            ai_response = self.llm_manager.generate_response(
                prompt="Generate a response explaining that at least 2 variables are needed for custom analysis. Include examples of available variables.",
                context={
                    'available_variables': available_vars,
                    'error_type': 'insufficient_variables',
                    'provided_variables': variables
                },
                session_id=session_id
            )
            
            # Format response
            from ..ai_utils import convert_markdown_to_html
            ai_response_html = convert_markdown_to_html(ai_response)
            
            # Log assistant response
            if self.interaction_logger and session_id:
                self.interaction_logger.log_message(session_id, 'assistant', ai_response_html)
            
            return {
                "status": "success", 
                "response": ai_response_html,
                "action": "suggest_variables"
            }
        
        # Get available variables
        available_vars = []
        if data_handler:
            available_vars = data_handler.get_available_variables()
        
        # Create case-insensitive lookup dictionary for fuzzy matching
        available_vars_lookup = {var.lower(): var for var in available_vars}
        
        # Perform fuzzy, case-insensitive variable matching
        valid_variables = []
        invalid_variables = []
        matched_mapping = {}  # Keep track of how user variables were matched
        
        for var in variables:
            var_lower = var.lower()
            if var_lower in available_vars_lookup:
                # Direct case-insensitive match
                valid_variables.append(available_vars_lookup[var_lower])
                matched_mapping[var] = available_vars_lookup[var_lower]
            else:
                # Try partial matching
                matched = False
                for av_lower, av in available_vars_lookup.items():
                    # Check if user's variable is a substring of actual variable or vice versa
                    if var_lower in av_lower or av_lower in var_lower:
                        valid_variables.append(av)
                        matched_mapping[var] = av
                        matched = True
                        logger.info(f"Fuzzy matched '{var}' to '{av}'")
                        break
                
                if not matched:
                    invalid_variables.append(var)
        
        # Remove duplicates while preserving order
        seen = set()
        valid_variables = [x for x in valid_variables if not (x in seen or seen.add(x))]
        
        if len(valid_variables) < 2:
            # Not enough valid variables
            # Find similar variables for suggestions
            suggestions = {}
            for invalid_var in invalid_variables:
                invalid_lower = invalid_var.lower()
                similar_vars = []
                
                for av in available_vars:
                    av_lower = av.lower()
                    # Very basic similarity - check if they share at least 2 characters
                    if len(set(invalid_lower).intersection(set(av_lower))) >= 2:
                        similar_vars.append(av)
                
                suggestions[invalid_var] = similar_vars[:3]  # Limit to top 3 suggestions
            
            ai_response = self.llm_manager.generate_response(
                prompt="Generate a helpful response explaining that the provided variables are not valid. Include examples of available variables and suggestions for similar variables.",
                context={
                    'available_variables': available_vars[:10],
                    'error_type': 'invalid_variables',
                    'provided_variables': variables,
                    'invalid_variables': invalid_variables,
                    'similar_suggestions': suggestions
                },
                session_id=session_id
            )
            
            # Format response
            from ..ai_utils import convert_markdown_to_html
            ai_response_html = convert_markdown_to_html(ai_response)
            
            # Log assistant response
            if self.interaction_logger and session_id:
                self.interaction_logger.log_message(session_id, 'assistant', ai_response_html)
            
            return {
                "status": "success", 
                "response": ai_response_html,
                "action": "suggest_variables"
            }
        
        # We have valid variables, check if any fuzzy matching occurred and inform the user
        has_fuzzy_matches = any(var != matched_mapping.get(var, var) for var in variables if var in matched_mapping)
        
        # Generate confirmation message
        if has_fuzzy_matches:
            # Prepare matching information for variables that were fuzzy matched
            match_info = []
            for var in variables:
                if var in matched_mapping and var != matched_mapping[var]:
                    match_info.append(f"'{var}' → '{matched_mapping[var]}'")
            
            match_text = ", ".join(match_info)
            variable_list = ", ".join(valid_variables)
            confirmation = f"I understood your variable selection and matched: {match_text}.\n\nI'll run a custom analysis using these variables: **{variable_list}**. Would you like to proceed?"
        else:
            variable_list = ", ".join(valid_variables)
            confirmation = f"I'll run a custom analysis using these variables: **{variable_list}**. Would you like to proceed?"
        
        # Format response
        from ..ai_utils import convert_markdown_to_html
        confirmation_html = convert_markdown_to_html(confirmation)
        
        # Log assistant response
        if self.interaction_logger and session_id:
            self.interaction_logger.log_message(session_id, 'assistant', confirmation_html)
        
        return {
            "status": "success", 
            "response": confirmation_html,
            "action": "confirm_custom_analysis",
            "pending_variables": valid_variables
        }
    
    def _handle_request_visualization(self, entities, session_state, data_handler, session_id):
        """Handle request to generate visualization"""
        # Implementation for handling visualization requests
        return self._handle_general_response(entities, session_state, data_handler, session_id)
    
    def _handle_explain_methodology(self, entities, session_state, data_handler, session_id):
        """Handle request to explain methodology"""
        methodology_type = entities.get('methodology_type', 'general')
        
        try:
            explanation = self.llm_manager.explain_methodology(
                session_id, 
                methodology_type, 
                context=session_state
            )
            
            # Format response
            from ..ai_utils import convert_markdown_to_html
            explanation_html = convert_markdown_to_html(explanation)
            
            # Log assistant response
            if self.interaction_logger and session_id:
                self.interaction_logger.log_message(session_id, 'assistant', explanation_html)
            
            return {
                "status": "success",
                "response": explanation_html
            }
        except Exception as e:
            logger.error(f"Error explaining methodology: {str(e)}", exc_info=True)
            return self._handle_general_response(entities, session_state, data_handler, session_id)
    
    def _handle_explain_variable(self, entities, session_state, data_handler, session_id):
        """Handle request to explain a variable"""
        variable_name = entities.get('variable_name')
        
        if not variable_name:
            return self._handle_general_response(entities, session_state, data_handler, session_id)
        
        try:
            explanation = self.llm_manager.explain_variable(
                session_id, 
                variable_name, 
                context=session_state
            )
            
            # Format response
            from ..ai_utils import convert_markdown_to_html
            explanation_html = convert_markdown_to_html(explanation)
            
            # Log assistant response
            if self.interaction_logger and session_id:
                self.interaction_logger.log_message(session_id, 'assistant', explanation_html)
            
            return {
                "status": "success",
                "response": explanation_html
            }
        except Exception as e:
            logger.error(f"Error explaining variable: {str(e)}", exc_info=True)
            return self._handle_general_response(entities, session_state, data_handler, session_id)
    
    def _handle_malaria_information(self, entities, session_state, data_handler, session_id):
        """Handle request for general malaria information"""
        return self._handle_general_response(entities, session_state, data_handler, session_id)
    
    def _handle_greeting(self, entities, session_state, data_handler, session_id):
        """Handle greeting from user"""
        greeting_response = """
        Hello! I'm the Malaria Risk Prioritization Tool (MRPT) assistant. I can help you with:
        
        - Running analysis on malaria risk data
        - Creating visualizations to understand risk patterns
        - Explaining variables and methodology
        - Answering questions about malaria risk factors
        
        To get started, you can upload your data files or use our sample data.
        """
        
        # Format response
        from ..ai_utils import convert_markdown_to_html
        greeting_html = convert_markdown_to_html(greeting_response)
        
        # Log assistant response
        if self.interaction_logger and session_id:
            self.interaction_logger.log_message(session_id, 'assistant', greeting_html)
        
        return {
            "status": "success",
            "response": greeting_html
        }
    
    def _handle_general_response(self, entities, session_state, data_handler, session_id):
        """Handle general responses for other intents"""
        try:
            # Use generate_general_response for anything we don't have a specific handler for
            context = dict(session_state or {})
            
            # Add any entities as context
            if entities:
                context['entities'] = entities
            
            ai_response = self.llm_manager.generate_general_response(
                session_id, 
                "",  # Empty message to force use of context
                context=context
            )
            
            # Format response
            from ..ai_utils import convert_markdown_to_html
            ai_response_html = convert_markdown_to_html(ai_response)
            
            # Log assistant response
            if self.interaction_logger and session_id:
                self.interaction_logger.log_message(session_id, 'assistant', ai_response_html)
            
            return {
                "status": "success",
                "response": ai_response_html
            }
        except Exception as e:
            logger.error(f"Error generating general response: {str(e)}", exc_info=True)
            # Provide a helpful fallback message
            fallback_html = """
            <p>I'm sorry, I don't have enough information to answer that question properly. Could you provide more details?</p>
            """
            return {"status": "success", "response": fallback_html}
    
    def _legacy_run_custom_analysis(self, data_handler, selected_variables, question=None):
        """Legacy method for running custom analysis when analysis service is not available"""
        if not data_handler:
            return {"status": "error", "message": "No data available for analysis"}
            
        if not selected_variables or len(selected_variables) < 2:
            return {"status": "error", "message": "At least 2 variables are required for analysis"}
        
        try:
            # Validate variables
            available_vars = data_handler.get_available_variables()
            valid_variables = [var for var in selected_variables if var in available_vars]
            
            if len(valid_variables) < 2:
                return {
                    "status": "error", 
                    "message": "Not enough valid variables selected"
                }
            
            # Run the analysis using full analysis pipeline
            result = data_handler.run_full_analysis(
                selected_variables=valid_variables,
                llm_manager=self.llm_manager
            )
            
            # Format the response as expected
            if result.get('status') == 'success':
                return {
                    "status": "success",
                    "high_risk_wards": result.get("high_risk_wards", []),
                    "medium_risk_wards": result.get("medium_risk_wards", []),
                    "low_risk_wards": result.get("low_risk_wards", []),
                    "variables_used": valid_variables,
                    "summary": result.get("summary", {})
                }
            else:
                return {
                    "status": "error",
                    "message": result.get("message", "Error running custom analysis")
                }
        except Exception as e:
            logger.error(f"Error running custom analysis: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": f"Error running custom analysis: {str(e)}"
            } 