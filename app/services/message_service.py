"""
Message service for handling user chat messages and AI responses.
"""
import logging
import json
import re
from functools import lru_cache
from datetime import datetime

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
        
        # Import validation service
        from ..data.validation import DataValidator
        self.validation_service = DataValidator(interaction_logger)
    
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
        
        # Handle the intent
        intent = nlu_result.get('intent')
        
        logger.info(f"Processing intent '{intent}' for session {session_id}")
        logger.info(f"NLU result: {nlu_result}")
        
        if intent == 'run_standard_analysis':
            return self._handle_run_standard_analysis(nlu_result, session_state, data_handler, session_id)
        elif intent == 'run_custom_analysis':
            return self._handle_custom_analysis(nlu_result, session_state, data_handler, session_id)
        elif intent in ['generate_report', 'create_report']:
            return self._handle_generate_report(nlu_result, session_state, data_handler, session_id)
        elif intent in ['show_visualization', 'show_map', 'show_plot', 'show_chart', 'request_visualization']:
            return self._handle_show_visualization(nlu_result, session_state, data_handler, session_id)
        elif intent == 'explain_results':
            return self._handle_explain_results(nlu_result, session_state, data_handler, session_id)
        elif intent == 'data_inquiry':
            return self._handle_data_inquiry(nlu_result, session_state, data_handler, session_id)
        else:
            # Use flexible LLM-based response for unrecognized intents
            logger.info(f"Unrecognized intent '{intent}', using LLM fallback")
            return self._handle_unrecognized_intent(user_message, nlu_result, session_state, data_handler, session_id)
    
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
            from ..core.llm_manager import convert_markdown_to_html
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
                    data_handler=data_handler,
                    selected_variables=pending_variables,
                    question=None,
                    session_id=session_id
                )
            else:
                # Fallback if analysis service not available
                result = self._legacy_run_custom_analysis(data_handler, pending_variables, message)
            
            # Check the result
            if result.get('status') == 'success':
                return self._generate_analysis_success_response(result, session_id, data_handler)
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
            from ..core.llm_manager import convert_markdown_to_html
            ai_response_html = convert_markdown_to_html(ai_response)
            
            # Log assistant response
            if self.interaction_logger and session_id:
                self.interaction_logger.log_message(session_id, 'assistant', ai_response_html)
            
            return {
                "status": "success", 
                "response": ai_response_html
            }
    
    def _generate_analysis_success_response(self, result, session_id, data_handler=None):
        """Generate a response for successful analysis with exact legacy format"""
        # Get report data for the response
        variables_used = result.get('variables_used', [])
        
        # DEBUG: Log what we received
        logger.info(f"Generating analysis success response for session {session_id}")
        logger.info(f"Variables used: {variables_used}")
        logger.info(f"data_handler type: {type(data_handler) if data_handler else None}")
        logger.info(f"data_handler has vulnerability_rankings: {hasattr(data_handler, 'vulnerability_rankings') if data_handler else False}")
        
        # Get actual vulnerability counts from data_handler
        high_risk_count = 0
        medium_risk_count = 0
        low_risk_count = 0
        high_risk_wards = []
        
        # Try to get data_handler from parameter first, then from analysis_service or container
        if not data_handler:
            logger.warning("No data_handler provided directly, attempting to retrieve from services")
            try:
                if self.analysis_service and hasattr(self.analysis_service, 'data_service'):
                    data_handler = self.analysis_service.data_service.get_handler(session_id)
                    logger.info(f"Retrieved data_handler from analysis_service: {type(data_handler) if data_handler else None}")
                elif hasattr(self, 'container') and hasattr(self.container, 'data_service'):
                    data_handler = self.container.data_service.get_handler(session_id)
                    logger.info(f"Retrieved data_handler from container: {type(data_handler) if data_handler else None}")
            except Exception as e:
                logger.warning(f"Could not get data_handler for vulnerability counts: {str(e)}")
        
        # Extract vulnerability category counts from data_handler if available
        if data_handler and hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
            rankings = data_handler.vulnerability_rankings
            logger.info(f"vulnerability_rankings shape: {rankings.shape}")
            logger.info(f"vulnerability_rankings columns: {list(rankings.columns)}")
            
            if 'vulnerability_category' in rankings.columns:
                high_risk_df = rankings[rankings['vulnerability_category'] == 'High']
                medium_risk_df = rankings[rankings['vulnerability_category'] == 'Medium']
                low_risk_df = rankings[rankings['vulnerability_category'] == 'Low']
                
                high_risk_count = len(high_risk_df)
                medium_risk_count = len(medium_risk_df)
                low_risk_count = len(low_risk_df)
                
                logger.info(f"Vulnerability counts - High: {high_risk_count}, Medium: {medium_risk_count}, Low: {low_risk_count}")
                
                # Get top 5 high risk wards if available
                if 'WardName' in rankings.columns and not high_risk_df.empty:
                    high_risk_wards = high_risk_df.sort_values('overall_rank')['WardName'].head(5).tolist()
                    logger.info(f"Top high risk wards: {high_risk_wards}")
            else:
                logger.warning("vulnerability_rankings missing 'vulnerability_category' column")
        else:
            logger.warning(f"No vulnerability_rankings available - data_handler: {data_handler is not None}, has_attr: {hasattr(data_handler, 'vulnerability_rankings') if data_handler else False}, is_not_none: {data_handler.vulnerability_rankings is not None if data_handler and hasattr(data_handler, 'vulnerability_rankings') else False}")
        
        # Determine if this was a custom analysis based on selection method
        selection_method = result.get('selection_method', '').lower()
        is_custom = selection_method in ['user_specified', 'custom']
        
        # Use LLM to generate dynamic, context-aware analysis response
        if self.llm_manager:
            try:
                context = {
                    'analysis_type': 'custom' if is_custom else 'standard',
                    'variables_used': variables_used,
                    'high_risk_count': int(high_risk_count) if high_risk_count else 0,
                    'medium_risk_count': int(medium_risk_count) if medium_risk_count else 0,
                    'low_risk_count': int(low_risk_count) if low_risk_count else 0,
                    'high_risk_wards': high_risk_wards[:5],
                    'total_wards': int(high_risk_count + medium_risk_count + low_risk_count) if all([high_risk_count, medium_risk_count, low_risk_count]) else 0,
                    'selection_method': selection_method
                }
                
                prompt = f"""Generate a comprehensive analysis completion response that celebrates the successful completion of {'custom' if is_custom else 'standard'} malaria risk analysis. The response should:

1. **Celebrate the completion** - acknowledge the successful analysis
2. **Highlight key findings** - mention specific numbers like {high_risk_count} high-risk, {medium_risk_count} medium-risk, {low_risk_count} low-risk wards
3. **PROMINENTLY display the variables used** - Create a clear section showing "Variables Used in Analysis: {', '.join(variables_used)}" 
4. **List specific visualizations available** - Be specific about what maps and charts they can now request:
   - "Show composite map" - Overall risk visualization
   - "Show vulnerability map" - Risk classification map
   - "Show vulnerability plot" - Rankings and charts
   - "Show variable map for [variable]" - Individual variable maps
   - "Show decision tree" - Decision analysis
5. **Mention report generation** - "Generate report" for comprehensive analysis
6. **Custom analysis options** - They can run custom analysis with their own variable combinations
7. **{'IMPORTANT for custom analysis: Emphasize that all subsequent visualizations and maps will now be based on this custom analysis using the specified variables, not any previous standard analysis.' if is_custom else ''}**
8. **Be encouraging and actionable**

Context: {context}

Variables analyzed: {variables_used}
High-risk wards (examples): {high_risk_wards[:3] if high_risk_wards else 'N/A'}

IMPORTANT: Make sure to clearly show the variables that were used in the composite score calculation. Format them in a prominent way like:

**🔬 Variables Used in Composite Score Calculation:**
• {variables_used[0] if variables_used else 'Variable 1'}
• {variables_used[1] if len(variables_used) > 1 else 'Variable 2'}
• etc.

{'CRITICAL for custom analysis: Make it clear that any maps or visualizations they request going forward will be based on THIS custom analysis with THESE specific variables.' if is_custom else ''}

Make it conversational and include actual clickable suggestions like "Try saying: 'Show composite map'" or "Ask me: 'Generate a report'"
"""

                ai_response = self.llm_manager.generate_response(
                    prompt=prompt,
                    context=context,
                    system_message="You are an expert malaria epidemiologist providing analysis results to public health officials. Be specific, actionable, and encouraging.",
                    session_id=session_id
                )
                
                # Format as HTML
                from ..core.llm_manager import convert_markdown_to_html
                response_html = convert_markdown_to_html(ai_response)
                
            except Exception as e:
                logger.error(f"Error generating LLM response: {str(e)}")
                # Fallback to legacy format
                response_html = self._generate_legacy_analysis_response(
                    variables_used, high_risk_count, medium_risk_count, low_risk_count, 
                    high_risk_wards, is_custom
                )
        else:
            # Fallback to legacy format
            response_html = self._generate_legacy_analysis_response(
                variables_used, high_risk_count, medium_risk_count, low_risk_count, 
                high_risk_wards, is_custom
            )
        
        # Log assistant response 
        if self.interaction_logger and session_id:
            self.interaction_logger.log_message(session_id, 'assistant', response_html)
        
        return {
            "status": "success", 
            "response": response_html,
            "action": "analysis_complete",
            "session_updates": {
                "analysis_complete": True,
                "analysis_type": "custom" if is_custom else "standard",
                "variables_used": variables_used,
                "high_risk_count": high_risk_count,
                "medium_risk_count": medium_risk_count,
                "low_risk_count": low_risk_count,
                "last_analysis_timestamp": datetime.now().isoformat()
            }
        }
    
    def _generate_legacy_analysis_response(self, variables_used, high_risk_count, medium_risk_count, 
                                         low_risk_count, high_risk_wards, is_custom):
        """Generate legacy-style analysis response as fallback"""
        variables_text = ", ".join(variables_used) if variables_used else "No variables"
        top_wards_text = ", ".join(high_risk_wards) if high_risk_wards else "N/A"
        
        # Create prominent variables section
        variables_section = ""
        if variables_used:
            variables_bullets = "".join([f"<li><strong>{var}</strong></li>" for var in variables_used])
            variables_section = f"""<div style="background: #f8f9fa; padding: 15px; border-left: 4px solid #28a745; margin: 15px 0;">
<h4>🔬 Variables Used in Composite Score Calculation:</h4>
<ul style="margin: 10px 0; padding-left: 20px;">
{variables_bullets}
</ul>
</div>"""
        
        return f"""<p><strong>{'Custom a' if is_custom else 'A'}nalysis completed successfully!</strong></p>
{variables_section}
<p>I've analyzed your data {'with the variables you specified' if is_custom else 'using AI-selected variables'}. Key results:</p>
<ul>
<li><strong>Vulnerability Classification:</strong>
<ul>
<li>{high_risk_count} wards classified as <strong>High</strong> vulnerability</li>
<li>{medium_risk_count} wards classified as <strong>Medium</strong> vulnerability</li>
<li>{low_risk_count} wards classified as <strong>Low</strong> vulnerability</li>
</ul>
</li>
<li><strong>Top 5 High-Risk Wards:</strong> {top_wards_text}</li>
</ul>
{'<div style="background: #e7f3ff; padding: 10px; border-left: 4px solid #2196f3; margin: 10px 0;"><p><strong>📊 Important:</strong> All subsequent visualizations (maps, plots, charts) will now be based on this custom analysis using your specified variables.</p></div>' if is_custom else ''}
<p><strong>🎯 What you can do next:</strong></p>
<ul>
<li>"<strong>Show composite map</strong>" - Overall risk visualization</li>
<li>"<strong>Show vulnerability map</strong>" - Risk classification map</li>
<li>"<strong>Show vulnerability plot</strong>" - Rankings and charts</li>
<li>"<strong>Show variable map for {variables_used[0] if variables_used else 'population'}</strong>" - Individual variable maps</li>
<li>"<strong>Generate report</strong>" - Comprehensive analysis document</li>
<li>"<strong>Run custom analysis with [your variables]</strong>" - Try different variable combinations</li>
</ul>
<p>What would you like to explore first?</p>"""
    
    def _handle_run_standard_analysis(self, entities, session_state, data_handler, session_id):
        """Handle request to run standard analysis"""
        # Check if data is loaded
        if not all([session_state.get('csv_loaded', False), session_state.get('shapefile_loaded', False)]):
            return {
                "status": "error", 
                "response": "Please upload both data files first.", 
                "action": "error"
            }
        
        # Log that we're starting the analysis
        logger.info(f"Starting standard analysis for session: {session_id}")
        
        try:
            # Run the standard analysis through the analysis service
            if self.analysis_service:
                result = self.analysis_service.run_standard_analysis(
                    data_handler, 
                    session_id=session_id
                )
            else:
                # Fallback to legacy method if analysis service not available
                result = self._legacy_run_standard_analysis(data_handler, session_id)
            
            # Check the result
            if result.get('status') == 'success':
                return self._generate_analysis_success_response(result, session_id, data_handler)
            else:
                # Log the error
                if self.interaction_logger and session_id:
                    self.interaction_logger.log_error(
                        session_id,
                        'standard_analysis_error',
                        result.get('message', 'Unknown error')
                    )
                
                return {
                    "status": "error",
                    "response": f"Error running analysis: {result.get('message', 'Unknown error')}",
                    "action": "error"
                }
        except Exception as e:
            logger.error(f"Error running standard analysis: {str(e)}", exc_info=True)
            
            # Log the error
            if self.interaction_logger and session_id:
                self.interaction_logger.log_error(session_id, 'standard_analysis_exception', str(e))
            
            return {
                "status": "error",
                "response": f"Error running analysis: {str(e)}",
                "action": "error"
            }
    
    def _legacy_run_standard_analysis(self, data_handler, session_id):
        """Legacy method for running standard analysis when analysis service is not available"""
        if not data_handler:
            return {"status": "error", "message": "No data available for analysis"}
        
        try:
            # Run the full analysis using default variables
            result = data_handler.run_full_analysis(
                selected_variables=None,  # Use default variables
                llm_manager=self.llm_manager
            )
            
            return result
        except Exception as e:
            logger.error(f"Error in legacy standard analysis: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": f"Error running standard analysis: {str(e)}"
            }
    
    def _handle_show_visualization(self, nlu_result, session_state, data_handler, session_id):
        """
        Handle visualization requests by actually generating and displaying them
        """
        if not data_handler:
            return {
                "status": "error", 
                "response": "<p>No data available for visualization. Please upload data files first.</p>"
            }
        
        # Extract visualization details from entities
        entities = nlu_result.get('entities', {})
        viz_type_raw = entities.get('visualization_type', 'composite_map')
        variable = entities.get('variable_name')
        variables = entities.get('variables', [])
        
        logger.info(f"Handling visualization request: type='{viz_type_raw}', variable='{variable}', variables={variables}")
        
        # Detect visualization type using fuzzy matching
        viz_type = self._detect_visualization_type(viz_type_raw)
        
        # If no specific variable provided, try to extract from variables list or viz type
        if not variable and variables:
            variable = variables[0] if variables else None
        
        logger.info(f"Resolved visualization: type='{viz_type}', variable='{variable}'")
        
        # Check if analysis is required for this visualization type
        analysis_required_types = ['composite_map', 'vulnerability_map', 'vulnerability_plot', 'decision_tree']
        
        if viz_type in analysis_required_types:
            if not session_state.get('analysis_complete'):
                return {
                    "status": "error",
                    "response": "<p>Please run the analysis before requesting this visualization.</p>"
                }
            
            # Verify that data_handler has current analysis results
            if not (hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None):
                logger.warning(f"Data handler missing vulnerability rankings for visualization {viz_type}")
                return {
                    "status": "error",
                    "response": "<p>Analysis results not found in data handler. Please run the analysis again.</p>"
                }
            
            # Log what analysis results we're using for the visualization
            analysis_type = session_state.get('analysis_type', 'unknown')
            variables_used = session_state.get('variables_used', [])
            logger.info(f"Generating {viz_type} visualization using {analysis_type} analysis results")
            logger.info(f"Analysis variables: {variables_used}")
            logger.info(f"Vulnerability rankings shape: {data_handler.vulnerability_rankings.shape}")
        
        try:
            # Get visualization service from container
            from flask import current_app
            if hasattr(current_app, 'services') and hasattr(current_app.services, 'visualization_service'):
                visualization_service = current_app.services.visualization_service
                
                # Prepare parameters
                params = {
                    'variable': variable,
                    'threshold': 30  # Default threshold
                }
                
                # Generate the actual visualization
                result = visualization_service.generate_visualization(
                    viz_type=viz_type,
                    data_handler=data_handler,
                    params=params,
                    session_id=session_id
                )
                
                if result.get('status') == 'success':
                    # Generate AI explanation for the visualization
                    if self.llm_manager:
                        try:
                            explanation = self.llm_manager.explain_visualization(
                                session_id=session_id,
                                viz_type=viz_type,
                                context={
                                    'variable': variable,
                                    'viz_metadata': result
                                }
                            )
                            
                            from ..core.llm_manager import convert_markdown_to_html
                            explanation_html = convert_markdown_to_html(explanation)
                            
                        except Exception as e:
                            logger.warning(f"Failed to generate LLM explanation: {str(e)}")
                            explanation_html = f"Here's your {viz_type.replace('_', ' ')} visualization."
                    else:
                        explanation_html = f"Here's your {viz_type.replace('_', ' ')} visualization."
                    
                    # Log assistant response
                    if self.interaction_logger and session_id:
                        self.interaction_logger.log_message(session_id, 'assistant', explanation_html)
                    
                    # Return visualization response with proper pagination metadata
                    return {
                        "status": "success",
                        "response": explanation_html,
                        "action": "show_visualization",
                        "visualization": result.get('image_path') or result.get('file_path') or result.get('path'),
                        "viz_type": viz_type,
                        "variable": variable,
                        "current_page": result.get('current_page', 1),
                        "total_pages": result.get('total_pages', 1),
                        "items_per_page": result.get('items_per_page', 4),
                        "metadata": result
                    }
                else:
                    return {
                        "status": "error",
                        "response": f"<p>Error generating visualization: {result.get('message', 'Unknown error')}</p>"
                    }
            else:
                logger.error("Visualization service not available")
                return {
                    "status": "error",
                    "response": "<p>Visualization service is not available. Please check the system configuration.</p>"
                }
                
        except Exception as e:
            logger.error(f"Error handling visualization request: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "response": f"<p>Error generating visualization: {str(e)}</p>"
            }

    def _handle_generate_report(self, nlu_result, session_state, data_handler, session_id):
        """
        Handle report generation requests with format detection and session state auto-fix
        """
        # Check analysis completion with auto-fix
        analysis_complete = session_state.get('analysis_complete', False)
        
        if not analysis_complete:
            # Auto-fix: Check if analysis was actually completed but session flag is wrong
            if (data_handler and 
                hasattr(data_handler, 'vulnerability_rankings') and 
                data_handler.vulnerability_rankings is not None):
                
                logger.info(f"Session {session_id}: Auto-fixing analysis_complete flag")
                analysis_complete = True
            else:
                return {
                    "status": "error",
                    "response": "<p>Please complete the analysis before generating a report.</p>"
                }
        
        if not data_handler:
            return {
                "status": "error",
                "response": "<p>No data available for report generation.</p>"
            }
        
        try:
            # Extract format from user message or entities
            format_type = 'pdf'  # default
            
            # Check entities first
            entities = nlu_result.get('entities', {})
            other_entities = entities.get('other_entities', '').lower()
            
            # Check original message for format keywords
            original_message = nlu_result.get('original_message', '').lower()
            
            # ENHANCED: Add detailed debug logging for format detection
            logger.info(f"Session {session_id}: Format detection debug:")
            logger.info(f"  - entities: {entities}")
            logger.info(f"  - other_entities: '{other_entities}'")
            logger.info(f"  - original_message: '{original_message}'")
            
            # Format detection logic
            if 'html' in other_entities or 'html' in original_message:
                format_type = 'html'
                logger.info(f"  - DETECTED HTML format")
            elif 'markdown' in other_entities or 'markdown' in original_message or '.md' in original_message:
                format_type = 'markdown'
                logger.info(f"  - DETECTED MARKDOWN format")
            elif 'pdf' in other_entities or 'pdf' in original_message:
                format_type = 'pdf'
                logger.info(f"  - DETECTED PDF format")
            else:
                logger.info(f"  - NO FORMAT DETECTED, using default: {format_type}")
            
            logger.info(f"Session {session_id}: Final format_type = '{format_type}' from message: '{original_message}'")
            
            # Get report service from container
            from flask import current_app
            if hasattr(current_app, 'services') and hasattr(current_app.services, 'report_service'):
                report_service = current_app.services.report_service
                
                # Generate the report with detected format
                result = report_service.generate_report(
                    data_handler=data_handler,
                    session_id=session_id,
                    format_type=format_type,
                    custom_sections=None,
                    detail_level='standard'
                )
                
                if result['status'] == 'success':
                    # Create appropriate response based on format - USER WANTS PDF + DASHBOARD
                    format_display = format_type.upper()
                    
                    if format_type.lower() == 'html':
                        # HTML request = Dashboard
                        file_icon = '📊'
                        response_html = f"""<p><strong>Interactive Dashboard generated successfully!</strong></p>
<p>Your comprehensive malaria risk analysis dashboard is ready for viewing.</p>
<p><a href="{result.get('report_url')}" class="btn btn-primary" target="_blank">{file_icon} View Interactive Dashboard</a></p>"""
                    else:
                        # PDF or other = PDF-styled report
                        file_icon = '📄'
                        response_html = f"""<p><strong>Report generated successfully!</strong></p>
<p>Your comprehensive malaria risk analysis report is ready for download.</p>
<p><a href="{result.get('report_url')}" class="btn btn-primary" target="_blank">{file_icon} Download PDF Report</a></p>"""
                    
                    # ALWAYS generate a dashboard as well (user wants both)
                    try:
                        dashboard_result = report_service.generate_dashboard(
                            data_handler=data_handler,
                            session_id=session_id
                        )
                        if dashboard_result.get('status') == 'success':
                            response_html += f"""<p><a href="{dashboard_result.get('report_url')}" class="btn btn-info" target="_blank">📊 View Interactive Dashboard</a></p>"""
                    except Exception as e:
                        logger.warning(f"Could not generate additional dashboard: {str(e)}")
                    
                    # Log assistant response
                    if self.interaction_logger and session_id:
                        self.interaction_logger.log_message(session_id, 'assistant', response_html)
                    
                    return {
                        "status": "success",
                        "response": response_html,
                        "action": "report_generated",
                        "download_url": result.get('report_url'),
                        "format": format_type,
                        "session_updates": {
                            "analysis_complete": True  # Ensure session state is corrected
                        }
                    }
                else:
                    return {
                        "status": "error",
                        "response": f"<p>Error generating report: {result.get('message', 'Unknown error')}</p>"
                    }
            else:
                return {
                    "status": "error",
                    "response": "<p>Report service is not available. Please check the system configuration.</p>"
                }
                
        except Exception as e:
            logger.error(f"Error handling report request: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "response": f"<p>Error generating report: {str(e)}</p>"
            }

    def _handle_explain_results(self, nlu_result, session_state, data_handler, session_id):
        """
        Handle requests to explain analysis results
        """
        if not session_state.get('analysis_complete'):
            return {
                "status": "error",
                "response": "<p>Please complete the analysis before asking for explanations.</p>"
            }
        
        # Use LLM to generate explanation
        if self.llm_manager:
            try:
                context = {
                    'analysis_complete': True,
                    'variables_used': session_state.get('variables_used', []),
                    'high_risk_count': session_state.get('high_risk_count', 0),
                    'medium_risk_count': session_state.get('medium_risk_count', 0),
                    'low_risk_count': session_state.get('low_risk_count', 0)
                }
                
                prompt = """The user is asking for an explanation of their malaria risk analysis results. 
                
Generate a comprehensive explanation that covers:
1. What the analysis found (risk distribution)
2. What the variables mean in the context of malaria risk
3. How to interpret the vulnerability categories
4. Actionable insights for public health interventions

Be specific about their results and make it accessible to public health officials."""

                ai_response = self.llm_manager.generate_response(
                    prompt=prompt,
                    context=context,
                    system_message="You are a malaria epidemiologist explaining analysis results to public health officials.",
                    session_id=session_id
                )
                
                from ..core.llm_manager import convert_markdown_to_html
                response_html = convert_markdown_to_html(ai_response)
                
                # Log assistant response
                if self.interaction_logger and session_id:
                    self.interaction_logger.log_message(session_id, 'assistant', response_html)
                
                return {"status": "success", "response": response_html}
                
            except Exception as e:
                logger.error(f"Error generating explanation: {str(e)}")
                return {
                    "status": "success",
                    "response": "<p>I can help explain your analysis results. Could you ask a more specific question about the findings?</p>"
                }
        else:
            return {
                "status": "success",
                "response": "<p>I can help explain your analysis results. Could you ask a more specific question about the findings?</p>"
            }

    def _handle_data_inquiry(self, nlu_result, session_state, data_handler, session_id):
        """
        Handle data-related inquiries
        """
        if not data_handler:
            return {
                "status": "success",
                "response": "<p>I'd be happy to help with data questions! Please upload your data files first so I can assist you better.</p>"
            }
        
        # Use LLM to handle data inquiries
        if self.llm_manager:
            try:
                available_vars = data_handler.get_available_variables() if data_handler else []
                
                context = {
                    'has_data': True,
                    'available_variables': available_vars,
                    'data_loaded': session_state.get('csv_loaded', False) and session_state.get('shapefile_loaded', False),
                    'analysis_complete': session_state.get('analysis_complete', False)
                }
                
                user_message = nlu_result.get('original_message', 'data inquiry')
                
                prompt = f"""The user is asking about their data: "{user_message}"

Available variables in their dataset: {', '.join(available_vars[:10])}

Generate a helpful response about their data, including:
1. What data they have loaded
2. Available variables for analysis
3. Data quality insights if relevant
4. Suggestions for next steps

Be specific about their actual data."""

                ai_response = self.llm_manager.generate_response(
                    prompt=prompt,
                    context=context,
                    system_message="You are a data analysis expert helping users understand their malaria risk data.",
                    session_id=session_id
                )
                
                from ..core.llm_manager import convert_markdown_to_html
                response_html = convert_markdown_to_html(ai_response)
                
                # Log assistant response
                if self.interaction_logger and session_id:
                    self.interaction_logger.log_message(session_id, 'assistant', response_html)
                
                return {"status": "success", "response": response_html}
                
            except Exception as e:
                logger.error(f"Error handling data inquiry: {str(e)}")
                return {
                    "status": "success",
                    "response": f"<p>You have {len(available_vars)} variables available for analysis. What would you like to know about your data?</p>"
                }
        else:
            available_vars = data_handler.get_available_variables() if data_handler else []
            return {
                "status": "success",
                "response": f"<p>You have {len(available_vars)} variables available for analysis. What would you like to know about your data?</p>"
            }

    def _handle_custom_analysis(self, nlu_result, session_state, data_handler, session_id):
        """
        Handle custom analysis with flexible variable validation and dynamic responses
        """
        # Extract variables mentioned by the user
        user_variables = nlu_result.get('entities', {}).get('variables', [])
        
        if not user_variables:
            # Use LLM to ask for clarification in a conversational way
            if self.llm_manager:
                try:
                    available_vars = data_handler.get_available_variables() if data_handler else []
                    sample_vars = available_vars[:10] if len(available_vars) > 10 else available_vars
                    
                    prompt = f"""The user wants to run a custom analysis but didn't specify which variables to use. 
                    
Available variables in their dataset include: {', '.join(sample_vars)}

Generate a helpful, conversational response that:
1. Acknowledges their request for custom analysis
2. Asks them to specify which variables they'd like to focus on
3. Gives examples using the actual variable names from their data
4. Mentions they can use common names like 'population', 'rainfall', etc. and the system will find the best matches
5. Keep it friendly and encouraging

Be specific about their data and make suggestions based on malaria risk analysis best practices."""

                    response = self.llm_manager.generate_response(
                        prompt=prompt,
                        context={'available_variables': sample_vars, 'action': 'custom_analysis_request'},
                        system_message="You are a helpful malaria analysis expert. Be conversational and specific.",
                        session_id=session_id
                    )
                    
                    from ..core.llm_manager import convert_markdown_to_html
                    response_html = convert_markdown_to_html(response)
                    
                except Exception as e:
                    logger.error(f"Error generating LLM response for variable request: {str(e)}")
                    # Fallback
                    available_vars = data_handler.get_available_variables() if data_handler else []
                    sample_vars = available_vars[:8] if len(available_vars) > 8 else available_vars
                    response_html = f"""<p>I'd be happy to run a custom analysis for you! Could you please specify which variables you'd like me to focus on?</p>
<p><strong>Available variables in your dataset include:</strong></p>
<ul>{''.join(f'<li>{var}</li>' for var in sample_vars)}</ul>
<p>You can use common names like "population", "rainfall", "temperature", etc., and I'll find the best matches in your data.</p>
<p><strong>Example:</strong> "Run analysis with population, rainfall, and temperature"</p>"""
            else:
                # Fallback without LLM
                available_vars = data_handler.get_available_variables() if data_handler else []
                sample_vars = available_vars[:8] if len(available_vars) > 8 else available_vars
                response_html = f"""<p>I'd be happy to run a custom analysis for you! Could you please specify which variables you'd like me to focus on?</p>
<p><strong>Available variables in your dataset include:</strong></p>
<ul>{''.join(f'<li>{var}</li>' for var in sample_vars)}</ul>
<p>You can use common names like "population", "rainfall", "temperature", etc., and I'll find the best matches in your data.</p>
<p><strong>Example:</strong> "Run analysis with population, rainfall, and temperature"</p>"""
            
            return {"status": "success", "response": response_html}
        
        # Get available columns from data handler
        available_columns = data_handler.get_available_variables() if data_handler else []
        
        # Use flexible validation to match user variables to actual columns
        validation_result = self.validation_service.validate_variables(user_variables, available_columns)
        
        valid_variables = validation_result['valid']
        invalid_variables = validation_result['invalid']
        variable_mapping = validation_result['mapping']
        
        # Log the validation results
        logger.info(f"Variable validation for session {session_id}:")
        logger.info(f"  User input: {user_variables}")
        logger.info(f"  Valid matches: {valid_variables}")
        logger.info(f"  Invalid: {invalid_variables}")
        logger.info(f"  Mapping: {variable_mapping}")
        
        # Handle validation results
        if len(valid_variables) < 2:
            # Not enough valid variables - ask for clarification with LLM
            if self.llm_manager:
                try:
                    sample_vars = available_columns[:10] if len(available_columns) > 10 else available_columns
                    
                    context = {
                        'user_variables': user_variables,
                        'valid_matches': valid_variables,
                        'invalid_inputs': invalid_variables,
                        'mapping': variable_mapping,
                        'available_variables': sample_vars
                    }
                    
                    prompt = f"""The user tried to specify variables for custom analysis, but we need clarification:

User specified: {user_variables}
Valid matches found: {valid_variables}
Couldn't match: {invalid_variables}
Variable mapping: {variable_mapping}

Available dataset variables: {sample_vars}

Generate a helpful response that:
1. Acknowledges what they correctly specified (if any)
2. Explains what couldn't be matched (if any) 
3. Suggests specific alternatives from their actual data
4. Asks for at least 2-3 variables total for meaningful analysis
5. Be encouraging and specific about their dataset

Context: {context}"""

                    response = self.llm_manager.generate_response(
                        prompt=prompt,
                        context=context,
                        system_message="You are helping with malaria risk analysis. Be specific about the user's dataset and provide clear guidance.",
                        session_id=session_id
                    )
                    
                    from ..core.llm_manager import convert_markdown_to_html
                    response_html = convert_markdown_to_html(response)
                    
                except Exception as e:
                    logger.error(f"Error generating LLM clarification response: {str(e)}")
                    # Fallback response
                    if valid_variables:
                        response_html = f"""<p>Good! I found matches for: <strong>{', '.join(valid_variables)}</strong></p>"""
                    else:
                        response_html = "<p>I couldn't find matches for those variable names.</p>"
                    
                    if invalid_variables:
                        response_html += f"""<p>However, I couldn't match: <strong>{', '.join(invalid_variables)}</strong></p>"""
                    
                    sample_vars = available_columns[:8] if len(available_columns) > 8 else available_columns
                    response_html += f"""<p>For meaningful analysis, please specify at least 2-3 variables from your dataset:</p>
<ul>{''.join(f'<li>{var}</li>' for var in sample_vars)}</ul>
<p>Try again with specific variable names or common terms like "population", "rainfall", etc.</p>"""
            else:
                # Fallback without LLM
                if valid_variables:
                    response_html = f"""<p>Good! I found matches for: <strong>{', '.join(valid_variables)}</strong></p>"""
                else:
                    response_html = "<p>I couldn't find matches for those variable names.</p>"
                
                if invalid_variables:
                    response_html += f"""<p>However, I couldn't match: <strong>{', '.join(invalid_variables)}</strong></p>"""
                
                sample_vars = available_columns[:8] if len(available_columns) > 8 else available_columns
                response_html += f"""<p>For meaningful analysis, please specify at least 2-3 variables from your dataset:</p>
<ul>{''.join(f'<li>{var}</li>' for var in sample_vars)}</ul>
<p>Try again with specific variable names or common terms like "population", "rainfall", etc.</p>"""
            
            return {"status": "success", "response": response_html}
        
        # Enough valid variables - proceed with analysis
        logger.info(f"Running custom analysis with variables: {valid_variables}")
        
        try:
            # Run the analysis using the validated variables
            result = self.analysis_service.run_custom_analysis(
                data_handler=data_handler,
                selected_variables=valid_variables,
                question=None,
                session_id=session_id
            )
            
            if result.get('status') == 'success':
                # Generate success response with variable mapping information
                success_response = self._generate_analysis_success_response(result, session_id, data_handler)
                
                # CRITICAL: Verify that custom analysis results are properly stored in data_handler
                logger.info(f"Custom analysis completed for session {session_id}")
                logger.info(f"Variables used: {valid_variables}")
                
                # Verify data_handler has been updated with custom analysis results
                if hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None:
                    logger.info(f"✅ Data handler updated - vulnerability rankings shape: {data_handler.vulnerability_rankings.shape}")
                    logger.info(f"✅ Vulnerability categories: {data_handler.vulnerability_rankings['vulnerability_category'].value_counts().to_dict()}")
                else:
                    logger.warning("⚠️ Data handler may not have been properly updated with custom analysis results")
                
                # Verify the variables used in analysis match what we requested
                if hasattr(data_handler, 'composite_variables'):
                    actual_variables = data_handler.composite_variables
                    logger.info(f"Variables in data_handler.composite_variables: {actual_variables}")
                    if set(actual_variables) != set(valid_variables):
                        logger.warning(f"⚠️ Variable mismatch - requested: {valid_variables}, actual: {actual_variables}")
                
                # Add variable mapping information to the response if there were substitutions
                if variable_mapping and any(k != v for k, v in variable_mapping.items()):
                    mapping_info = []
                    for user_input, actual_col in variable_mapping.items():
                        if user_input != actual_col:
                            mapping_info.append(f'"{user_input}" → {actual_col}')
                    
                    if mapping_info:
                        mapping_html = f"""<div class="variable-mapping-info">
<p><strong>Variable Mapping:</strong> {', '.join(mapping_info)}</p>
</div>"""
                        success_response['response'] = mapping_html + success_response['response']
                
                return success_response
            else:
                error_msg = result.get('message', 'Analysis failed')
                logger.error(f"Custom analysis failed: {error_msg}")
                return {"status": "error", "response": f"<p>Analysis failed: {error_msg}</p>"}
                
        except Exception as e:
            logger.error(f"Error during custom analysis: {str(e)}")
            return {"status": "error", "response": f"<p>Analysis failed: {str(e)}</p>"}

    def _handle_unrecognized_intent(self, user_message, nlu_result, session_state, data_handler, session_id):
        """
        Handle unrecognized intents with LLM flexibility (RESTORED LEGACY FLEXIBILITY)
        """
        logger.info(f"Handling unrecognized intent for message: '{user_message}' with session state: {session_state}")
        
        # Use LLM to generate intelligent, context-aware responses
        if self.llm_manager:
            try:
                # Gather context about current session state
                context = {
                    'user_message': user_message,
                    'session_state': session_state,
                    'has_data': data_handler is not None,
                    'analysis_complete': session_state.get('analysis_complete', False),
                    'available_actions': self._get_available_actions(session_state, data_handler),
                    'nlu_confidence': nlu_result.get('confidence', 0.0),
                    'extracted_entities': nlu_result.get('entities', {})
                }
                
                # Create dynamic prompt based on context
                if not data_handler:
                    # No data loaded yet
                    prompt = f"""User asked: "{user_message}"

The user hasn't uploaded data yet. Generate a helpful response that:
1. Acknowledges their question/request
2. Explains they need to upload data first (CSV and shapefile)
3. Guides them on what they can do next
4. Be encouraging and conversational
5. If their question seems related to analysis, mention what they'll be able to do once data is uploaded

Context: {context}"""
                
                elif not context['analysis_complete']:
                    # Data loaded but no analysis
                    available_vars = data_handler.get_available_variables() if data_handler else []
                    sample_vars = available_vars[:8] if len(available_vars) > 8 else available_vars
                    
                    prompt = f"""User asked: "{user_message}"

The user has data loaded but hasn't run analysis yet. Their dataset has variables like: {', '.join(sample_vars)}

Generate a helpful response that:
1. Acknowledges their question/request
2. Explains they need to run analysis first if their request requires it
3. Suggests specific actions they can take (like "Run the analysis" or "Run analysis with [specific variables]")
4. If they're asking about specific variables, help them understand what's available
5. Be conversational and specific about their data

Context: {context}"""
                
                else:
                    # Analysis complete - can do everything
                    prompt = f"""User asked: "{user_message}"

The user has completed analysis and can now access all features. Generate a helpful response that:
1. Acknowledges their question/request 
2. Suggests specific actions they can take
3. Offers to show visualizations, generate reports, or explain results
4. Be specific about what's possible with their completed analysis
5. If unclear, ask clarifying questions to better help them

Context: {context}"""
                
                # Generate dynamic response
                ai_response = self.llm_manager.generate_response(
                    prompt=prompt,
                    context=context,
                    system_message="You are a helpful malaria risk analysis assistant. Be conversational, specific, and always try to guide users toward productive next steps.",
                    session_id=session_id
                )
                
                # Convert to HTML
                from ..core.llm_manager import convert_markdown_to_html
                response_html = convert_markdown_to_html(ai_response)
                
                return {"status": "success", "response": response_html}
                
            except Exception as e:
                logger.error(f"Error generating LLM response for unrecognized intent: {str(e)}")
                # Fall through to legacy fallback
        
        # Legacy fallback responses (more flexible than before)
        if not data_handler:
            return {
                "status": "success",
                "response": """<p>I'd be happy to help! To get started with malaria vulnerability analysis, please:</p>
<ol>
<li><strong>Upload your CSV data file</strong> (with demographic, environmental, or health variables)</li>
<li><strong>Upload your shapefile</strong> (geographic boundaries for your study area)</li>
</ol>
<p>Once your data is loaded, I can help you run analysis, create visualizations, and generate comprehensive reports. What specific aspect of malaria risk are you most interested in analyzing?</p>"""
            }
        elif not session_state.get('analysis_complete'):
            available_vars = data_handler.get_available_variables() if data_handler else []
            sample_vars = ', '.join(available_vars[:6]) if len(available_vars) > 6 else ', '.join(available_vars)
            
            return {
                "status": "success", 
                "response": f"""<p>I see you have data loaded with variables like: <strong>{sample_vars}</strong></p>
<p>To help you with "{user_message}", I'll need to analyze your data first. You can:</p>
<ul>
<li><strong>"Run the analysis"</strong> - I'll automatically select the best variables</li>
<li><strong>"Run analysis with [specific variables]"</strong> - You choose which variables to focus on</li>
</ul>
<p>Once analysis is complete, I can help you with visualizations, reports, and detailed insights about malaria vulnerability patterns in your data.</p>"""
            }
        else:
            return {
                "status": "success",
                "response": f"""<p>I understand you're asking about "{user_message}". With your analysis complete, I can help you:</p>
<ul>
<li><strong>Show visualizations:</strong> "Show composite map", "Show vulnerability plot", etc.</li>
<li><strong>Generate reports:</strong> "Generate a report" for comprehensive analysis</li>
<li><strong>Explain results:</strong> Ask about specific wards, variables, or patterns</li>
<li><strong>Custom analysis:</strong> "Analyze with [specific variables]"</li>
</ul>
<p>What would you like to explore first?</p>"""
            }
    
    def _get_available_actions(self, session_state, data_handler):
        """Get list of available actions based on current state"""
        actions = []
        
        if not data_handler:
            actions = ['upload_data', 'load_sample_data']
        elif not session_state.get('analysis_complete'):
            actions = ['run_analysis', 'run_custom_analysis', 'explore_data']
        else:
            actions = ['generate_visualizations', 'generate_report', 'custom_analysis', 'explain_results']
        
        return actions

    def _detect_visualization_type(self, viz_type_raw):
        """
        Flexible detection of visualization type from natural language input.
        Handles synonyms, partial matches, and variations.
        """
        if not viz_type_raw:
            return 'composite_map'  # Default
        
        # Convert to lowercase for case-insensitive matching
        input_text = str(viz_type_raw).lower()
        
        # Define comprehensive patterns for each visualization type
        viz_patterns = {
            'composite_map': [
                # Direct matches
                'composite', 'composite map', 'composite risk', 'composite score',
                'risk map', 'risk composite', 'combined map', 'overall risk',
                'aggregated risk', 'total risk', 'merged map', 'combined risk',
                # Variations
                'composite vulnerability', 'composite analysis', 'composite view',
                'risk distribution', 'overall map', 'combined score'
            ],
            'variable_map': [
                # Direct matches  
                'variable', 'variable map', 'single variable', 'individual variable',
                'data map', 'variable distribution', 'single map',
                # Normalized variants
                'normalized', 'normalized map', 'standardized', 'standardized map',
                'norm map', 'scaled map', 'normalized variable'
            ],
            'vulnerability_map': [
                # Direct matches
                'vulnerability', 'vulnerability map', 'vuln map', 'risk classification',
                'vulnerability classification', 'vulnerability zones', 'risk zones',
                'high risk', 'vulnerability areas', 'risk categories',
                # Variations
                'classified map', 'categorized map', 'risk levels', 'vulnerability levels'
            ],
            'vulnerability_plot': [
                # Direct matches
                'vulnerability plot', 'vulnerability chart', 'ranking', 'vulnerability ranking',
                'risk plot', 'risk chart', 'risk ranking', 'vulnerability graph',
                # Chart/plot synonyms
                'plot', 'chart', 'graph', 'ranking chart', 'ranking plot',
                'vulnerability distribution', 'risk distribution plot'
            ],
            'decision_tree': [
                # Direct matches
                'decision tree', 'tree', 'decision', 'classification tree',
                'tree diagram', 'tree chart', 'decision diagram',
                # Variations
                'tree analysis', 'tree visualization', 'decision analysis'
            ],
            'urban_extent_map': [
                # Direct matches
                'urban', 'urban extent', 'urban map', 'urban analysis',
                'urban classification', 'urban areas', 'city extent',
                # Variations
                'urban boundary', 'urban zones', 'urbanization', 'urban development'
            ]
        }
        
        # Score each visualization type based on keyword matches
        scores = {}
        for viz_type, patterns in viz_patterns.items():
            score = 0
            for pattern in patterns:
                if pattern in input_text:
                    # Exact phrase match gets higher score
                    if pattern == input_text.strip():
                        score += 10  # Exact match bonus
                    else:
                        score += len(pattern.split())  # Multi-word patterns get higher scores
            scores[viz_type] = score
        
        # Find the best match
        if scores and max(scores.values()) > 0:
            best_match = max(scores, key=scores.get)
            logger.info(f"Fuzzy matched '{viz_type_raw}' to '{best_match}' with score {scores[best_match]}")
            return best_match
        
        # Fallback: try partial word matching for common terms
        fallback_patterns = {
            'composite': 'composite_map',
            'risk': 'composite_map', 
            'vulnerability': 'vulnerability_map',
            'vuln': 'vulnerability_map',
            'variable': 'variable_map',
            'plot': 'vulnerability_plot',
            'chart': 'vulnerability_plot', 
            'ranking': 'vulnerability_plot',
            'tree': 'decision_tree',
            'decision': 'decision_tree',
            'urban': 'urban_extent_map',
            'normalized': 'variable_map',
            'map': 'composite_map'  # Generic "map" defaults to composite
        }
        
        for keyword, viz_type in fallback_patterns.items():
            if keyword in input_text:
                logger.info(f"Fallback matched '{viz_type_raw}' to '{viz_type}' via keyword '{keyword}'")
                return viz_type
        
        # Ultimate fallback
        logger.warning(f"Could not detect visualization type from '{viz_type_raw}', defaulting to composite_map")
        return 'composite_map'
 