"""
Analysis service for running malaria risk analysis operations.
"""
import logging
from flask import current_app, session

logger = logging.getLogger(__name__)

class AnalysisService:
    """
    Service for handling analysis operations.
    
    This class separates analysis business logic from HTTP request handling.
    """
    
    def __init__(self, llm_manager=None, interaction_logger=None):
        """
        Initialize the analysis service with optional dependencies.
        
        Args:
            llm_manager: LLM manager for AI operations
            interaction_logger: Logger for user interactions
        """
        self.llm_manager = llm_manager
        self.interaction_logger = interaction_logger
    
    def run_standard_analysis(self, data_handler, session_id=None):
        """
        Run standard analysis on provided data.
        
        Args:
            data_handler: Data handler with loaded data
            session_id: User session ID for logging
            
        Returns:
            dict: Analysis results and status
        """
        if not data_handler:
            return {"status": "error", "message": "No data available for analysis"}
        
        try:
            # Log operation
            if self.interaction_logger and session_id:
                self.interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type="standard_analysis",
                    details={"analysis_type": "standard"},
                    success=True
                )
            
            # Run the analysis using the full analysis pipeline
            result = data_handler.run_full_analysis(
                llm_manager=self.llm_manager  # Pass LLM manager for AI-driven variable selection
            )
            
            # Format the response as expected
            if result.get('status') == 'success':
                return {
                    "status": "success",
                    "high_risk_wards": result.get("high_risk_wards", []),
                    "medium_risk_wards": result.get("medium_risk_wards", []),
                    "low_risk_wards": result.get("low_risk_wards", []),
                    "variables_used": result.get("variables_used", []),
                    "summary": result.get("summary", {})
                }
            else:
                return {
                    "status": "error",
                    "message": result.get("message", "Error running full analysis")
                }
        except Exception as e:
            logger.error(f"Error running standard analysis: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": f"Error running analysis: {str(e)}"
            }
    
    def run_custom_analysis(self, data_handler, selected_variables, question=None, session_id=None):
        """
        Run custom analysis with user-selected variables.
        
        Args:
            data_handler: Data handler with loaded data
            selected_variables: List of variable names to use
            question: Optional user question for context
            session_id: User session ID for logging
            
        Returns:
            dict: Analysis results and status
        """
        if not data_handler:
            return {"status": "error", "message": "No data available for analysis"}
            
        if not selected_variables or len(selected_variables) < 2:
            return {"status": "error", "message": "At least 2 variables are required for analysis"}
        
        try:
            # Log operation
            if self.interaction_logger and session_id:
                self.interaction_logger.log_analysis_event(
                    session_id=session_id,
                    event_type="custom_analysis",
                    details={"analysis_type": "custom", "variables": selected_variables},
                    success=True
                )
            
            # Get available variables
            available_vars = data_handler.get_available_variables()
            
            # Create case-insensitive lookup dictionary for fuzzy matching
            available_vars_lookup = {var.lower(): var for var in available_vars}
            
            # Perform fuzzy, case-insensitive variable matching
            valid_variables = []
            invalid_variables = []
            
            for var in selected_variables:
                var_lower = var.lower()
                if var_lower in available_vars_lookup:
                    # Direct case-insensitive match
                    valid_variables.append(available_vars_lookup[var_lower])
                else:
                    # Try partial matching
                    matched = False
                    for av_lower, av in available_vars_lookup.items():
                        # Check if user's variable is a substring of actual variable or vice versa
                        if var_lower in av_lower or av_lower in var_lower:
                            valid_variables.append(av)
                            matched = True
                            logger.info(f"Fuzzy matched '{var}' to '{av}'")
                            break
                            
                    if not matched:
                        invalid_variables.append(var)
            
            # Remove duplicates while preserving order
            seen = set()
            valid_variables = [x for x in valid_variables if not (x in seen or seen.add(x))]
            
            if len(valid_variables) < 2:
                # Provide helpful feedback about invalid variables
                invalid_msg = f"Could not find variables matching: {', '.join(invalid_variables)}" if invalid_variables else ""
                suggestion_msg = ""
                
                if available_vars:
                    suggestion_msg = f"\n\nAvailable variables include: {', '.join(available_vars[:10])}"
                    if len(available_vars) > 10:
                        suggestion_msg += f" and {len(available_vars) - 10} more"
                
                return {
                    "status": "error", 
                    "message": f"Not enough valid variables selected. {invalid_msg}{suggestion_msg}"
                }
            
            # Run the analysis using the full analysis pipeline with selected variables
            result = data_handler.run_full_analysis(
                selected_variables=valid_variables,
                llm_manager=self.llm_manager,  # Pass LLM manager for context
                custom_relationships=None  # Could be added as a parameter in the future
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
    
    def explain_variable_selection(self, variables, data_handler=None):
        """
        Generate explanation for why variables were selected.
        
        Args:
            variables: List of variable names
            data_handler: Optional data handler for additional context
            
        Returns:
            dict: Explanation and status
        """
        if not self.llm_manager:
            return {"status": "error", "message": "LLM manager not available"}
            
        if not variables:
            return {"status": "error", "message": "No variables provided"}
            
        try:
            # Get variable descriptions if data handler is available
            var_descriptions = {}
            if data_handler:
                var_descriptions = data_handler.get_variable_descriptions(variables)
            
            # Generate explanations using LLM
            explanations = self.llm_manager.explain_variable_selection(
                variables=variables,
                explanations=var_descriptions
            )
            
            return {
                "status": "success",
                "explanations": explanations
            }
        except Exception as e:
            logger.error(f"Error explaining variable selection: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": f"Error generating explanation: {str(e)}"
            } 