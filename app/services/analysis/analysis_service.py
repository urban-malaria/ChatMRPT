def run_standard_analysis(self, data_handler=None, session_id=None):
    """
    Run standard analysis with AI-selected variables.
    
    Args:
        data_handler: Data handler with loaded data
        session_id: User session ID
        
    Returns:
        Dict with analysis results
    """
    if not data_handler:
        logger.error(f"Data handler is None for session {session_id}")
        return {
            "status": "error",
            "message": "Data handler not available",
            "variables_used": []
        }
    
    try:
        # Select optimal variables
        selected_variables = self._select_optimal_variables(data_handler, session_id)
        
        # Use enhanced analysis function with fallback support
        try:
            from ...data.analysis import run_vulnerability_analysis
            result = run_vulnerability_analysis(
                data_handler=data_handler,
                selected_variables=selected_variables,
                session_id=session_id,
                logger=logger
            )
            
            # Log successful completion
            if result.get('status') == 'success':
                logger.info(f"Standard analysis completed for session {session_id}")
                logger.info(f"Variables used: {result.get('variables_used', [])}")
            else:
                logger.error(f"Analysis failed: {result.get('message', 'Unknown error')}")
            
            return result
            
        except ImportError as e:
            logger.error(f"Error importing analysis module: {str(e)}")
            return {
                "status": "error",
                "message": "Analysis module not available",
                "variables_used": selected_variables
            }
        except Exception as e:
            logger.error(f"Error running analysis: {str(e)}")
            return {
                "status": "error",
                "message": f"Error running analysis: {str(e)}",
                "variables_used": selected_variables
            }
    except Exception as e:
        logger.error(f"Error in run_standard_analysis: {str(e)}")
        return {
            "status": "error",
            "message": f"Error in analysis preparation: {str(e)}",
            "variables_used": []
        }

def run_custom_analysis(self, data_handler=None, selected_variables=None, question=None, session_id=None):
    """
    Run custom analysis with user-specified variables.
    
    Args:
        data_handler: Data handler with loaded data
        selected_variables: Variables to use in analysis
        question: User question (optional)
        session_id: User session ID
        
    Returns:
        Dict with analysis results
    """
    if not data_handler:
        logger.error(f"Data handler is None for session {session_id}")
        return {
            "status": "error",
            "message": "Data handler not available",
            "variables_used": []
        }
    
    if not selected_variables or len(selected_variables) < 2:
        logger.error(f"Not enough variables selected for custom analysis: {selected_variables}")
        return {
            "status": "error",
            "message": "Not enough variables selected for analysis",
            "variables_used": selected_variables or []
        }
    
    try:
        # Use enhanced analysis function with fallback support
        try:
            from ...data.analysis import run_vulnerability_analysis
            result = run_vulnerability_analysis(
                data_handler=data_handler,
                selected_variables=selected_variables,
                session_id=session_id,
                logger=logger
            )
            
            # Add custom selection method
            if result.get('status') == 'success':
                result['selection_method'] = 'user_specified'
                logger.info(f"Custom analysis completed for session {session_id}")
                logger.info(f"Variables used: {result.get('variables_used', [])}")
            else:
                logger.error(f"Custom analysis failed: {result.get('message', 'Unknown error')}")
            
            return result
            
        except ImportError as e:
            logger.error(f"Error importing analysis module: {str(e)}")
            return {
                "status": "error",
                "message": "Analysis module not available",
                "variables_used": selected_variables
            }
        except Exception as e:
            logger.error(f"Error running custom analysis: {str(e)}")
            return {
                "status": "error",
                "message": f"Error running analysis: {str(e)}",
                "variables_used": selected_variables
            }
    except Exception as e:
        logger.error(f"Error in run_custom_analysis: {str(e)}")
        return {
            "status": "error",
            "message": f"Error in analysis preparation: {str(e)}",
            "variables_used": selected_variables or []
        } 