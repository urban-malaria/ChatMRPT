"""
Visualization service for generating and managing data visualizations.
"""
import os
import logging
import uuid
from flask import current_app

logger = logging.getLogger(__name__)

class VisualizationService:
    """
    Service for handling visualization operations.
    
    This class separates visualization logic from HTTP request handling.
    """
    
    def __init__(self, llm_manager=None, interaction_logger=None):
        """
        Initialize the visualization service with optional dependencies.
        
        Args:
            llm_manager: LLM manager for AI operations
            interaction_logger: Logger for user interactions
        """
        self.llm_manager = llm_manager
        self.interaction_logger = interaction_logger
    
    def generate_visualization(self, viz_type, data_handler, params=None, session_id=None):
        """
        Generate a visualization based on the specified type and parameters.
        
        Args:
            viz_type: Type of visualization to generate (map, boxplot, etc.)
            data_handler: Data handler with loaded data
            params: Visualization parameters (variables, etc.)
            session_id: User session ID for logging
            
        Returns:
            dict: Visualization data and status
        """
        if not data_handler:
            return {"status": "error", "message": "No data available for visualization"}
        
        if not viz_type:
            return {"status": "error", "message": "No visualization type specified"}
        
        # Set default parameters if not provided
        if params is None:
            params = {}
        
        try:
            # Log operation
            if self.interaction_logger and session_id:
                self.interaction_logger.log_visualization_metadata(
                    session_id=session_id,
                    viz_type=viz_type,
                    variables_used=params.get('variables', [params.get('variable')] if params.get('variable') else []),
                    data_summary=None,
                    visual_elements=params
                )
            
            # Import visualization module
            import app.models.visualization as viz
            
            # Generate unique filename for visualization
            reports_folder = current_app.config.get('REPORTS_FOLDER')
            viz_filename = f"{session_id}_{viz_type}_{uuid.uuid4().hex[:8]}.html"
            viz_path = os.path.join(reports_folder, viz_filename)
            
            result = None
            
            # Call appropriate visualization function based on type
            if viz_type == 'composite_map':
                # Use the viz module's create_composite_map function
                model_index = params.get('model_index')
                result = viz.create_composite_map(
                    data_handler=data_handler,
                    model_index=model_index
                )
                
                # The function already creates and returns the HTML file path
                if result.get('status') == 'success' and 'image_path' in result:
                    result['file_path'] = result['image_path']
                
            elif viz_type == 'variable_map':
                variable = params.get('variable')
                if not variable:
                    return {"status": "error", "message": "No variable specified for variable map"}
                
                # Use the viz module's create_variable_map function
                result = viz.create_variable_map(
                    data_handler=data_handler,
                    variable_name=variable
                )
                
                # If successful, save the figure to HTML
                if result.get('status') == 'success' and 'figure' in result:
                    fig = result.pop('figure')  # Remove figure from result dict
                    html_path = viz.create_plotly_html(fig, viz_filename)
                    result['file_path'] = html_path
                elif result.get('status') == 'success' and 'image_path' in result:
                    # Some functions may return image_path directly
                    result['file_path'] = result['image_path']
                
            elif viz_type == 'vulnerability_map':
                # Use the viz module's create_vulnerability_map function
                result = viz.create_vulnerability_map(data_handler=data_handler)
                
                # If successful, save the figure to HTML or use provided path
                if result.get('status') == 'success' and 'figure' in result:
                    fig = result.pop('figure')  # Remove figure from result dict
                    html_path = viz.create_plotly_html(fig, viz_filename)
                    result['file_path'] = html_path
                elif result.get('status') == 'success' and 'image_path' in result:
                    result['file_path'] = result['image_path']
                
            elif viz_type == 'vulnerability_plot':
                # Use the viz module's create_vulnerability_plot function
                result = viz.create_vulnerability_plot(data_handler=data_handler)
                
                # If successful, save the figure to HTML or use provided path
                if result.get('status') == 'success' and 'figure' in result:
                    fig = result.pop('figure')  # Remove figure from result dict
                    html_path = viz.create_plotly_html(fig, viz_filename)
                    result['file_path'] = html_path
                elif result.get('status') == 'success' and 'image_path' in result:
                    result['file_path'] = result['image_path']
                
            elif viz_type == 'urban_extent_map':
                threshold = params.get('threshold', 30)
                # Use the viz module's create_urban_extent_map function
                result = viz.create_urban_extent_map(
                    data_handler=data_handler,
                    threshold=threshold
                )
                
                # If successful, save the figure to HTML or use provided path
                if result.get('status') == 'success' and 'figure' in result:
                    fig = result.pop('figure')  # Remove figure from result dict
                    html_path = viz.create_plotly_html(fig, viz_filename)
                    result['file_path'] = html_path
                elif result.get('status') == 'success' and 'image_path' in result:
                    result['file_path'] = result['image_path']
                
            elif viz_type == 'decision_tree':
                # Use the viz module's create_decision_tree_plot function
                result = viz.create_decision_tree_plot(data_handler=data_handler)
                
                # If successful, save the figure to HTML or use provided path
                if result.get('status') == 'success' and 'figure' in result:
                    fig = result.pop('figure')  # Remove figure from result dict
                    html_path = viz.create_plotly_html(fig, viz_filename)
                    result['file_path'] = html_path
                elif result.get('status') == 'success' and 'image_path' in result:
                    result['file_path'] = result['image_path']
                
            # Handle box plot and other plot types
            elif viz_type == 'boxplot' or viz_type == 'vulnerability_boxplot':
                # Box plot visualization may need wards_per_page
                wards_per_page = params.get('wards_per_page', 20)
                
                # Use box_plot_function or similar
                if hasattr(data_handler, 'composite_scores') and data_handler.composite_scores:
                    result = viz.box_plot_function(
                        data_handler.composite_scores['scores'],
                        wards_per_page=wards_per_page
                    )
                    
                    # If successful and has plots, save first plot to HTML
                    if result.get('status') == 'success' and 'plots' in result and result['plots']:
                        fig = result['plots'][0]
                        html_path = viz.create_plotly_html(fig, viz_filename)
                        result['file_path'] = html_path
                    elif result.get('status') == 'success' and 'image_path' in result:
                        result['file_path'] = result['image_path']
                else:
                    return {"status": "error", "message": "Composite scores not available for boxplot"}
            else:
                return {"status": "error", "message": f"Unsupported visualization type: {viz_type}"}
            
            # Check result
            if not result or result.get('status') == 'error':
                return {
                    "status": "error",
                    "message": result.get('message', f"Error generating {viz_type}")
                }
            
            # Return visualization details
            # Standardize the result format
            return {
                "status": "success",
                "visualization_type": viz_type,
                "filename": viz_filename,
                "title": result.get('title', f"{viz_type.replace('_', ' ').title()}"),
                "description": result.get('description', ""),
                "metadata": result.get('metadata', {}),
                "file_path": result.get('file_path', ''),
                "image_path": result.get('image_path', result.get('file_path', '')),
                "current_page": result.get('current_page', 1),
                "total_pages": result.get('total_pages', 1),
                "data_summary": result.get('data_summary', {}),
                "visual_elements": result.get('visual_elements', {})
            }
            
        except Exception as e:
            logger.error(f"Error generating {viz_type}: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": f"Error generating visualization: {str(e)}"
            }
    
    def explain_visualization(self, viz_type, data_handler=None, context=None, session_id=None):
        """
        Generate explanation for a visualization type.
        
        Args:
            viz_type: Type of visualization to explain
            data_handler: Optional data handler for additional context
            context: Additional context information
            session_id: User session ID for logging
            
        Returns:
            str: Explanation of the visualization
        """
        if not self.llm_manager:
            return "Visualization explanations are currently unavailable."
            
        try:
            # Get data summary if data handler is available
            data_summary = None
            if data_handler:
                data_summary = data_handler.get_data_summary()
            
            # Generate explanation using LLM
            explanation = self.llm_manager.explain_visualization(
                session_id=session_id,
                viz_type=viz_type,
                context=context,
                question=None
            )
            
            return explanation
        except Exception as e:
            logger.error(f"Error explaining visualization: {str(e)}", exc_info=True)
            return f"This visualization helps you understand malaria risk patterns. It shows data in a {viz_type.replace('_', ' ')} format."
    
    def get_available_visualizations(self, data_handler, analysis_complete=False):
        """
        Get list of available visualizations based on current data and analysis status.
        
        Args:
            data_handler: Data handler with loaded data
            analysis_complete: Whether analysis has been completed
            
        Returns:
            list: Available visualization types
        """
        if not data_handler:
            return []
        
        available_viz = []
        
        # Basic visualizations always available with data
        if data_handler.has_geo_data():
            available_viz.append({
                "id": "variable_map",
                "name": "Variable Map",
                "description": "Map showing distribution of a single variable",
                "requires_analysis": False
            })
            
            available_viz.append({
                "id": "boxplot",
                "name": "Variable Boxplot",
                "description": "Boxplot showing distribution of selected variables",
                "requires_analysis": False
            })
        
        # Analysis-dependent visualizations
        if analysis_complete:
            available_viz.append({
                "id": "composite_map",
                "name": "Vulnerability Map",
                "description": "Map showing overall vulnerability classification",
                "requires_analysis": True
            })
            
            available_viz.append({
                "id": "risk_summary",
                "name": "Risk Summary",
                "description": "Summary of risk classification results",
                "requires_analysis": True
            })
        
        return available_viz
    
    def navigate_visualization(self, viz_type, direction, current_state, data_handler, session_id=None):
        """
        Navigate through a visualization (e.g., change page, change variable).
        
        Args:
            viz_type: Type of visualization
            direction: Navigation direction (next, prev, etc.)
            current_state: Current visualization state
            data_handler: Data handler with loaded data
            session_id: User session ID for logging
            
        Returns:
            dict: Updated visualization state
        """
        if not data_handler:
            return {"status": "error", "message": "No data available"}
        
        try:
            # Handle different visualization types
            if viz_type == 'boxplot':
                return self._navigate_boxplot(direction, current_state, data_handler, session_id)
            elif viz_type in ['composite_map', 'variable_map']:
                return self._navigate_map(viz_type, direction, current_state, data_handler, session_id)
            else:
                return {"status": "error", "message": f"Navigation not supported for {viz_type}"}
                
        except Exception as e:
            logger.error(f"Error navigating {viz_type}: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": f"Error navigating visualization: {str(e)}"
            }
    
    def _navigate_boxplot(self, direction, current_state, data_handler, session_id):
        """Handle boxplot navigation"""
        # Get pagination information
        page = current_state.get('page', 1)
        page_size = current_state.get('page_size', 5)
        variables = data_handler.get_available_variables()
        
        # Calculate total pages
        total_variables = len(variables)
        total_pages = (total_variables + page_size - 1) // page_size
        
        # Update page based on direction
        if direction == 'next':
            page = min(page + 1, total_pages)
        elif direction == 'prev':
            page = max(page - 1, 1)
        elif direction == 'first':
            page = 1
        elif direction == 'last':
            page = total_pages
        
        # Calculate slice for current page
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_variables)
        current_variables = variables[start_idx:end_idx]
        
        # Generate new visualization
        params = {
            'variables': current_variables,
            'page': page,
            'page_size': page_size
        }
        
        return self.generate_visualization('boxplot', data_handler, params, session_id)
    
    def _navigate_map(self, viz_type, direction, current_state, data_handler, session_id):
        """Handle map navigation"""
        if viz_type == 'variable_map':
            # For variable maps, navigation means changing the variable
            current_variable = current_state.get('variable')
            variables = data_handler.get_available_variables()
            
            if not current_variable or current_variable not in variables:
                # Default to first variable
                next_variable = variables[0] if variables else None
            else:
                # Find current index
                try:
                    current_idx = variables.index(current_variable)
                    
                    # Update index based on direction
                    if direction == 'next':
                        next_idx = (current_idx + 1) % len(variables)
                    elif direction == 'prev':
                        next_idx = (current_idx - 1) % len(variables)
                    else:
                        next_idx = current_idx
                        
                    next_variable = variables[next_idx]
                except (ValueError, IndexError):
                    next_variable = variables[0] if variables else None
            
            # Generate new visualization with updated variable
            if next_variable:
                params = {'variable': next_variable}
                return self.generate_visualization('variable_map', data_handler, params, session_id)
            else:
                return {"status": "error", "message": "No variables available for mapping"}
                
        elif viz_type == 'composite_map':
            # For composite maps, navigation might mean changing the classification or threshold
            risk_level = current_state.get('risk_level', 'all')
            
            # Update risk level based on direction
            risk_levels = ['all', 'high', 'medium', 'low']
            if risk_level not in risk_levels:
                risk_level = 'all'
                
            try:
                current_idx = risk_levels.index(risk_level)
                
                # Update index based on direction
                if direction == 'next':
                    next_idx = (current_idx + 1) % len(risk_levels)
                elif direction == 'prev':
                    next_idx = (current_idx - 1) % len(risk_levels)
                else:
                    next_idx = current_idx
                    
                next_risk_level = risk_levels[next_idx]
            except (ValueError, IndexError):
                next_risk_level = 'all'
            
            # Generate new visualization with updated risk level
            params = {'risk_level': next_risk_level}
            return self.generate_visualization('composite_map', data_handler, params, session_id)
        
        return {"status": "error", "message": f"Navigation not supported for {viz_type}"} 