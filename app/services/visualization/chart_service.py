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
        Universal, dynamic navigation for any visualization type
        
        Args:
            viz_type: Type of visualization (composite_map, boxplot, variable_map, etc.)
            direction: Navigation direction (next, prev, first, last)
            current_state: Current pagination state
            data_handler: Data handler with analysis results
            session_id: Session ID for logging
        
        Returns:
            dict: Navigation result with updated visualization
        """
        try:
            # Extract current page and get dynamic parameters
            current_page = current_state.get('current_page', 1)
            metadata = current_state.get('metadata', {})
            
            # Get dynamic pagination parameters based on visualization type
            if viz_type == 'composite_map':
                items_per_page = metadata.get('items_per_page', getattr(data_handler, 'map_items_per_page', 10))
                total_items = len(data_handler.vulnerability_rankings) if hasattr(data_handler, 'vulnerability_rankings') else 0
                
            elif viz_type == 'boxplot':
                items_per_page = metadata.get('variables_per_page', getattr(data_handler, 'boxplot_variables_per_page', 5))
                variables = data_handler.get_available_variables() if hasattr(data_handler, 'get_available_variables') else []
                total_items = len(variables)
                
            elif viz_type == 'variable_map':
                # For variable maps, each "page" is a different variable
                variables = data_handler.get_available_variables() if hasattr(data_handler, 'get_available_variables') else []
                total_items = len(variables)
                items_per_page = 1  # One variable per page
                
            else:
                # Generic approach for unknown visualization types
                items_per_page = metadata.get('items_per_page', 10)
                total_items = metadata.get('total_items', 1)
            
            # Calculate total pages dynamically
            total_pages = max(1, (total_items + items_per_page - 1) // items_per_page) if items_per_page > 0 else 1
            
            # Calculate new page based on direction
            if direction == 'next':
                new_page = min(current_page + 1, total_pages)
            elif direction == 'prev':
                new_page = max(current_page - 1, 1)
            elif direction == 'first':
                new_page = 1
            elif direction == 'last':
                new_page = total_pages
            else:
                new_page = current_page
            
            # Only regenerate if page actually changed
            if new_page == current_page:
                return {
                    'status': 'success',
                    'message': f'Already on {direction} page',
                    'current_page': current_page,
                    'total_pages': total_pages,
                    'viz_type': viz_type
                }
            
            # Use the specific navigation method for the visualization type
            if viz_type == 'composite_map':
                return self.navigate_composite_map(data_handler, new_page, session_id)
            elif viz_type == 'boxplot':
                return self.navigate_boxplot(data_handler, new_page, session_id)
            elif viz_type == 'variable_map':
                return self._navigate_variable_map(data_handler, new_page, session_id)
            else:
                # Generic navigation for unknown types
                return self._navigate_generic(viz_type, data_handler, new_page, items_per_page, session_id)
                
        except Exception as e:
            logger.error(f"Error navigating {viz_type} visualization: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error navigating {viz_type}: {str(e)}'
            }
    
    def _navigate_variable_map(self, data_handler, page, session_id=None):
        """Navigate variable maps (each page shows a different variable)"""
        try:
            variables = data_handler.get_available_variables()
            if not variables:
                return {
                    'status': 'error',
                    'message': 'No variables available for mapping.'
                }
            
            # Validate page
            page = max(1, min(int(page), len(variables)))
            selected_variable = variables[page - 1]
            
            # Generate map for selected variable
            result = self.generate_visualization(
                'variable_map',
                data_handler,
                {
                    'variable': selected_variable,
                    'page': page,
                    'total_pages': len(variables)
                },
                session_id
            )
            
            if result['status'] == 'success':
                result.update({
                    'current_page': page,
                    'total_pages': len(variables),
                    'selected_variable': selected_variable,
                    'viz_type': 'variable_map'
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error navigating variable map: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error navigating variable map: {str(e)}'
            }
    
    def _navigate_generic(self, viz_type, data_handler, page, items_per_page, session_id=None):
        """Generic navigation for unknown visualization types"""
        try:
            # Generate visualization with pagination parameters
            result = self.generate_visualization(
                viz_type,
                data_handler,
                {
                    'page': page,
                    'items_per_page': items_per_page
                },
                session_id
            )
            
            if result['status'] == 'success':
                result.update({
                    'current_page': page,
                    'viz_type': viz_type
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error in generic navigation for {viz_type}: {str(e)}")
            return {
                'status': 'error',
                'message': f'Error navigating {viz_type}: {str(e)}'
            }
    
    def navigate_composite_map(self, data_handler, page, session_id=None):
        """
        Navigate composite maps with pagination matching the original ChatMRPT implementation
        
        Args:
            data_handler: Data handler with composite score data
            page: Target page number
            session_id: Session ID for logging
        
        Returns:
            dict: Navigation result with visualization path and metadata
        """
        try:
            # Check if composite scores are available
            if not hasattr(data_handler, 'composite_scores') or data_handler.composite_scores is None:
                return {
                    'status': 'error',
                    'message': 'Composite scores not available. Calculate composite scores first.'
                }
                
            # Import visualization module
            import app.visualization as viz
            
            # Call the create_composite_map function directly with the page parameter
            # This follows the original implementation in ChatMRPT-main
            result = viz.create_composite_map(
                data_handler=data_handler,
                model_index=page  # In the original implementation, model_index is used as the page number
            )
            
            if result['status'] == 'success':
                # Update session with new page info
                if 'current_page' in result:
                    # Make sure it's always an integer
                    result['current_page'] = int(result['current_page'])
                
                if 'total_pages' in result:
                    # Make sure it's always an integer
                    result['total_pages'] = int(result['total_pages'])
                
                # Add AI explanation
                explanation = self.explain_composite_map_navigation(
                    data_handler=data_handler,
                    page_data=result,
                    session_id=session_id
                )
                result['ai_response'] = explanation
                
                logger.info(f"Session {session_id}: Successfully navigated to composite map page {page}")
                return result
            else:
                return result
            
        except Exception as e:
            logger.error(f"Error navigating composite map to page {page}: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error navigating composite map: {str(e)}'
            }
    
    def navigate_boxplot(self, data_handler, page, session_id=None):
        """
        Navigate boxplots with pagination matching the original ChatMRPT implementation
        
        Args:
            data_handler: Data handler with boxplot data
            page: Target page number  
            session_id: Session ID for logging
        
        Returns:
            dict: Navigation result with visualization path and metadata
        """
        try:
            # Check if box plot data is available - this is the key check
            if not hasattr(data_handler, 'boxwhisker_plot') or not data_handler.boxwhisker_plot:
                return {
                    'status': 'error',
                    'message': 'Box plot data not available. Please run analysis first.'
                }
            
            # Get total pages from the boxwhisker_plot data
            total_pages = data_handler.boxwhisker_plot.get('total_pages', 1)
            
            # Validate and normalize page number
            page = max(1, min(int(page), total_pages))
            
            # Get the pre-generated plot for this page directly from the plot list
            if 'plots' in data_handler.boxwhisker_plot and len(data_handler.boxwhisker_plot['plots']) >= page:
                plot_fig = data_handler.boxwhisker_plot['plots'][page - 1]
            else:
                return {
                    'status': 'error',
                    'message': f'Invalid page number: {page}. Valid range is 1-{total_pages}'
                }
            
            # Get reports folder from config
            reports_folder = current_app.config.get('REPORTS_FOLDER')
            if not reports_folder:
                reports_folder = os.path.join(current_app.instance_path, 'sessions', session_id, 'reports')
                os.makedirs(reports_folder, exist_ok=True)
            
            # Save as HTML with unique filename
            viz_filename = f"vulnerability_plot_page{page}_{session_id}.html"
            
            # Import visualization module for the HTML generation function
            import app.visualization as viz
            html_path = viz.create_plotly_html(plot_fig, viz_filename)
            
            # Gather information about the wards on this page
            wards_info = data_handler.boxwhisker_plot.get('page_data', {}).get(str(page), [])
            
            # Prepare result with essential data
            result = {
                'status': 'success',
                'message': f'Successfully navigated to box plot page {page}',
                'image_path': html_path,
                'file_path': html_path,
                'current_page': int(page),
                'total_pages': int(total_pages),
                'viz_type': 'vulnerability_plot'
            }
            
            # Add AI explanation
            explanation = self.explain_boxplot_navigation(
                data_handler=data_handler,
                page_data={
                    'current_page': page,
                    'total_pages': total_pages,
                    'wards_info': wards_info
                },
                session_id=session_id
            )
            result['ai_response'] = explanation
            
            logger.info(f"Session {session_id}: Successfully navigated to boxplot page {page}/{total_pages}")
            return result
            
        except Exception as e:
            logger.error(f"Error navigating boxplot to page {page}: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'message': f'Error navigating boxplot: {str(e)}'
            }
    
    def explain_composite_map_navigation(self, data_handler, page_data, session_id):
        """Generate AI explanation for composite map navigation"""
        try:
            if self.llm_manager:
                # Create context for this specific page
                viz_metadata = {
                    'type': 'composite_map',
                    'data_summary': page_data.get('data_summary', {}),
                    'visual_elements': page_data.get('visual_elements', {}),
                    'current_page': page_data.get('current_page', 1),
                    'total_pages': page_data.get('total_pages', 1),
                    'model_details': page_data.get('model_details', {})
                }
                
                ai_response = self.llm_manager.explain_visualization(
                    session_id=session_id,
                    viz_type='composite_map',
                    context=viz_metadata
                )
                
                # Convert markdown to HTML
                from ...core.llm_manager import convert_markdown_to_html
                return convert_markdown_to_html(ai_response)
            else:
                # Fallback explanation
                page = page_data.get('current_page', 1)
                return f"<p>This composite map shows vulnerability classification for page {page}. Areas are color-coded by risk level: <strong>red</strong> for high risk, <strong>yellow</strong> for medium risk, and <strong>green</strong> for low risk.</p>"
                
        except Exception as e:
            logger.error(f"Error generating composite map explanation: {str(e)}")
            return "<p>This composite map shows vulnerability classification with color-coded risk levels.</p>"
    
    def explain_boxplot_navigation(self, data_handler, page_data, session_id):
        """Generate AI explanation for boxplot navigation"""
        try:
            if self.llm_manager:
                # Get current page information
                page = page_data.get('current_page', 1)
                total_pages = page_data.get('total_pages', 1)
                
                # Get ward information for this page
                # First try the provided wards_info
                wards_info = page_data.get('wards_info', [])
                
                # If not available, try to get from data_handler
                if not wards_info and hasattr(data_handler, 'boxwhisker_plot'):
                    wards_info = data_handler.boxwhisker_plot.get('page_data', {}).get(str(page), [])
                
                # Prepare the context for the LLM
                viz_metadata = {
                    'type': 'vulnerability_plot',
                    'data_summary': {
                        'current_page': page,
                        'total_pages': total_pages,
                        'wards_shown': [ward.get('ward_name', '') for ward in wards_info[:5]] if wards_info else []
                    },
                    'visual_elements': {
                        'plot_type': 'Box and whisker',
                        'color_scheme': 'By vulnerability category'
                    },
                    'ward_details': wards_info[:5] if wards_info else []  # Limit to first 5 for context size
                }
                
                ai_response = self.llm_manager.explain_visualization(
                    session_id=session_id,
                    viz_type='vulnerability_plot',
                    context=viz_metadata
                )
                
                # Convert markdown to HTML
                from ...core.llm_manager import convert_markdown_to_html
                return convert_markdown_to_html(ai_response)
            else:
                # Fallback explanation
                page = page_data.get('current_page', 1)
                total = page_data.get('total_pages', 1)
                return f"<p>This box plot shows vulnerability distribution for page {page} of {total}. Each box represents the distribution of vulnerability scores, with outliers shown as individual points.</p>"
                
        except Exception as e:
            logger.error(f"Error generating boxplot explanation: {str(e)}")
            return "<p>This box plot shows the distribution of vulnerability scores across different categories.</p>" 