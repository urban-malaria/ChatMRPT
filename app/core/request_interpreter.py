"""
True py-sidebot Implementation for ChatMRPT

Clean implementation following py-sidebot's actual pattern:
1. Create chat session with system prompt
2. Register actual Python functions as tools
3. Pass user messages directly to chat session
4. Let LLM handle tool selection and execution

Key components preserved:
- Memory integration
- Visualization explanation
- Data schema handling
- Conversational data access
- Streaming support
- Special workflows (permissions, forks)
"""

import logging
import json
import time
from typing import Dict, Any, List, Optional
from flask import current_app

logger = logging.getLogger(__name__)


class RequestInterpreter:
    """
    True py-sidebot inspired request interpreter for ChatMRPT.
    
    Simple approach: register Python functions as tools and let LLM choose.
    """
    
    def __init__(self, llm_manager, data_service, analysis_service, visualization_service):
        self.llm_manager = llm_manager
        self.data_service = data_service
        self.analysis_service = analysis_service
        self.visualization_service = visualization_service
        
        # py-sidebot approach: Simple conversation storage
        self.conversation_history = {}
        
        # Initialize memory system if available
        try:
            from app.services.memory_service import get_memory_service
            self.memory = get_memory_service()
        except Exception as e:
            logger.debug(f"Memory service not available: {e}")
            self.memory = None
        
        # Initialize conversational data access
        self.conversational_data_access = None
        
        # py-sidebot pattern: Register tools as actual Python functions
        self.tools = {}
        self._register_tools()
    
    def _register_tools(self):
        """Register actual Python functions as tools - true py-sidebot style."""
        logger.info("Registering tools - py-sidebot pattern")
        
        # Register analysis tools
        self.tools['run_complete_analysis'] = self._run_complete_analysis
        self.tools['run_composite_analysis'] = self._run_composite_analysis
        self.tools['run_pca_analysis'] = self._run_pca_analysis
        
        # Register visualization tools
        self.tools['create_vulnerability_map'] = self._create_vulnerability_map
        self.tools['create_box_plot'] = self._create_box_plot
        self.tools['create_pca_map'] = self._create_pca_map
        self.tools['create_variable_distribution'] = self._create_variable_distribution
        
        # Register data tools
        self.tools['execute_data_query'] = self._execute_data_query
        
        # Register explanation tools
        self.tools['explain_analysis_methodology'] = self._explain_analysis_methodology
        
        logger.info(f"Registered {len(self.tools)} tools")
    
    def process_message(self, user_message: str, session_id: str, session_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """py-sidebot pattern: Pass message directly to LLM with tools."""
        start_time = time.time()
        
        try:
            logger.info(f"Processing message for session {session_id}: {user_message[:100]}...")
            
            # Handle special workflows first
            special_result = self._handle_special_workflows(user_message, session_id, session_data)
            if special_result:
                return special_result
            
            # Get session context
            session_context = self._get_session_context(session_id, session_data)
            
            # Simple routing: no data = conversational, with data = tools available
            if not session_context.get('data_loaded', False):
                return self._simple_conversational_response(user_message, session_context)
            
            # py-sidebot approach: LLM with all tools
            result = self._llm_with_tools(user_message, session_context, session_id)
            
            # Store conversation
            self._store_conversation(session_id, user_message, result.get('response', ''))
            
            result['total_time'] = time.time() - start_time
            return result
            
        except Exception as e:
            logger.error(f"Error in py-sidebot processing: {e}")
            return {
                'status': 'error',
                'response': f'I encountered an issue: {str(e)}',
                'tools_used': []
            }
    
    def process_message_streaming(self, user_message: str, session_id: str, session_data: Dict[str, Any] = None):
        """Streaming version for better UX."""
        try:
            # Handle special workflows
            special_result = self._handle_special_workflows(user_message, session_id, session_data)
            if special_result:
                yield {
                    'content': special_result.get('response', ''),
                    'status': special_result.get('status', 'success'),
                    'done': True
                }
                return
            
            # Get session context
            session_context = self._get_session_context(session_id, session_data)
            
            if not session_context.get('data_loaded', False):
                response = self._simple_conversational_response(user_message, session_context)
                yield {
                    'content': response.get('response', ''),
                    'status': 'success',
                    'done': True
                }
                return
            
            # Stream with tools
            yield from self._stream_with_tools(user_message, session_context, session_id)
            
        except Exception as e:
            logger.error(f"Error in streaming: {e}")
            yield {
                'content': f'I encountered an issue: {str(e)}',
                'status': 'error',
                'done': True
            }
    
    def _llm_with_tools(self, user_message: str, session_context: Dict, session_id: str) -> Dict[str, Any]:
        """py-sidebot pattern: Pass message to LLM with all tools available."""
        system_prompt = self._build_system_prompt(session_context)
        
        # Convert tools to OpenAI function format
        functions = []
        for tool_name, tool_func in self.tools.items():
            functions.append({
                'name': tool_name,
                'description': tool_func.__doc__ or f"Execute {tool_name}",
                'parameters': self._get_tool_parameters(tool_name)
            })
        
        # Single LLM call
        response = self.llm_manager.generate_with_functions(
            messages=[{"role": "user", "content": user_message}],
            system_prompt=system_prompt,
            functions=functions,
            temperature=0.7,
            session_id=session_id
        )
        
        # Process response
        return self._process_llm_response(response, user_message, session_id)
    
    def _stream_with_tools(self, user_message: str, session_context: Dict, session_id: str):
        """Stream LLM response with tools."""
        system_prompt = self._build_system_prompt(session_context)
        
        functions = []
        for tool_name, tool_func in self.tools.items():
            functions.append({
                'name': tool_name,
                'description': tool_func.__doc__ or f"Execute {tool_name}",
                'parameters': self._get_tool_parameters(tool_name)
            })
        
        for chunk in self.llm_manager.generate_with_functions_streaming(
            messages=[{"role": "user", "content": user_message}],
            system_prompt=system_prompt,
            functions=functions,
            temperature=0.7,
            session_id=session_id
        ):
            if chunk.get('function_call'):
                # Execute tool
                function_name = chunk['function_call']['name']
                print(f"\n🛠️  TOOL CALL DETECTED: {function_name}")
                print(f"📋 Arguments: {chunk['function_call']['arguments']}")
                
                if function_name in self.tools:
                    try:
                        args = json.loads(chunk['function_call']['arguments'])
                        args['session_id'] = session_id  # Ensure session_id is included
                        
                        print(f"🚀 Executing tool: {function_name}")
                        result = self.tools[function_name](**args)
                        print(f"✅ Tool {function_name} completed")
                        
                        # Handle structured responses from tools
                        if isinstance(result, dict) and 'response' in result:
                            # Tool returned structured response with visualizations
                            yield {
                                'content': result['response'],
                                'status': result.get('status', 'success'),
                                'visualizations': result.get('visualizations', []),
                                'tools_used': result.get('tools_used', [function_name]),
                                'done': True
                            }
                            response_content = result['response']
                        else:
                            # Tool returned simple string response
                            yield {
                                'content': result,
                                'status': 'success',
                                'tools_used': [function_name],
                                'done': True
                            }
                            response_content = result
                        
                        self._store_conversation(session_id, user_message, response_content)
                        return
                    except Exception as e:
                        yield {
                            'content': f"Error executing {function_name}: {str(e)}",
                            'status': 'error',
                            'done': True
                        }
                        return
            
            if chunk.get('done'):
                content = chunk.get('content', '')
                yield {
                    'content': content,
                    'status': 'success',
                    'done': True
                }
                self._store_conversation(session_id, user_message, content)
                return
            else:
                yield {
                    'content': chunk.get('content', ''),
                    'status': 'success',
                    'done': False
                }
    
    def _process_llm_response(self, response: Dict, user_message: str, session_id: str) -> Dict[str, Any]:
        """Process LLM response and execute tools if needed."""
        if 'function_call' in response and response['function_call']:
            function_name = response['function_call']['name']
            
            if function_name in self.tools:
                try:
                    args = json.loads(response['function_call']['arguments'])
                    args['session_id'] = session_id  # Ensure session_id is included
                    
                    result = self.tools[function_name](**args)
                    
                    # Handle structured responses from tools
                    if isinstance(result, dict) and 'response' in result:
                        # Tool returned structured response with visualizations
                        return {
                            'response': result['response'],
                            'visualizations': result.get('visualizations', []),
                            'tools_used': result.get('tools_used', [function_name]),
                            'status': result.get('status', 'success')
                        }
                    else:
                        # Tool returned simple string response
                        return {
                            'response': result,
                            'tools_used': [function_name],
                            'status': 'success'
                        }
                except Exception as e:
                    return {
                        'response': f"Error executing {function_name}: {str(e)}",
                        'tools_used': [],
                        'status': 'error'
                    }
        
        # Pure conversational response
        return {
            'response': response.get('content', 'No response'),
            'tools_used': [],
            'status': 'success'
        }
    
    # Tool Functions - These are the actual functions registered as tools
    def _run_complete_analysis(self, session_id: str, variables: Optional[List[str]] = None):
        """Run complete dual-method malaria risk analysis (composite scoring + PCA)."""
        try:
            result = self.analysis_service.run_complete_analysis(session_id, variables=variables)
            message = result.get('message', 'Complete analysis finished successfully')
            
            # Auto-explain any visualizations
            if result.get('visualizations'):
                explanations = []
                for viz in result['visualizations']:
                    if viz.get('file_path'):
                        explanation = self._explain_visualization_universally(
                            viz['file_path'], viz.get('type', 'visualization'), session_id
                        )
                        explanations.append(explanation)
                if explanations:
                    message += "\\n\\n" + "\\n\\n".join(explanations)
            
            return message
        except Exception as e:
            return f"Error running complete analysis: {str(e)}"
    
    def _run_composite_analysis(self, session_id: str, variables: Optional[List[str]] = None):
        """Run composite scoring malaria risk analysis with equal weights."""
        try:
            result = self.analysis_service.run_composite_analysis(session_id, variables=variables)
            return result.get('message', 'Composite analysis completed successfully')
        except Exception as e:
            return f"Error running composite analysis: {str(e)}"
    
    def _run_pca_analysis(self, session_id: str, variables: Optional[List[str]] = None):
        """Run PCA malaria risk analysis."""
        try:
            result = self.analysis_service.run_pca_analysis(session_id, variables=variables)
            return result.get('message', 'PCA analysis completed successfully')
        except Exception as e:
            return f"Error running PCA analysis: {str(e)}"
    
    def _create_vulnerability_map(self, session_id: str, method: str = 'composite'):
        """Create vulnerability choropleth map for malaria risk visualization."""
        try:
            result = self.visualization_service.create_vulnerability_map(session_id, method=method)
            message = result.get('message', f'Vulnerability map created using {method} method')
            
            # Auto-explain the visualization if file_path exists
            if result.get('file_path'):
                explanation = self._explain_visualization_universally(
                    result['file_path'], 'vulnerability_map', session_id
                )
                message += f"\\n\\n{explanation}"
            
            # Return structured response if successful
            if result.get('status') == 'success' and result.get('web_path'):
                return {
                    'response': message,
                    'visualizations': [{
                        'type': result.get('visualization_type', 'vulnerability_map'),
                        'file_path': result.get('file_path', ''),
                        'path': result.get('web_path', ''),
                        'url': result.get('web_path', ''),
                        'title': f"Vulnerability Map ({method.title()} Method)",
                        'description': f"Ward vulnerability classification using {method} method"
                    }],
                    'tools_used': ['create_vulnerability_map'],
                    'status': 'success'
                }
            else:
                return f"Error creating vulnerability map: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error creating vulnerability map: {str(e)}"
    
    def _create_box_plot(self, session_id: str, method: str = 'composite'):
        """Create box plots showing vulnerability score distributions."""
        try:
            result = self.visualization_service.create_box_plot(session_id, method=method)
            message = result.get('message', f'Box plot created for {method} scores')
            
            # Auto-explain the visualization if file_path exists
            if result.get('file_path'):
                explanation = self._explain_visualization_universally(
                    result['file_path'], 'box_plot', session_id
                )
                message += f"\\n\\n{explanation}"
            
            # Return structured response if successful
            if result.get('status') == 'success' and result.get('web_path'):
                return {
                    'response': message,
                    'visualizations': [{
                        'type': result.get('visualization_type', 'box_plot'),
                        'file_path': result.get('file_path', ''),
                        'path': result.get('web_path', ''),
                        'url': result.get('web_path', ''),
                        'title': f"Box Plot ({method.title()} Method)",
                        'description': f"Vulnerability score distribution using {method} method"
                    }],
                    'tools_used': ['create_box_plot'],
                    'status': 'success'
                }
            else:
                return f"Error creating box plot: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error creating box plot: {str(e)}"
    
    def _create_pca_map(self, session_id: str):
        """Create PCA-based vulnerability map."""
        try:
            result = self.visualization_service.create_pca_map(session_id)
            message = result.get('message', 'PCA vulnerability map created')
            
            # Auto-explain the visualization if file_path exists
            if result.get('file_path'):
                explanation = self._explain_visualization_universally(
                    result['file_path'], 'pca_map', session_id
                )
                message += f"\\n\\n{explanation}"
            
            # Return structured response if successful
            if result.get('status') == 'success' and result.get('web_path'):
                return {
                    'response': message,
                    'visualizations': [{
                        'type': result.get('visualization_type', 'pca_map'),
                        'file_path': result.get('file_path', ''),
                        'path': result.get('web_path', ''),
                        'url': result.get('web_path', ''),
                        'title': "PCA Vulnerability Map",
                        'description': "Ward vulnerability classification using PCA method"
                    }],
                    'tools_used': ['create_pca_map'],
                    'status': 'success'
                }
            else:
                return f"Error creating PCA map: {result.get('message', 'Unknown error')}"
        except Exception as e:
            return f"Error creating PCA map: {str(e)}"
    
    def _create_variable_distribution(self, session_id: str, variable_name: str):
        """Create spatial distribution map for any variable from the dataset."""
        try:
            from app.tools.variable_distribution import VariableDistribution
            
            # Create the tool instance
            tool = VariableDistribution(variable_name=variable_name)
            
            # Execute the tool
            result = tool.execute(session_id)
            
            if result.success:
                message = result.message
                
                # Auto-explain the visualization if it was created
                if result.data and result.data.get('file_path'):
                    explanation = self._explain_visualization_universally(
                        result.data['file_path'], 'variable_distribution', session_id
                    )
                    message += f"\n\n{explanation}"
                
                # Return structured response with visualization data
                return {
                    'response': message,
                    'visualizations': [{
                        'type': result.data.get('chart_type', 'variable_distribution'),
                        'file_path': result.data.get('file_path', ''),
                        'path': result.data.get('web_path', ''),  # Frontend expects 'path' key
                        'url': result.data.get('web_path', ''),   # Also provide 'url' for compatibility
                        'title': f"{result.data.get('variable', 'Variable')} Distribution",
                        'description': f"Spatial distribution of {result.data.get('variable', 'variable')} across study area"
                    }] if result.data else [],
                    'tools_used': ['create_variable_distribution'],
                    'status': 'success'
                }
            else:
                return f"Error creating variable distribution: {result.message}"
        except Exception as e:
            return f"Error creating variable distribution: {str(e)}"
    
    def _execute_data_query(self, session_id: str, query: str, code: Optional[str] = None):
        """Execute data queries, generate plots, and perform statistical analysis on the malaria data."""
        try:
            # Check if data is available via data service first
            data_handler = self.data_service.get_handler(session_id)
            if not data_handler or not hasattr(data_handler, 'csv_data') or data_handler.csv_data is None:
                return "Error executing query: No data available. Please upload data first."
            
            # Initialize conversational data access for this session
            # Always create a new instance to ensure proper session context
            logger.info(f"Creating ConversationalDataAccess for session: {session_id}")
            from app.services.conversational_data_access import ConversationalDataAccess
            conversational_data_access = ConversationalDataAccess(session_id, self.llm_manager)
            
            if code:
                logger.info(f"Executing provided code: {code}")
                result = conversational_data_access.execute_code(code)
            else:
                logger.info(f"Processing query: {query}")
                result = conversational_data_access.process_query(query)
            
            if result.get('success'):
                # Get the formatted output from the executed code
                output = result.get('output', '').strip()
                plot_data = result.get('plot_data')
                
                # Return structured response with visualizations
                response_data = {
                    'response': output if output else f"Query executed successfully: {query}",
                    'visualizations': [],
                    'tools_used': ['execute_data_query'],
                    'status': 'success'
                }
                
                # Add visualization if plot was generated
                if plot_data:
                    # Determine specific chart type from query context
                    viz_type = self._determine_chart_type_from_query(query)
                    response_data['visualizations'].append({
                        'type': viz_type,
                        'data': plot_data,
                        'title': f'Analysis Visualization - {query[:50]}...' if len(query) > 50 else f'Analysis Visualization - {query}'
                    })
                
                return response_data
            else:
                error_msg = f"Error executing query: {result.get('error', 'Unknown error')}"
                return error_msg
        except Exception as e:
            return f"Error in data query: {str(e)}"
    
    def _determine_chart_type_from_query(self, query: str) -> str:
        """Determine specific chart type from user query to avoid visualization conflicts."""
        query_lower = query.lower()
        
        # Check for specific chart types mentioned in the query
        if 'scatter' in query_lower:
            return 'scatter_plot'
        elif 'histogram' in query_lower or 'hist' in query_lower:
            return 'histogram'
        elif ('distribution' in query_lower and 'plot' in query_lower) or 'distplot' in query_lower:
            return 'histogram'  # Most distribution plots are histograms
        elif 'box plot' in query_lower or 'boxplot' in query_lower:
            return 'box_plot'
        elif 'bar chart' in query_lower or 'bar plot' in query_lower or 'barplot' in query_lower:
            return 'bar_chart'
        elif 'line chart' in query_lower or 'line plot' in query_lower or 'lineplot' in query_lower:
            return 'line_plot'
        elif 'pie chart' in query_lower or 'pie plot' in query_lower:
            return 'pie_chart'
        elif 'heatmap' in query_lower or 'heat map' in query_lower:
            return 'heatmap'
        elif 'density' in query_lower and 'plot' in query_lower:
            return 'density_plot'
        elif 'violin' in query_lower:
            return 'violin_plot'
        elif 'correlation' in query_lower and 'plot' in query_lower:
            return 'scatter_plot'  # Correlation plots are usually scatter plots
        else:
            # Return a unique type for each general plot to prevent conflicts
            import time
            return f'conversational_plot_{int(time.time())}'
    
    def _explain_analysis_methodology(self, session_id: str, method: str = 'both'):
        """Explain how malaria risk analysis methodologies work (composite scoring, PCA, or both)."""
        try:
            # Use LLM to generate methodology explanation
            if method == 'composite':
                explanation = """**Composite Scoring Methodology**

Composite scoring combines multiple malaria risk indicators into a single vulnerability score:

1. **Variable Selection**: Selects scientifically-validated variables based on Nigerian geopolitical zones
2. **Normalization**: Standardizes all variables to 0-1 scale for fair comparison
3. **Equal Weighting**: All variables contribute equally to the final score
4. **Aggregation**: Sums normalized values to create composite vulnerability score
5. **Ranking**: Ranks wards from highest to lowest risk for intervention targeting

This method is transparent, interpretable, and follows WHO guidelines for malaria stratification."""
            
            elif method == 'pca':
                explanation = """**Principal Component Analysis (PCA) Methodology**

PCA reduces dimensionality while preserving variance in malaria risk data:

1. **Data Standardization**: Centers and scales all variables
2. **Covariance Analysis**: Identifies relationships between variables
3. **Component Extraction**: Finds principal components that explain maximum variance
4. **Weight Calculation**: Automatically determines variable importance
5. **Score Generation**: Creates data-driven vulnerability scores
6. **Interpretation**: First component typically captures overall malaria risk

This method is statistically robust and reveals hidden patterns in the data."""
            
            else:  # both
                explanation = """**Dual-Method Malaria Risk Analysis**

ChatMRPT uses both composite scoring and PCA for comprehensive assessment:

**Composite Scoring**: Transparent, equal-weighted approach following WHO guidelines
- Pros: Interpretable, policy-friendly, scientifically grounded
- Use when: Clear intervention priorities needed

**PCA Analysis**: Statistical approach revealing data patterns
- Pros: Objective, captures complex relationships, statistically robust
- Use when: Exploring underlying risk structures

**Comparison**: Both methods often agree on high-risk areas but may differ in rankings. Use both for robust decision-making and to validate findings."""
            
            return explanation
        except Exception as e:
            return f"Error explaining methodology: {str(e)}"
    
    # Helper Methods
    def _get_tool_parameters(self, tool_name: str) -> Dict[str, Any]:
        """Get parameter schema for a tool."""
        base_params = {
            'type': 'object',
            'properties': {
                'session_id': {'type': 'string', 'description': 'Session identifier'}
            },
            'required': ['session_id']
        }
        
        if 'analysis' in tool_name:
            base_params['properties']['variables'] = {
                'type': 'array',
                'items': {'type': 'string'},
                'description': 'Optional custom variables for analysis'
            }
        
        if 'map' in tool_name or 'plot' in tool_name:
            base_params['properties']['method'] = {
                'type': 'string',
                'enum': ['composite', 'pca'],
                'description': 'Analysis method to visualize'
            }
        
        if tool_name == 'execute_data_query':
            base_params['properties'].update({
                'query': {'type': 'string', 'description': 'Natural language query about the data'},
                'code': {'type': 'string', 'description': 'Optional Python code to execute'}
            })
            base_params['required'].append('query')
        
        if tool_name == 'explain_analysis_methodology':
            base_params['properties']['method'] = {
                'type': 'string',
                'enum': ['composite', 'pca', 'both'],
                'description': 'Which methodology to explain'
            }
        
        if tool_name == 'create_variable_distribution':
            base_params['properties']['variable_name'] = {
                'type': 'string',
                'description': 'Name of the variable to visualize (e.g., pfpr, rainfall, housing_quality)'
            }
            base_params['required'].append('variable_name')
        
        return base_params
    
    def _build_system_prompt(self, session_context: Dict) -> str:
        """Build system prompt with session context."""
        base_prompt = """You are a specialized malaria epidemiologist embedded in ChatMRPT, a malaria risk assessment system.

## Your Expertise
- Malaria transmission dynamics and vector ecology
- Urban microstratification for intervention targeting
- Statistical analysis (composite scoring, PCA)
- Nigerian health systems and geopolitical zones
- WHO guidelines for malaria program planning

## Current Session"""
        
        context_info = f"""
- Geographic Area: {session_context.get('state_name', 'Not specified')}
- Data Status: {session_context.get('current_data', 'No data uploaded')}
- Analysis Complete: {session_context.get('analysis_complete', False)}
"""
        
        if session_context.get('data_schema'):
            context_info += f"- {session_context['data_schema']}\\n"
        
        tool_guidance = """
## Available Tools
Use the appropriate tool based on user requests:

- **run_complete_analysis**: For full dual-method analysis
- **run_composite_analysis**: For composite scoring only  
- **run_pca_analysis**: For PCA analysis only
- **create_vulnerability_map**: For choropleth risk maps
- **create_box_plot**: For vulnerability score distributions
- **create_variable_distribution**: For spatial distribution maps of any variable
- **execute_data_query**: For data questions, plots, statistics
- **explain_analysis_methodology**: For methodology explanations

Always provide malaria epidemiology context and intervention guidance with your responses."""
        
        return f"{base_prompt}{context_info}{tool_guidance}"
    
    def _simple_conversational_response(self, user_message: str, session_context: Dict) -> Dict[str, Any]:
        """Simple conversational response when no data available."""
        system_prompt = self._build_system_prompt(session_context)
        
        response = self.llm_manager.generate_response(
            prompt=user_message,
            system_message=system_prompt,
            temperature=0.7
        )
        
        return {
            'response': response,
            'tools_used': [],
            'status': 'success'
        }
    
    def _get_session_context(self, session_id: str, session_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get comprehensive session context."""
        try:
            # Use provided session data or try to get from Flask session
            if session_data is None:
                try:
                    from flask import session
                    session_data = dict(session)
                except RuntimeError:
                    # Working outside of request context, use empty session
                    session_data = {}
            
            # Check data availability
            data_loaded = session_data.get('data_loaded', False) or session_data.get('csv_loaded', False)
            
            context = {
                'data_loaded': data_loaded,
                'state_name': session_data.get('state_name', 'Not specified'),
                'current_data': f"CSV loaded: {session_data.get('csv_loaded', False)}, Shapefile: {session_data.get('shapefile_loaded', False)}" if data_loaded else "No data uploaded",
                'analysis_complete': session_data.get('analysis_complete', False),
                'ward_column': session_data.get('ward_column', 'Not identified'),
                'variables_used': session_data.get('variables_used', []),
                'conversation_history': self.conversation_history.get(session_id, [])
            }
            
            # Add data schema if available
            if data_loaded and self.data_service:
                try:
                    data_handler = self.data_service.get_handler(session_id)
                    if data_handler and hasattr(data_handler, 'csv_data') and data_handler.csv_data is not None:
                        df = data_handler.csv_data
                        context['data_schema'] = f"Dataset: {len(df)} rows, {len(df.columns)} columns"
                except Exception as e:
                    logger.debug(f"Could not get data schema: {e}")
            
            # Add memory context if available
            if self.memory:
                try:
                    memory_context = self.memory.get_context(session_id)
                    if memory_context:
                        context['recent_topics'] = getattr(memory_context, 'entities_mentioned', [])[:5]
                except Exception as e:
                    logger.debug(f"Memory context error: {e}")
            
            return context
            
        except Exception as e:
            logger.error(f"Error getting session context: {e}")
            return {'data_loaded': False, 'state_name': 'Not specified'}
    
    def _handle_special_workflows(self, user_message: str, session_id: str, session_data: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Handle special workflows like permissions and forks."""
        if session_data is None:
            try:
                from flask import session
                session_data = dict(session)
            except RuntimeError:
                # Working outside of request context
                session_data = {}
        
        # Analysis permission workflow
        if session_data.get('should_ask_analysis_permission', False):
            if self._is_confirmation_message(user_message):
                # Note: session state updates would need to be handled differently in streaming context
                return self._execute_automatic_analysis(session_id)
        
        # Data description workflow
        if session_data.get('should_describe_data', False):
            result = self._generate_automatic_data_description(session_id)
            self._store_conversation(session_id, user_message, result.get('response', ''))
            return result
        
        # Fork detection for what-if scenarios
        if 'what if' in user_message.lower() or 'suppose' in user_message.lower():
            fork_id = f"{session_id}_fork_{int(time.time())}"
            return {
                'response': f"🔀 **Exploring scenario**: {user_message}\\n\\nLet me help you explore this what-if scenario...",
                'status': 'success',
                'forked': True,
                'fork_id': fork_id
            }
        
        return None
    
    def _store_conversation(self, session_id: str, user_message: str, response: str):
        """Store conversation with memory integration."""
        if session_id not in self.conversation_history:
            self.conversation_history[session_id] = []
        
        self.conversation_history[session_id].append({
            'user': user_message,
            'assistant': response,
            'timestamp': time.time()
        })
        
        # Keep only last 10 exchanges
        if len(self.conversation_history[session_id]) > 10:
            self.conversation_history[session_id] = self.conversation_history[session_id][-10:]
        
        # Store in memory if available
        if self.memory:
            try:
                self.memory.store_conversation_turn(
                    session_id=session_id,
                    user_message=user_message,
                    assistant_response=response,
                    timestamp=time.time()
                )
            except Exception as e:
                logger.debug(f"Memory storage error: {e}")
    
    def _explain_visualization_universally(self, viz_path: str, viz_type: str, session_id: str) -> str:
        """Universal visualization explanation."""
        try:
            from app.services.universal_viz_explainer import get_universal_viz_explainer
            explainer = get_universal_viz_explainer(llm_manager=self.llm_manager)
            return explainer.explain_visualization(viz_path, viz_type, session_id)
        except Exception as e:
            logger.error(f"Visualization explanation error: {e}")
            viz_name = viz_type.replace('_', ' ').title()
            return f"📊 **{viz_name} Created** - This visualization shows malaria risk analysis results for intervention planning."
    
    def _is_confirmation_message(self, message: str) -> bool:
        """Check if message is confirmation."""
        return message.lower().strip() in ['yes', 'y', 'ok', 'okay', 'sure', 'proceed', 'continue']
    
    def _execute_automatic_analysis(self, session_id: str) -> Dict[str, Any]:
        """Execute automatic analysis after permission."""
        try:
            result = self._run_complete_analysis(session_id)
            return {
                'response': result,
                'status': 'success',
                'tools_used': ['run_complete_analysis']
            }
        except Exception as e:
            return {
                'response': f'Error running automatic analysis: {str(e)}',
                'status': 'error',
                'tools_used': []
            }
    
    def _generate_automatic_data_description(self, session_id: str) -> Dict[str, Any]:
        """Generate automatic data description."""
        try:
            result = self._execute_data_query(session_id, "Describe the uploaded dataset including number of wards, variables, and data quality")
            return {
                'response': result,
                'status': 'success',
                'tools_used': ['execute_data_query']
            }
        except Exception as e:
            return {
                'response': f'Error describing data: {str(e)}',
                'status': 'error',
                'tools_used': []
            }