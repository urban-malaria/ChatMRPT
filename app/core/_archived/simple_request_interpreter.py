"""
Simplified Request Interpreter for ChatMRPT

Following py-sidebot's architecture:
- Direct tool registration
- LLM handles tool selection
- No complex routing logic
- Clear, simple flow
"""

import logging
import json
import time
from typing import Dict, Any, List, Optional
from flask import current_app

from .unified_data_state import get_data_state
from .direct_tools import TOOLS

logger = logging.getLogger(__name__)


class SimpleRequestInterpreter:
    """
    Simplified request interpreter following py-sidebot pattern.
    
    Key principles:
    1. Tools are just functions
    2. LLM decides which tool to use
    3. No complex routing or pattern matching
    4. State is managed by UnifiedDataState
    """
    
    def __init__(self, llm_manager):
        self.llm_manager = llm_manager
        self.conversation_history = {}
        
        # Register all tools as simple functions
        self.tools = TOOLS.copy()
        
        # Add any additional tools needed
        self._register_additional_tools()
    
    def _register_additional_tools(self):
        """Register any additional tools beyond the core set."""
        # Analysis tools
        self.tools['run_malaria_risk_analysis'] = self._run_malaria_risk_analysis
        # Disabled single-method tools to prevent confusion
        # self.tools['run_composite_analysis'] = self._run_composite_analysis
        # self.tools['run_pca_analysis'] = self._run_pca_analysis
        
        # Visualization tools
        self.tools['create_box_plot'] = self._create_box_plot
        self.tools['create_decision_tree'] = self._create_decision_tree
        self.tools['create_urban_extent_map'] = self._create_urban_extent_map
        
        logger.info(f"Registered {len(self.tools)} tools total")
    
    def process_message(self, user_message: str, session_id: str, session_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Process user message with simple, direct flow.
        
        No complex routing - just:
        1. Build context
        2. Give to LLM with tools
        3. Execute any tool calls
        4. Return response
        """
        try:
            logger.info(f"Processing message for session {session_id}: {user_message[:100]}...")
            
            # Get current data state
            data_state = get_data_state(session_id)
            
            # Build system prompt based on current state
            system_prompt = self._build_simple_system_prompt(data_state)
            
            # Convert tools to OpenAI function format
            functions = self._get_tool_definitions()
            
            # Single LLM call with all tools available
            response = self.llm_manager.generate_with_functions(
                messages=[{"role": "user", "content": user_message}],
                system_prompt=system_prompt,
                functions=functions,
                temperature=0.7,
                session_id=session_id
            )
            
            # Process response
            return self._process_llm_response(response, session_id)
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return {
                'status': 'error',
                'response': f'I encountered an issue: {str(e)}',
                'tools_used': []
            }
    
    def process_message_streaming(self, user_message: str, session_id: str, session_data: Dict[str, Any] = None):
        """Streaming version for better UX."""
        try:
            # Get current data state
            data_state = get_data_state(session_id)
            
            # Build system prompt
            system_prompt = self._build_simple_system_prompt(data_state)
            
            # Get tool definitions
            functions = self._get_tool_definitions()
            
            # Stream response
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
                    if function_name in self.tools:
                        try:
                            args = json.loads(chunk['function_call'].get('arguments', '{}'))
                            args['session_id'] = session_id
                            
                            logger.debug(f"Executing tool: {function_name}")
                            result = self.tools[function_name](**args)
                            
                            # Handle different result types
                            if isinstance(result, dict) and 'response' in result:
                                yield {
                                    'content': result['response'],
                                    'status': result.get('status', 'success'),
                                    'visualizations': result.get('visualizations', []),
                                    'tools_used': [function_name],
                                    'done': True
                                }
                            else:
                                yield {
                                    'content': str(result),
                                    'status': 'success',
                                    'tools_used': [function_name],
                                    'done': True
                                }
                        except Exception as e:
                            logger.error(f"Tool execution error: {e}")
                            yield {
                                'content': f"Error executing {function_name}: {str(e)}",
                                'status': 'error',
                                'done': True
                            }
                    else:
                        yield {
                            'content': f"Unknown tool: {function_name}",
                            'status': 'error',
                            'done': True
                        }
                else:
                    # Regular content
                    yield {
                        'content': chunk.get('content', ''),
                        'status': 'success',
                        'done': chunk.get('done', False)
                    }
                    
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            yield {
                'content': f'Error: {str(e)}',
                'status': 'error',
                'done': True
            }
    
    def _build_simple_system_prompt(self, data_state) -> str:
        """Build system prompt based on current data state."""
        stage = data_state.get_stage()
        
        base_prompt = """You are a malaria epidemiologist assistant in ChatMRPT.

Your role is to help users analyze malaria risk data and create intervention strategies.
"""
        
        if stage == 'no_data':
            state_prompt = """
No data is currently loaded. Guide the user to upload their data:
- CSV file with ward-level data
- Shapefile with ward boundaries
"""
        elif stage == 'pre_analysis':
            state_prompt = f"""
Data loaded: {data_state.current_data.shape if data_state.current_data is not None else 'Unknown shape'}

You can:
- Explore the data (use execute_sql_query or execute_python_code)
- Run malaria risk analysis (use run_malaria_risk_analysis)
- Answer questions about the data
"""
        else:  # post_analysis
            state_prompt = f"""
Analysis complete! Full results available.

You can:
- Query rankings: "Show top 10 highest risk wards" → execute_sql_query
- Analyze relationships: "Correlation between rainfall and malaria" → execute_python_code  
- Create visualizations: "Show vulnerability map" → create_vulnerability_map
- Compare methods: "How do PCA and composite rankings differ?" → execute_sql_query or execute_python_code

The data includes analysis results: composite_score, pca_score, rankings, and risk categories.
"""
        
        return base_prompt + state_prompt
    
    def _get_tool_definitions(self) -> List[Dict]:
        """Get tool definitions in OpenAI function format."""
        definitions = []
        
        # Define tool schemas
        tool_schemas = {
            'execute_sql_query': {
                'description': 'Execute SQL query on the dataset. Table name is always "df".',
                'parameters': {
                    'type': 'object',
                    'properties': {
                        'query': {
                            'type': 'string',
                            'description': 'SQL query to execute (e.g., SELECT * FROM df ORDER BY composite_score DESC LIMIT 10)'
                        }
                    },
                    'required': ['query']
                }
            },
            'execute_python_code': {
                'description': 'Execute Python code for data analysis. Variables available: df, pd, np, plt, sns.',
                'parameters': {
                    'type': 'object',
                    'properties': {
                        'code': {
                            'type': 'string',
                            'description': 'Python code to execute'
                        }
                    },
                    'required': ['code']
                }
            },
            'create_vulnerability_map': {
                'description': 'Create a vulnerability map visualization',
                'parameters': {
                    'type': 'object',
                    'properties': {
                        'method': {
                            'type': 'string',
                            'enum': ['composite', 'pca'],
                            'description': 'Analysis method to visualize'
                        }
                    },
                    'required': []
                }
            },
            'run_malaria_risk_analysis': {
                'description': 'Run complete malaria risk analysis (both composite and PCA methods)',
                'parameters': {
                    'type': 'object',
                    'properties': {},
                    'required': []
                }
            }
        }
        
        # Create definitions for registered tools
        for tool_name, tool_func in self.tools.items():
            if tool_name in tool_schemas:
                definitions.append({
                    'name': tool_name,
                    'description': tool_schemas[tool_name]['description'],
                    'parameters': tool_schemas[tool_name]['parameters']
                })
            else:
                # Generic definition for other tools
                definitions.append({
                    'name': tool_name,
                    'description': tool_func.__doc__ or f'Execute {tool_name}',
                    'parameters': {
                        'type': 'object',
                        'properties': {},
                        'required': []
                    }
                })
        
        return definitions
    
    def _process_llm_response(self, response: Dict, session_id: str) -> Dict[str, Any]:
        """Process LLM response and execute any tool calls."""
        if 'function_call' in response:
            function_name = response['function_call']['name']
            
            if function_name in self.tools:
                try:
                    args = json.loads(response['function_call'].get('arguments', '{}'))
                    args['session_id'] = session_id
                    
                    result = self.tools[function_name](**args)
                    
                    if isinstance(result, dict):
                        return result
                    else:
                        return {
                            'response': str(result),
                            'status': 'success',
                            'tools_used': [function_name]
                        }
                except Exception as e:
                    return {
                        'response': f"Error executing {function_name}: {str(e)}",
                        'status': 'error',
                        'tools_used': [function_name]
                    }
            else:
                return {
                    'response': f"Unknown tool: {function_name}",
                    'status': 'error',
                    'tools_used': []
                }
        else:
            # Direct response without tool use
            return {
                'response': response.get('content', ''),
                'status': 'success',
                'tools_used': []
            }
    
    # Simple tool wrappers that delegate to existing functionality
    async def _run_malaria_risk_analysis(self, session_id: str) -> Dict[str, Any]:
        """Run complete analysis."""
        try:
            from app.tools.complete_analysis_tools import RunMalariaRiskAnalysis
            tool = RunMalariaRiskAnalysis()
            result = tool.execute(session_id=session_id)
            
            if result.success:
                return {
                    'response': result.message,
                    'status': 'success',
                    'visualizations': result.data.get('visualizations', []) if result.data else []
                }
            else:
                return {
                    'response': result.message,
                    'status': 'error'
                }
        except Exception as e:
            return {'response': f'Error: {str(e)}', 'status': 'error'}
    
    async def _run_composite_analysis(self, session_id: str) -> Dict[str, Any]:
        """Run composite analysis only."""
        try:
            from app.services.container import get_service_container
            container = get_service_container()
            analysis_service = container.get('analysis_service')
            
            result = analysis_service.run_composite_analysis(session_id)
            return {
                'response': result.get('message', 'Composite analysis complete'),
                'status': result.get('status', 'success')
            }
        except Exception as e:
            return {'response': f'Error: {str(e)}', 'status': 'error'}
    
    async def _run_pca_analysis(self, session_id: str) -> Dict[str, Any]:
        """Run PCA analysis only."""
        try:
            from app.services.container import get_service_container
            container = get_service_container()
            analysis_service = container.get('analysis_service')
            
            result = analysis_service.run_pca_analysis(session_id)
            return {
                'response': result.get('message', 'PCA analysis complete'),
                'status': result.get('status', 'success')
            }
        except Exception as e:
            return {'response': f'Error: {str(e)}', 'status': 'error'}
    
    async def _create_box_plot(self, session_id: str, method: str = 'composite') -> Dict[str, Any]:
        """Create box plot visualization."""
        try:
            from app.services.container import get_service_container
            container = get_service_container()
            viz_service = container.get('visualization_service')
            
            result = viz_service.create_box_plot(session_id, method=method)
            
            if result.get('status') == 'success':
                return {
                    'response': f'Created {method} box plot',
                    'visualizations': [{
                        'type': 'box_plot',
                        'path': result.get('web_path', ''),
                        'title': f'Box Plot ({method.title()} Method)'
                    }],
                    'status': 'success'
                }
            else:
                return {
                    'response': result.get('message', 'Failed to create box plot'),
                    'status': 'error'
                }
        except Exception as e:
            return {'response': f'Error: {str(e)}', 'status': 'error'}
    
    async def _create_decision_tree(self, session_id: str) -> Dict[str, Any]:
        """Create decision tree visualization."""
        try:
            from app.services.container import get_service_container
            container = get_service_container()
            viz_service = container.get('visualization_service')
            
            # Decision tree typically uses composite method
            data_handler = container.get('data_service').get_handler(session_id)
            unified_dataset = data_handler.get_unified_dataset()
            
            if unified_dataset is None:
                return {
                    'response': 'No analysis data available',
                    'status': 'error'
                }
            
            result = viz_service.decision_tree(unified_dataset, session_id=session_id)
            
            if result.get('status') == 'success':
                return {
                    'response': 'Created decision tree visualization',
                    'visualizations': [{
                        'type': 'decision_tree',
                        'path': result.get('web_path', ''),
                        'title': 'Decision Tree Analysis'
                    }],
                    'status': 'success'
                }
            else:
                return {
                    'response': result.get('message', 'Failed to create decision tree'),
                    'status': 'error'
                }
        except Exception as e:
            return {'response': f'Error: {str(e)}', 'status': 'error'}
    
    async def _create_urban_extent_map(self, session_id: str) -> Dict[str, Any]:
        """Create urban extent map."""
        try:
            from app.services.container import get_service_container
            container = get_service_container()
            viz_service = container.get('visualization_service')
            
            data_handler = container.get('data_service').get_handler(session_id)
            unified_dataset = data_handler.get_unified_dataset()
            
            if unified_dataset is None:
                return {
                    'response': 'No analysis data available',
                    'status': 'error'
                }
            
            result = viz_service.urban_extent_map(unified_dataset, session_id=session_id)
            
            if result.get('status') == 'success':
                return {
                    'response': 'Created urban extent map',
                    'visualizations': [{
                        'type': 'urban_extent_map',
                        'path': result.get('web_path', ''),
                        'title': 'Urban Extent Map'
                    }],
                    'status': 'success'
                }
            else:
                return {
                    'response': result.get('message', 'Failed to create urban extent map'),
                    'status': 'error'
                }
        except Exception as e:
            return {'response': f'Error: {str(e)}', 'status': 'error'}