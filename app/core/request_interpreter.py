"""
Request Interpreter for ChatMRPT

Central brain that parses natural language user messages into structured tool calls.
Routes between Data Agent (session data analysis) and Knowledge Agent (explanations).
Handles end-to-end interaction logic as outlined in the architecture.
"""

import logging
import json
import time
from typing import Dict, Any, List, Optional
from flask import current_app
from difflib import get_close_matches

# Import unified memory system
from app.core.unified_memory import get_unified_memory, MemoryType, MemoryPriority

logger = logging.getLogger(__name__)


class RequestInterpreter:
    """
    Central Request Interpreter for ChatMRPT
    
    Parses natural language into structured tool calls and routes requests
    to appropriate agents (Data Agent vs Knowledge Agent).
    """
    
    def __init__(self, llm_manager, data_service, analysis_service, visualization_service):
        self.llm_manager = llm_manager
        self.data_service = data_service
        self.analysis_service = analysis_service
        self.visualization_service = visualization_service
        
        # Initialize unified memory system
        self.memory = get_unified_memory()
        
        # Initialize tiered tool loading system (ONLY system needed)
        from .tiered_tool_loader import get_tiered_tool_loader
        self.tiered_loader = get_tiered_tool_loader()
        
        # Common ambiguous phrases that need clarification
        self.ambiguous_patterns = {
            'analysis': ['composite analysis', 'PCA analysis', 'statistical analysis', 'spatial analysis'],
            'ranking': ['composite rankings', 'PCA rankings', 'vulnerability rankings'],
            'map': ['vulnerability map', 'risk map', 'choropleth map'],
            'top': ['top vulnerable wards', 'highest risk areas', 'most affected regions'],
            'data': ['upload data', 'analyze data', 'view data', 'data summary']
        }
        
        # Intelligent defaults for common ambiguous requests (simplified)
        self.intelligent_defaults = {
            ('analysis', 'ranking', 'top wards', 'risk assessment'): {
                'default_tools': ['getcompositerankings'],  # Use tiered loader tool
                'default_params': {'top_n': 10},
                'message': "Running vulnerability analysis by default"
            },
            ('vulnerable areas',): {
                'default_tools': ['getcompositerankings'], 
                'default_params': {'top_n': 15},
                'message': "Showing top vulnerable areas by default"
            }
        }

    # Removed redundant legacy tool registry methods - using tiered loader only

    def parse_request(self, user_message: str, session_id: str) -> Dict[str, Any]:
        """Parse user request into structured intent using tiered tool loader."""
        try:
            # Check for analysis permission workflow
            from flask import session
            if session.get('should_ask_analysis_permission', False):
                if self._is_confirmation_message(user_message):
                    # Clear the flag and trigger analysis
                    session['should_ask_analysis_permission'] = False
                    return self._create_automatic_analysis_intent()
            
            # Get available tools from tiered loader only
            available_tool_names = self.tiered_loader.get_all_available_tool_names()
            tool_names = set(available_tool_names)
            
            # Get basic tool schemas from tiered loader (avoids 30-second delay from heavy registry)
            tool_schemas = self.tiered_loader.get_basic_tool_schemas()
            
            # Create comprehensive tool documentation
            tool_documentation = self._generate_enhanced_tool_documentation_from_schemas(tool_schemas)
            
            # Get session context for better understanding
            from flask import session
            session_context = {
                'data_uploaded': session.get('csv_loaded', False) and session.get('shapefile_loaded', False),
                'analysis_complete': session.get('analysis_complete', False),
                'analysis_type': session.get('analysis_type', 'none'),
                'variables_used': session.get('variables_used', [])
            }
            
            # Create context-aware system prompt
            context_info = ""
            if session_context['data_uploaded']:
                context_info += "✅ User has uploaded CSV and shapefile data.\n"
                context_info += "✅ Data is available for analysis and querying.\n"
            if session_context['analysis_complete']:
                context_info += f"✅ User has completed {session_context['analysis_type']} analysis.\n"
                context_info += "✅ Analysis results and unified dataset are available for querying.\n"
                context_info += "⚠️  User may be asking follow-up questions about previous analysis results.\n"
                context_info += "⚠️  Always use data analysis tools for questions about wards, rankings, settlements, etc.\n"
            if session_context['variables_used']:
                context_info += f"📊 Previous analysis used variables: {', '.join(session_context['variables_used'][:5])}.\n"
            
            system_prompt = f"""
            Parse user requests into structured tool calls for ChatMRPT malaria analysis system.

            SESSION CONTEXT:
            {context_info}

            AVAILABLE TOOLS AND PARAMETERS:
            {tool_documentation}

            PARAMETER VALIDATION:
            - All tools are auto-validated using Pydantic models
            - Required parameters MUST be provided
            - Use exact parameter names as specified in tool schemas
            - For ward-specific queries, use the ward_name parameter where available

            Return JSON in this EXACT format:
            {{
                "intent_type": "data_analysis|knowledge|system",
                "primary_goal": "brief description",
                "tool_calls": [
                    {{
                        "tool_name": "exact_tool_name",
                        "parameters": {{"param1": "value1"}},
                        "reasoning": "why this tool"
                    }}
                ],
                "requires_session_data": true|false,
                "routing": "data_agent|knowledge_agent",
                "direct_response": "For knowledge questions, provide explanation here"
            }}

            INTELLIGENT CLASSIFICATION RULES:
            
            1. GREETINGS: Simple hello/hi responses
               - Basic greetings like "hello", "hi" -> simple_greeting
            
            2. SYSTEM QUESTIONS: About ChatMRPT itself  
               - "who are you", "what can you do", "tell me about yourself" -> explain_concept with concept="ChatMRPT"
               - "help", "help me", "I need help", "what do I do" -> show_help_options
            
            3. KNOWLEDGE/EDUCATIONAL: Any question about malaria, health, epidemiology, etc.
               - Provide direct explanations without using tools
               - Return explanation directly in the response text
               - Set intent_type="knowledge" with empty tool_calls array
               - Include comprehensive malaria expertise in response
            
            4. DATA ANALYSIS: Questions involving uploaded data, rankings, maps, comparisons
               - Use appropriate data analysis tools
               
            CRITICAL: Don't hardcode question patterns! Users ask differently. Use LLM intelligence to:
            - Classify intent (greeting/knowledge/data analysis/system)  
            - Extract the core concept/topic flexibly
            - Default to explain_concept for educational questions when unsure
            
            DATA ANALYSIS QUERIES (never treat as greetings):
            - "What are the top N..." -> get_composite_rankings + get_pca_rankings (both with top_n=N)
            - "Show me the top N..." -> get_composite_rankings + get_pca_rankings (both with top_n=N)
            - "Which wards..." -> get_composite_rankings + get_pca_rankings (top_n based on context)
            - "How does..." -> appropriate analysis tool
            - "top 10", "top 15", "top 20" -> extract number and use as top_n parameter
            - "Ward X vulnerability/ranking" -> get_ward_information with ward_name="X" 
            - "What's Kumbashi ranking" -> get_ward_information with ward_name="Kumbashi"
            - "Top wards/highest risk/malaria burden" -> get_composite_rankings + get_pca_rankings
            - "Show scatter plot X vs Y" -> scatter_plot with x_variable="X", y_variable="Y"
            - Comparison questions -> call both relevant ranking tools with same top_n
            - Visualization requests -> map to available visualization tools only
            
            SETTLEMENT ANALYSIS QUERIES:
            - "Which settlement types..." -> Use createsettlementanalysismap or getdescriptivestatistics with settlement variables
            - "Settlement vulnerability/risk" -> Use ward data tools and settlement analysis (createsettlementanalysismap)
            - "Informal vs formal" -> Use createsettlementanalysismap with analysis_focus="settlement_types"
            - "Highest malaria vulnerability" -> Use getcompositerankings with settlement context or createsettlementanalysismap
            
            RESOURCE ALLOCATION QUERIES:
            - "Budget for nets/resources" -> Always use intervention targeting tools
            - "Which wards should I prioritize" -> Use get_composite_rankings or intervention tools
            - "Resource allocation/distribution" -> Use intervention targeting or optimization tools

            CRITICAL TOP_N PARAMETER EXTRACTION:
            - Extract numbers from user queries: "top 10" -> top_n=10, "top 15" -> top_n=15
            - Default to top_n=10 if no number specified
            - For comparative questions asking about both methods, call BOTH tools with SAME top_n
            - Examples: "top 10 composite and PCA" -> get_composite_rankings(top_n=10) + get_pca_rankings(top_n=10)

            CRITICAL: Questions starting with "What are", "What is the", "Which", "How", "Where", "When", "Why" 
            that contain data terms (wards, risk, ranking, score, malaria, vulnerability) are DATA QUERIES, NOT greetings!

            CUSTOM VARIABLE SELECTION PARSING:
            - Extract custom variables from natural language patterns
            - "run composite with pfpr, elevation, housing" -> composite_variables=["pfpr", "elevation", "housing"]
            - "use these variables for PCA: rainfall, population" -> pca_variables=["rainfall", "population"]
            - "analyze using pfpr and elevation for composite, rainfall for PCA" -> both parameters
            - "run analysis with custom variables x, y, z" -> depends on context (composite or PCA)
            - Look for keywords: "with", "using", "variables", "include", "custom"
            - Variable names can be separated by: commas, "and", "plus", semicolons
            - Clean variable names: strip whitespace, remove quotes, handle common variations

            PARAMETER VALIDATION:
            - Ward queries: Extract top_n from user request or default to 10
            - Scatter plots: Use x_variable and y_variable, suggest actual column names like "composite_score", "pca_score", "population_density", "elevation"
            - Maps: Use method="composite" or "pca" or "auto"
            - Custom variables: Parse from natural language patterns and clean variable names

            AMBIGUOUS REQUEST HANDLING:
            When user request is vague or could mean multiple things:
            1. If asking about "analysis" without specifying type, return empty tool_calls and set intent_type="clarification_needed"
            2. If asking about "data" without context, return empty tool_calls and set intent_type="clarification_needed"
            3. If mentioning tools that don't exist, return empty tool_calls and set intent_type="clarification_needed"
            4. If parameters are missing or unclear, return empty tool_calls and set intent_type="clarification_needed"
            
            Example ambiguous requests that need clarification:
            - "run analysis" (which type?)
            - "show me the data" (what aspect?)
            - "what about ward X" (what information?)
            - "make a chart" (what kind?)
            - "do the thing" (what thing?)

            Only use exact tool names and parameters from the available list. If unsure, set intent_type="clarification_needed" instead of guessing.

            FEW-SHOT EXAMPLES FOR PRECISE TOOL SELECTION:

            Example 1:
            User: "What are the top 10 most vulnerable wards?"
            Response: {{
                "intent_type": "data_analysis",
                "primary_goal": "Get top 10 most vulnerable wards",
                "tool_calls": [
                    {{
                        "tool_name": "getcompositerankings",
                        "parameters": {{"top_n": 10}},
                        "reasoning": "User wants vulnerability rankings"
                    }}
                ],
                "requires_session_data": true,
                "routing": "data_agent"
            }}
            
            Example: Settlement Analysis
            User: "Which settlement types have the highest malaria vulnerability?"
            Response: {{
                "intent_type": "data_analysis",
                "primary_goal": "Analyze settlement vulnerability patterns",
                "tool_calls": [
                    {{
                        "tool_name": "createsettlementanalysismap",
                        "parameters": {{"analysis_focus": "settlement_types"}},
                        "reasoning": "User wants settlement type vulnerability analysis"
                    }}
                ],
                "requires_session_data": true,
                "routing": "data_agent"
            }}

            Example: Variable Distribution
            User: "Show me the distribution of pfpr variable"
            Response: {{
                "intent_type": "data_analysis",
                "primary_goal": "Visualize spatial distribution of pfpr variable",
                "tool_calls": [
                    {{
                        "tool_name": "variable_distribution",
                        "parameters": {{"variable_name": "pfpr"}},
                        "reasoning": "User wants to see spatial distribution map of pfpr variable"
                    }}
                ],
                "requires_session_data": true,
                "routing": "data_agent"
            }}

            Example 2:
            User: "What is malaria transmission?"
            Response: {{
                "intent_type": "knowledge",
                "primary_goal": "Explain malaria transmission",
                "tool_calls": [],
                "requires_session_data": false,
                "routing": "knowledge_agent",
                "direct_response": "Malaria is transmitted through the bite of infected female Anopheles mosquitoes. When an infected mosquito bites a person, it injects Plasmodium parasites into the bloodstream. These parasites travel to the liver, multiply, and then return to infect red blood cells. The cycle continues when another mosquito bites the infected person and picks up the parasites, becoming a vector for further transmission. Key factors affecting transmission include mosquito breeding sites (stagnant water), climate conditions, and human behavior patterns."
            }}

            Example 3:
            User: "If ITN coverage in Kano Municipal increases by 30%, what happens?"
            Response: {{
                "intent_type": "data_analysis",
                "primary_goal": "Simulate coverage increase impact",
                "tool_calls": [
                    {{
                        "tool_name": "simulatecoverageimpact",
                        "parameters": {{
                            "ward_name": "Kano Municipal",
                            "coverage_increase": 0.3,
                            "intervention_type": "ITN"
                        }},
                        "reasoning": "User wants scenario simulation for specific ward"
                    }}
                ],
                "requires_session_data": true,
                "routing": "data_agent"
            }}

            Example 4: Custom Variable Selection
            User: "Run composite analysis using pfpr, elevation, and housing_quality"
            Response: {{
                "intent_type": "data_analysis",
                "primary_goal": "Run composite analysis with custom variables",
                "tool_calls": [
                    {{
                        "tool_name": "run_complete_analysis",
                        "parameters": {{
                            "composite_variables": ["pfpr", "elevation", "housing_quality"],
                            "include_visualizations": true
                        }},
                        "reasoning": "User specified custom variables for composite analysis"
                    }}
                ],
                "requires_session_data": true,
                "routing": "data_agent"
            }}

            Example 5: Custom Variables for Both Methods
            User: "Do PCA with rainfall, population_density and composite with pfpr, elevation"
            Response: {{
                "intent_type": "data_analysis",
                "primary_goal": "Run dual analysis with different custom variables",
                "tool_calls": [
                    {{
                        "tool_name": "run_complete_analysis",
                        "parameters": {{
                            "composite_variables": ["pfpr", "elevation"],
                            "pca_variables": ["rainfall", "population_density"],
                            "include_visualizations": true
                        }},
                        "reasoning": "User specified different custom variables for each method"
                    }}
                ],
                "requires_session_data": true,
                "routing": "data_agent"
            }}

            Example 6: Knowledge Query
            User: "What is malaria transmission?"
            Response: {{
                "intent_type": "knowledge",
                "primary_goal": "Explain malaria transmission",
                "tool_calls": [
                    {{
                        "tool_name": "explain_concept",
                        "parameters": {{"concept": "transmission"}},
                        "reasoning": "Educational question about malaria concept"
                    }}
                ],
                "requires_session_data": false,
                "routing": "knowledge_agent"
            }}

            Example 7:
            User: "Hello"
            Response: {{
                "intent_type": "system",
                "primary_goal": "Respond to greeting",
                "tool_calls": [
                    {{
                        "tool_name": "simple_greeting",
                        "parameters": {{}},
                        "reasoning": "Simple greeting requires greeting response"
                    }}
                ],
                "requires_session_data": false,
                "routing": "knowledge_agent"
            }}

            Example 8:
            User: "run analysis"
            Response: {{
                "intent_type": "clarification_needed",
                "primary_goal": "Need to clarify analysis type",
                "tool_calls": [],
                "requires_session_data": true,
                "routing": "data_agent"
            }}

            Use these patterns as guidance for consistent tool selection and parameter extraction.
            """
            
            user_prompt = f'Parse this request: "{user_message}"'
            
            parse_response = self.llm_manager.generate_response(
                prompt=user_prompt,
                system_message=system_prompt,
                temperature=0.3,
                max_tokens=800,
                session_id=session_id
            )
            
            logger.info(f"🔧 DEBUG: LLM raw response: {parse_response[:500]}...")
            
            # Extract JSON - WITH CLARIFICATION FALLBACK
            clean_response = parse_response.strip()
            if clean_response.startswith('```json'):
                clean_response = clean_response[7:-3]
            
            logger.info(f"🔧 DEBUG: Cleaned response: {clean_response[:300]}...")
            
            try:
                parsed_intent = json.loads(clean_response)
            except json.JSONDecodeError as e:
                # Instead of failing, try to understand what went wrong and ask for clarification
                return self._generate_clarification_response(
                    user_message, 
                    "I'm having trouble understanding your request. Could you please rephrase it or be more specific?",
                    session_id
                )
            
            logger.info(f"🔧 DEBUG: Parsed intent tool_calls: {len(parsed_intent.get('tool_calls', []))}")
            
            # Check if clarification is needed
            if parsed_intent.get('intent_type') == 'clarification_needed':
                # Try to apply intelligent defaults before asking for clarification
                default_response = self._try_intelligent_defaults(user_message, session_id)
                if default_response:
                    return default_response
                
                # If no defaults apply, determine what kind of clarification to provide
                clarification_type = self._determine_clarification_type(user_message, parsed_intent)
                return self._generate_contextual_clarification(
                    user_message, 
                    clarification_type,
                    session_id
                )
            
            # Validate tool calls - WITH HELPFUL SUGGESTIONS
            for tool_call in parsed_intent.get('tool_calls', []):
                tool_name = tool_call.get('tool_name')
                if not self.tiered_loader.is_tool_available(tool_name):
                    # Find similar tool names
                    close_matches = get_close_matches(tool_name, available_tool_names, n=3, cutoff=0.6)
                    
                    if close_matches:
                        suggestion_text = f"Did you mean one of these: {', '.join(close_matches)}?"
                    else:
                        # Try to understand what the user might want
                        suggestion_text = self._suggest_relevant_tools(user_message, available_tool_names)
                    
                    return self._generate_clarification_response(
                        user_message,
                        f"I couldn't find a tool called '{tool_name}'. {suggestion_text}",
                        session_id,
                        available_tools=close_matches if close_matches else None
                    )
            
            # Don't inject session_id here - it's passed separately to execute_tool_with_registry
            for tool_call in parsed_intent['tool_calls']:
                if 'parameters' not in tool_call:
                    tool_call['parameters'] = {}
            
            return {
                'status': 'success',
                'message': 'Request parsed successfully',
                'parsed_intent': parsed_intent,
                'original_message': user_message
            }
                
        except Exception as e:
            logger.error(f"Error parsing request: {e}")
            return {
                'status': 'error',
                'message': f'Request parsing exception: {str(e)}',
                'original_message': user_message
            }


    def execute_intent(self, parsed_intent: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Execute the parsed intent by calling appropriate tools via dynamic registry."""
        try:
            tool_calls = parsed_intent.get('tool_calls', [])
            logger.info(f"🔧 DEBUG: Executing {len(tool_calls)} tool calls via dynamic registry")
            execution_results = []
            
            # Check if unified dataset needs to be created before executing data-dependent tools
            data_dependent_tools = [tc for tc in tool_calls if self._requires_unified_dataset(tc.get('tool_name'))]
            if data_dependent_tools:
                logger.info(f"🔧 DEBUG: {len(data_dependent_tools)} tools require unified dataset, checking availability...")
                
                # Try to ensure unified dataset exists
                unified_check_result = self._ensure_unified_dataset_exists(session_id)
                if not unified_check_result['success']:
                    logger.warning(f"🔧 DEBUG: Unified dataset not available: {unified_check_result['message']}")
                    # Continue anyway - tools will handle missing data gracefully
                else:
                    logger.info(f"🔧 DEBUG: Unified dataset is available for analysis tools")
            
            for tool_call in tool_calls:
                tool_name = tool_call.get('tool_name')
                parameters = tool_call.get('parameters', {})
                
                logger.info(f"🔧 DEBUG: Executing tool '{tool_name}' with params: {parameters}")
                
                # Use dynamic registry to execute tool
                try:
                    result = self.execute_tool_with_registry(tool_name, session_id, **parameters)
                    logger.info(f"🔧 DEBUG: Tool '{tool_name}' executed, result status: {result.get('status') if isinstance(result, dict) else 'unknown'}")
                    
                    if isinstance(result, dict) and result.get('status') == 'error':
                        logger.error(f"🔧 DEBUG: Tool '{tool_name}' error details: {result.get('message', 'No error message')}")
                    
                    # Ensure result is a dictionary
                    if not isinstance(result, dict):
                        result = {'status': 'success', 'data': result}
                    
                    # Ensure tool_name is set
                    if 'tool_name' not in result:
                        result['tool_name'] = tool_name
                        
                    execution_results.append(result)
                    
                except Exception as tool_error:
                    logger.error(f"🔧 DEBUG: Tool '{tool_name}' FAILED with error: {tool_error}")
                    execution_results.append({
                        'tool_name': tool_name,
                        'status': 'error',
                        'message': str(tool_error),
                        'error_details': f"Exception: {type(tool_error).__name__}"
                    })
            
            successful_tools = [r for r in execution_results if r.get('status') == 'success']
            logger.info(f"🔧 DEBUG: Execution complete - {len(successful_tools)}/{len(tool_calls)} tools successful")
            
            # Auto-trigger comprehensive summary if both composite and PCA analyses just completed
            successful_tool_names = [r.get('tool_name') for r in successful_tools]
            if ('run_composite_analysis' in successful_tool_names and 'run_pca_analysis' in successful_tool_names):
                logger.info("🔧 DEBUG: Both analyses completed - triggering comprehensive summary")
                try:
                    summary_result = self.execute_tool_with_registry(
                        'generate_comprehensive_analysis_summary', 
                        session_id
                    )
                    if isinstance(summary_result, dict):
                        summary_result['tool_name'] = 'generate_comprehensive_analysis_summary'
                        execution_results.append(summary_result)
                        if summary_result.get('status') == 'success':
                            successful_tools.append(summary_result)
                            logger.info("🔧 DEBUG: Comprehensive summary generated successfully")
                except Exception as e:
                    logger.error(f"🔧 DEBUG: Failed to generate comprehensive summary: {e}")
            
            overall_status = 'success' if len(successful_tools) > 0 else 'error'
            
            return {
                'status': overall_status,
                'message': f'Executed {len(successful_tools)}/{len(tool_calls)} tools successfully',
                'intent_type': parsed_intent.get('intent_type'),
                'primary_goal': parsed_intent.get('primary_goal'),
                'results': execution_results,
                'successful_tools': len(successful_tools),
                'failed_tools': len(tool_calls) - len(successful_tools)
            }
            
        except Exception as e:
            logger.error(f"🔧 DEBUG: Execute intent failed completely: {e}")
            return {
                'status': 'error',
                'message': f'Error executing intent: {str(e)}',
                'results': []
            }


    def format_response(self, execution_results: Dict[str, Any], user_message: str, session_id: str = None) -> Dict[str, Any]:
        """Format execution results into comprehensive chat response - NO FALLBACKS."""
        try:
            if execution_results.get('status') == 'error':
                # Generate helpful error response
                return self._generate_helpful_error_response(
                    user_message, 
                    execution_results.get('message', 'Unknown error'),
                    session_id
                )
            
            results = execution_results.get('results', [])
            successful_results = [r for r in results if r.get('status') == 'success']
            failed_results = [r for r in results if r.get('status') == 'error']
            
            if not successful_results:
                # All tools failed - provide helpful guidance and trigger help
                error_details = [f"{r.get('tool_name')}: {r.get('message')}" for r in failed_results]
                
                # Try to invoke help tool automatically
                try:
                    help_result = self.execute_tool_with_registry(
                        'show_help_options',
                        session_id,
                        error_context=f"All requested operations failed: {'; '.join(error_details[:2])}"
                    )
                    if help_result.get('status') == 'success' and help_result.get('response'):
                        return {
                            'status': 'error',
                            'response': help_result['response'],
                            'visualizations': [],
                            'error_handled': True,
                            'help_provided': True
                        }
                except Exception as e:
                    logger.error(f"Failed to invoke help tool: {e}")
                
                # Fallback to error response
                return self._generate_helpful_error_response(
                    user_message,
                    f"I encountered issues with the analysis. Details: {'; '.join(error_details[:2])}",
                    session_id
                )
            
            # Collect visualizations and data
            visualizations = []
            explanations = []
            
            for result in successful_results:
                # Extract visualizations
                if 'web_path' in result:
                    visualizations.append({
                        'type': result.get('chart_type', result.get('tool_name', 'chart')),
                        'url': result['web_path'],
                        'title': result.get('message', 'Visualization'),
                        'tool': result.get('tool_name')
                    })
                
                # Extract explanations
                if 'explanation' in result:
                    explanations.append({
                        'tool': result.get('tool_name'),
                        'explanation': result['explanation']
                    })
            
            # Generate response text - NO FALLBACKS  
            response_text = self._generate_response_text(user_message, successful_results, session_id)
            
            return {
                'status': 'success',
                'message': f"✅ {len(successful_results)}/{len(results)} tools successful",
                'response': response_text,
                'visualizations': visualizations,
                'explanations': explanations,
                'tools_used': [r.get('tool_name') for r in successful_results],
                'failed_tools': failed_results if failed_results else None
            }
            
        except Exception as e:
            logger.error(f"Error formatting response: {e}")
            return {
                'status': 'error',
                'response': f'❌ RESPONSE FORMATTING FAILED: {str(e)}',
                'visualizations': []
            }


    def process_message(self, user_message: str, session_id: str) -> Dict[str, Any]:
        """Main entry point - processes user message end-to-end with unified memory integration."""
        start_time = time.time()
        tools_used = []
        
        try:
            logger.info(f"Processing message for session {session_id}: {user_message[:100]}...")
            
            # Get conversation context from unified memory
            context = self.memory.get_context(session_id)
            relevant_memories = self.memory.recall(
                query=user_message,
                memory_types=[MemoryType.CONVERSATION, MemoryType.ANALYSIS_CONTEXT],
                limit=3,
                session_only=True
            )
            
            # Check for automatic data description workflow
            from flask import session
            if session.get('should_describe_data', False):
                # Clear the flag to prevent repeated triggering
                session['should_describe_data'] = False
                
                # Generate automatic data description
                result = self._generate_automatic_data_description(session_id)
                tools_used.append('automatic_data_description')
                
                # Store in memory before returning
                self._store_conversation_in_memory(
                    user_message, result.get('response', ''), 
                    tools_used, time.time() - start_time, True
                )
                return result
            
            # Step 1: Parse request - WITH CLARIFICATION
            parse_result = self.parse_request(user_message, session_id)
            
            # Handle clarification responses
            if parse_result.get('status') == 'clarification_needed':
                self._store_conversation_in_memory(
                    user_message, parse_result.get('response', ''), 
                    [], time.time() - start_time, True
                )
                return parse_result
            
            if parse_result['status'] == 'error':
                # Instead of showing technical error, ask for clarification
                result = self._generate_clarification_response(
                    user_message,
                    "I'm having trouble understanding what you'd like me to do. Could you please provide more details or rephrase your request?",
                    session_id
                )
                self._store_conversation_in_memory(
                    user_message, result.get('response', ''), 
                    [], time.time() - start_time, False
                )
                return result
            
            # Check for direct response from parsing (knowledge questions)
            parsed_intent = parse_result['parsed_intent']
            if 'direct_response' in parsed_intent and parsed_intent['direct_response']:
                logger.info("🚀 Using direct response from parsing - no tool execution needed")
                result = {
                    'status': 'success',
                    'response': parsed_intent['direct_response'],
                    'visualizations': [],
                    'intent_type': parsed_intent.get('intent_type', 'knowledge'),
                    'method': 'direct_parsing_response'
                }
                
                self._store_conversation_in_memory(
                    user_message, result['response'], 
                    ['knowledge_response'], time.time() - start_time, True
                )
                return result
            
            # Track tools that will be used
            if 'tool_calls' in parsed_intent:
                tools_used = [tool.get('tool_name') for tool in parsed_intent['tool_calls']]
            
            # Step 2: Execute intent - NO FALLBACKS
            execution_results = self.execute_intent(parse_result['parsed_intent'], session_id)
            
            # Step 3: Format response - NO FALLBACKS
            formatted_response = self.format_response(execution_results, user_message, session_id)
            
            # Store conversation in unified memory
            success = formatted_response.get('status') == 'success'
            response_text = formatted_response.get('response', '')
            
            self._store_conversation_in_memory(
                user_message, response_text, tools_used, 
                time.time() - start_time, success
            )
            
            # Store analysis results if any
            if success and execution_results.get('successful_results'):
                self._store_analysis_results_in_memory(execution_results['successful_results'])
            
            return formatted_response
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return {
                'status': 'error',
                'response': f'❌ MESSAGE PROCESSING FAILED: {str(e)}',
                'visualizations': []
            }

    def _generate_automatic_data_description(self, session_id: str) -> Dict[str, Any]:
        """DISABLED: Old duplicate data description system - now handled by frontend file-uploader.js"""
        from flask import session
        
        # The new file-uploader.js handles the upload display professionally
        # This old system was creating duplicate messages with emojis
        logger.info(f"Automatic data description disabled - handled by frontend for session {session_id}")
        
        # Set flag for analysis permission handling (this is still needed)
        session['should_ask_analysis_permission'] = True
        
        return {
            'status': 'success',
            'response': '',  # Empty response - frontend handles the display
            'visualizations': [],
            'automatic_workflow': 'data_description_complete'
        }

    def _is_confirmation_message(self, user_message: str) -> bool:
        """Check if user message is a confirmation for running analysis."""
        confirmation_patterns = [
            'yes', 'yeah', 'yep', 'sure', 'ok', 'okay', 'proceed', 'go ahead', 
            'run', 'start', 'analyze', 'do it', 'continue', 'confirm', 'agreed',
            'composite', 'pca', 'analysis', 'first run'
        ]
        
        # Comprehensive analysis patterns (from workflow guidance)
        comprehensive_patterns = [
            'comprehensive analysis', 'complete analysis', 'full analysis',
            'run comprehensive analysis', 'proceed with analysis',
            'ready to analyze', 'run the analysis', 'start analysis',
            'comprehensive malaria risk analysis', 'ready for analysis'
        ]
        
        user_lower = user_message.lower().strip()
        
        # Check for comprehensive analysis phrases first (more specific)
        if any(pattern in user_lower for pattern in comprehensive_patterns):
            return True
        
        # Direct confirmation words
        if any(pattern in user_lower for pattern in confirmation_patterns):
            return True
            
        # Negative patterns to avoid false positives
        negative_patterns = ['no', 'not', "don't", 'stop', 'cancel', 'wait']
        if any(pattern in user_lower for pattern in negative_patterns):
            return False
            
        return False

    def _create_automatic_analysis_intent(self) -> Dict[str, Any]:
        """Create intent for automatic composite and PCA analysis."""
        return {
            'status': 'success',
            'parsed_intent': {
                'intent_type': 'data_analysis',
                'primary_goal': 'Run complete dual-method analysis (Composite + PCA)',
                'tool_calls': [
                    {
                        'tool_name': 'runcompleteanalysis',
                        'parameters': {
                            'include_visualizations': True,
                            'create_unified_dataset': True
                        },
                        'reasoning': 'User confirmed they want to run both composite and PCA analysis together with unified dataset creation'
                    }
                ],
                'requires_session_data': True,
                'routing': 'data_agent',
                'automatic_workflow': 'complete_analysis_granted'
            }
        }

    def _generate_response_text(self, user_message: str, successful_results: List[Dict], session_id: str = None) -> str:
        """Generate natural language response using LLM to interpret tool results."""
        try:
            # HIGHEST PRIORITY: Simple greetings get short responses
            for result in successful_results:
                tool_name = result.get('tool_name', '')
                if tool_name == 'simple_greeting' and 'greeting' in result:
                    return result['greeting']
            
            # SECOND PRIORITY: Use comprehensive analysis summary (already LLM-generated)
            for result in successful_results:
                tool_name = result.get('tool_name', '')
                if tool_name == 'generate_comprehensive_analysis_summary' and 'message' in result:
                    return result['message']
            
            # THIRD PRIORITY: Use detailed explanations from knowledge tools (already LLM-generated)
            knowledge_explanations = []
            for result in successful_results:
                tool_name = result.get('tool_name', '')
                if tool_name in ['explain_concept', 'explain_methodology', 'explain_variable', 'interpret_results'] and 'explanation' in result:
                    knowledge_explanations.append(result['explanation'])
            
            # If we have knowledge explanations, combine them naturally
            if knowledge_explanations:
                if len(knowledge_explanations) == 1:
                    return knowledge_explanations[0]
                else:
                    # Use LLM to combine multiple explanations with smooth transitions
                    return self._combine_explanations_intelligently(knowledge_explanations, user_message, session_id)
            
            # FOURTH PRIORITY: Use detailed message from analysis tools (already generated)
            for result in successful_results:
                tool_name = result.get('tool_name', '')
                # Check for complete analysis results
                if tool_name in ['runcompleteanalysis', 'runcompositeanalysis', 'runpcaanalysis'] and 'message' in result:
                    return result['message']
                # Check for upload analysis
                if tool_name == 'analyze_uploaded_data_and_recommend' and 'message' in result:
                    return result['message']
            
            # FIFTH PRIORITY: Check for any tool with a detailed message
            for result in successful_results:
                if 'message' in result and len(result.get('message', '')) > 100:
                    # If the tool provided a detailed message, use it directly
                    return result['message']
            
            # FOR ALL OTHER TOOLS: Use LLM to generate conversational response
            return self._generate_llm_conversational_response(user_message, successful_results, session_id)
            
        except Exception as e:
            logger.error(f"Error generating response text: {e}")
            return f"I've processed your request successfully, but encountered an issue formatting the response: {str(e)}"

    def _generate_llm_conversational_response(self, user_message: str, successful_results: List[Dict], session_id: str = None) -> str:
        """Use LLM to generate conversational response from tool results."""
        try:
            # Prepare context for LLM
            tools_used = []
            tool_data = {}
            visualizations = []
            
            for result in successful_results:
                tool_name = result.get('tool_name', 'unknown_tool')
                tools_used.append(tool_name)
                
                # Extract key data for LLM context
                if 'web_path' in result:
                    visualizations.append({
                        'type': result.get('chart_type', tool_name),
                        'variables': f"{result.get('y_variable', '')} vs {result.get('x_variable', '')}".strip(' vs'),
                        'method': result.get('method', '')
                    })
                
                # Store relevant data
                tool_data[tool_name] = {
                    'status': result.get('status'),
                    'message': result.get('message', ''),
                    'data': {k: v for k, v in result.items() if k not in ['status', 'message', 'tool_name']}
                }
            
            # Create LLM prompt
            system_prompt = """You are a malaria epidemiologist embedded in ChatMRPT, a specialized tool for malaria risk assessment in urban Nigeria. 

Your personality:
- Expert in malaria epidemiology, urban microstratification, and WHO guidelines
- Conversational, helpful, and educational
- Provide insights and context, not just data
- Explain the epidemiological significance of findings
- Offer practical recommendations

CRITICAL INSTRUCTIONS FOR DATA-DRIVEN RESPONSES:

1. **Ranking Questions**: When user asks for "top N wards" or "most vulnerable wards":
   - Use the EXACT ward names and scores from the tool results
   - Show the COMPLETE list requested (if user wants top 10, show all 10)
   - Include actual numerical scores, ranks, and percentiles
   - Example: "The top 10 most vulnerable wards are: 1. Rafin Gora (Score: 0.578, Rank: 1/275), 2. Tunga Wawa..."

2. **Ward-Specific Questions**: When user asks about a specific ward:
   - Use the ACTUAL risk factors, scores, and rankings from get_ward_information results
   - Reference specific environmental, health, and demographic data
   - Explain WHY the ward ranks high/low based on actual data
   - Example: "Rafin Gora ranks #1 due to its high flood risk (0.85), distance to water (1200m), and pfpr rate (0.34)..."

3. **PCA Analysis**: When PCA data is available:
   - Use the actual high/medium/low risk ward lists
   - Don't say "PCA doesn't highlight wards" if data exists
   - Reference the actual variance explained and components used

4. **Data Accuracy**: Always use the ACTUAL data from tool results, never make generic statements.

Respond naturally to the user's question based on the tool results provided. Be conversational but professional."""

            user_prompt = f"""User asked: "{user_message}"

I executed these tools successfully: {', '.join(tools_used)}

Tool Results Summary:
{self._format_tool_data_for_llm(tool_data)}

Visualizations created: {len(visualizations)} charts{"" if not visualizations else f" - {visualizations}"}

Please provide a conversational, expert response that:
1. Directly addresses what the user asked
2. Interprets the results with epidemiological context
3. Explains the significance of findings
4. Offers practical insights or recommendations
5. Maintains the personality of a malaria epidemiologist

Keep it natural and conversational, not rigid or template-like."""

            # Generate response using LLM
            llm_response = self.llm_manager.generate_response(
                prompt=user_prompt,
                system_message=system_prompt,
                temperature=0.7,
                max_tokens=1000,
                session_id=session_id
            )
            
            # Add visualization note if charts were created
            response_text = llm_response.strip()
            if visualizations:
                response_text += f"\n\n📈 **{len(visualizations)} interactive chart{'s' if len(visualizations) > 1 else ''} displayed below** - explore the data to identify patterns and specific areas of interest!"
            
            return response_text
            
        except Exception as e:
            logger.error(f"Error generating LLM conversational response: {e}")
            # Fallback to basic summary
            tool_names = [r.get('tool_name', 'tool') for r in successful_results]
            return f"I've successfully executed {len(successful_results)} analysis tools ({', '.join(tool_names)}) to address your question. The results are ready for your review."

    def _format_tool_data_for_llm(self, tool_data: Dict) -> str:
        """Format tool data for LLM consumption with comprehensive details."""
        formatted_parts = []
        
        for tool_name, data in tool_data.items():
            parts = [f"**{tool_name}:**"]
            
            # Don't truncate important messages for ranking tools
            if data['message']:
                message = data['message']
                # Only truncate non-ranking tool messages
                if tool_name not in ['get_composite_rankings', 'get_pca_rankings', 'get_ward_information'] and len(message) > 500:
                    message = message[:500] + "... [truncated]"
                parts.append(f"Message: {message}")
            
            # Enhanced data extraction for ranking tools
            tool_data_dict = data.get('data', {})
            
            if tool_name == 'get_composite_rankings':
                # Show full ward lists with scores for ranking queries
                if 'top_wards' in tool_data_dict:
                    parts.append(f"Top Wards: {tool_data_dict['top_wards']}")
                if 'bottom_wards' in tool_data_dict:
                    parts.append(f"Bottom Wards: {tool_data_dict['bottom_wards']}")
                if 'total_wards' in tool_data_dict:
                    parts.append(f"Total Wards: {tool_data_dict['total_wards']}")
                if 'score_column' in tool_data_dict:
                    parts.append(f"Score Column: {tool_data_dict['score_column']}")
                    
            elif tool_name == 'get_pca_rankings':
                # Show full PCA ranking data
                if 'high_risk_wards' in tool_data_dict:
                    parts.append(f"High Risk Wards: {tool_data_dict['high_risk_wards']}")
                if 'medium_risk_wards' in tool_data_dict:
                    parts.append(f"Medium Risk Wards: {tool_data_dict['medium_risk_wards']}")
                if 'low_risk_wards' in tool_data_dict:
                    parts.append(f"Low Risk Wards: {tool_data_dict['low_risk_wards']}")
                if 'category_column' in tool_data_dict:
                    parts.append(f"Category Column: {tool_data_dict['category_column']}")
                if 'total_wards' in tool_data_dict:
                    parts.append(f"Total Wards: {tool_data_dict['total_wards']}")
                    
            elif tool_name == 'get_ward_information':
                # Show comprehensive ward details
                if 'ward_found' in tool_data_dict:
                    parts.append(f"Ward Found: {tool_data_dict['ward_found']}")
                if 'risk_factors' in tool_data_dict:
                    parts.append(f"Risk Factors: {tool_data_dict['risk_factors']}")
                if 'ranking_info' in tool_data_dict:
                    parts.append(f"Rankings: {tool_data_dict['ranking_info']}")
                if 'vulnerability_category' in tool_data_dict:
                    parts.append(f"Vulnerability Category: {tool_data_dict['vulnerability_category']}")
                if 'total_wards_in_dataset' in tool_data_dict:
                    parts.append(f"Total Wards in Dataset: {tool_data_dict['total_wards_in_dataset']}")
                    
            else:
                # For other tools, show key data points
                key_fields = ['total_wards', 'ward_name', 'chart_type', 'x_variable', 'y_variable', 'method', 'file_path']
                
                for field in key_fields:
                    if field in tool_data_dict and tool_data_dict[field] is not None:
                        value = tool_data_dict[field]
                        # Only truncate large lists for non-ranking tools
                        if isinstance(value, list) and len(value) > 10:
                            value = value[:10] + ['...']
                        parts.append(f"{field}: {value}")
            
            formatted_parts.append(' | '.join(parts))
        
        return '\n'.join(formatted_parts)

    def _combine_explanations_intelligently(self, explanations: List[str], user_message: str, session_id: str = None) -> str:
        """Use LLM to combine multiple explanations with smooth transitions and proper structure."""
        try:
            system_prompt = """
            You are a malaria epidemiologist. You have multiple detailed explanations that need to be combined into a single, well-structured response.
            
            Your task:
            1. Combine the explanations into a cohesive, flowing response
            2. Add smooth transitions between topics using natural language
            3. Organize with clear **bold headers** and logical structure  
            4. Remove redundancy while keeping all important information
            5. Ensure the response feels natural, not like separate sections pasted together
            6. Keep the total length reasonable (600-800 words max)
            
            Structure guidelines:
            • Start with a brief intro connecting to the user's question
            • Use **bold headers** to organize main sections
            • Create natural flow: "Building on this understanding..." or "This connects directly to..."
            • End with a cohesive summary or practical next steps
            • Maintain the expert malaria epidemiologist voice throughout
            """
            
            user_prompt = f"""
            The user asked: "{user_message}"
            
            I have {len(explanations)} separate explanations that need to be combined into one cohesive response:
            
            {chr(10).join([f"--- EXPLANATION {i+1} ---{chr(10)}{explanation}{chr(10)}" for i, explanation in enumerate(explanations)])}
            
            Please combine these into a single, well-structured response that flows naturally and addresses the user's question comprehensively. Focus on creating smooth transitions and eliminating redundancy while maintaining all the important information.
            """
            
            combined_response = self.llm_manager.generate_response(
                prompt=user_prompt,
                system_message=system_prompt,
                temperature=0.7,
                max_tokens=1200,
                session_id=session_id
            )
            
            return combined_response.strip()
            
        except Exception as e:
            logger.error(f"Error combining explanations intelligently: {e}")
            # Fallback to simple combination with dividers
            return '\n\n---\n\n'.join(explanations)

    # ========================================================================
    # UNIFIED MEMORY INTEGRATION METHODS
    # ========================================================================
    
    def _store_conversation_in_memory(self, user_message: str, ai_response: str, 
                                    tools_used: List[str], response_time: float, 
                                    success: bool):
        """Store conversation turn in unified memory."""
        try:
            self.memory.add_conversation_turn(
                user_message=user_message,
                ai_response=ai_response,
                metadata={
                    'tools_used': tools_used,
                    'response_time': response_time,
                    'success': success,
                    'timestamp': time.time()
                }
            )
        except Exception as e:
            logger.error(f"Failed to store conversation in memory: {e}")
    
    def _store_analysis_results_in_memory(self, successful_results: List[Dict[str, Any]]):
        """Store analysis results in unified memory."""
        try:
            for result in successful_results:
                tool_name = result.get('tool_name', 'unknown')
                
                # Extract meaningful results
                analysis_data = {
                    'tool': tool_name,
                    'message': result.get('message', ''),
                    'data': result.get('data', {}),
                    'visualizations': result.get('web_path', ''),
                    'success': result.get('status') == 'success'
                }
                
                # Store in memory with high priority for analysis results
                self.memory.store_analysis_results(
                    analysis_type=tool_name,
                    results=analysis_data,
                    metadata={
                        'execution_time': result.get('execution_time', 0),
                        'tool_category': self._categorize_tool(tool_name)
                    }
                )
        except Exception as e:
            logger.error(f"Failed to store analysis results in memory: {e}")
    
    def _categorize_tool(self, tool_name: str) -> str:
        """Categorize tool for better memory organization."""
        if 'ranking' in tool_name.lower():
            return 'ranking_analysis'
        elif 'map' in tool_name.lower() or 'visualization' in tool_name.lower():
            return 'visualization'
        elif 'ward' in tool_name.lower():
            return 'ward_analysis'
        elif 'settlement' in tool_name.lower():
            return 'settlement_analysis'
        else:
            return 'general_analysis'
    
    def _get_memory_enhanced_context(self, user_message: str, session_id: str) -> str:
        """Get memory-enhanced context for better LLM responses."""
        try:
            # Get recent conversation context
            context = self.memory.get_context(session_id)
            
            # Get relevant memories
            relevant_memories = self.memory.recall(
                query=user_message,
                memory_types=[MemoryType.CONVERSATION, MemoryType.ANALYSIS_CONTEXT],
                limit=3,
                session_only=True
            )
            
            context_parts = []
            
            # Add recent analysis context
            if context.analysis_state:
                context_parts.append(f"Recent Analysis: {context.analysis_state}")
            
            # Add entities mentioned
            if context.entities_mentioned:
                context_parts.append(f"Key Topics: {', '.join(context.entities_mentioned[:5])}")
            
            # Add tools used
            if context.tools_used:
                context_parts.append(f"Recent Tools: {', '.join(context.tools_used[-3:])}")
            
            # Add relevant memories
            if relevant_memories:
                memory_summaries = []
                for memory in relevant_memories[:2]:  # Top 2 most relevant
                    if memory.memory_item.type == MemoryType.CONVERSATION:
                        content = memory.memory_item.content
                        if 'message' in content:
                            summary = content['message'].get('content', '')[:100]
                            memory_summaries.append(f"Previous: {summary}...")
                    elif memory.memory_item.type == MemoryType.ANALYSIS_CONTEXT:
                        analysis_type = memory.memory_item.content.get('analysis_type', 'analysis')
                        memory_summaries.append(f"Previous {analysis_type}")
                
                if memory_summaries:
                    context_parts.append(f"Relevant History: {'; '.join(memory_summaries)}")
            
            return " | ".join(context_parts) if context_parts else ""
            
        except Exception as e:
            logger.error(f"Failed to get memory-enhanced context: {e}")
            return ""

    # Legacy methods removed - all responses now generated by LLM
    
    def _generate_clarification_response(self, user_message: str, clarification_message: str, 
                                       session_id: str, available_tools: List[str] = None) -> Dict[str, Any]:
        """Generate a helpful clarification response when request is ambiguous."""
        try:
            # Use LLM to generate a natural clarification
            system_prompt = """You are ChatMRPT, a helpful malaria epidemiologist assistant.
            
The user's request was unclear or ambiguous. Your task is to:
1. Acknowledge their request in a friendly way
2. Explain what wasn't clear (without being technical)
3. Offer helpful suggestions or ask clarifying questions
4. If possible tools are provided, present them as options
5. Guide them to rephrase or be more specific

Be conversational and helpful, like a knowledgeable colleague would be.
Don't mention technical terms like "parsing", "JSON", or "tool validation".
"""
            
            user_prompt = f"""User said: "{user_message}"

Issue: {clarification_message}

{f"Possible tools that might help: {', '.join(available_tools)}" if available_tools else ""}

Please generate a friendly, helpful clarification response that guides the user."""

            clarification_response = self.llm_manager.generate_response(
                prompt=user_prompt,
                system_message=system_prompt,
                temperature=0.8,
                max_tokens=400,
                session_id=session_id
            )
            
            return {
                'status': 'clarification_needed',
                'response': clarification_response.strip(),
                'visualizations': [],
                'original_message': user_message,
                'suggestions': available_tools if available_tools else []
            }
            
        except Exception as e:
            logger.error(f"Error generating clarification: {e}")
            # Fallback to simple clarification
            return {
                'status': 'clarification_needed',
                'response': clarification_message,
                'visualizations': [],
                'original_message': user_message
            }
    
    def _suggest_relevant_tools(self, user_message: str, available_tools: List[str]) -> str:
        """Suggest relevant tools based on keywords in user message."""
        message_lower = user_message.lower()
        suggestions = []
        
        # Check for common patterns
        if any(word in message_lower for word in ['rank', 'top', 'vulnerable', 'highest']):
            suggestions.extend(['get_composite_rankings', 'get_pca_rankings'])
        
        if any(word in message_lower for word in ['map', 'visualize', 'show']):
            suggestions.append('create_vulnerability_map')
            
        if any(word in message_lower for word in ['scatter', 'plot', 'correlation']):
            suggestions.append('scatter_plot')
            
        if any(word in message_lower for word in ['explain', 'what is', 'tell me about']):
            suggestions.append('explain_concept')
            
        if any(word in message_lower for word in ['ward', 'specific area']):
            suggestions.append('get_ward_information')
        
        # Filter to only include available tools
        valid_suggestions = [s for s in suggestions if s in available_tools][:3]
        
        if valid_suggestions:
            return f"Based on your question, you might want to try: {', '.join(valid_suggestions)}"
        else:
            return "Could you please be more specific about what kind of analysis or information you're looking for?"
    
    def _generate_helpful_error_response(self, user_message: str, error_details: str, session_id: str) -> Dict[str, Any]:
        """Generate a helpful response when errors occur."""
        try:
            system_prompt = """You are ChatMRPT, a helpful malaria epidemiologist assistant.

An error occurred while processing the user's request. Your task is to:
1. Acknowledge the issue without being overly technical
2. Suggest what might have gone wrong in simple terms
3. Offer alternative approaches or clarifying questions
4. Guide them on how to proceed

Common issues and responses:
- No data uploaded: "It looks like you haven't uploaded any data yet. Would you like me to guide you through the data upload process?"
- Tool not found: "I couldn't find that specific analysis. Would you like me to show you what analyses are available?"
- Invalid parameters: "I need a bit more information to complete that analysis. Could you specify..."

Be helpful and solution-oriented, not just reporting errors."""

            user_prompt = f"""User said: "{user_message}"

Error encountered: {error_details}

Please generate a helpful response that:
1. Explains what might have gone wrong (simply)
2. Offers solutions or alternatives
3. Asks clarifying questions if needed
4. Maintains a helpful, professional tone"""

            error_response = self.llm_manager.generate_response(
                prompt=user_prompt,
                system_message=system_prompt,
                temperature=0.8,
                max_tokens=400,
                session_id=session_id
            )
            
            return {
                'status': 'error',
                'response': error_response.strip(),
                'visualizations': [],
                'original_message': user_message,
                'error_handled': True
            }
            
        except Exception as e:
            logger.error(f"Error generating helpful error response: {e}")
            # Fallback to simple error message
            return {
                'status': 'error',
                'response': "I encountered an issue processing your request. Could you please try rephrasing it or let me know what specific analysis you're looking for?",
                'visualizations': [],
                'original_message': user_message
            }
    
    def _determine_clarification_type(self, user_message: str, parsed_intent: Dict) -> str:
        """Determine what type of clarification is needed based on the ambiguous request."""
        message_lower = user_message.lower()
        
        # Analysis ambiguity
        if any(word in message_lower for word in ['analysis', 'analyze', 'run']):
            return 'analysis_type'
        
        # Data ambiguity
        if any(word in message_lower for word in ['data', 'show', 'display']) and not any(word in message_lower for word in ['map', 'chart', 'plot']):
            return 'data_view'
        
        # Visualization ambiguity
        if any(word in message_lower for word in ['chart', 'graph', 'plot', 'visualize', 'map']):
            return 'visualization_type'
        
        # Ward information ambiguity
        if 'ward' in message_lower and not any(word in message_lower for word in ['ranking', 'top', 'information']):
            return 'ward_info'
        
        # General ambiguity
        return 'general'
    
    def _generate_contextual_clarification(self, user_message: str, clarification_type: str, session_id: str) -> Dict[str, Any]:
        """Generate context-specific clarification based on the type of ambiguity."""
        clarification_prompts = {
            'analysis_type': {
                'message': "I can help you with different types of analysis. Which would you prefer?",
                'options': [
                    "Composite vulnerability analysis - combines multiple risk factors",
                    "PCA analysis - identifies key patterns in your data",
                    "Statistical analysis - correlations and relationships",
                    "Spatial analysis - geographic patterns"
                ],
                'suggestions': ['run_composite_analysis', 'run_pca_analysis', 'correlation_matrix']
            },
            'data_view': {
                'message': "I can show you data in different ways. What would you like to see?",
                'options': [
                    "Ward rankings by vulnerability",
                    "Summary statistics of your data",
                    "Specific ward information",
                    "Variable distributions"
                ],
                'suggestions': ['get_composite_rankings', 'descriptive_statistics', 'get_ward_information']
            },
            'visualization_type': {
                'message': "I can create different visualizations. Which type would help you most?",
                'options': [
                    "Vulnerability map - geographic risk distribution",
                    "Scatter plot - relationship between two variables",
                    "Box plot - distribution comparisons",
                    "Correlation matrix - variable relationships"
                ],
                'suggestions': ['create_vulnerability_map', 'scatter_plot', 'box_plot']
            },
            'ward_info': {
                'message': "What would you like to know about this ward?",
                'options': [
                    "Vulnerability ranking and score",
                    "Key risk factors",
                    "Comparison with other wards",
                    "Detailed statistics"
                ],
                'suggestions': ['get_ward_information', 'get_composite_rankings']
            },
            'general': {
                'message': "I can help with malaria risk analysis. What would you like to explore?",
                'options': [
                    "Upload and analyze data",
                    "View vulnerability rankings",
                    "Create risk maps",
                    "Learn about malaria concepts"
                ],
                'suggestions': []
            }
        }
        
        prompt_data = clarification_prompts.get(clarification_type, clarification_prompts['general'])
        
        # Use LLM to make the clarification more natural and contextual
        system_prompt = """You are ChatMRPT, a helpful malaria epidemiologist assistant.

The user made an ambiguous request. Based on the clarification type and options provided, 
create a natural, conversational response that:
1. Acknowledges their request
2. Explains the available options conversationally
3. Asks them to choose or be more specific
4. Feels like a helpful colleague, not a menu system

Don't just list options - weave them into natural language."""

        user_prompt = f"""User said: "{user_message}"

Clarification type: {clarification_type}
Base message: {prompt_data['message']}
Available options: {prompt_data['options']}

Generate a helpful, conversational clarification response."""

        try:
            clarification_response = self.llm_manager.generate_response(
                prompt=user_prompt,
                system_message=system_prompt,
                temperature=0.8,
                max_tokens=400,
                session_id=session_id
            )
            
            return {
                'status': 'clarification_needed',
                'response': clarification_response.strip(),
                'visualizations': [],
                'original_message': user_message,
                'suggestions': prompt_data.get('suggestions', [])
            }
            
        except Exception as e:
            # Fallback to structured response
            options_text = '\n'.join([f"• {opt}" for opt in prompt_data['options']])
            fallback_response = f"{prompt_data['message']}\n\n{options_text}"
            
            return {
                'status': 'clarification_needed',
                'response': fallback_response,
                'visualizations': [],
                'original_message': user_message,
                'suggestions': prompt_data.get('suggestions', [])
            }
    
    def _generate_tool_documentation(self) -> str:
        """Generate comprehensive tool documentation for LLM prompt"""
        documentation = []
        
        # Group tools by category for better organization
        tools_by_category = {}
        for tool_name in self.tool_registry.list_tools():
            metadata = self.tool_registry.get_tool_metadata(tool_name)
            if metadata:
                category = metadata.category.value
                if category not in tools_by_category:
                    tools_by_category[category] = []
                tools_by_category[category].append((tool_name, metadata))
        
        # Generate documentation for each category
        for category, tools in tools_by_category.items():
            documentation.append(f"\n{category.upper().replace('_', ' ')} TOOLS:")
            
            for tool_name, metadata in tools:
                # Basic tool info
                doc_lines = [f"• {tool_name}: {metadata.description}"]
                
                # Parameters
                if metadata.parameters and "properties" in metadata.parameters:
                    props = metadata.parameters["properties"]
                    required = metadata.parameters.get("required", [])
                    
                    param_docs = []
                    for param_name, param_info in props.items():
                        if param_name == "session_id":
                            continue  # Skip session_id as it's auto-provided
                        
                        param_type = param_info.get("type", "string")
                        param_desc = param_info.get("description", "")
                        is_required = param_name in required
                        
                        req_marker = " (REQUIRED)" if is_required else " (optional)"
                        param_docs.append(f"    - {param_name}: {param_type}{req_marker} - {param_desc}")
                    
                    if param_docs:
                        doc_lines.append("  Parameters:")
                        doc_lines.extend(param_docs)
                
                # Examples
                if metadata.examples:
                    doc_lines.append(f"  Examples: {', '.join(metadata.examples[:2])}")
                
                documentation.extend(doc_lines)
                documentation.append("")  # Empty line for readability
        
        return "\n".join(documentation)
    
    def _generate_enhanced_tool_documentation_from_schemas(self, schemas: List[Dict]) -> str:
        """Generate enhanced tool documentation from schema list"""
        documentation = []
        
        # Convert list of schemas to dict format for existing logic
        schemas_dict = {schema['name']: schema for schema in schemas}
        
        return self._generate_enhanced_tool_documentation(schemas_dict)
    
    def _generate_enhanced_tool_documentation(self, schemas: Dict[str, Dict]) -> str:
        """Generate enhanced tool documentation using combined schemas"""
        documentation = []
        
        # Categorize tools for better organization
        categories = {
            'core_analysis': [],
            'statistical': [],
            'visualization': [],
            'knowledge': [],
            'system': [],
            'other': []
        }
        
        for tool_name, schema in schemas.items():
            # Categorize based on tool name patterns
            if any(x in tool_name.lower() for x in ['composite', 'pca', 'ranking', 'vulnerability']):
                categories['core_analysis'].append((tool_name, schema))
            elif any(x in tool_name.lower() for x in ['stats', 'correlation', 'summary', 'anova', 'test']):
                categories['statistical'].append((tool_name, schema))
            elif any(x in tool_name.lower() for x in ['plot', 'chart', 'histogram', 'scatter', 'map', 'visual']):
                categories['visualization'].append((tool_name, schema))
            elif any(x in tool_name.lower() for x in ['explain', 'greeting', 'help', 'concept']):
                categories['knowledge'].append((tool_name, schema))
            elif any(x in tool_name.lower() for x in ['check', 'data', 'session', 'available', 'ward']):
                categories['system'].append((tool_name, schema))
            else:
                categories['other'].append((tool_name, schema))
        
        # Generate documentation for each category
        for category_name, tools in categories.items():
            if not tools:
                continue
                
            documentation.append(f"\n{category_name.upper().replace('_', ' ')} TOOLS:")
            
            for tool_name, schema in tools:
                # Basic tool info
                description = schema.get('description', 'No description available')
                doc_lines = [f"• {tool_name}: {description}"]
                
                # Parameters with enhanced details
                parameters = schema.get('parameters', {})
                if parameters and 'properties' in parameters:
                    props = parameters['properties']
                    required = parameters.get('required', [])
                    
                    param_docs = []
                    for param_name, param_info in props.items():
                        if param_name == "session_id":
                            continue  # Skip session_id as it's auto-provided
                        
                        param_type = param_info.get("type", "string")
                        param_desc = param_info.get("description", "")
                        is_required = param_name in required
                        
                        # Add enum values if available
                        enum_values = param_info.get("enum", [])
                        pattern = param_info.get("pattern", "")
                        
                        req_marker = " (REQUIRED)" if is_required else " (optional)"
                        param_line = f"    - {param_name}: {param_type}{req_marker} - {param_desc}"
                        
                        if enum_values:
                            param_line += f" Options: {enum_values}"
                        elif pattern:
                            param_line += f" Pattern: {pattern}"
                        
                        param_docs.append(param_line)
                    
                    if param_docs:
                        doc_lines.append("  Parameters:")
                        doc_lines.extend(param_docs)
                
                documentation.extend(doc_lines)
                documentation.append("")  # Empty line for readability
        
        return "\n".join(documentation)
    
    def execute_tool_with_registry(self, tool_name: str, session_id: str, **parameters):
        """Execute a tool using the tiered loading system (simplified)"""
        
        # Intercept explanation requests and handle directly (eliminates expensive nested LLM calls)
        if tool_name.lower() == 'explainconcept':
            logger.info(f"🚀 Handling explanation request directly (no nested LLM call): {parameters.get('concept', 'unknown concept')}")
            return self._handle_explanation_directly(session_id, **parameters)
        
        # Block redundant greeting system when Phase 1 upload is complete (professional integration)
        if tool_name.lower() in ['simple_greeting', 'simplegreeting']:
            from flask import session
            if session.get('raw_data_stored', False):
                logger.info(f"🚫 Blocking redundant greeting - Phase 1 upload completed")
                return {
                    'status': 'success',
                    'response': '',  # Silent - Phase 1 already handled greeting
                    'data': {}
                }
        
        logger.info(f"🚀 Executing tool '{tool_name}' via tiered loader")
        return self.tiered_loader.execute_tool(tool_name, session_id, **parameters)
    
    def _handle_explanation_directly(self, session_id: str, **parameters):
        """Handle explanation requests directly without nested LLM calls"""
        import time
        start_time = time.time()
        
        try:
            concept = parameters.get('concept', 'unknown topic')
            technical_level = parameters.get('technical_level', 'intermediate')
            include_context = parameters.get('include_context', True)
            
            # Build concise explanation prompt for the main LLM
            if concept.lower() in ['chatmrpt', 'system capabilities', 'what can you do']:
                explanation_prompt = f"Explain ChatMRPT's capabilities clearly at a {technical_level} level. Cover urban microstratification, risk analysis, mapping, and intervention targeting."
            
            elif any(keyword in concept.lower() for keyword in ['how to use', 'upload data', 'data accept', 'getting started', 'data format']):
                explanation_prompt = f"Provide step-by-step guidance for using ChatMRPT at a {technical_level} level: data requirements, upload process, and what happens after upload."
            
            else:
                explanation_prompt = f"Explain '{concept}' from a malaria epidemiology perspective at a {technical_level} level. Be comprehensive but concise."
            
            # Add session context if requested
            context_info = ""
            if include_context:
                from flask import session
                if session.get('csv_loaded') and session.get('shapefile_loaded'):
                    context_info = "\n\nNote: Reference the user's uploaded data when relevant to make the explanation more practical and applicable to their specific analysis."
            
            # Get explanation from main LLM (no nested call - same LLM with enhanced system prompt)
            explanation = self.llm_manager.generate_response(
                prompt=explanation_prompt + context_info,
                temperature=0.7,
                max_tokens=400,  # Reduced for faster response
                session_id=session_id
            )
            
            execution_time = time.time() - start_time
            
            return {
                'status': 'success',
                'message': f'Successfully explained "{concept}"',
                'response': explanation,
                'concept': concept,
                'technical_level': technical_level,
                'execution_time': round(execution_time, 2),
                'tool_name': 'explainconcept',
                'method': 'direct_explanation'  # Flag to show this was optimized
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in direct explanation handling: {e}")
            return {
                'status': 'error',
                'message': f'Failed to explain concept: {str(e)}',
                'execution_time': round(execution_time, 2),
                'tool_name': 'explainconcept'
            }
    
    def _try_intelligent_defaults(self, user_message: str, session_id: str) -> Optional[Dict[str, Any]]:
        """Try to apply intelligent defaults for common ambiguous requests"""
        user_message_lower = user_message.lower()
        
        # Check each pattern group for matches
        for pattern_group, default_config in self.intelligent_defaults.items():
            for pattern in pattern_group:
                if pattern in user_message_lower:
                    logger.info(f"🤖 Applying intelligent default for pattern: {pattern}")
                    
                    # Create tool calls using the default configuration
                    tool_calls = []
                    
                    for tool_name in default_config['default_tools']:
                        if self.tiered_loader.is_tool_available(tool_name):
                            tool_calls.append({
                                'tool_name': tool_name,
                                'parameters': default_config['default_params'].copy(),
                                'reasoning': f"Intelligent default for ambiguous request: {pattern}"
                            })
                            break  # Use only the first available tool
                
                # If no tools found, use the first one anyway (fallback)
                if not tool_calls and default_config['default_tools']:
                    tool_calls.append({
                        'tool_name': default_config['default_tools'][0],
                        'parameters': default_config['default_params'].copy(),
                        'reasoning': f"Fallback intelligent default for ambiguous request: {pattern}"
                    })
                
                # Check if session has data before proceeding
                if not self.validate_session_data_exists(session_id):
                    return {
                        'status': 'error',
                        'message': 'No data uploaded yet. Please upload your data first before running analysis.',
                        'visualizations': [],
                        'original_message': user_message
                    }
                
                # Execute the default tools
                default_intent = {
                    'intent_type': 'data_analysis',
                    'primary_goal': f"Applied intelligent default: {default_config['message']}",
                    'tool_calls': tool_calls,
                    'requires_session_data': True,
                    'routing': 'data_agent'
                }
                
                # Execute the intent
                execution_result = self.execute_intent(default_intent, session_id)
                
                # Add a note about the intelligent default being applied
                if execution_result.get('status') == 'success':
                    response_message = execution_result.get('response', '')
                    default_note = f"\n\n💡 Note: {default_config['message']}. If you wanted something different, please be more specific in your request."
                    execution_result['response'] = response_message + default_note
                
                return execution_result
        
        # Check for common phrases that might need number extraction
        import re
        number_matches = re.findall(r'\b(\d+)\b', user_message_lower)
        if number_matches and any(phrase in user_message_lower for phrase in ['top', 'highest', 'most', 'vulnerable', 'risk']):
            top_n = int(number_matches[0])
            if 1 <= top_n <= 100:  # Reasonable range
                logger.info(f"🤖 Applying intelligent default with extracted number: {top_n}")
                
                tool_calls = [{
                    'tool_name': 'getcompositerankings',
                    'parameters': {'top_n': top_n},
                    'reasoning': f"Extracted top_n={top_n} from user request"
                }]
                
                if not self.validate_session_data_exists(session_id):
                    return {
                        'status': 'error',
                        'message': 'No data uploaded yet. Please upload your data first before running analysis.',
                        'visualizations': [],
                        'original_message': user_message
                    }
                
                default_intent = {
                    'intent_type': 'data_analysis',
                    'primary_goal': f"Show top {top_n} vulnerability rankings",
                    'tool_calls': tool_calls,
                    'requires_session_data': True,
                    'routing': 'data_agent'
                }
                
                execution_result = self.execute_intent(default_intent, session_id)
                
                if execution_result.get('status') == 'success':
                    response_message = execution_result.get('response', '')
                    default_note = f"\n\n💡 Note: I extracted the number {top_n} from your request. If you wanted something different, please clarify."
                    execution_result['response'] = response_message + default_note
                
                return execution_result
        
        return None  # No intelligent defaults apply
    
    def validate_session_data_exists(self, session_id: str) -> bool:
        """Check if session has uploaded data"""
        from ..tools.base import validate_session_data_exists
        return validate_session_data_exists(session_id)
    
    def _requires_unified_dataset(self, tool_name: str) -> bool:
        """Check if a tool requires unified dataset"""
        # Tools that require access to session data
        data_dependent_tools = {
            'getwardriskscore', 'gettopriskwards', 'filterwardsbyrisk', 'getriskstatistics',
            'getwardinformation', 'getwardvariable', 'comparewards', 'searchwards',
            'getdescriptivestatistics', 'getcorrelationanalysis', 'performregressionanalysis',
            'createvulnerabilitymap', 'createpcamap', 'createscatterplot', 'createboxplot',
            'getinterventionpriorities', 'simulatecoverageincrease',
            # Add missing analysis tools
            'run_composite_analysis', 'run_pca_analysis', 'create_unified_dataset',
            'runcompositeanalysis', 'runpcaanalysis', 'createunifieddataset',
            'composite_analysis', 'pca_analysis', 'unified_dataset',
            # Complete analysis tools
            'runcompleteanalysis', 'run_complete_analysis', 'complete_analysis'
        }
        return tool_name.lower() in data_dependent_tools
    
    def _ensure_unified_dataset_exists(self, session_id: str) -> Dict[str, Any]:
        """Ensure unified dataset exists for session"""
        try:
            import os
            from ..data.unified_dataset_builder import load_unified_dataset, UnifiedDatasetBuilder
            
            # Check if unified dataset already exists
            unified_path = os.path.join(f"instance/uploads/{session_id}", "unified_dataset.geoparquet")
            if os.path.exists(unified_path):
                # Try to load it to ensure it's valid
                gdf = load_unified_dataset(session_id)
                if gdf is not None:
                    return {'success': True, 'message': 'Unified dataset exists and is valid'}
            
            # Check if we have the required base data to create unified dataset
            session_folder = f"instance/uploads/{session_id}"
            csv_exists = os.path.exists(os.path.join(session_folder, "processed_data.csv"))
            shapefile_exists = os.path.exists(os.path.join(session_folder, "shapefile", "processed.shp"))
            
            if not csv_exists or not shapefile_exists:
                return {
                    'success': False, 
                    'message': f'Missing base data - CSV: {csv_exists}, Shapefile: {shapefile_exists}'
                }
            
            # Try to create unified dataset
            logger.info(f"Creating unified dataset for session {session_id}")
            builder = UnifiedDatasetBuilder(session_id)
            result = builder.build_unified_dataset()
            
            if result['status'] == 'success':
                return {'success': True, 'message': 'Unified dataset created successfully'}
            else:
                return {'success': False, 'message': f'Failed to create unified dataset: {result.get("message")}'}
                
        except Exception as e:
            logger.error(f"Error ensuring unified dataset exists: {e}")
            return {'success': False, 'message': f'Error: {str(e)}'}