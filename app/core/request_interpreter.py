"""
Request Interpreter for ChatMRPT

Central brain that parses natural language user messages into structured tool calls.
Routes between Data Agent (session data analysis) and Knowledge Agent (explanations).
Handles end-to-end interaction logic as outlined in the architecture.
"""

import logging
import json
from typing import Dict, Any, List, Optional
from flask import current_app

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


    def parse_request(self, user_message: str, session_id: str) -> Dict[str, Any]:
        """Parse user request into structured intent - NO FALLBACKS."""
        try:
            from ..tools import get_all_tools
            
            # Get available tools
            all_tools = get_all_tools()
            tool_names = list(all_tools.keys())
            
            # Create system prompt
            system_prompt = f"""
            Parse user requests into structured tool calls for ChatMRPT malaria analysis system.

            Available tools: {', '.join(tool_names)}

            CRITICAL PARAMETER MAPPING:
            - get_composite_rankings(session_id, top_n=20) - NO 'ward' parameter
            - get_pca_rankings(session_id, top_n=20) - NO 'ward' parameter  
            - scatter_plot(session_id, x_variable, y_variable, color_by=None, size_by=None)
            - create_vulnerability_map(session_id, method="composite"|"pca"|"auto")
            - All visualization tools use x_variable/y_variable NOT x_axis/y_axis

            TOOL AVAILABILITY:
            - NO radar_chart tool exists
            - Use create_vulnerability_map instead of specific chart requests
            - For ward-specific queries, use top_n parameter and filter results

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
                "routing": "data_agent|knowledge_agent"
            }}

            INTELLIGENT CLASSIFICATION RULES:
            
            1. GREETINGS: Simple hello/hi responses
               - Basic greetings like "hello", "hi" -> simple_greeting
            
            2. SYSTEM QUESTIONS: About ChatMRPT itself  
               - "who are you", "what can you do", "tell me about yourself" -> explain_concept with concept="ChatMRPT"
            
            3. KNOWLEDGE/EDUCATIONAL: Any question about malaria, health, epidemiology, etc.
               - Extract the main topic/concept from the user's question
               - Use explain_concept with concept=[extracted main topic]
               - Examples: history, transmission, control methods, epidemiology, vectors, etc.
               - Be flexible in extracting concepts - users phrase things differently
            
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

            CRITICAL TOP_N PARAMETER EXTRACTION:
            - Extract numbers from user queries: "top 10" -> top_n=10, "top 15" -> top_n=15
            - Default to top_n=10 if no number specified
            - For comparative questions asking about both methods, call BOTH tools with SAME top_n
            - Examples: "top 10 composite and PCA" -> get_composite_rankings(top_n=10) + get_pca_rankings(top_n=10)

            CRITICAL: Questions starting with "What are", "What is the", "Which", "How", "Where", "When", "Why" 
            that contain data terms (wards, risk, ranking, score, malaria, vulnerability) are DATA QUERIES, NOT greetings!

            PARAMETER VALIDATION:
            - Ward queries: Extract top_n from user request or default to 10
            - Scatter plots: Use x_variable and y_variable, suggest actual column names like "composite_score", "pca_score", "population_density", "elevation"
            - Maps: Use method="composite" or "pca" or "auto"

            Only use exact tool names and parameters from the available list. Provide helpful error context if tools cannot satisfy the request.
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
            
            # Extract JSON - NO FALLBACKS
            clean_response = parse_response.strip()
            if clean_response.startswith('```json'):
                clean_response = clean_response[7:-3]
            
            logger.info(f"🔧 DEBUG: Cleaned response: {clean_response[:300]}...")
            
            try:
                parsed_intent = json.loads(clean_response)
            except json.JSONDecodeError as e:
                return {
                    'status': 'error',
                    'message': f'JSON parsing failed: {str(e)}',
                    'raw_response': parse_response
                }
            
            logger.info(f"🔧 DEBUG: Parsed intent tool_calls: {len(parsed_intent.get('tool_calls', []))}")
            
            # Validate tool calls - NO FALLBACKS
            for tool_call in parsed_intent.get('tool_calls', []):
                tool_name = tool_call.get('tool_name')
                if tool_name not in tool_names:
                    return {
                        'status': 'error',
                        'message': f'Invalid tool name: {tool_name}. Available tools: {tool_names[:10]}...',
                        'parsed_intent': parsed_intent
                    }
            
            # Inject session_id
            for tool_call in parsed_intent['tool_calls']:
                if 'parameters' not in tool_call:
                    tool_call['parameters'] = {}
                tool_call['parameters']['session_id'] = session_id
            
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
        """Execute the parsed intent by calling appropriate tools."""
        try:
            from ..tools import get_tool_function
            
            tool_calls = parsed_intent.get('tool_calls', [])
            logger.info(f"🔧 DEBUG: Executing {len(tool_calls)} tool calls")
            execution_results = []
            
            for tool_call in tool_calls:
                tool_name = tool_call.get('tool_name')
                parameters = tool_call.get('parameters', {})
                
                # 🔧 CRITICAL FIX: Inject session_id into ALL tool calls
                parameters['session_id'] = session_id
                
                logger.info(f"🔧 DEBUG: Looking for tool '{tool_name}'")
                tool_function = get_tool_function(tool_name)
                if not tool_function:
                    logger.error(f"🔧 DEBUG: Tool '{tool_name}' NOT FOUND")
                    execution_results.append({
                        'tool_name': tool_name,
                        'status': 'error',
                        'message': f'Tool {tool_name} not found'
                    })
                    continue
                
                logger.info(f"🔧 DEBUG: Tool '{tool_name}' found, executing with params: {parameters}")
                try:
                    result = tool_function(**parameters)
                    logger.info(f"🔧 DEBUG: Tool '{tool_name}' executed, result status: {result.get('status') if isinstance(result, dict) else 'unknown'}")
                    if isinstance(result, dict) and result.get('status') == 'error':
                        logger.error(f"🔧 DEBUG: Tool '{tool_name}' error details: {result.get('message', 'No error message')}")
                    
                    if not isinstance(result, dict):
                        result = {'status': 'success', 'data': result}
                    
                    result['tool_name'] = tool_name
                    execution_results.append(result)
                    
                except Exception as tool_error:
                    logger.error(f"🔧 DEBUG: Tool '{tool_name}' FAILED with error: {tool_error}")
                    execution_results.append({
                        'tool_name': tool_name,
                        'status': 'error',
                        'message': str(tool_error)
                    })
            
            successful_tools = [r for r in execution_results if r.get('status') == 'success']
            logger.info(f"🔧 DEBUG: Execution complete - {len(successful_tools)}/{len(tool_calls)} tools successful")
            
            # Auto-trigger comprehensive summary if both composite and PCA analyses just completed
            successful_tool_names = [r.get('tool_name') for r in successful_tools]
            if ('run_composite_analysis' in successful_tool_names and 'run_pca_analysis' in successful_tool_names):
                logger.info("🔧 DEBUG: Both analyses completed - triggering comprehensive summary")
                try:
                    summary_function = get_tool_function('generate_comprehensive_analysis_summary')
                    if summary_function:
                        summary_result = summary_function(session_id=session_id)
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
                return {
                    'status': 'error',
                    'response': f"❌ EXECUTION FAILED: {execution_results.get('message', 'Unknown error')}",
                    'visualizations': [],
                    'data_summary': None,
                    'debug_info': execution_results
                }
            
            results = execution_results.get('results', [])
            successful_results = [r for r in results if r.get('status') == 'success']
            failed_results = [r for r in results if r.get('status') == 'error']
            
            if not successful_results:
                return {
                    'status': 'error',
                    'response': f'❌ ALL TOOLS FAILED: {len(failed_results)} tools failed execution',
                    'visualizations': [],
                    'data_summary': None,
                    'failed_tools': failed_results
                }
            
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
        """Main entry point - processes user message end-to-end - NO FALLBACKS."""
        try:
            logger.info(f"Processing message for session {session_id}: {user_message[:100]}...")
            
            # Step 1: Parse request - NO FALLBACKS
            parse_result = self.parse_request(user_message, session_id)
            if parse_result['status'] == 'error':
                return {
                    'status': 'error',
                    'response': f'❌ REQUEST PARSING FAILED: {parse_result.get("message", "Unknown parsing error")}',
                    'visualizations': [],
                    'debug_info': parse_result
                }
            
            # Step 2: Execute intent - NO FALLBACKS
            execution_results = self.execute_intent(parse_result['parsed_intent'], session_id)
            
            # Step 3: Format response - NO FALLBACKS
            formatted_response = self.format_response(execution_results, user_message, session_id)
            
            return formatted_response
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return {
                'status': 'error',
                'response': f'❌ MESSAGE PROCESSING FAILED: {str(e)}',
                'visualizations': []
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
            
            # FOURTH PRIORITY: Use detailed message from upload analysis tools (already LLM-generated)
            for result in successful_results:
                tool_name = result.get('tool_name', '')
                if tool_name == 'analyze_uploaded_data_and_recommend' and 'message' in result:
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

    # Legacy methods removed - all responses now generated by LLM