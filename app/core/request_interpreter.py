"""
Conversational Request Interpreter for ChatMRPT

Clean, conversational approach using LLM's natural abilities instead of complex pattern matching.
Inspired by py-sidebot's simple, effective design.
"""

import logging
import json
import time
from typing import Dict, Any, List, Optional
from flask import current_app

# Removed complex unified memory system - using py-sidebot lightweight approach

logger = logging.getLogger(__name__)


class RequestInterpreter:
    """
    Conversational Request Interpreter for ChatMRPT
    
    Uses LLM's natural conversation abilities with function calling
    instead of complex pattern matching and hardcoded rules.
    """
    
    def __init__(self, llm_manager, data_service, analysis_service, visualization_service):
        self.llm_manager = llm_manager
        self.data_service = data_service
        self.analysis_service = analysis_service
        self.visualization_service = visualization_service
        
        # Using py-sidebot lightweight approach - no complex memory system
        self.conversation_history = {}  # Simple in-memory conversation storage
        
        # Initialize tiered tool loading system
        from .tiered_tool_loader import get_tiered_tool_loader
        self.tiered_loader = get_tiered_tool_loader()
    
    def process_message(self, user_message: str, session_id: str) -> Dict[str, Any]:
        """Main conversational processing - single method with function calling."""
        start_time = time.time()
        timing_breakdown = {
            'total_start': start_time,
            'context_retrieval': 0,
            'prompt_building': 0,
            'llm_processing': 0,
            'tool_execution': 0,
            'response_formatting': 0
        }
        
        try:
            logger.info(f"Processing conversational message for session {session_id}: {user_message[:100]}...")
            
            # Check for analysis permission workflow
            from flask import session
            if session.get('should_ask_analysis_permission', False):
                if self._is_confirmation_message(user_message):
                    # Clear the flag and trigger analysis
                    session['should_ask_analysis_permission'] = False
                    return self._execute_automatic_analysis(session_id)
            
            # Check for automatic data description workflow
            if session.get('should_describe_data', False):
                session['should_describe_data'] = False
                result = self._generate_automatic_data_description(session_id)
                # Store in simple conversation history
                self._store_conversation_simple(session_id, user_message, result.get('response', ''))
                return result
            
            # Check for fork intent (what-if scenarios)
            fork_intent = self._detect_fork_intent(user_message)
            
            if fork_intent['should_fork']:
                # Create fork for scenario exploration
                fork_id = self.fork_conversation(session_id, fork_intent['scenario_name'])
                
                if fork_id:
                    # Switch to fork temporarily for this response
                    self.switch_to_fork(fork_id)
                    
                    # Update session_id to fork_id for this processing
                    session_id = fork_id
                    
                    # Add fork context to response
                    fork_context = f"🔀 **Exploring scenario**: {fork_intent['scenario_name']}\\n\\n"
                else:
                    fork_context = ""
            else:
                fork_context = ""
            
            # Get session context for conversation
            context_start = time.time()
            session_context = self._get_session_context(session_id)
            timing_breakdown['context_retrieval'] = time.time() - context_start
            
            # Build conversational system prompt
            prompt_start = time.time()
            system_prompt = self._build_conversational_prompt(session_context)
            timing_breakdown['prompt_building'] = time.time() - prompt_start
            
            # Get available tools as functions
            available_functions = self._get_available_functions()
            
            # Build conversation messages
            messages = self._build_conversation_messages(user_message, session_context, session_id)
            
            # Single LLM call with function calling
            llm_start = time.time()
            response = self.llm_manager.generate_with_functions(
                messages=messages,
                system_prompt=system_prompt,
                functions=available_functions,
                temperature=0.7,
                session_id=session_id
            )
            timing_breakdown['llm_processing'] = time.time() - llm_start
            
            # Process the response
            tool_start = time.time()
            result = self._process_llm_response(response, user_message, session_id)
            timing_breakdown['tool_execution'] = time.time() - tool_start
            
            # Add fork context to response if we forked
            if fork_context:
                result['response'] = fork_context + result.get('response', '')
                result['forked'] = True
                result['fork_id'] = session_id
            
            # Update conversation history
            response_start = time.time()
            self._update_conversation_history(session_id, user_message, result.get('response', ''))
            
            # Store in memory
            tools_used = result.get('tools_used', [])
            success = result.get('status') == 'success'
            
            # Store conversation in simple memory (py-sidebot approach)
            self._store_conversation_simple(session_id, user_message, result.get('response', ''))
            
            # Calculate final timing
            timing_breakdown['response_formatting'] = time.time() - response_start
            timing_breakdown['total_duration'] = time.time() - start_time
            
            # Add detailed timing to result for enhanced monitoring
            result['timing_breakdown'] = timing_breakdown
            result['performance_metrics'] = {
                'total_time': timing_breakdown['total_duration'],
                'llm_percentage': round((timing_breakdown['llm_processing'] / timing_breakdown['total_duration']) * 100, 1),
                'tool_percentage': round((timing_breakdown['tool_execution'] / timing_breakdown['total_duration']) * 100, 1),
                'context_percentage': round((timing_breakdown['context_retrieval'] / timing_breakdown['total_duration']) * 100, 1),
                'bottleneck': max(timing_breakdown, key=timing_breakdown.get) if timing_breakdown else 'unknown'
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error in conversational processing: {e}")
            return {
                'status': 'error',
                'response': f'I encountered an issue processing your request: {str(e)}',
                'visualizations': []
            }
    
    def process_message_streaming(self, user_message: str, session_id: str):
        """Main conversational processing with streaming for better UX."""
        start_time = time.time()
        
        try:
            logger.info(f"Processing streaming message for session {session_id}: {user_message[:100]}...")
            
            # Check for analysis permission workflow
            from flask import session
            if session.get('should_ask_analysis_permission', False):
                if self._is_confirmation_message(user_message):
                    # Clear the flag and trigger analysis
                    session['should_ask_analysis_permission'] = False
                    result = self._execute_automatic_analysis(session_id)
                    yield {
                        'content': result.get('response', ''),
                        'status': result.get('status', 'success'),
                        'visualizations': result.get('visualizations', []),
                        'tools_used': result.get('tools_used', []),
                        'done': True
                    }
                    return
            
            # Check for automatic data description workflow
            if session.get('should_describe_data', False):
                session['should_describe_data'] = False
                result = self._generate_automatic_data_description(session_id)
                yield {
                    'content': result.get('response', ''),
                    'status': result.get('status', 'success'),
                    'visualizations': result.get('visualizations', []),
                    'automatic_workflow': result.get('automatic_workflow', ''),
                    'done': True
                }
                return
            
            # Check for fork intent (what-if scenarios)
            fork_intent = self._detect_fork_intent(user_message)
            
            if fork_intent['should_fork']:
                # Create fork for scenario exploration
                fork_id = self.fork_conversation(session_id, fork_intent['scenario_name'])
                
                if fork_id:
                    # Switch to fork temporarily for this response
                    self.switch_to_fork(fork_id)
                    
                    # Update session_id to fork_id for this processing
                    session_id = fork_id
                    
                    # Add fork context to response
                    fork_context = f"🔀 **Exploring scenario**: {fork_intent['scenario_name']}\\n\\n"
                    yield {
                        'content': fork_context,
                        'status': 'success',
                        'forked': True,
                        'fork_id': fork_id,
                        'done': False
                    }
                else:
                    yield {
                        'content': "Failed to create scenario fork. Continuing with main conversation.\\n\\n",
                        'status': 'warning',
                        'done': False
                    }
            
            # Get session context for conversation
            session_context = self._get_session_context(session_id)
            
            # Build conversational system prompt
            system_prompt = self._build_conversational_prompt(session_context)
            
            # Get available tools as functions
            available_functions = self._get_available_functions()
            
            # Build conversation messages
            messages = self._build_conversation_messages(user_message, session_context, session_id)
            
            # Stream LLM response with function calling
            accumulated_content = ""
            tools_used = []
            
            for chunk in self.llm_manager.generate_with_functions_streaming(
                messages=messages,
                system_prompt=system_prompt,
                functions=available_functions,
                temperature=0.7,
                session_id=session_id
            ):
                accumulated_content += chunk.get('content', '')
                
                if chunk.get('function_call'):
                    # Function call detected - execute it
                    function_name = chunk['function_call']['name']
                    function_args = json.loads(chunk['function_call']['arguments'])
                    
                    logger.info(f"Executing function: {function_name} with args: {function_args}")
                    
                    # Execute tool
                    tool_result = self.execute_tool_with_registry(function_name, session_id, **function_args)
                    tools_used.append(function_name)
                    
                    # Send tool execution update
                    yield {
                        'content': f"\\n\\n*Executing {function_name}...*\\n\\n",
                        'status': 'processing',
                        'tools_used': tools_used,
                        'done': False
                    }
                    
                    # Send tool result
                    if tool_result.get('status') == 'success':
                        tool_response = tool_result.get('message', tool_result.get('response', 'Analysis completed successfully'))
                        
                        # Extract visualizations if any
                        visualizations = []
                        if 'web_path' in tool_result:
                            visualizations.append({
                                'type': tool_result.get('chart_type', function_name),
                                'url': tool_result['web_path'],
                                'title': tool_result.get('message', 'Visualization'),
                                'tool': function_name
                            })
                        
                        yield {
                            'content': tool_response,
                            'status': 'success',
                            'visualizations': visualizations,
                            'tools_used': tools_used,
                            'done': True
                        }
                    else:
                        yield {
                            'content': f"I encountered an issue with the {function_name} analysis: {tool_result.get('message', 'Unknown error')}",
                            'status': 'error',
                            'tools_used': tools_used,
                            'done': True
                        }
                    
                    # Update conversation history
                    final_response = accumulated_content + (tool_result.get('message', '') if tool_result.get('status') == 'success' else '')
                    self._update_conversation_history(session_id, user_message, final_response)
                    
                    # Store in memory
                    self._store_conversation_in_memory(
                        user_message, final_response, 
                        tools_used, time.time() - start_time, tool_result.get('status') == 'success'
                    )
                    return
                
                elif chunk.get('done'):
                    # Pure conversational response - no function call
                    yield {
                        'content': '',  # Already sent incrementally
                        'status': 'success',
                        'visualizations': [],
                        'tools_used': [],
                        'done': True
                    }
                    
                    # Update conversation history
                    self._update_conversation_history(session_id, user_message, accumulated_content)
                    
                    # Store in memory
                    self._store_conversation_in_memory(
                        user_message, accumulated_content, 
                        [], time.time() - start_time, True
                    )
                    return
                
                else:
                    # Stream content chunk
                    yield {
                        'content': chunk.get('content', ''),
                        'status': 'streaming',
                        'done': False
                    }
                    
        except Exception as e:
            logger.error(f"Error in streaming conversational processing: {e}")
            yield {
                'content': f'I encountered an issue processing your request: {str(e)}',
                'status': 'error',
                'visualizations': [],
                'done': True
            }
    
    def _get_session_context(self, session_id: str) -> Dict[str, Any]:
        """Get comprehensive session context for conversation."""
        from flask import session
        
        context = {
            'data_loaded': session.get('csv_loaded', False) and session.get('shapefile_loaded', False),
            'analysis_complete': session.get('analysis_complete', False),
            'analysis_type': session.get('analysis_type', 'none'),
            'variables_used': session.get('variables_used', []),
            'state_name': session.get('state_name', 'Not specified'),
            'ward_column': session.get('ward_column', 'Not identified'),
            'conversation_history': session.get('conversation_history', [])
        }
        
        # Get memory context if available
        if self.memory:
            try:
                memory_context = self.memory.get_context(session_id)
                if memory_context:
                    context.update({
                        'recent_topics': getattr(memory_context, 'entities_mentioned', [])[:5],
                        'recent_tools': getattr(memory_context, 'tools_used', [])[-3:],
                        'analysis_state': getattr(memory_context, 'analysis_state', None)
                    })
            except Exception as e:
                logger.warning(f"Failed to get memory context: {e}")
        
        return context
    
    def _build_conversational_prompt(self, session_context: Dict) -> str:
        """Build the new conversational system prompt with data schema context (py-sidebot approach)."""
        
        # Core identity and expertise
        identity_section = """You are ChatMRPT, a malaria epidemiologist and statistician embedded in a specialized urban microstratification platform powered by GPT-4o.

## Your Expertise
- **Malaria risk assessment** in endemic countries, focusing on urban sub-Saharan Africa (especially Nigeria)
- **Urban microstratification** - ranking administrative units (wards, districts) by malaria risk for targeted interventions
- **WHO guidelines** for malaria intervention planning and resource allocation
- **Reprioritization strategies** - identifying areas for deprioritization or enhanced targeting based on burden, morphology, and intervention coverage
- **Two core analytical methods**:
  • Composite Risk Scoring: summing normalized values across malaria risk factors
  • Principal Component Analysis (PCA): dimensionality reduction with weighted variable combinations

## Common User Needs You Support
Based on typical requests, you help with:

### 1. Burden Stratification & Prioritization
- Ranking wards by composite risk scores
- Identifying top decile/quintile risk areas
- Finding high-burden wards with low intervention coverage
- Spatial dependency analysis of malaria burden

### 2. Urban Morphology & Settlement Analysis
- Classifying wards by settlement type (formal/informal/non-residential)
- Identifying morphology profiles (compactness, building density)
- Finding industrial zones affecting risk scores
- Determining likely uninhabited areas

### 3. Variable-Specific Insights
- Test Positivity Rate (TPR) analysis among under-fives
- Mapping relationships between variables and risk
- Identifying data quality issues (missing values, imputation)
- Understanding variable contributions to scores

### 4. Environmental & Spatial Drivers
- Flood-prone area identification
- Water body proximity analysis
- Elevation profiling and low-lying area risks
- Vegetation density impacts

### 5. Intervention Targeting & Reprioritization
- ITN distribution recommendations
- IRS eligibility based on burden and settlement type
- SMC round planning
- CHW deployment strategies
- Deprioritization decisions for resource optimization

### 6. Scenario Simulation ("What-If" Analysis)
- Impact of increased ITN coverage
- Effects of variable exclusion/inclusion
- Custom risk thresholds
- Settlement-specific classifications

### 7. Data Interpretation & Methodology
- Composite score calculation explanations
- PCA vs Composite scoring guidance
- Variable weighting options
- Data source transparency
- Missing value handling"""

        # Current session context with data schema (py-sidebot approach)
        data_schema_section = ""
        if session_context.get('data_schema'):
            data_schema_section = f"""
## Current Dataset Schema
{session_context['data_schema']}"""

        session_section = f"""
## Current Session Status
- **Geographic Area**: {session_context.get('state_name', 'Not specified - please specify the state you are working with')}
- **Data Uploaded**: {session_context.get('current_data', 'No data uploaded')}
- **Analysis Complete**: {session_context.get('analysis_complete', False)}
- **Ward Column**: {session_context.get('ward_column', 'Not identified')}
- **Available Variables**: {len(session_context.get('variables_used', []))} variables loaded
- **Recent Topics**: {', '.join(session_context.get('recent_topics', [])[:3]) if session_context.get('recent_topics') else 'None'}{data_schema_section}"""

        # Workflow guidance
        workflow_section = """
## Core Workflow Support
1. **Data Upload** → CSV/Excel demographics + shapefile boundaries
2. **Data Validation** → Confirm ward column and variable names
3. **Analysis Planning** → Discuss methodology options and variable selection
4. **Execution** → Run composite scoring and/or PCA analysis
5. **Interpretation** → Explain results with epidemiological context
6. **Visualization** → Generate risk maps, rankings, charts
7. **Intervention Planning** → Recommend targeting strategies

### Key Terminology You Use
- **Reprioritization**: Systematic reallocation of resources based on updated risk assessments
- **Deprioritization**: Identifying low-risk areas where resources can be safely reduced
- **Prioritization**: Targeting high-risk areas for intervention focus
- **Composite Score**: Sum of normalized risk variables (0-1 scale per variable)
- **PCA Score**: First principal component capturing maximum variance
- **Settlement Types**: Formal (planned), Informal (unplanned), Non-residential
- **TPR**: Test Positivity Rate among children under 5 years
- **Urban Extent**: Percentage of ward area classified as urban
- **Compactness**: Building density measure indicating settlement patterns

### State & Geographic Tracking
- Always ask for and track the **state name** for the geographic area
- Use this context in all responses: "In [State Name], the highest risk wards are..."
- Reference specific ward names when available
- Acknowledge LGA (Local Government Area) hierarchies when relevant"""

        # Communication guidelines
        guidelines_section = """
## Communication Guidelines

### Data Upload Workflow
1. When users upload CSV/Excel files:
   - Search for "wardname" column (case-insensitive)
   - If not found, identify likely alternatives ("ward", "district", "area", "lga")
   - Ask user to confirm ward column before proceeding
   - Validate first row contains variable names (discard if entirely numeric)
   - Check for key variables: TPR, population, elevation, flood risk, settlement data

### Communication Style
- **Match user expertise level**: Simplify for program staff, use technical language for data scientists
- **Maintain friendly, respectful tone**
- **Reference state name, ward names, and data context** in all relevant responses
- **Provide step-by-step explanations** for analyses and visualizations
- **Maintain session memory** for continuity across interactions

### Handling Common Questions
- **"Which wards to deprioritize?"** → Identify low-risk wards with current high coverage
- **"What variables matter most?"** → Reference PCA loadings or composite score contributions
- **"Can I use custom variables?"** → Yes, guide them through variable selection
- **"What if ITN coverage increases by X%?"** → Suggest scenario simulation with fork
- **"How to interpret the results?"** → Explain scores, rankings, and practical implications
- **"Missing data handling?"** → Explain spatial mean imputation for TPR, other strategies

### Analysis Approach
- **Explore before executing**: Discuss options, trade-offs, scenarios
- **Ask clarifying questions** when requests are ambiguous
- **Explain epidemiological significance** of findings
- **Provide actionable insights** for malaria program decision-making
- **Reference actual ward names and data** in responses, not generic examples
- **Offer both technical explanations and practical recommendations**"""

        # Conversation flow
        flow_section = f"""
## Conversation Flow
- **For hypothetical questions** ("what if..."): Create scenario forks for safe exploration
- **For analysis requests**: Confirm parameters, then execute appropriate functions
- **For data questions**: Access session data directly and provide contextual responses
- **For explanations**: Use domain expertise to explain concepts clearly
- **For unclear requests**: Ask clarifying questions rather than assuming

### Scenario Fork Management
- **When users say "what if..."**: Automatically create scenario forks for safe exploration
- **Current session type**: {'Fork' if session_context.get('is_fork') else 'Main conversation'}
- **Parent session**: {session_context.get('parent_session_id', 'N/A')}
- **Available forks**: {len(session_context.get('conversation_forks', []))} scenarios created

### Function Usage
- Use available functions when users are ready to execute analysis
- For scenario exploration, use `create_scenario_fork` for what-if analysis
- Use `return_to_main_conversation` to return from scenario forks
- Always explain methodology and interpret results with epidemiological context
- Show actual data (ward names, scores, rankings) not generic responses

### Natural Capabilities (No functions needed)
- **Greetings & explanations** - Respond using domain expertise
- **Data inquiries** - Access session data directly for ward lookups, scores, rankings
- **Simple visualizations** - Generate basic charts using available data
- **Concept explanations** - Explain malaria epidemiology, methods, terminology
- **Result interpretation** - Analyze findings and provide epidemiological insights
- **Intervention recommendations** - Suggest targeting strategies based on risk patterns"""

        return f"{identity_section}\n{session_section}\n{workflow_section}\n{guidelines_section}\n{flow_section}"
    
    def _get_available_functions(self) -> List[Dict]:
        """Get available tools as OpenAI-style functions."""
        functions = []
        
        # Core Analysis Functions
        functions.extend([
            {
                "name": "run_complete_analysis",
                "description": "Execute both composite and PCA analysis with optional custom variables",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "composite_variables": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Custom variables for composite analysis"
                        },
                        "pca_variables": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Custom variables for PCA analysis"
                        }
                    }
                }
            },
            {
                "name": "run_composite_analysis",
                "description": "Run composite risk scoring with custom variables - allows variable selection and weighting",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "variables": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Variables to include (e.g., TPR, elevation, flood_risk, population_density)"
                        },
                        "weights": {
                            "type": "object",
                            "description": "Optional custom weights for variables (default: equal weights)",
                            "additionalProperties": {"type": "number"}
                        }
                    }
                }
            },
            {
                "name": "run_pca_analysis",
                "description": "Run Principal Component Analysis with custom variables",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "variables": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Variables to include in PCA analysis"
                        }
                    }
                }
            }
        ])
        
        # Essential Visualization Functions
        functions.extend([
            {
                "name": "create_vulnerability_map",
                "description": "Generate choropleth vulnerability maps",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "method": {
                            "type": "string",
                            "enum": ["composite", "pca", "auto"],
                            "description": "Analysis method to visualize"
                        }
                    }
                }
            },
            {
                "name": "create_scatter_plot",
                "description": "Create scatter plots for variable relationships",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "x_variable": {
                            "type": "string",
                            "description": "Variable for x-axis"
                        },
                        "y_variable": {
                            "type": "string",
                            "description": "Variable for y-axis"
                        }
                    },
                    "required": ["x_variable", "y_variable"]
                }
            }
        ])
        
        # Data Processing Functions
        functions.extend([
            {
                "name": "get_ward_information",
                "description": "Get comprehensive information about a specific ward",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ward_name": {
                            "type": "string",
                            "description": "Name of the ward to analyze"
                        }
                    },
                    "required": ["ward_name"]
                }
            },
            {
                "name": "get_top_risk_wards",
                "description": "Get top N highest risk wards for prioritization or find low-risk wards for deprioritization",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "top_n": {
                            "type": "integer",
                            "description": "Number of wards to return",
                            "default": 10
                        },
                        "method": {
                            "type": "string",
                            "enum": ["composite", "pca", "both"],
                            "description": "Analysis method to use",
                            "default": "both"
                        },
                        "risk_level": {
                            "type": "string",
                            "enum": ["highest", "lowest", "decile", "quintile"],
                            "description": "Risk level to retrieve",
                            "default": "highest"
                        }
                    }
                }
            }
        ])
        
        # Knowledge Functions
        functions.extend([
            {
                "name": "explain_analysis_methodology",
                "description": "Explain the composite and PCA analysis methods, including reprioritization concepts",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "methods": {
                            "type": "array",
                            "items": {"type": "string", "enum": ["composite", "pca", "reprioritization", "deprioritization"]},
                            "description": "Methods or concepts to explain"
                        },
                        "technical_level": {
                            "type": "string",
                            "enum": ["basic", "intermediate", "advanced"],
                            "description": "Level of technical detail",
                            "default": "intermediate"
                        }
                    }
                }
            }
        ])
        
        # Fork Management Functions
        functions.extend([
            {
                "name": "create_scenario_fork",
                "description": "Create a new scenario fork for what-if analysis",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "scenario_name": {
                            "type": "string",
                            "description": "Name for the scenario being explored"
                        },
                        "description": {
                            "type": "string",
                            "description": "Description of what will be tested in this scenario"
                        }
                    },
                    "required": ["scenario_name"]
                }
            },
            {
                "name": "return_to_main_conversation",
                "description": "Return to the main conversation from a scenario fork",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "save_insights": {
                            "type": "boolean",
                            "description": "Whether to save insights from the fork",
                            "default": False
                        }
                    }
                }
            },
            {
                "name": "list_scenario_forks",
                "description": "List all scenario forks created during this conversation",
                "parameters": {
                    "type": "object",
                    "properties": {}
                }
            }
        ])
        
        return functions
    
    def _build_conversation_messages(self, user_message: str, session_context: Dict, session_id: str) -> List[Dict]:
        """Build message array for conversation."""
        messages = []
        
        # Add recent conversation history
        history = session_context.get('conversation_history', [])
        if history:
            # Add last 5 exchanges to maintain context
            for exchange in history[-5:]:
                messages.append({"role": "user", "content": exchange.get('user', '')})
                messages.append({"role": "assistant", "content": exchange.get('assistant', '')})
        
        # Add current user message
        messages.append({"role": "user", "content": user_message})
        
        return messages
    
    def _process_llm_response(self, response: Dict, user_message: str, session_id: str) -> Dict[str, Any]:
        """Process LLM response with function calls."""
        try:
            # Check if LLM called functions
            if response.get('function_call'):
                # Execute the function
                function_name = response['function_call']['name']
                function_args = json.loads(response['function_call']['arguments'])
                
                logger.info(f"Executing function: {function_name} with args: {function_args}")
                
                # Execute tool
                tool_result = self.execute_tool_with_registry(function_name, session_id, **function_args)
                
                # Format response
                if tool_result.get('status') == 'success':
                    response_text = tool_result.get('message', tool_result.get('response', 'Analysis completed successfully'))
                    visualizations = []
                    
                    
                    # Extract visualizations if any
                    if 'web_path' in tool_result:
                        visualizations.append({
                            'type': tool_result.get('chart_type', function_name),
                            'url': tool_result['web_path'],
                            'title': tool_result.get('message', 'Visualization'),
                            'tool': function_name
                        })
                    
                    return {
                        'status': 'success',
                        'response': response_text,
                        'visualizations': visualizations,
                        'tools_used': [function_name]
                    }
                else:
                    return {
                        'status': 'error',
                        'response': f"I encountered an issue with the {function_name} analysis: {tool_result.get('message', 'Unknown error')}",
                        'visualizations': []
                    }
            
            # No function call - pure conversational response
            return {
                'status': 'success',
                'response': response.get('content', response.get('message', 'I can help you with malaria risk analysis. What would you like to explore?')),
                'visualizations': [],
                'tools_used': []
            }
            
        except Exception as e:
            logger.error(f"Error processing LLM response: {e}")
            return {
                'status': 'error',
                'response': f'I encountered an issue processing your request: {str(e)}',
                'visualizations': []
            }
    
    def _is_confirmation_message(self, user_message: str) -> bool:
        """Check if user message is a confirmation for running analysis."""
        confirmation_patterns = [
            'yes', 'yeah', 'yep', 'sure', 'ok', 'okay', 'proceed', 'go ahead', 
            'run', 'start', 'analyze', 'do it', 'continue', 'confirm', 'agreed',
            'composite', 'pca', 'analysis', 'first run'
        ]
        
        # Comprehensive analysis patterns
        comprehensive_patterns = [
            'comprehensive analysis', 'complete analysis', 'full analysis',
            'run comprehensive analysis', 'proceed with analysis',
            'ready to analyze', 'run the analysis', 'start analysis',
            'comprehensive malaria risk analysis', 'ready for analysis'
        ]
        
        user_lower = user_message.lower().strip()
        
        # Check for comprehensive analysis phrases first
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
    
    def _execute_automatic_analysis(self, session_id: str) -> Dict[str, Any]:
        """Execute automatic complete analysis when user confirms."""
        try:
            result = self.execute_tool_with_registry('runcompleteanalysis', session_id)
            
            if result.get('status') == 'success':
                return {
                    'status': 'success',
                    'response': result.get('message', 'Analysis completed successfully'),
                    'visualizations': [],
                    'tools_used': ['runcompleteanalysis']
                }
            else:
                return {
                    'status': 'error',
                    'response': f"Analysis failed: {result.get('message', 'Unknown error')}",
                    'visualizations': []
                }
                
        except Exception as e:
            logger.error(f"Error in automatic analysis: {e}")
            return {
                'status': 'error',
                'response': f'Failed to run analysis: {str(e)}',
                'visualizations': []
            }
    
    def _generate_automatic_data_description(self, session_id: str) -> Dict[str, Any]:
        """Generate automatic data description (disabled - handled by frontend)."""
        from flask import session
        
        logger.info(f"Automatic data description disabled - handled by frontend for session {session_id}")
        
        # Set flag for analysis permission handling
        session['should_ask_analysis_permission'] = True
        
        return {
            'status': 'success',
            'response': '',  # Empty response - frontend handles the display
            'visualizations': [],
            'automatic_workflow': 'data_description_complete'
        }
    
    def _update_conversation_history(self, session_id: str, user_message: str, assistant_response: str):
        """Update conversation history in session."""
        try:
            from flask import session
            
            if 'conversation_history' not in session:
                session['conversation_history'] = []
            
            session['conversation_history'].append({
                'user': user_message,
                'assistant': assistant_response,
                'timestamp': time.time()
            })
            
            # Keep only last 10 exchanges
            if len(session['conversation_history']) > 10:
                session['conversation_history'] = session['conversation_history'][-10:]
                
        except Exception as e:
            logger.error(f"Error updating conversation history: {e}")
    
    def _store_conversation_in_memory(self, user_message: str, ai_response: str, 
                                    tools_used: List[str], response_time: float, 
                                    success: bool):
        """Store conversation turn in unified memory."""
        if not self.memory:
            return
            
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
    
    def execute_tool_with_registry(self, tool_name: str, session_id: str, **parameters):
        """Execute a tool using the tiered loading system."""
        
        # Handle fork management functions directly
        if tool_name == 'create_scenario_fork':
            return self._handle_create_scenario_fork(session_id, **parameters)
        elif tool_name == 'return_to_main_conversation':
            return self._handle_return_to_main(session_id, **parameters)
        elif tool_name == 'list_scenario_forks':
            return self._handle_list_forks(session_id, **parameters)
        
        # Regular tool execution
        logger.info(f"🚀 Executing tool '{tool_name}' via tiered loader")
        return self.tiered_loader.execute_tool(tool_name, session_id, **parameters)
    
    def _handle_create_scenario_fork(self, session_id: str, **parameters) -> Dict[str, Any]:
        """Handle creating a scenario fork."""
        scenario_name = parameters.get('scenario_name', 'exploration')
        description = parameters.get('description', '')
        
        fork_id = self.fork_conversation(session_id, scenario_name)
        
        if fork_id:
            return {
                'status': 'success',
                'message': f"🔀 Created scenario fork: **{scenario_name}**\n\n{description}\n\nYou can now explore this scenario safely. Use 'return_to_main_conversation' to go back to your original analysis.",
                'fork_id': fork_id,
                'scenario_name': scenario_name
            }
        else:
            return {
                'status': 'error',
                'message': 'Failed to create scenario fork. Please try again.',
                'fork_id': None
            }
    
    def _handle_return_to_main(self, session_id: str, **parameters) -> Dict[str, Any]:
        """Handle returning to main conversation."""
        from flask import session
        
        # Get parent session ID
        parent_session_id = session.get('parent_session_id')
        
        if parent_session_id:
            success = self.return_to_main_session(parent_session_id)
            
            if success:
                return {
                    'status': 'success',
                    'message': "🔙 Returned to main conversation. Your original analysis is restored.",
                    'returned_to': parent_session_id
                }
            else:
                return {
                    'status': 'error',
                    'message': 'Failed to return to main conversation. Please try again.',
                    'returned_to': None
                }
        else:
            return {
                'status': 'error',
                'message': 'You are already in the main conversation.',
                'returned_to': None
            }
    
    def _handle_list_forks(self, session_id: str, **parameters) -> Dict[str, Any]:
        """Handle listing scenario forks."""
        forks = self.get_conversation_forks(session_id)
        
        if forks:
            fork_list = []
            for fork in forks:
                fork_list.append(f"• **{fork['scenario']}** (created {time.strftime('%H:%M:%S', time.localtime(fork['created_at']))})")
            
            message = f"🔀 **Scenario Forks Created:**\n\n" + "\n".join(fork_list)
            message += f"\n\n*Total: {len(forks)} scenario forks*"
        else:
            message = "No scenario forks have been created yet. Say 'what if...' to explore alternatives!"
        
        return {
            'status': 'success',
            'message': message,
            'forks': forks
        }
    
    def fork_conversation(self, session_id: str, scenario_name: str = None) -> str:
        """Fork conversation for scenario exploration like py-sidebot."""
        try:
            from flask import session
            import os
            import shutil
            
            # Generate fork ID
            timestamp = int(time.time())
            scenario_suffix = f"_{scenario_name}" if scenario_name else ""
            fork_id = f"{session_id}_fork{scenario_suffix}_{timestamp}"
            
            logger.info(f"🔀 Forking conversation: {session_id} → {fork_id}")
            
            # Copy session data to fork
            original_session_data = dict(session)
            
            # Create fork session data
            fork_session_data = original_session_data.copy()
            fork_session_data.update({
                'parent_session_id': session_id,
                'is_fork': True,
                'fork_scenario': scenario_name or 'exploration',
                'fork_created_at': timestamp,
                'session_id': fork_id  # Update session ID
            })
            
            # Copy uploaded files if they exist
            original_upload_dir = f"instance/uploads/{session_id}"
            fork_upload_dir = f"instance/uploads/{fork_id}"
            
            if os.path.exists(original_upload_dir):
                logger.info(f"📁 Copying session files: {original_upload_dir} → {fork_upload_dir}")
                shutil.copytree(original_upload_dir, fork_upload_dir)
            
            # Store fork session data (we'll set it when user switches to fork)
            if not hasattr(self, '_fork_sessions'):
                self._fork_sessions = {}
            self._fork_sessions[fork_id] = fork_session_data
            
            # Add fork to parent session tracking
            if 'conversation_forks' not in session:
                session['conversation_forks'] = []
            session['conversation_forks'].append({
                'fork_id': fork_id,
                'scenario': scenario_name or 'exploration',
                'created_at': timestamp
            })
            
            logger.info(f"✅ Fork created successfully: {fork_id}")
            return fork_id
            
        except Exception as e:
            logger.error(f"❌ Error forking conversation: {e}")
            return None
    
    def switch_to_fork(self, fork_id: str) -> bool:
        """Switch to a forked session."""
        try:
            from flask import session
            
            if hasattr(self, '_fork_sessions') and fork_id in self._fork_sessions:
                # Store current session as backup
                if not hasattr(self, '_session_backup'):
                    self._session_backup = {}
                self._session_backup[session.get('session_id', 'unknown')] = dict(session)
                
                # Switch to fork session
                fork_data = self._fork_sessions[fork_id]
                session.clear()
                session.update(fork_data)
                
                logger.info(f"🔄 Switched to fork: {fork_id}")
                return True
            else:
                logger.warning(f"⚠️ Fork not found: {fork_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error switching to fork: {e}")
            return False
    
    def return_to_main_session(self, main_session_id: str) -> bool:
        """Return to main session from fork."""
        try:
            from flask import session
            
            if hasattr(self, '_session_backup') and main_session_id in self._session_backup:
                # Restore main session
                main_session_data = self._session_backup[main_session_id]
                session.clear()
                session.update(main_session_data)
                
                logger.info(f"🔙 Returned to main session: {main_session_id}")
                return True
            else:
                logger.warning(f"⚠️ Main session backup not found: {main_session_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error returning to main session: {e}")
            return False
    
    def get_conversation_forks(self, session_id: str) -> List[Dict]:
        """Get list of conversation forks for a session."""
        from flask import session
        return session.get('conversation_forks', [])
    
    def _detect_fork_intent(self, user_message: str) -> Dict[str, Any]:
        """Detect if user wants to fork conversation for what-if scenarios."""
        fork_indicators = [
            'what if', 'suppose', 'let\'s try', 'how about', 'alternatively', 
            'what would happen if', 'let me explore', 'can we try', 'test this scenario',
            'hypothetically', 'for comparison', 'different approach', 'another way'
        ]
        
        message_lower = user_message.lower()
        
        for indicator in fork_indicators:
            if indicator in message_lower:
                return {
                    'should_fork': True,
                    'trigger_phrase': indicator,
                    'scenario_name': self._extract_scenario_name(user_message, indicator)
                }
        
        return {'should_fork': False}
    
    def _extract_scenario_name(self, user_message: str, trigger_phrase: str) -> str:
        """Extract scenario name from user message."""
        # Simple extraction - could be enhanced
        message_lower = user_message.lower()
        
        if 'variable' in message_lower:
            return 'custom_variables'
        elif 'threshold' in message_lower:
            return 'threshold_test'
        elif 'method' in message_lower:
            return 'method_comparison'
        elif 'ward' in message_lower:
            return 'ward_focus'
        else:
            return 'exploration'
    
    # Legacy methods for backward compatibility
    def parse_request(self, user_message: str, session_id: str) -> Dict[str, Any]:
        """Legacy method - redirects to new conversational processing."""
        logger.info("Using legacy parse_request - redirecting to conversational processing")
        result = self.process_message(user_message, session_id)
        
        # Convert to legacy format for compatibility
        if result.get('status') == 'success':
            return {
                'status': 'success',
                'parsed_intent': {
                    'intent_type': 'data_analysis',
                    'primary_goal': 'Conversational processing',
                    'tool_calls': [],
                    'requires_session_data': True
                }
            }
        else:
            return {
                'status': 'error',
                'message': result.get('response', 'Error in conversational processing')
            }
    
    def execute_intent(self, parsed_intent: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Legacy method - maintained for backward compatibility."""
        logger.info("Using legacy execute_intent - this should be replaced with conversational processing")
        return {
            'status': 'success',
            'message': 'Legacy method - use conversational processing instead',
            'results': []
        }
    
    def format_response(self, execution_results: Dict[str, Any], user_message: str, session_id: str = None) -> Dict[str, Any]:
        """Legacy method - maintained for backward compatibility."""
        logger.info("Using legacy format_response - this should be replaced with conversational processing")
        return {
            'status': 'success',
            'response': 'Legacy method - use conversational processing instead',
            'visualizations': []
        }
    
    def _generate_simple_response(self, user_message: str, session_id: str) -> Dict[str, Any]:
        """Generate a simple LLM response without Flask session dependencies for streaming."""
        try:
            # Simple system prompt without session-specific workflows
            system_prompt = """You are ChatMRPT, a specialized assistant for malaria risk assessment and urban microstratification.

You help with:
- Malaria risk analysis using composite scoring and PCA
- Urban settlement analysis and intervention targeting  
- Data interpretation and visualization
- Epidemiological insights and recommendations

Provide helpful, conversational responses about malaria analysis and public health topics."""

            # Simple message structure
            messages = [
                {"role": "user", "content": user_message}
            ]
            
            # Generate response using LLM manager
            try:
                logger.info("Calling LLM manager for simple response...")
                response = self.llm_manager.generate_response(
                    prompt=user_message,
                    system_message=system_prompt,
                    temperature=0.7,
                    session_id=session_id
                )
                logger.info(f"LLM response received: {len(response)} characters")
            except Exception as llm_error:
                logger.error(f"LLM call failed: {llm_error}")
                # Fallback to a simple response
                response = f"I'm ChatMRPT, your malaria risk analysis assistant. I encountered a technical issue but I'm here to help with malaria analysis, risk assessment, and intervention planning. How can I assist you?"
            
            return {
                'status': 'success',
                'response': response,
                'visualizations': [],
                'tools_used': [],
                'intent_type': 'conversational',
                'fallback': True
            }
            
        except Exception as e:
            logger.error(f"Error in simple response generation: {e}")
            return {
                'status': 'error', 
                'response': f'I encountered an issue processing your request: {str(e)}', 
                'visualizations': [],
                'fallback': True
            }
    
    # ========================================================================
    # PY-SIDEBOT LIGHTWEIGHT MEMORY IMPLEMENTATION
    # ========================================================================
    
    def _store_conversation_simple(self, session_id: str, user_message: str, assistant_response: str):
        """Store conversation in simple in-memory history (py-sidebot approach)."""
        if session_id not in self.conversation_history:
            self.conversation_history[session_id] = []
        
        self.conversation_history[session_id].append({
            'user': user_message,
            'assistant': assistant_response,
            'timestamp': time.time()
        })
        
        # Keep only last 10 exchanges per session to prevent memory bloat
        if len(self.conversation_history[session_id]) > 10:
            self.conversation_history[session_id] = self.conversation_history[session_id][-10:]
    
    def _get_session_context(self, session_id: str) -> Dict:
        """Get session context including conversation history and data schema."""
        context = {
            'session_id': session_id,
            'conversation_history': self.conversation_history.get(session_id, []),
            'data_schema': None,
            'current_data': None
        }
        
        # Add data schema context (py-sidebot approach)
        try:
            data_handler = self.data_service.get_handler(session_id)
            if data_handler and hasattr(data_handler, 'df') and data_handler.df is not None:
                # Generate data schema like py-sidebot
                context['data_schema'] = self._generate_data_schema(data_handler.df)
                context['current_data'] = 'CSV data loaded with shapefile'
        except Exception as e:
            logger.debug(f"Could not get data context: {e}")
        
        return context
    
    def _generate_data_schema(self, df) -> str:
        """Generate data schema description like py-sidebot's df_to_schema."""
        try:
            schema_parts = []
            schema_parts.append(f"Dataset: {len(df)} rows, {len(df.columns)} columns")
            
            # Column information
            for col in df.columns:
                dtype = str(df[col].dtype)
                non_null = df[col].notna().sum()
                
                if df[col].dtype == 'object':
                    unique_count = df[col].nunique()
                    if unique_count <= 10:
                        unique_vals = df[col].unique()[:5]
                        schema_parts.append(f"- {col}: categorical ({non_null}/{len(df)} non-null, {unique_count} unique values: {list(unique_vals)})")
                    else:
                        schema_parts.append(f"- {col}: text ({non_null}/{len(df)} non-null, {unique_count} unique values)")
                else:
                    min_val = df[col].min()
                    max_val = df[col].max()
                    schema_parts.append(f"- {col}: numeric ({non_null}/{len(df)} non-null, range: {min_val} to {max_val})")
            
            return "\n".join(schema_parts)
        except Exception as e:
            logger.error(f"Error generating data schema: {e}")
            return "Data schema unavailable"