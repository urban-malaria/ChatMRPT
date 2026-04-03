"""
Pure LLM Manager for ChatMRPT - Zero Pattern Matching

This module provides clean LLM functionality with no business logic,
no hardcoded mappings, and no pattern matching. Pure LLM-first design.
"""

import json
import time
import logging
import openai
import importlib
from typing import Dict, Any, Optional, List
from flask import current_app

logger = logging.getLogger(__name__)


class LLMManager:
    """
    Pure LLM interface with zero pattern matching.
    
    This class handles only LLM communication - no business logic,
    no hardcoded tool mappings, no format detection patterns.
    """
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4o", interaction_logger=None):
        """Initialize pure LLM manager."""
        import os
        # Use OpenAI for proper function calling support
        self.api_key = api_key or self._get_api_key_from_config()
        self.model = model
        self.interaction_logger = interaction_logger
        self.client = None
        
        try:
            if self.api_key:
                self.client = openai.OpenAI(api_key=self.api_key)
                logger.info(f"🧠 Pure LLM Manager initialized with {self.model} (OpenAI)")
            else:
                logger.warning("No OpenAI API key found - will try to get from config later")
                self.client = None
        except Exception as e:
            logger.error(f"Error initializing OpenAI client: {str(e)}")
            self.client = None
    
    def _get_api_key_from_config(self) -> Optional[str]:
        """Get API key from Flask config."""
        try:
            return current_app.config.get('OPENAI_API_KEY')
        except:
            import os
            return os.environ.get('OPENAI_API_KEY')
    
    def generate_response(self, prompt: str, context: Optional[Any] = None, 
                         system_message: Optional[str] = None, temperature: float = 0.7, 
                         max_tokens: int = 1000, session_id: Optional[str] = None) -> str:
        """Generate pure LLM response with no business logic."""
        if not self.client:
            if not self.api_key:
                return "Error: No API key available for LLM"
            try:
                self.client = openai.OpenAI(api_key=self.api_key)
            except Exception as e:
                return f"Error initializing OpenAI client: {str(e)}"
        
        messages = []
        
        # System message
        if system_message:
            messages.append({"role": "system", "content": system_message})
        else:
            messages.append({
                "role": "system", 
                "content": """You are a malaria epidemiologist embedded in ChatMRPT, a malaria risk assessment system for urban microstratification in Nigeria.

EXPERTISE: Malaria biology, transmission, vector control, epidemiology, urban microstratification, PCA analysis, vulnerability mapping, and intervention targeting.

CHATMRPT: Analyzes ward-level data, creates risk maps, generates visualizations, supports CSV/shapefile uploads.

STYLE: Clear, professional, friendly. Provide direct explanations for "what is" and "explain" questions. Adapt technical level to context."""
            })
        
        # Context (let LLM decide how to use it)
        if context:
            if isinstance(context, str):
                messages.append({"role": "system", "content": f"Context: {context}"})
            elif isinstance(context, dict):
                try:
                    context_str = json.dumps(context, indent=2, default=str)
                    messages.append({"role": "system", "content": f"Data context:\n{context_str}"})
                except:
                    messages.append({"role": "system", "content": f"Context: {str(context)}"})
        
        messages.append({"role": "user", "content": prompt})
        
        start_time = time.time()
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            
            llm_response = response.choices[0].message.content
            
            # Simple logging without pattern matching
            if self.interaction_logger and session_id:
                latency = time.time() - start_time
                tokens_used = response.usage.total_tokens if hasattr(response, 'usage') else None
                self.interaction_logger.log_llm_interaction(
                    session_id=session_id,
                    prompt_type="standard",
                    prompt=prompt,
                    prompt_context=context,
                    response=llm_response,
                    tokens_used=tokens_used,
                    latency=latency
                )
            
            return llm_response
            
        except Exception as e:
            error_message = f"Error calling LLM: {str(e)}"
            logger.error(error_message)
            
            if self.interaction_logger and session_id:
                self.interaction_logger.log_error(
                    session_id=session_id,
                    error_type="llm_api_error",
                    error_message=str(e)
                )
            
            return f"I apologize, but I encountered an error: {str(e)}"
    
    def generate_with_tools(self, message: str, system_prompt: str, tools: List[Dict[str, Any]], 
                           context: Optional[str] = None, session_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate response with tool calling - pure dynamic tool resolution."""
        if not self.client:
            if not self.api_key:
                return {'response': "Error: No API key available", 'tool_calls': []}
            try:
                self.client = openai.OpenAI(api_key=self.api_key)
            except Exception as e:
                return {'response': f"Error initializing client: {str(e)}", 'tool_calls': []}
        
        messages = [{"role": "system", "content": system_prompt}]
        
        if context:
            messages.append({"role": "system", "content": f"Session context: {context}"})
        
        messages.append({"role": "user", "content": message})
        
        # Convert tools to OpenAI format and create dynamic tool map
        openai_tools = []
        tool_map = {}
        
        for tool in tools:
            if "function" in tool:
                function_def = tool["function"]
                openai_tool = {
                    "type": "function",
                    "function": {
                        "name": function_def["name"],
                        "description": function_def["description"],
                        "parameters": function_def["parameters"]
                    }
                }
                openai_tools.append(openai_tool)
                
                # Dynamic tool resolution - no hardcoding!
                tool_name = function_def["name"]
                tool_function = self._resolve_tool_dynamically(tool_name)
                if tool_function:
                    tool_map[tool_name] = tool_function
                else:
                    logger.warning(f"Could not resolve tool: {tool_name}")
            else:
                # Legacy flat format
                openai_tool = {
                    "type": "function",
                    "function": {
                        "name": tool["name"],
                        "description": tool["description"],
                        "parameters": tool["parameters"]
                    }
                }
                openai_tools.append(openai_tool)
                tool_map[tool["name"]] = tool["function"]
        
        start_time = time.time()
        
        try:
            # Initial LLM call with tools
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=openai_tools,
                tool_choice="auto",
                temperature=0.7,
                max_tokens=1000
            )
            
            assistant_message = response.choices[0].message
            tool_calls = []
            
            if assistant_message.tool_calls:
                # Add assistant message to conversation
                messages.append({
                    "role": "assistant",
                    "content": assistant_message.content,
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": tc.type,
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments
                            }
                        }
                        for tc in assistant_message.tool_calls
                    ]
                })
                
                # Execute tools dynamically
                for tool_call in assistant_message.tool_calls:
                    function_name = tool_call.function.name
                    function_args = json.loads(tool_call.function.arguments)
                    
                    if function_name in tool_map:
                        try:
                            tool_result = tool_map[function_name](**function_args)
                            
                            # Add tool result to conversation
                            messages.append({
                                "role": "tool",
                                "tool_call_id": tool_call.id,
                                "content": json.dumps(tool_result, default=str)
                            })
                            
                            tool_calls.append({
                                "function": function_name,
                                "arguments": function_args,
                                "result": tool_result
                            })
                            
                        except Exception as e:
                            error_msg = f"Error executing {function_name}: {str(e)}"
                            messages.append({
                                "role": "tool",
                                "tool_call_id": tool_call.id,
                                "content": json.dumps({"error": error_msg})
                            })
                            tool_calls.append({
                                "function": function_name,
                                "arguments": function_args,
                                "error": error_msg
                            })
                    else:
                        error_msg = f"Function {function_name} not found"
                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": json.dumps({"error": error_msg})
                        })
                        tool_calls.append({
                            "function": function_name,
                            "arguments": function_args,
                            "error": error_msg
                        })
                
                # Get final response after tool execution
                final_response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=0.7,
                    max_tokens=1000
                )
                
                final_content = final_response.choices[0].message.content
            else:
                # No tools called, use original response
                final_content = assistant_message.content
                
            # Log the interaction
            if self.interaction_logger and session_id:
                latency = time.time() - start_time
                self.interaction_logger.log_llm_interaction(
                    session_id=session_id,
                    prompt_type="tool_calling",
                    prompt=message,
                    prompt_context=context,
                    response=final_content,
                    tokens_used=None,
                    latency=latency
                )
            
            return {
                'response': final_content,
                'tool_calls': tool_calls
            }
        
        except Exception as e:
            error_message = f"Error in tool calling: {str(e)}"
            logger.error(error_message)
            
            if self.interaction_logger and session_id:
                self.interaction_logger.log_error(
                    session_id=session_id,
                    error_type="tool_calling_error",
                    error_message=str(e)
                )
            
            return {
                'response': f"I apologize, but I encountered an error: {str(e)}",
                'tool_calls': []
            }
    
    def generate_with_functions(self, messages: List[Dict], system_prompt: str, 
                              functions: List[Dict], temperature: float = 0.7, 
                              session_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate response with OpenAI function calling - new conversational approach."""
        if not self.client:
            if not self.api_key:
                return {'content': "Error: No API key available", 'function_call': None}
            try:
                self.client = openai.OpenAI(api_key=self.api_key)
            except Exception as e:
                return {'content': f"Error initializing client: {str(e)}", 'function_call': None}
        
        # Build complete messages array
        complete_messages = [{"role": "system", "content": system_prompt}]
        complete_messages.extend(messages)
        
        # Convert functions to OpenAI format
        openai_functions = []
        for func in functions:
            openai_functions.append({
                "type": "function",
                "function": {
                    "name": func["name"],
                    "description": func["description"],
                    "parameters": func["parameters"]
                }
            })
        
        start_time = time.time()
        
        try:
            # Track detailed LLM timing
            api_call_start = time.time()
            response = self.client.chat.completions.create(
                model=self.model,
                messages=complete_messages,
                tools=openai_functions,
                tool_choice="auto",
                temperature=temperature,
                max_tokens=1000
            )
            api_call_duration = time.time() - api_call_start
            
            assistant_message = response.choices[0].message
            
            # Check if function was called
            if assistant_message.tool_calls:
                # Return the first function call (simplified for now)
                first_tool_call = assistant_message.tool_calls[0]
                
                # Log the interaction with enhanced timing
                if self.interaction_logger and session_id:
                    latency = time.time() - start_time
                    self.interaction_logger.log_llm_interaction(
                        session_id=session_id,
                        prompt_type="function_calling",
                        prompt=messages[-1].get('content', ''),
                        prompt_context=system_prompt,
                        response=f"Function call: {first_tool_call.function.name}",
                        tokens_used=response.usage.total_tokens if hasattr(response, 'usage') else None,
                        latency=latency,
                        enhanced_timing={
                            'api_call_duration_ms': api_call_duration * 1000,
                            'total_processing_ms': latency * 1000,
                            'overhead_ms': (latency - api_call_duration) * 1000,
                            'model_used': self.model,
                            'prompt_tokens': response.usage.prompt_tokens if hasattr(response, 'usage') else None,
                            'completion_tokens': response.usage.completion_tokens if hasattr(response, 'usage') else None,
                            'tokens_per_second': response.usage.total_tokens / latency if hasattr(response, 'usage') and latency > 0 else None
                        }
                    )
                
                return {
                    'content': assistant_message.content,
                    'function_call': {
                        'name': first_tool_call.function.name,
                        'arguments': first_tool_call.function.arguments
                    }
                }
            else:
                # No function call, return conversational response
                
                # Log the interaction with enhanced timing
                if self.interaction_logger and session_id:
                    latency = time.time() - start_time
                    self.interaction_logger.log_llm_interaction(
                        session_id=session_id,
                        prompt_type="conversational",
                        prompt=messages[-1].get('content', ''),
                        prompt_context=system_prompt,
                        response=assistant_message.content,
                        tokens_used=response.usage.total_tokens if hasattr(response, 'usage') else None,
                        latency=latency,
                        enhanced_timing={
                            'api_call_duration_ms': api_call_duration * 1000,
                            'total_processing_ms': latency * 1000,
                            'overhead_ms': (latency - api_call_duration) * 1000,
                            'model_used': self.model,
                            'prompt_tokens': response.usage.prompt_tokens if hasattr(response, 'usage') else None,
                            'completion_tokens': response.usage.completion_tokens if hasattr(response, 'usage') else None,
                            'tokens_per_second': response.usage.total_tokens / latency if hasattr(response, 'usage') and latency > 0 else None
                        }
                    )
                
                return {
                    'content': assistant_message.content,
                    'function_call': None
                }
                
        except Exception as e:
            error_message = f"Error in function calling: {str(e)}"
            logger.error(error_message)
            
            if self.interaction_logger and session_id:
                self.interaction_logger.log_error(
                    session_id=session_id,
                    error_type="function_calling_error",
                    error_message=str(e)
                )
            
            return {
                'content': f"I apologize, but I encountered an error: {str(e)}",
                'function_call': None
            }
    
    def generate_with_functions_streaming(self, messages: List[Dict], system_prompt: str, 
                                        functions: List[Dict], temperature: float = 0.7, 
                                        session_id: Optional[str] = None):
        """Generate streaming response with sentence-based chunking (FIXED)."""
        if not self.client:
            if not self.api_key:
                yield {'content': "Error: No API key available", 'function_call': None, 'done': True}
                return
            try:
                self.client = openai.OpenAI(api_key=self.api_key)
            except Exception as e:
                yield {'content': f"Error initializing client: {str(e)}", 'function_call': None, 'done': True}
                return
        
        # Build complete messages array
        complete_messages = [{"role": "system", "content": system_prompt}]
        complete_messages.extend(messages)
        
        # Convert functions to OpenAI format
        openai_functions = []
        for func in functions:
            openai_functions.append({
                "type": "function",
                "function": {
                    "name": func["name"],
                    "description": func["description"],
                    "parameters": func["parameters"]
                }
            })
        
        start_time = time.time()
        
        try:
            # Create streaming response
            response = self.client.chat.completions.create(
                model=self.model,
                messages=complete_messages,
                tools=openai_functions,
                tool_choice="auto",
                temperature=temperature,
                max_tokens=1000,
                stream=True
            )
            
            # Simple streaming - just pass through what OpenAI sends
            collected_content = ""
            tool_calls_accumulator = {}
            
            for chunk in response:
                # Stream content directly as it arrives from OpenAI
                if chunk.choices[0].delta.content:
                    chunk_content = chunk.choices[0].delta.content
                    collected_content += chunk_content
                    
                    # Simply yield each chunk immediately
                    yield {
                        'content': chunk_content,
                        'function_call': None,
                        'done': False
                    }
                
                # Properly accumulate tool calls
                if chunk.choices[0].delta.tool_calls:
                    for tool_call_chunk in chunk.choices[0].delta.tool_calls:
                        # Get the index of this tool call (default to 0)
                        idx = tool_call_chunk.index if hasattr(tool_call_chunk, 'index') else 0
                        
                        # Initialize accumulator for this tool call if needed
                        if idx not in tool_calls_accumulator:
                            tool_calls_accumulator[idx] = {
                                'id': '',
                                'type': 'function',
                                'function': {
                                    'name': '',
                                    'arguments': ''
                                }
                            }
                        
                        # Accumulate the tool call data
                        if hasattr(tool_call_chunk, 'id') and tool_call_chunk.id:
                            tool_calls_accumulator[idx]['id'] = tool_call_chunk.id
                        
                        if hasattr(tool_call_chunk, 'function'):
                            if hasattr(tool_call_chunk.function, 'name') and tool_call_chunk.function.name:
                                tool_calls_accumulator[idx]['function']['name'] = tool_call_chunk.function.name
                            if hasattr(tool_call_chunk.function, 'arguments') and tool_call_chunk.function.arguments:
                                tool_calls_accumulator[idx]['function']['arguments'] += tool_call_chunk.function.arguments
            
            # No need to check for remaining content - we stream everything immediately
            
            # Handle tool calls if present
            if tool_calls_accumulator:
                # Get the first complete tool call
                first_tool_call = tool_calls_accumulator.get(0, {})
                
                # Log the interaction
                if self.interaction_logger and session_id:
                    latency = time.time() - start_time
                    self.interaction_logger.log_llm_interaction(
                        session_id=session_id,
                        prompt_type="streaming_function_calling",
                        prompt=messages[-1].get('content', '') if messages else '',
                        prompt_context=system_prompt,
                        response=f"Function call: {first_tool_call.get('function', {}).get('name', 'unknown')}",
                        tokens_used=None,
                        latency=latency
                    )
                
                yield {
                    'content': '',  # No additional content for function calls
                    'function_call': {
                        'name': first_tool_call.get('function', {}).get('name', ''),
                        'arguments': first_tool_call.get('function', {}).get('arguments', '{}')
                    } if first_tool_call.get('function') else None,
                    'done': True
                }
            else:
                # No function call, conversational response complete
                
                # Log the interaction
                if self.interaction_logger and session_id:
                    latency = time.time() - start_time
                    self.interaction_logger.log_llm_interaction(
                        session_id=session_id,
                        prompt_type="streaming_conversational",
                        prompt=messages[-1].get('content', '') if messages else '',
                        prompt_context=system_prompt,
                        response=collected_content,
                        tokens_used=None,
                        latency=latency
                    )
                
                yield {
                    'content': '',  # Final completion marker
                    'function_call': None,
                    'done': True
                }
                
        except Exception as e:
            error_message = f"Error in streaming function calling: {str(e)}"
            logger.error(error_message)
            
            if self.interaction_logger and session_id:
                self.interaction_logger.log_error(
                    session_id=session_id,
                    error_type="streaming_function_calling_error",
                    error_message=str(e)
                )
            
            yield {
                'content': f"I apologize, but I encountered an error: {str(e)}",
                'function_call': None,
                'done': True
            }
    
    def _resolve_tool_dynamically(self, tool_name: str):
        """Dynamically resolve tool functions by name."""
        # REMOVED: Old tool resolution to avoid triggering 20-second registry initialization
        # The tiered loader should be used for tool resolution instead
        logger.warning(f"Tool resolution for {tool_name} skipped - use tiered loader instead")
        return None
    
    def explain_visualization(self, session_id: str, viz_type: str, 
                            context: Optional[Any] = None, question: Optional[str] = None) -> str:
        """Generate explanation for visualizations."""
        system_prompt = """
        You are a malaria epidemiologist and statistician embedded in the ChatMRPT system. 
        Explain visualizations clearly and accurately using accessible language. 
        Focus on what the visualization shows and its public health implications.
        Reference specific wards, state context, and data patterns when available.
        Maintain a friendly, respectful tone while providing professional insights.
        """
        
        prompt = f"Explain this {viz_type} visualization"
        if question:
            prompt += f" and answer: {question}"
        
        return self.generate_response(
            prompt=prompt,
            context=context,
            system_message=system_prompt,
            session_id=session_id
        )

    def generate_with_image(self, prompt: str, image_url: str, session_id: Optional[str] = None) -> str:
        """
        Generate response using image input (for visualization explanations).

        This method supports vision-enabled models like GPT-4V for analyzing
        visualization images in py-sidebot style.
        """
        if not self.client:
            if not self.api_key:
                return "Error: No API key available for image analysis"
            try:
                self.client = openai.OpenAI(api_key=self.api_key)
            except Exception as e:
                return f"Error initializing OpenAI client: {str(e)}"

        try:
            # Check if the model supports vision
            vision_models = ['gpt-4o', 'gpt-4-turbo', 'gpt-4-vision-preview']
            if self.model not in vision_models:
                # Fallback to text-only response
                return self.generate_response(
                    prompt=f"Explain this malaria visualization: {prompt}",
                    system_message="You are a malaria epidemiologist explaining visualizations to public health officials.",
                    session_id=session_id
                )

            messages = [
                {
                    "role": "system",
                    "content": "You are a malaria epidemiologist explaining visualizations to public health officials. Focus on epidemiological significance, intervention guidance, and actionable insights."
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": image_url}}
                    ]
                }
            ]

            start_time = time.time()

            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.7,
                max_tokens=500
            )

            llm_response = response.choices[0].message.content

            # Log the interaction
            if self.interaction_logger and session_id:
                latency = time.time() - start_time
                self.interaction_logger.log_llm_interaction(
                    session_id=session_id,
                    prompt_type="image_analysis",
                    prompt=prompt,
                    response=llm_response,
                    tokens_used=response.usage.total_tokens if hasattr(response, 'usage') else None,
                    latency=latency,
                    enhanced_timing={
                        'model_used': self.model,
                        'image_analysis': True,
                        'prompt_tokens': response.usage.prompt_tokens if hasattr(response, 'usage') else None,
                        'completion_tokens': response.usage.completion_tokens if hasattr(response, 'usage') else None
                    }
                )

            return llm_response

        except Exception as e:
            error_message = f"Error in image analysis: {str(e)}"
            logger.error(error_message)

            if self.interaction_logger and session_id:
                self.interaction_logger.log_error(
                    session_id=session_id,
                    error_type="image_analysis_error",
                    error_message=str(e)
                )

            # Fallback to text-only explanation
            return self.generate_response(
                prompt=f"Explain this malaria visualization: {prompt}",
                system_message="You are a malaria epidemiologist explaining visualizations to public health officials.",
                session_id=session_id
            )


def get_llm_manager(interaction_logger=None) -> LLMManager:
    """Factory function to create LLM manager."""
    return LLMManager(interaction_logger=interaction_logger)


def convert_markdown_to_html(markdown_text: str) -> str:
    """Convert markdown text to HTML."""
    try:
        import markdown
        return markdown.markdown(markdown_text)
    except ImportError:
        # Simple fallback if markdown package not available
        return markdown_text.replace('\n', '<br>')


def select_optimal_variables_with_llm(llm_manager, available_vars: List[str], csv_data, 
                                    relationships: Optional[Dict] = None,
                                    min_vars: int = 3, max_vars: int = 5) -> tuple:
    """
    Use LLM to select optimal variables for malaria risk analysis.
    
    Returns:
        tuple: (selected_variables, explanation)
    """
    try:
        system_prompt = """
        You are an expert epidemiologist specializing in malaria risk analysis.
        Select the most relevant variables for composite malaria risk scoring.
        
        Consider:
        - Health indicators (malaria prevalence, treatment access)
        - Environmental factors (climate, water sources)
        - Socioeconomic factors (poverty, education)
        - Infrastructure (healthcare access, transportation)
        
        Return a JSON object with selected variables and explanation.
        """
        
        var_list = ", ".join(available_vars[:20])  # Limit for prompt length
        
        prompt = f"""
        Select {min_vars}-{max_vars} most important variables from: {var_list}
        
        Return JSON:
        {{
            "selected_variables": ["var1", "var2", ...],
            "explanation": "Brief explanation of selection rationale"
        }}
        """
        
        response = llm_manager.generate_response(
            prompt=prompt,
            system_message=system_prompt,
            temperature=0.3
        )
        
        # Parse JSON response
        try:
            result = json.loads(response)
            selected_vars = result.get('selected_variables', available_vars[:max_vars])
            explanation = result.get('explanation', 'Variables selected by LLM')
            
            # Validate selected variables exist
            valid_vars = [var for var in selected_vars if var in available_vars]
            
            if len(valid_vars) < min_vars:
                # Fallback: add more variables
                remaining = [var for var in available_vars if var not in valid_vars]
                valid_vars.extend(remaining[:min_vars - len(valid_vars)])
            
            return valid_vars[:max_vars], explanation
            
        except json.JSONDecodeError:
            # Fallback: return first variables
            return available_vars[:max_vars], "Default variable selection (JSON parsing failed)"
    
    except Exception as e:
        logger.error(f"Error in LLM variable selection: {e}")
        return available_vars[:max_vars], "Default variable selection (error occurred)" 