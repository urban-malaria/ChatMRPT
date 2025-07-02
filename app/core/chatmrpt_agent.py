"""
ChatMRPT ReAct Agent - Phase 3 Implementation

This module implements a ReAct-based conversational agent for malaria analytics.
The agent integrates with existing ChatMRPT infrastructure while adding intelligent
reasoning and tool orchestration capabilities.

ReAct Pattern: Reasoning + Acting
- Reasoning: Think step-by-step about what needs to be done
- Acting: Execute tools and analyze results
- Iteration: Continue reasoning and acting until query is resolved
"""

import logging
import os
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, asdict

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    openai = None

from .conversation_memory import get_conversation_memory, ConversationTurn
from .tool_registry import get_tool_registry
from .session_state import SessionState, ConversationMode, needs_agent_processing

logger = logging.getLogger(__name__)


@dataclass
class ReActStep:
    """Represents a single step in ReAct reasoning."""
    step_number: int
    thought: str
    action: str
    action_input: Dict[str, Any]
    observation: str
    reasoning: str
    step_type: str  # 'reasoning', 'acting', 'final'
    execution_time: float = 0.0
    success: bool = True
    

@dataclass
class AgentResponse:
    """Complete agent response with reasoning trace."""
    final_answer: str
    reasoning_steps: List[ReActStep]
    tools_used: List[str]
    total_execution_time: float
    success: bool
    conversation_mode: ConversationMode
    confidence_score: float = 0.0
    requires_clarification: bool = False
    clarification_question: str = ""


class ChatMRPTAgent:
    """
    ReAct-based conversational agent for malaria analytics.
    
    Integrates with existing ChatMRPT infrastructure while adding
    intelligent reasoning and tool orchestration capabilities.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize the ChatMRPT ReAct Agent."""
        self.config = config or self._load_config()
        self.memory_manager = get_conversation_memory()
        self.tool_registry = get_tool_registry()
        self.openai_client = self._initialize_openai()
        
        # Agent configuration
        self.max_iterations = self.config.get('max_iterations', 5)
        self.reasoning_temperature = self.config.get('reasoning_temperature', 0.1)
        self.enable_reflection = self.config.get('enable_reflection', True)
        self.malaria_expertise_level = self.config.get('malaria_expertise_level', 'expert')
        
        # Expose affordance scorer for testing
        self.affordance_scorer = getattr(self.tool_registry, '_affordance_scorer', None)
        
        logger.info(f"ChatMRPT Agent initialized with {self.max_iterations} max iterations")
    
    def _select_relevant_tools(self, query: str, session_id: str) -> List[Tuple[str, float]]:
        """Select relevant tools for a query (exposed for testing)."""
        conversation_context = self._get_conversation_context(session_id, query)
        context = {"user_role": "analyst", "has_data": True}
        return self._select_candidate_tools(query, conversation_context, context)
    
    def _load_config(self) -> Dict[str, Any]:
        """Load agent configuration from environment variables."""
        return {
            'max_iterations': int(os.getenv('AGENT_MAX_ITERATIONS', '5')),
            'reasoning_temperature': float(os.getenv('AGENT_TEMPERATURE', '0.1')),
            'enable_reflection': os.getenv('ENABLE_AGENT_REFLECTION', 'true').lower() == 'true',
            'malaria_expertise_level': os.getenv('MALARIA_EXPERTISE_LEVEL', 'expert'),
            'openai_model': os.getenv('OPENAI_MODEL_NAME', 'gpt-4o'),
            'max_tokens': int(os.getenv('AGENT_MAX_TOKENS', '2000')),
            'enable_tool_chaining': os.getenv('ENABLE_TOOL_CHAINING', 'true').lower() == 'true'
        }
    
    def _initialize_openai(self) -> Optional[Any]:
        """Initialize OpenAI client for reasoning."""
        if not OPENAI_AVAILABLE:
            logger.warning("OpenAI not available - agent will use fallback reasoning")
            return None
        
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("OpenAI API key not found - agent will use fallback reasoning")
            return None
        
        try:
            client = openai.OpenAI(api_key=api_key)
            logger.info("OpenAI client initialized for agent reasoning")
            return client
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {e}")
            return None
    
    def process_conversational_query(self, query: str, session_id: str, 
                                   context: Dict[str, Any] = None) -> AgentResponse:
        """
        Main entry point for conversational queries.
        
        Args:
            query: User query to process
            session_id: Session identifier
            context: Additional context (user role, session state, etc.)
            
        Returns:
            AgentResponse with reasoning trace and final answer
        """
        start_time = time.time()
        context = context or {}
        
        try:
            # Determine if this needs agent processing
            session_state = context.get('session_state')
            if session_state and not needs_agent_processing(query, session_state):
                return self._handle_simple_query(query, session_id, context)
            
            # Get conversation context from memory
            conversation_context = self._get_conversation_context(query, session_id)
            
            # Perform intelligent tool selection
            candidate_tools = self._select_candidate_tools(query, conversation_context, context)
            
            # Execute ReAct reasoning loop
            reasoning_steps, final_answer = self._execute_react_loop(
                query=query,
                session_id=session_id,
                candidate_tools=candidate_tools,
                context=context,
                conversation_context=conversation_context
            )
            
            # Calculate execution metrics
            total_time = time.time() - start_time
            tools_used = list(set(step.action for step in reasoning_steps if step.action != 'final_answer'))
            
            # Create response
            response = AgentResponse(
                final_answer=final_answer,
                reasoning_steps=reasoning_steps,
                tools_used=tools_used,
                total_execution_time=total_time,
                success=True,
                conversation_mode=ConversationMode.AGENT_ANALYSIS,
                confidence_score=self._calculate_confidence_score(reasoning_steps)
            )
            
            # Store conversation in memory
            self._store_conversation_turn(query, response, session_id, context)
            
            # Update tool usage patterns
            self._update_tool_usage_feedback(reasoning_steps)
            
            return response
            
        except Exception as e:
            logger.error(f"Error in agent processing: {e}")
            return self._create_error_response(str(e), time.time() - start_time)
    
    def _handle_simple_query(self, query: str, session_id: str, context: Dict[str, Any]) -> AgentResponse:
        """Handle simple queries that don't need agent reasoning."""
        response_text = self._generate_simple_response(query, context)
        
        return AgentResponse(
            final_answer=response_text,
            reasoning_steps=[],
            tools_used=[],
            total_execution_time=0.1,
            success=True,
            conversation_mode=ConversationMode.SIMPLE_CHAT,
            confidence_score=0.9
        )
    
    def _get_conversation_context(self, query: str, session_id: str) -> str:
        """Get relevant conversation context from memory."""
        if not self.memory_manager.enabled:
            return ""
        
        try:
            contexts = self.memory_manager.retrieve_relevant_context(
                query=query,
                session_id=session_id,
                limit=3
            )
            
            if contexts:
                context_texts = [ctx['content'] for ctx in contexts]
                return " ".join(context_texts)
            
            return ""
        except Exception as e:
            logger.warning(f"Failed to retrieve conversation context: {e}")
            return ""
    
    def _select_candidate_tools(self, query: str, conversation_context: str, 
                              context: Dict[str, Any]) -> List[Tuple[str, float]]:
        """Select candidate tools using intelligent tool selection."""
        try:
            user_context = {
                'user_role': context.get('user_role', 'analyst'),
                'has_data': context.get('has_data', True),
                'session_state': context.get('session_state')
            }
            
            candidates = self.tool_registry.select_tools_for_query(
                query=query,
                conversation_context=conversation_context,
                user_context=user_context,
                limit=8
            )
            
            logger.debug(f"Selected {len(candidates)} candidate tools for query: {query}")
            return candidates
            
        except Exception as e:
            logger.error(f"Error selecting candidate tools: {e}")
            return []
    
    def _execute_react_loop(self, query: str, session_id: str, 
                          candidate_tools: List[Tuple[str, float]], 
                          context: Dict[str, Any], conversation_context: str) -> Tuple[List[ReActStep], str]:
        """Execute the main ReAct reasoning loop."""
        reasoning_steps = []
        current_context = f"User Query: {query}\n\nAvailable Context: {conversation_context}"
        
        for iteration in range(self.max_iterations):
            # Reasoning step
            thought, action, action_input = self._reason_about_next_step(
                current_context, candidate_tools, reasoning_steps, iteration
            )
            
            if action == "final_answer":
                # Final reasoning step
                final_step = ReActStep(
                    step_number=iteration + 1,
                    thought=thought,
                    action=action,
                    action_input=action_input,
                    observation="",
                    reasoning="Concluded reasoning and provided final answer",
                    step_type="final",
                    success=True
                )
                reasoning_steps.append(final_step)
                return reasoning_steps, action_input.get('answer', thought)
            
            # Acting step - execute tool
            start_time = time.time()
            observation, success = self._execute_tool_action(action, action_input, session_id)
            execution_time = time.time() - start_time
            
            # Create reasoning step
            step = ReActStep(
                step_number=iteration + 1,
                thought=thought,
                action=action,
                action_input=action_input,
                observation=observation,
                reasoning=f"Executed {action} to gather information",
                step_type="acting",
                execution_time=execution_time,
                success=success
            )
            reasoning_steps.append(step)
            
            # Update context with observation
            current_context += f"\n\nStep {iteration + 1}: {action} -> {observation}"
            
            # Check if we have enough information
            if self._has_sufficient_information(reasoning_steps, query):
                break
        
        # If we reach max iterations, create final answer
        final_answer = self._synthesize_final_answer(reasoning_steps, query)
        final_step = ReActStep(
            step_number=len(reasoning_steps) + 1,
            thought="Reached maximum iterations, synthesizing final answer",
            action="final_answer",
            action_input={'answer': final_answer},
            observation="",
            reasoning="Synthesized answer from available information",
            step_type="final",
            success=True
        )
        reasoning_steps.append(final_step)
        
        return reasoning_steps, final_answer
    
    def _reason_about_next_step(self, current_context: str, candidate_tools: List[Tuple[str, float]], 
                              reasoning_steps: List[ReActStep], iteration: int) -> Tuple[str, str, Dict[str, Any]]:
        """Reason about the next step using LLM or fallback logic."""
        if self.openai_client:
            return self._llm_reasoning(current_context, candidate_tools, reasoning_steps, iteration)
        else:
            return self._fallback_reasoning(current_context, candidate_tools, reasoning_steps, iteration)
    
    def _llm_reasoning(self, current_context: str, candidate_tools: List[Tuple[str, float]], 
                      reasoning_steps: List[ReActStep], iteration: int) -> Tuple[str, str, Dict[str, Any]]:
        """Use LLM for reasoning about next step."""
        try:
            prompt = self._create_react_prompt(current_context, candidate_tools, reasoning_steps, iteration)
            
            response = self.openai_client.chat.completions.create(
                model=self.config['openai_model'],
                messages=[
                    {"role": "system", "content": self._get_system_prompt()},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.reasoning_temperature,
                max_tokens=self.config['max_tokens']
            )
            
            content = response.choices[0].message.content
            return self._parse_llm_response(content, candidate_tools)
            
        except Exception as e:
            logger.error(f"LLM reasoning failed: {e}")
            return self._fallback_reasoning(current_context, candidate_tools, reasoning_steps, iteration)
    
    def _fallback_reasoning(self, current_context: str, candidate_tools: List[Tuple[str, float]], 
                           reasoning_steps: List[ReActStep], iteration: int) -> Tuple[str, str, Dict[str, Any]]:
        """Fallback reasoning when LLM unavailable."""
        # Simple heuristic-based reasoning
        if not candidate_tools:
            return "No tools available", "final_answer", {"answer": "I don't have access to the tools needed to answer this query."}
        
        # If we haven't used any tools yet, use the highest confidence tool
        if not reasoning_steps:
            tool_name, confidence = candidate_tools[0]
            return f"I should use {tool_name} to start analysis", tool_name, {"session_id": "placeholder"}
        
        # If we've used some tools, check if we need more information
        used_tools = set(step.action for step in reasoning_steps)
        unused_tools = [tool for tool, conf in candidate_tools if tool not in used_tools]
        
        if unused_tools and len(reasoning_steps) < 3:
            next_tool = unused_tools[0]
            return f"I need more information, using {next_tool}", next_tool, {"session_id": "placeholder"}
        
        # Otherwise, provide final answer
        return "I have gathered sufficient information", "final_answer", {"answer": "Based on the analysis performed, here are the results from the tools executed."}
    
    def _create_react_prompt(self, current_context: str, candidate_tools: List[Tuple[str, float]], 
                            reasoning_steps: List[ReActStep], iteration: int) -> str:
        """Create ReAct-style prompt for LLM reasoning."""
        tools_text = "\n".join([f"- {tool} (confidence: {conf:.2f})" for tool, conf in candidate_tools[:5]])
        
        steps_text = ""
        for step in reasoning_steps:
            steps_text += f"\nStep {step.step_number}:\nThought: {step.thought}\nAction: {step.action}\nObservation: {step.observation}\n"
        
        prompt = f"""
You are a malaria epidemiology expert analyzing data to help with intervention targeting.

Context:
{current_context}

Available Tools:
{tools_text}

Previous Steps:
{steps_text}

Current Step {iteration + 1}:
Think step by step about what you need to do next. You can either:
1. Use a tool to gather more information
2. Provide a final answer if you have sufficient information

Respond in this exact format:
Thought: [Your reasoning about what to do next]
Action: [tool_name OR "final_answer"]
Action Input: [tool parameters as JSON OR {{"answer": "your final answer"}}]
"""
        return prompt
    
    def _get_system_prompt(self) -> str:
        """Get system prompt for the agent."""
        return f"""
You are an expert malaria epidemiologist and data analyst working with ChatMRPT, a malaria risk prediction tool.

Your expertise level: {self.malaria_expertise_level}

You help users analyze malaria risk data, create visualizations, and make intervention recommendations.
You have access to various analytical tools and should use them systematically to provide comprehensive answers.

Key principles:
1. Think step by step (ReAct pattern)
2. Use tools to gather data before making conclusions
3. Provide evidence-based recommendations
4. Consider epidemiological best practices
5. Be precise with data interpretation

Always structure your thinking clearly and use available tools effectively.
"""
    
    def _parse_llm_response(self, content: str, candidate_tools: List[Tuple[str, float]]) -> Tuple[str, str, Dict[str, Any]]:
        """Parse LLM response to extract thought, action, and action input."""
        try:
            lines = content.strip().split('\n')
            thought = ""
            action = ""
            action_input = {}
            
            for line in lines:
                if line.startswith("Thought:"):
                    thought = line.replace("Thought:", "").strip()
                elif line.startswith("Action:"):
                    action = line.replace("Action:", "").strip()
                elif line.startswith("Action Input:"):
                    input_text = line.replace("Action Input:", "").strip()
                    try:
                        import json
                        action_input = json.loads(input_text)
                    except:
                        action_input = {"query": input_text}
            
            # Validate action exists in tools
            if action != "final_answer":
                available_tools = [tool for tool, _ in candidate_tools]
                if action not in available_tools:
                    # Fallback to first available tool
                    if available_tools:
                        action = available_tools[0]
                    else:
                        action = "final_answer"
                        action_input = {"answer": thought}
            
            return thought, action, action_input
            
        except Exception as e:
            logger.error(f"Failed to parse LLM response: {e}")
            return "Parsing error occurred", "final_answer", {"answer": "I encountered an error processing the request."}
    
    def _execute_tool_action(self, action: str, action_input: Dict[str, Any], session_id: str) -> Tuple[str, bool]:
        """Execute a tool action and return observation."""
        try:
            # Remove session_id from action_input to avoid duplicate parameter
            tool_params = action_input.copy()
            tool_params.pop('session_id', None)
            
            result = self.tool_registry.execute_tool(action, session_id, **tool_params)
            
            if isinstance(result, dict):
                if result.get('status') == 'success':
                    message = result.get('message', '')
                    data = result.get('data', {})
                    
                    # Format observation based on result
                    if data:
                        observation = f"{message}\nKey findings: {str(data)[:200]}..."
                    else:
                        observation = message
                    
                    return observation, True
                else:
                    error_msg = result.get('message', 'Tool execution failed')
                    return f"Error: {error_msg}", False
            
            return str(result)[:300] + "...", True
            
        except Exception as e:
            logger.error(f"Tool execution failed for {action}: {e}")
            return f"Error executing {action}: {str(e)}", False
    
    def _has_sufficient_information(self, reasoning_steps: List[ReActStep], query: str) -> bool:
        """Determine if we have sufficient information to answer the query."""
        # Simple heuristic: if we have at least 2 successful tool executions, we likely have enough info
        successful_actions = sum(1 for step in reasoning_steps if step.success and step.step_type == "acting")
        return successful_actions >= 2
    
    def _synthesize_final_answer(self, reasoning_steps: List[ReActStep], query: str) -> str:
        """Synthesize final answer from reasoning steps."""
        observations = []
        for step in reasoning_steps:
            if step.success and step.observation:
                observations.append(f"From {step.action}: {step.observation}")
        
        if observations:
            return f"Based on the analysis:\n\n" + "\n\n".join(observations)
        else:
            return "I was unable to gather sufficient information to fully answer your query."
    
    def _calculate_confidence_score(self, reasoning_steps: List[ReActStep]) -> float:
        """Calculate confidence score based on reasoning steps."""
        if not reasoning_steps:
            return 0.5
        
        # Base confidence on successful tool executions
        successful_steps = sum(1 for step in reasoning_steps if step.success)
        total_steps = len(reasoning_steps)
        
        if total_steps == 0:
            return 0.5
        
        base_confidence = successful_steps / total_steps
        
        # Boost confidence if multiple tools were used successfully
        if successful_steps >= 2:
            base_confidence += 0.2
        
        return min(1.0, base_confidence)
    
    def _store_conversation_turn(self, query: str, response: AgentResponse, 
                               session_id: str, context: Dict[str, Any]):
        """Store conversation turn in memory."""
        if not self.memory_manager.enabled:
            return
        
        try:
            turn = ConversationTurn(
                session_id=session_id,
                user_input=query,
                ai_response=response.final_answer,
                timestamp=datetime.utcnow().isoformat(),
                conversation_type="agent_analysis",
                tools_used=response.tools_used,
                user_role=context.get('user_role', 'analyst'),
                quality_score=response.confidence_score,
                response_time=response.total_execution_time
            )
            
            self.memory_manager.store_conversation_turn(turn)
            
        except Exception as e:
            logger.warning(f"Failed to store conversation turn: {e}")
    
    def _update_tool_usage_feedback(self, reasoning_steps: List[ReActStep]):
        """Update tool usage feedback for learning."""
        for step in reasoning_steps:
            if step.step_type == "acting" and step.action != "final_answer":
                self.tool_registry.update_tool_usage_feedback(
                    tool_name=step.action,
                    success=step.success,
                    execution_time=step.execution_time
                )
    
    def _generate_simple_response(self, query: str, context: Dict[str, Any]) -> str:
        """Generate simple response for non-analytical queries."""
        greetings = ["hello", "hi", "hey", "good morning", "good afternoon"]
        if any(greeting in query.lower() for greeting in greetings):
            return "Hello! I'm ChatMRPT, your malaria risk analysis assistant. How can I help you with your data analysis today?"
        
        return "I'm here to help with malaria risk analysis. Please let me know what kind of analysis you'd like to perform on your data."
    
    def _create_error_response(self, error_message: str, execution_time: float) -> AgentResponse:
        """Create error response."""
        return AgentResponse(
            final_answer=f"I encountered an error while processing your request: {error_message}",
            reasoning_steps=[],
            tools_used=[],
            total_execution_time=execution_time,
            success=False,
            conversation_mode=ConversationMode.SIMPLE_CHAT,
            confidence_score=0.0
        )
    
    def get_agent_stats(self) -> Dict[str, Any]:
        """Get agent statistics."""
        return {
            'max_iterations': self.max_iterations,
            'reasoning_temperature': self.reasoning_temperature,
            'enable_reflection': self.enable_reflection,
            'malaria_expertise_level': self.malaria_expertise_level,
            'openai_available': self.openai_client is not None,
            'memory_enabled': self.memory_manager.enabled,
            'tools_available': len(self.tool_registry.list_tools())
        }


# Global agent instance
_agent = None


def get_chatmrpt_agent() -> ChatMRPTAgent:
    """Get the global ChatMRPT agent instance."""
    global _agent
    if _agent is None:
        _agent = ChatMRPTAgent()
    return _agent


def init_chatmrpt_agent(config: Dict[str, Any] = None) -> ChatMRPTAgent:
    """Initialize ChatMRPT agent with custom configuration."""
    global _agent
    _agent = ChatMRPTAgent(config)
    return _agent