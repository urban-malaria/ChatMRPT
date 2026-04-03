"""
LangGraph Agent for Data Analysis - Fixed to work without langchain_openai
Uses direct HTTP calls to VLLM's OpenAI-compatible API
"""

import os
import json
import logging
import requests
from typing import Literal, List, Dict, Any
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage, BaseMessage
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import StateGraph
from langgraph.prebuilt import ToolNode

from .state import DataAnalysisState
from ..tools.python_tool import analyze_data, get_data_summary
from ..prompts.system_prompt import MAIN_SYSTEM_PROMPT, get_error_handling_prompt
from ..formatters.response_formatter import format_analysis_response
from app.core.llm_adapter import LLMAdapter  # Use existing LLM adapter

logger = logging.getLogger(__name__)


class SimpleLLM:
    """Simple LLM wrapper that works with VLLM without langchain_openai"""
    
    def __init__(self, base_url: str, model: str):
        self.base_url = base_url
        self.model = model
        self.api_url = f"{base_url}/v1/chat/completions"
    
    def invoke(self, messages: List[BaseMessage]) -> AIMessage:
        """Call VLLM with messages and return AIMessage"""
        # Convert messages to OpenAI format
        formatted_messages = []
        for msg in messages:
            if isinstance(msg, HumanMessage):
                formatted_messages.append({"role": "user", "content": msg.content})
            elif isinstance(msg, AIMessage):
                formatted_messages.append({"role": "assistant", "content": msg.content})
            elif hasattr(msg, 'role'):
                formatted_messages.append({"role": msg.role, "content": msg.content})
            else:
                # System or other
                formatted_messages.append({"role": "system", "content": str(msg.content)})
        
        # Call VLLM API with optimized settings
        try:
            response = requests.post(
                self.api_url,
                json={
                    "model": self.model,
                    "messages": formatted_messages,
                    "temperature": 0.3,  # Lower temp for faster, more focused responses
                    "max_tokens": 500   # Reduced for quicker responses
                },
                headers={"Authorization": "Bearer dummy"},
                timeout=55  # Increased timeout to prevent 504 errors
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                return AIMessage(content=content)
            else:
                logger.error(f"VLLM API error: {response.status_code} - {response.text}")
                return AIMessage(content="I encountered an error processing your request.")
                
        except Exception as e:
            logger.error(f"Error calling VLLM: {e}")
            return AIMessage(content=f"Error: {str(e)}")
    
    def bind_tools(self, tools):
        """Placeholder for tool binding - returns self for compatibility"""
        return self


class DataAnalysisAgent:
    """
    Main agent for data analysis using LangGraph.
    Fixed to work without langchain_openai dependency.
    """
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        
        # Use LLMAdapter for consistency with rest of system
        self.llm_adapter = LLMAdapter(backend='vllm')
        
        # Also create simple LLM for langchain compatibility
        vllm_base = os.environ.get('VLLM_BASE_URL', 'http://172.31.45.157:8000')
        self.llm = SimpleLLM(base_url=vllm_base, model="Qwen/Qwen3-8B")
        
        # Set up tools
        self.tools = [analyze_data]
        self.model = self.llm.bind_tools(self.tools)
        self.tool_node = ToolNode(self.tools)
        
        # Build the graph
        self.graph = self._build_graph()
        
        # Track conversation history
        self.chat_history: List[BaseMessage] = []
    
    def _build_graph(self):
        """
        Build the LangGraph workflow.
        Exact pattern from AgenticDataAnalysis backend.py
        """
        workflow = StateGraph(DataAnalysisState)
        
        # Add nodes
        workflow.add_node('agent', self._agent_node)
        workflow.add_node('tools', self._tools_node)
        
        # Add conditional routing
        workflow.add_conditional_edges('agent', self._route_to_tools)
        
        # Add edge from tools back to agent
        workflow.add_edge('tools', 'agent')
        
        # Set entry point
        workflow.set_entry_point('agent')
        
        return workflow.compile()
    
    def _create_data_summary(self, state: DataAnalysisState) -> str:
        """
        Create a summary of available data.
        Based on AgenticDataAnalysis nodes.py create_data_summary
        """
        return get_data_summary(self.session_id)
    
    def _agent_node(self, state: DataAnalysisState):
        """
        Agent node that processes messages and decides on actions.
        Simplified version without complex langchain features.
        """
        # Add data context to the conversation
        data_summary = self._create_data_summary(state)
        
        # Get the last user message
        messages = state.get("messages", [])
        if not messages:
            return {"messages": [], "intermediate_outputs": []}
        
        last_message = messages[-1]
        user_query = last_message.content if hasattr(last_message, 'content') else str(last_message)
        
        # Quick response for common queries about data structure
        if any(phrase in user_query.lower() for phrase in ["what's in my data", "what is in my data", "describe my data", "show my data"]):
            # Provide immediate response without calling LLM
            response = f"I can see your uploaded data:\n\n{data_summary}\n\nWould you like me to:\n- Show summary statistics?\n- Check for missing values?\n- Create visualizations?\n- Perform specific analysis?"
        else:
            # Build context for LLM
            context_msg = f"""You are analyzing data for the user.

Available data context:
{data_summary}

User query: {user_query}

Provide a helpful response. If the user is asking about their data, describe what you see.
If they want analysis, suggest using the analyze_data tool."""
            
            # Use LLMAdapter for response with optimized settings
            response = self.llm_adapter.generate(
                prompt=user_query,
                context={"data_summary": data_summary},
                max_tokens=500,  # Reduced for faster responses
                temperature=0.3  # Lower temp for more focused analysis
            )
        
        # Create AIMessage from response
        ai_message = AIMessage(content=response)
        
        # Check if we should call tools based on keywords
        should_analyze = any(word in user_query.lower() for word in [
            'analyze', 'plot', 'chart', 'visualize', 'graph', 'distribution',
            'summary', 'statistics', 'correlation', 'missing', 'describe'
        ])
        
        if should_analyze:
            # Add a tool call indicator (simplified)
            ai_message.tool_calls = [{"name": "analyze_data", "args": {"query": user_query}}]
        
        logger.info(f"Agent response: {response[:200]}...")
        
        return {
            "messages": [ai_message],
            "intermediate_outputs": [{"agent_response": response}]
        }
    
    def _tools_node(self, state: DataAnalysisState):
        """
        Tools node that executes tool calls.
        Simplified to use ToolNode directly.
        """
        # Add session_id to state for tool access
        state_with_session = {**state, "session_id": self.session_id}
        
        # Use ToolNode to handle execution
        result = self.tool_node.invoke(state_with_session)
        
        # Format any tool responses for user-friendliness
        if "messages" in result and result["messages"]:
            for msg in result["messages"]:
                if isinstance(msg, ToolMessage):
                    # Format the content if needed
                    if "Error" in msg.content or "error" in msg.content.lower():
                        msg.content = get_error_handling_prompt(msg.content)
                    else:
                        # Parse for any technical content and format
                        msg.content = format_analysis_response(msg.content, {})
        
        return result
    
    def _route_to_tools(self, state: DataAnalysisState) -> Literal["tools", "__end__"]:
        """
        Route to tools if the last message has tool calls.
        Based on AgenticDataAnalysis route_to_tools function.
        """
        if messages := state.get("messages", []):
            ai_message = messages[-1]
        else:
            return "__end__"
        
        if hasattr(ai_message, "tool_calls") and ai_message.tool_calls:
            return "tools"
        return "__end__"
    
    async def analyze(self, user_query: str) -> Dict[str, Any]:
        """
        Main entry point for analysis requests.
        
        Args:
            user_query: User's question or request
            
        Returns:
            Analysis results with visualizations and insights
        """
        # Prepare initial state
        input_state = {
            "messages": self.chat_history + [HumanMessage(content=user_query)],
            "session_id": self.session_id,
            "input_data": {},
            "intermediate_outputs": [],
            "current_variables": {},
            "output_plots": [],
            "insights": [],
            "errors": []
        }
        
        try:
            # Run the graph with recursion limit
            result = self.graph.invoke(input_state, {"recursion_limit": 10})
            
            # Update chat history
            self.chat_history = result.get("messages", [])
            
            # Extract the final response
            final_message = self.chat_history[-1] if self.chat_history else None
            
            # Prepare response
            response = {
                "success": True,
                "message": final_message.content if final_message else "Analysis complete.",
                "visualizations": result.get("output_plots", []),
                "insights": result.get("insights", []),
                "session_id": self.session_id
            }
            
            # Load and include any generated visualizations
            if result.get("output_plots"):
                import pickle
                viz_data = []
                for plot_path in result["output_plots"]:
                    if os.path.exists(plot_path):
                        with open(plot_path, 'rb') as f:
                            fig = pickle.load(f)
                            # Convert to HTML for embedding
                            viz_data.append({
                                "type": "plotly",
                                "html": fig.to_html(include_plotlyjs='cdn')
                            })
                response["visualization_data"] = viz_data
            
            return response
            
        except Exception as e:
            logger.error(f"Analysis error: {str(e)}")
            return {
                "success": False,
                "message": get_error_handling_prompt(str(e)),
                "session_id": self.session_id
            }
    
    def reset(self):
        """Reset the agent's conversation history."""
        self.chat_history = []