"""
Memory Tools for ChatMRPT

These tools enable conversational continuity by providing access to previous
responses, analysis results, and conversation context.
"""

import logging
from typing import Dict, Any, Optional, List
import json
import os
from datetime import datetime

logger = logging.getLogger(__name__)


def _get_session_memory(session_id: str):
    """Get session memory instance."""
    try:
        from ..services.session_memory import SessionMemory
        # Use the default memory storage path that matches the rest of the app
        return SessionMemory(session_id, storage_path="instance/memory")
    except Exception as e:
        logger.error(f"Error accessing session memory: {e}")
        return None


def get_conversation_history(session_id: str, last_n_messages: int = 10) -> Dict[str, Any]:
    """
    Get recent conversation history to enable follow-up questions.
    
    Handles requests like:
    - "What did you say about that earlier?"
    - "Can you elaborate on your previous analysis?"
    """
    try:
        memory = _get_session_memory(session_id)
        if not memory:
            return {"error": "Session memory not available"}
        
        recent_messages = memory.conversation_history[-last_n_messages:]
        
        if not recent_messages:
            return {"error": "No conversation history found for this session"}
        
        # Format conversation for easy reference
        conversation = []
        for msg in recent_messages:
            conversation.append({
                'timestamp': msg.timestamp,
                'role': 'User' if msg.type.value == 'user' else 'Assistant',
                'content': msg.content,
                'metadata': msg.metadata
            })
        
        # Get key topics discussed
        key_topics = list(memory.key_entities.keys()) if memory.key_entities else []
        
        return {
            'status': 'success',
            'conversation_history': conversation,
            'key_topics_discussed': key_topics,
            'total_messages': len(memory.conversation_history),
            'analysis_context_available': memory.analysis_context is not None,
            'conversation_summary': _summarize_conversation(conversation)
        }
        
    except Exception as e:
        logger.error(f"Error retrieving conversation history: {e}")
        return {"error": f"Failed to retrieve conversation history: {str(e)}"}


def find_previous_discussion(session_id: str, topic: str) -> Dict[str, Any]:
    """
    Find previous discussions about a specific topic.
    
    Handles requests like:
    - "What did we discuss about flood risk?"
    - "Show me what you said about Wangara ward earlier"
    """
    try:
        memory = _get_session_memory(session_id)
        if not memory:
            return {"error": "Session memory not available"}
        
        # Find messages containing the topic
        relevant_messages = memory.find_previous_mentions(topic)
        
        if not relevant_messages:
            return {
                'status': 'success',
                'topic_searched': topic,
                'messages_found': [],
                'summary': f"No previous discussions found about '{topic}'"
            }
        
        # Format found messages
        formatted_messages = []
        for msg in relevant_messages:
            formatted_messages.append({
                'timestamp': msg.timestamp,
                'role': 'User' if msg.type.value == 'user' else 'Assistant',
                'content': msg.content,
                'relevance_snippet': _extract_relevant_snippet(msg.content, topic)
            })
        
        return {
            'status': 'success',
            'topic_searched': topic,
            'messages_found': formatted_messages,
            'total_matches': len(relevant_messages),
            'summary': f"Found {len(relevant_messages)} previous discussions about '{topic}'"
        }
        
    except Exception as e:
        logger.error(f"Error finding previous discussion: {e}")
        return {"error": f"Failed to search previous discussions: {str(e)}"}


def get_analysis_context(session_id: str) -> Dict[str, Any]:
    """
    Get current analysis context to enable informed follow-up questions.
    
    Handles requests like:
    - "Tell me more about the analysis you just ran"
    - "What variables were used in the analysis?"
    """
    try:
        memory = _get_session_memory(session_id)
        if not memory:
            return {"error": "Session memory not available"}
        
        if not memory.analysis_context:
            return {
                'status': 'success',
                'has_analysis': False,
                'message': 'No analysis has been completed in this session yet'
            }
        
        context = memory.analysis_context
        
        return {
            'status': 'success',
            'has_analysis': True,
            'analysis_details': {
                'status': context.status,
                'method': context.composite_method,
                'variables_used': context.variables_used or [],
                'pca_components': context.pca_components,
                'analysis_timestamp': context.analysis_timestamp,
                'top_risk_wards': context.top_risk_wards or [],
                'low_risk_wards': context.low_risk_wards or [],
                'method_agreement': context.method_agreement or {},
                'score_ranges': context.score_ranges or {}
            },
            'methodology_explanation': memory.get_method_explanation_context(),
            'quick_facts': _generate_analysis_quick_facts(context)
        }
        
    except Exception as e:
        logger.error(f"Error retrieving analysis context: {e}")
        return {"error": f"Failed to retrieve analysis context: {str(e)}"}


def save_analysis_result(session_id: str, analysis_type: str, tool_name: str, 
                        result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Save analysis results for future reference.
    
    This tool allows other tools to save their results to memory
    so users can refer back to them later.
    """
    try:
        memory = _get_session_memory(session_id)
        if not memory:
            return {"error": "Session memory not available"}
        
        # Save as a special analysis message
        metadata = {
            'analysis_type': analysis_type,
            'tool_name': tool_name,
            'result_summary': _create_result_summary(result),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Create a human-readable summary of the analysis
        summary_content = f"Analysis completed: {analysis_type} using {tool_name}"
        if 'status' in result and result['status'] == 'success':
            if 'summary' in result:
                summary_content += f". Key findings: {_extract_key_findings(result)}"
        
        from ..services.session_memory import MessageType
        memory.add_message(
            MessageType.ANALYSIS,
            summary_content,
            metadata
        )
        
        return {
            'status': 'success',
            'message': 'Analysis result saved to session memory',
            'can_reference': True
        }
        
    except Exception as e:
        logger.error(f"Error saving analysis result: {e}")
        return {"error": f"Failed to save analysis result: {str(e)}"}


def get_previous_analysis_results(session_id: str, analysis_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Get previous analysis results from memory.
    
    Handles requests like:
    - "Show me the results from my earlier flood analysis"
    - "What were the findings from the ITN targeting analysis?"
    """
    try:
        memory = _get_session_memory(session_id)
        if not memory:
            return {"error": "Session memory not available"}
        
        # Find analysis messages
        analysis_messages = [
            msg for msg in memory.conversation_history 
            if msg.type.value == 'analysis' and msg.metadata
        ]
        
        if analysis_type:
            # Filter by analysis type
            analysis_messages = [
                msg for msg in analysis_messages 
                if msg.metadata.get('analysis_type', '').lower() == analysis_type.lower()
            ]
        
        if not analysis_messages:
            filter_text = f" of type '{analysis_type}'" if analysis_type else ""
            return {
                'status': 'success',
                'analysis_results': [],
                'message': f"No previous analysis results found{filter_text}"
            }
        
        # Format results
        formatted_results = []
        for msg in analysis_messages[-5:]:  # Last 5 analysis results
            formatted_results.append({
                'timestamp': msg.timestamp,
                'analysis_type': msg.metadata.get('analysis_type'),
                'tool_name': msg.metadata.get('tool_name'),
                'summary': msg.content,
                'key_findings': msg.metadata.get('result_summary', {})
            })
        
        return {
            'status': 'success',
            'analysis_results': formatted_results,
            'total_analyses': len(analysis_messages),
            'filter_applied': analysis_type,
            'message': f"Found {len(formatted_results)} previous analysis results"
        }
        
    except Exception as e:
        logger.error(f"Error retrieving previous analysis results: {e}")
        return {"error": f"Failed to retrieve previous analysis results: {str(e)}"}


def compare_with_previous_analysis(session_id: str, current_result: Dict[str, Any], 
                                  comparison_type: str = "latest") -> Dict[str, Any]:
    """
    Compare current analysis with previous results.
    
    Handles requests like:
    - "How does this compare to my earlier analysis?"
    - "Show me the differences from the last time"
    """
    try:
        memory = _get_session_memory(session_id)
        if not memory:
            return {"error": "Session memory not available"}
        
        # Get previous analysis results
        previous_results = get_previous_analysis_results(session_id)
        
        if not previous_results.get('analysis_results'):
            return {
                'status': 'success',
                'comparison_available': False,
                'message': 'No previous analysis results available for comparison'
            }
        
        # Get comparison target
        if comparison_type == "latest":
            target_analysis = previous_results['analysis_results'][-1]
        else:
            # Could add more sophisticated selection logic here
            target_analysis = previous_results['analysis_results'][-1]
        
        # Perform comparison
        comparison = _perform_result_comparison(current_result, target_analysis)
        
        return {
            'status': 'success',
            'comparison_available': True,
            'current_analysis': _create_result_summary(current_result),
            'previous_analysis': target_analysis,
            'comparison': comparison,
            'comparison_summary': _generate_comparison_summary(comparison)
        }
        
    except Exception as e:
        logger.error(f"Error comparing with previous analysis: {e}")
        return {"error": f"Failed to compare with previous analysis: {str(e)}"}


def _summarize_conversation(conversation: List[Dict]) -> str:
    """Generate a brief summary of the conversation."""
    if not conversation:
        return "No conversation to summarize"
    
    user_messages = [msg for msg in conversation if msg['role'] == 'User']
    assistant_messages = [msg for msg in conversation if msg['role'] == 'Assistant']
    
    summary_parts = []
    
    if user_messages:
        summary_parts.append(f"User asked {len(user_messages)} questions")
    
    if assistant_messages:
        summary_parts.append(f"Assistant provided {len(assistant_messages)} responses")
    
    # Extract key topics
    topics = set()
    for msg in conversation:
        content_lower = msg['content'].lower()
        if 'ward' in content_lower:
            topics.add('ward analysis')
        if any(term in content_lower for term in ['flood', 'elevation', 'rain']):
            topics.add('environmental factors')
        if any(term in content_lower for term in ['itn', 'irs', 'intervention']):
            topics.add('interventions')
        if any(term in content_lower for term in ['risk', 'burden', 'score']):
            topics.add('risk assessment')
    
    if topics:
        summary_parts.append(f"Topics: {', '.join(topics)}")
    
    return '; '.join(summary_parts)


def _extract_relevant_snippet(content: str, topic: str, context_chars: int = 100) -> str:
    """Extract relevant snippet around the topic mention."""
    content_lower = content.lower()
    topic_lower = topic.lower()
    
    index = content_lower.find(topic_lower)
    if index == -1:
        return content[:100] + "..." if len(content) > 100 else content
    
    start = max(0, index - context_chars // 2)
    end = min(len(content), index + len(topic) + context_chars // 2)
    
    snippet = content[start:end]
    if start > 0:
        snippet = "..." + snippet
    if end < len(content):
        snippet = snippet + "..."
    
    return snippet


def _create_result_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    """Create a summary of analysis results for storage."""
    summary = {}
    
    if 'status' in result:
        summary['status'] = result['status']
    
    # Extract key numeric results
    for key in ['total_wards', 'high_risk_wards', 'percentage', 'score', 'rank']:
        if key in result:
            summary[key] = result[key]
    
    # Extract summary information
    if 'summary' in result:
        summary['main_findings'] = result['summary']
    
    return summary


def _extract_key_findings(result: Dict[str, Any]) -> str:
    """Extract key findings from analysis result."""
    findings = []
    
    if 'summary' in result and isinstance(result['summary'], dict):
        summary = result['summary']
        for key, value in summary.items():
            if isinstance(value, (int, float)) and 'count' in key.lower():
                findings.append(f"{key}: {value}")
    
    return '; '.join(findings[:3]) if findings else "Analysis completed successfully"


def _perform_result_comparison(current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    """Compare two analysis results."""
    comparison = {
        'changes_detected': False,
        'numerical_changes': {},
        'categorical_changes': {},
        'new_findings': [],
        'summary': 'Results are similar to previous analysis'
    }
    
    # Compare numerical values
    current_summary = _create_result_summary(current)
    previous_summary = previous.get('key_findings', {})
    
    for key in current_summary:
        if key in previous_summary:
            current_val = current_summary[key]
            previous_val = previous_summary[key]
            
            if isinstance(current_val, (int, float)) and isinstance(previous_val, (int, float)):
                change = current_val - previous_val
                if abs(change) > 0:
                    comparison['numerical_changes'][key] = {
                        'previous': previous_val,
                        'current': current_val,
                        'change': change
                    }
                    comparison['changes_detected'] = True
    
    if comparison['changes_detected']:
        comparison['summary'] = 'Results show changes from previous analysis'
    
    return comparison


def _generate_comparison_summary(comparison: Dict[str, Any]) -> str:
    """Generate a human-readable comparison summary."""
    if not comparison['changes_detected']:
        return "Results are consistent with previous analysis"
    
    changes = []
    for key, change_info in comparison['numerical_changes'].items():
        change_val = change_info['change']
        direction = "increased" if change_val > 0 else "decreased"
        changes.append(f"{key} {direction} by {abs(change_val)}")
    
    return f"Changes detected: {'; '.join(changes)}"


def _generate_analysis_quick_facts(context) -> List[str]:
    """Generate quick facts about the analysis."""
    facts = []
    
    if context.variables_used:
        facts.append(f"Analyzed {len(context.variables_used)} variables")
    
    if context.top_risk_wards:
        facts.append(f"Identified {len(context.top_risk_wards)} high-risk wards")
    
    if context.analysis_timestamp:
        facts.append(f"Analysis completed at {context.analysis_timestamp[:19]}")
    
    if context.method_agreement:
        agreement = context.method_agreement.get('high_risk_agreement', 'Unknown')
        facts.append(f"Method agreement: {agreement}")
    
    return facts