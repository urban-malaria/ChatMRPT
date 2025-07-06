"""
Adaptive Prompt Templates for ChatMRPT

This module creates dynamic prompt templates that evolve based on:
1. User expertise level (detected from conversation patterns)
2. Conversation flow and topic focus
3. Analysis state and available results
4. Previously successful interactions
"""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum

from ..services.session_memory import SessionMemory, MessageType

logger = logging.getLogger(__name__)


class UserExpertiseLevel(Enum):
    """Detected user expertise levels"""
    BEGINNER = "beginner"  # New to malaria programs, needs basic explanations
    OPERATIONAL = "operational"  # Field implementer, needs practical guidance
    TECHNICAL = "technical"  # Epidemiologist/researcher, wants methodology details
    POLICY = "policy"  # Decision maker, needs strategic insights


class ConversationFocus(Enum):
    """Detected conversation focus areas"""
    METHODOLOGY = "methodology"  # Wants to understand how analysis works
    RESULTS = "results"  # Exploring analysis outputs and rankings  
    IMPLEMENTATION = "implementation"  # Planning actual ITN distribution
    COMPARISON = "comparison"  # Comparing methods or areas
    TROUBLESHOOTING = "troubleshooting"  # Having issues or errors


@dataclass
class UserProfile:
    """Detected user characteristics from conversation"""
    expertise_level: UserExpertiseLevel
    primary_focus: ConversationFocus
    technical_language_comfort: float  # 0.0-1.0
    prefers_detailed_explanations: bool
    mentioned_experience: List[str]  # Previous ITN campaigns, roles, etc.
    question_complexity_trend: float  # 0.0-1.0, increasing over time


class AdaptivePromptManager:
    """
    Manages adaptive prompt templates that evolve based on conversation patterns.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.prompt_templates = self._initialize_templates()
    
    def _initialize_templates(self) -> Dict[str, Dict[str, str]]:
        """Initialize base prompt templates for different user types"""
        
        return {
            "intent_analysis": {
                "beginner": """
                Analyze this user's intent with beginner-friendly context.
                They may need basic explanations of malaria concepts and ITN programs.
                Focus on practical, actionable guidance rather than technical details.
                """,
                
                "operational": """
                Analyze this field implementer's intent with operational context.
                They understand malaria programs but need practical deployment guidance.
                Focus on actionable insights for field operations and resource allocation.
                """,
                
                "technical": """
                Analyze this epidemiologist's intent with technical rigor.
                They want methodological details, statistical explanations, and scientific context.
                Provide comprehensive technical analysis and methodology insights.
                """,
                
                "policy": """
                Analyze this decision-maker's intent with strategic focus.
                They need high-level insights for policy decisions and resource allocation.
                Emphasize implications, recommendations, and strategic considerations.
                """
            },
            
            "tool_selection": {
                "beginner": """
                Select tools that provide clear, accessible results with built-in explanations.
                Prioritize tools that include context and guidance for interpretation.
                Avoid complex technical tools unless specifically requested.
                """,
                
                "operational": """
                Select tools that provide actionable field guidance and practical insights.
                Focus on implementation-ready outputs and resource planning tools.
                Include both analysis results and operational recommendations.
                """,
                
                "technical": """
                Select tools that provide comprehensive technical analysis and methodology details.
                Include statistical outputs, method comparisons, and detailed explanations.
                Prioritize tools that show underlying calculations and assumptions.
                """,
                
                "policy": """
                Select tools that provide strategic insights and high-level recommendations.
                Focus on summary tools, priority rankings, and resource allocation guidance.
                Emphasize tools that support decision-making and policy development.
                """
            },
            
            "parameter_extraction": {
                "methodology_focused": """
                Extract parameters with emphasis on analytical method preferences.
                Look for specific method requests (composite vs PCA) and technical specifications.
                Consider previous methodology questions in parameter interpretation.
                """,
                
                "results_focused": """
                Extract parameters with emphasis on result exploration preferences.
                Look for ranking requests, ward investigations, and comparison criteria.
                Consider previous result queries in parameter interpretation.
                """,
                
                "implementation_focused": """
                Extract parameters with emphasis on deployment and resource planning.
                Look for coverage targets, resource constraints, and priority thresholds.
                Consider operational context in parameter interpretation.
                """
            }
        }
    
    def detect_user_profile(self, session_memory: SessionMemory) -> UserProfile:
        """Detect user characteristics from conversation history"""
        
        conversation = session_memory.conversation_history
        if not conversation:
            # Default profile for new users
            return UserProfile(
                expertise_level=UserExpertiseLevel.OPERATIONAL,
                primary_focus=ConversationFocus.RESULTS,
                technical_language_comfort=0.5,
                prefers_detailed_explanations=True,
                mentioned_experience=[],
                question_complexity_trend=0.5
            )
        
        # Analyze user messages only
        user_messages = [msg for msg in conversation if msg.type == MessageType.USER]
        
        # Detect expertise level
        expertise_level = self._detect_expertise_level(user_messages)
        
        # Detect primary focus
        primary_focus = self._detect_conversation_focus(user_messages)
        
        # Analyze technical language comfort
        technical_comfort = self._analyze_technical_language_comfort(user_messages)
        
        # Detect preference for detailed explanations
        detailed_prefs = self._detect_explanation_preferences(user_messages)
        
        # Extract mentioned experience
        experience = self._extract_mentioned_experience(user_messages)
        
        # Analyze question complexity trend
        complexity_trend = self._analyze_question_complexity_trend(user_messages)
        
        return UserProfile(
            expertise_level=expertise_level,
            primary_focus=primary_focus,
            technical_language_comfort=technical_comfort,
            prefers_detailed_explanations=detailed_prefs,
            mentioned_experience=experience,
            question_complexity_trend=complexity_trend
        )
    
    def _detect_expertise_level(self, user_messages: List) -> UserExpertiseLevel:
        """Detect user expertise from language patterns"""
        
        technical_indicators = [
            'methodology', 'principal component', 'eigenvalue', 'variance',
            'epidemiological', 'statistical', 'covariance', 'correlation',
            'regression', 'multivariate', 'standardization', 'normalization'
        ]
        
        operational_indicators = [
            'deployment', 'field', 'campaign', 'coverage', 'distribution',
            'implementation', 'logistics', 'resources', 'team', 'target'
        ]
        
        policy_indicators = [
            'strategy', 'budget', 'allocation', 'priority', 'policy',
            'decision', 'recommendation', 'investment', 'impact', 'outcome'
        ]
        
        beginner_indicators = [
            'what is', 'how do', 'explain', 'I don\'t understand', 'help me',
            'basic', 'simple', 'new to', 'first time'
        ]
        
        all_text = ' '.join([msg.content.lower() for msg in user_messages])
        
        # Count indicators
        technical_score = sum(1 for term in technical_indicators if term in all_text)
        operational_score = sum(1 for term in operational_indicators if term in all_text)
        policy_score = sum(1 for term in policy_indicators if term in all_text)
        beginner_score = sum(1 for term in beginner_indicators if term in all_text)
        
        # Determine expertise level
        scores = {
            UserExpertiseLevel.TECHNICAL: technical_score,
            UserExpertiseLevel.OPERATIONAL: operational_score,
            UserExpertiseLevel.POLICY: policy_score,
            UserExpertiseLevel.BEGINNER: beginner_score
        }
        
        return max(scores, key=scores.get)
    
    def _detect_conversation_focus(self, user_messages: List) -> ConversationFocus:
        """Detect primary conversation focus"""
        
        focus_indicators = {
            ConversationFocus.METHODOLOGY: [
                'methodology', 'method', 'how', 'why', 'explain', 'algorithm',
                'calculation', 'approach', 'technique', 'analysis'
            ],
            ConversationFocus.RESULTS: [
                'results', 'ranking', 'top', 'vulnerable', 'risk', 'score',
                'show me', 'list', 'ward', 'area', 'highest', 'lowest'
            ],
            ConversationFocus.IMPLEMENTATION: [
                'distribution', 'deploy', 'implement', 'coverage', 'target',
                'priority', 'resource', 'allocation', 'planning', 'strategy'
            ],
            ConversationFocus.COMPARISON: [
                'compare', 'versus', 'difference', 'better', 'worse',
                'similar', 'contrast', 'between', 'which'
            ]
        }
        
        all_text = ' '.join([msg.content.lower() for msg in user_messages])
        
        focus_scores = {}
        for focus, indicators in focus_indicators.items():
            score = sum(1 for term in indicators if term in all_text)
            focus_scores[focus] = score
        
        return max(focus_scores, key=focus_scores.get) if focus_scores else ConversationFocus.RESULTS
    
    def _analyze_technical_language_comfort(self, user_messages: List) -> float:
        """Analyze user's comfort with technical language"""
        
        if not user_messages:
            return 0.5
        
        technical_terms_used = 0
        total_terms_available = 0
        
        technical_vocabulary = [
            'composite', 'pca', 'principal component', 'eigenvalue', 'variance',
            'correlation', 'standardization', 'normalization', 'methodology',
            'epidemiological', 'statistical', 'algorithm', 'multivariate'
        ]
        
        all_text = ' '.join([msg.content.lower() for msg in user_messages])
        
        for term in technical_vocabulary:
            total_terms_available += 1
            if term in all_text:
                technical_terms_used += 1
        
        return technical_terms_used / total_terms_available if total_terms_available > 0 else 0.5
    
    def _detect_explanation_preferences(self, user_messages: List) -> bool:
        """Detect if user prefers detailed explanations"""
        
        detail_request_patterns = [
            'explain', 'detail', 'how', 'why', 'more information',
            'tell me more', 'elaborate', 'comprehensive', 'thorough'
        ]
        
        brief_request_patterns = [
            'quick', 'summary', 'brief', 'just', 'simple', 'short'
        ]
        
        all_text = ' '.join([msg.content.lower() for msg in user_messages])
        
        detail_score = sum(1 for pattern in detail_request_patterns if pattern in all_text)
        brief_score = sum(1 for pattern in brief_request_patterns if pattern in all_text)
        
        return detail_score >= brief_score
    
    def _extract_mentioned_experience(self, user_messages: List) -> List[str]:
        """Extract mentioned experience and background"""
        
        experience_patterns = [
            'previous campaign', 'last time', 'experience with', 'worked on',
            'implemented', 'deployed', 'managed', 'coordinated', 'led'
        ]
        
        experience = []
        all_text = ' '.join([msg.content.lower() for msg in user_messages])
        
        for pattern in experience_patterns:
            if pattern in all_text:
                experience.append(pattern)
        
        return experience
    
    def _analyze_question_complexity_trend(self, user_messages: List) -> float:
        """Analyze if questions are getting more complex over time"""
        
        if len(user_messages) < 2:
            return 0.5
        
        complexity_indicators = [
            'methodology', 'technical', 'detailed', 'complex', 'advanced',
            'comprehensive', 'statistical', 'mathematical', 'algorithm'
        ]
        
        # Calculate complexity for first half vs second half of conversation
        mid_point = len(user_messages) // 2
        first_half = user_messages[:mid_point]
        second_half = user_messages[mid_point:]
        
        first_complexity = sum(1 for msg in first_half 
                             for indicator in complexity_indicators 
                             if indicator in msg.content.lower())
        
        second_complexity = sum(1 for msg in second_half 
                              for indicator in complexity_indicators 
                              if indicator in msg.content.lower())
        
        # Normalize by number of messages
        first_avg = first_complexity / len(first_half) if first_half else 0
        second_avg = second_complexity / len(second_half) if second_half else 0
        
        # Return trend (0.0 = decreasing complexity, 1.0 = increasing complexity)
        if first_avg + second_avg == 0:
            return 0.5
        
        return second_avg / (first_avg + second_avg)
    
    def get_adaptive_prompt(self, 
                          prompt_type: str, 
                          user_profile: UserProfile, 
                          base_prompt: str) -> str:
        """Get adaptive prompt based on user profile"""
        
        templates = self.prompt_templates.get(prompt_type, {})
        
        # Select template based on user characteristics
        if prompt_type == "intent_analysis" or prompt_type == "tool_selection":
            template_key = user_profile.expertise_level.value
            adaptive_section = templates.get(template_key, "")
            
        elif prompt_type == "parameter_extraction":
            template_key = f"{user_profile.primary_focus.value}_focused"
            adaptive_section = templates.get(template_key, "")
            
        else:
            adaptive_section = ""
        
        # Combine base prompt with adaptive section
        if adaptive_section:
            enhanced_prompt = f"""
{base_prompt}

ADAPTIVE CONTEXT (based on conversation analysis):
{adaptive_section}

USER PROFILE:
- Expertise: {user_profile.expertise_level.value}
- Focus: {user_profile.primary_focus.value}
- Technical comfort: {user_profile.technical_language_comfort:.1f}/1.0
- Prefers detailed explanations: {user_profile.prefers_detailed_explanations}
"""
            return enhanced_prompt
        
        return base_prompt