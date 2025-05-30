"""
Advanced Intent Recognition for ChatMRPT

This module implements sophisticated intent recognition using multiple approaches:
- Semantic similarity with embeddings
- Context-aware confidence scoring  
- LLM-based classification with improved prompting
- Hierarchical intent classification
- Multi-modal intent fusion
"""

import logging
import json
import re
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass
from enum import Enum
import numpy as np
from datetime import datetime

# For semantic similarity (we'll use sentence-transformers if available)
try:
    from sentence_transformers import SentenceTransformer
    EMBEDDINGS_AVAILABLE = True
except ImportError:
    EMBEDDINGS_AVAILABLE = False
    SentenceTransformer = None

from ..core import SessionState, WorkflowStage, DataState, AnalysisState

logger = logging.getLogger(__name__)


class IntentCategory(Enum):
    """High-level intent categories"""
    META_TOOL = "meta_tool"           # Questions about the tool itself
    ACTION_REQUEST = "action_request" # Requests to perform actions
    DATA_INQUIRY = "data_inquiry"     # Questions about data
    ANALYSIS_INQUIRY = "analysis_inquiry"  # Questions about analysis
    HELP_REQUEST = "help_request"     # General help requests
    CONVERSATION = "conversation"     # Greetings, thanks, etc.
    UNKNOWN = "unknown"               # Unclear intent


@dataclass
class IntentResult:
    """Result of intent recognition"""
    intent: str
    category: IntentCategory
    confidence: float
    entities: Dict[str, Any]
    method_used: str
    alternative_intents: List[Tuple[str, float]]
    context_factors: Dict[str, Any]


@dataclass
class IntentTemplate:
    """Template for defining intents with multiple classification methods"""
    name: str
    category: IntentCategory
    description: str
    examples: List[str]
    keywords: List[str]
    patterns: List[str]
    semantic_anchors: List[str]  # Key phrases for semantic similarity
    context_requirements: Optional[Dict[str, Any]] = None
    confidence_boost_conditions: Optional[Dict[str, float]] = None


class AdvancedIntentRecognizer:
    """
    Advanced intent recognition using multiple NLP approaches
    """
    
    def __init__(self, llm_manager=None, use_embeddings: bool = True):
        """
        Initialize advanced intent recognizer
        
        Args:
            llm_manager: LLM manager for LLM-based classification
            use_embeddings: Whether to use semantic embeddings
        """
        self.llm_manager = llm_manager
        self.use_embeddings = use_embeddings and EMBEDDINGS_AVAILABLE
        
        # Initialize embedding model if available
        self.embedding_model = None
        if self.use_embeddings:
            try:
                self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
                logger.info("Loaded sentence transformer model for semantic intent recognition")
            except Exception as e:
                logger.warning(f"Could not load embedding model: {e}")
                self.use_embeddings = False
        
        # Initialize intent templates
        self._initialize_intent_templates()
        
        # Pre-compute embeddings for semantic similarity
        if self.use_embeddings:
            self._compute_intent_embeddings()
    
    def _initialize_intent_templates(self):
        """Initialize comprehensive intent templates"""
        self.intent_templates = {
            # Meta-tool intents (questions about the tool itself)
            'tool_capabilities': IntentTemplate(
                name='tool_capabilities',
                category=IntentCategory.META_TOOL,
                description="User asking about what the tool can do",
                examples=[
                    "What can you do?",
                    "What are your capabilities?", 
                    "Tell me about this tool",
                    "What kind of analysis can you perform?",
                    "How does this system work?"
                ],
                keywords=['capabilities', 'features', 'what', 'can', 'do', 'tool', 'system'],
                patterns=[
                    r'what (can|are) (you|your)',
                    r'tell me about (this|your)',
                    r'how (do|does) (you|this) work',
                    r'what (is|are) (this|your) (tool|system|capabilities)'
                ],
                semantic_anchors=[
                    "what can you do",
                    "tool capabilities",
                    "system features",
                    "what are you capable of"
                ]
            ),
            
            'file_requirements': IntentTemplate(
                name='file_requirements',
                category=IntentCategory.META_TOOL,
                description="User asking about required file formats",
                examples=[
                    "What files do I need?",
                    "What format should my data be in?",
                    "CSV requirements",
                    "What should my shapefile contain?",
                    "How do I prepare my data?"
                ],
                keywords=['files', 'format', 'csv', 'shapefile', 'data', 'requirements', 'prepare'],
                patterns=[
                    r'what (files|data|format)',
                    r'(csv|shapefile) requirements',
                    r'how (to|do) prepare',
                    r'what should (my|the) (data|csv|file)'
                ],
                semantic_anchors=[
                    "file requirements",
                    "data format requirements", 
                    "what files do I need",
                    "CSV format"
                ]
            ),
            
            'workflow_help': IntentTemplate(
                name='workflow_help',
                category=IntentCategory.META_TOOL,
                description="User asking about the workflow or process",
                examples=[
                    "How do I start?",
                    "What are the steps?",
                    "Guide me through the process",
                    "How does the workflow work?",
                    "What do I do first?"
                ],
                keywords=['start', 'steps', 'process', 'workflow', 'guide', 'first', 'begin'],
                patterns=[
                    r'how (to|do i?) (start|begin)',
                    r'what (are|is) (the|first) steps?',
                    r'guide me',
                    r'(workflow|process).*(work|steps)',
                    r'how.*workflow'
                ],
                semantic_anchors=[
                    "how to start",
                    "workflow steps", 
                    "guide me through",
                    "what are the steps"
                ]
            ),
            
            'current_status': IntentTemplate(
                name='current_status',
                category=IntentCategory.META_TOOL,
                description="User asking about their current progress",
                examples=[
                    "Where am I?",
                    "What's my progress?",
                    "What can I do now?",
                    "What's next?",
                    "Am I ready for analysis?"
                ],
                keywords=['where', 'progress', 'status', 'now', 'next', 'ready', 'current'],
                patterns=[
                    r'where am i(\s+(in|at))?',
                    r'what.*progress',
                    r'what.*can.*now',
                    r'what.*next',
                    r'am i ready',
                    r'(my|current).*(status|progress)'
                ],
                semantic_anchors=[
                    "where am I in the process",
                    "current status",
                    "what can I do now",
                    "what's next"
                ]
            ),
            
            # Action request intents
            'start_analysis': IntentTemplate(
                name='start_analysis',
                category=IntentCategory.ACTION_REQUEST,
                description="User wants to start malaria risk analysis",
                examples=[
                    "Start the analysis",
                    "Run malaria risk analysis", 
                    "Analyze my data",
                    "Begin the analysis",
                    "Perform analysis"
                ],
                keywords=['start', 'run', 'analyze', 'analysis', 'begin', 'perform'],
                patterns=[
                    r'(start|run|begin) (the )?analysis',
                    r'analyze (my )?data',
                    r'perform.*analysis'
                ],
                semantic_anchors=[
                    "start analysis",
                    "run malaria analysis",
                    "analyze data",
                    "begin analysis"
                ],
                context_requirements={'data_state': [DataState.BOTH_LOADED, DataState.VALIDATED]}
            ),
            
            'create_map': IntentTemplate(
                name='create_map',
                category=IntentCategory.ACTION_REQUEST,
                description="User wants to create visualizations/maps",
                examples=[
                    "Create a map",
                    "Show me the vulnerability map",
                    "Generate visualization",
                    "Plot the results",
                    "Make a chart"
                ],
                keywords=['map', 'visualize', 'plot', 'chart', 'show', 'create', 'generate'],
                patterns=[
                    r'(create|make|show|generate) (a |the )?(map|visualization|chart|plot)',
                    r'visualize',
                    r'plot.*results'
                ],
                semantic_anchors=[
                    "create map",
                    "show visualization",
                    "vulnerability map",
                    "generate chart"
                ],
                context_requirements={'analysis_state': [AnalysisState.COMPLETE]}
            ),
            
            'view_rankings': IntentTemplate(
                name='view_rankings',
                category=IntentCategory.ACTION_REQUEST,
                description="User wants to see vulnerability rankings",
                examples=[
                    "Show me the rankings",
                    "Which areas are most vulnerable?",
                    "Top risk areas",
                    "Vulnerability rankings",
                    "Highest risk wards"
                ],
                keywords=['rankings', 'vulnerable', 'risk', 'top', 'highest', 'areas', 'wards'],
                patterns=[
                    r'(show|view).*(rankings|rank)',
                    r'(most|highest).*(vulnerable|risk)',
                    r'top.*risk',
                    r'vulnerability.*rankings'
                ],
                semantic_anchors=[
                    "vulnerability rankings",
                    "most vulnerable areas",
                    "highest risk",
                    "show rankings"
                ],
                context_requirements={'analysis_state': [AnalysisState.COMPLETE]}
            ),
            
            'generate_report': IntentTemplate(
                name='generate_report',
                category=IntentCategory.ACTION_REQUEST,
                description="User wants to generate a report",
                examples=[
                    "Generate a report",
                    "Create report",
                    "Export results",
                    "Download report", 
                    "Summarize findings"
                ],
                keywords=['report', 'generate', 'create', 'export', 'download', 'summarize'],
                patterns=[
                    r'(generate|create|make).*report',
                    r'export.*results',
                    r'download.*report',
                    r'summarize.*findings'
                ],
                semantic_anchors=[
                    "generate report",
                    "create analysis report",
                    "export results",
                    "download report"
                ],
                context_requirements={'analysis_state': [AnalysisState.COMPLETE]}
            ),
            
            # Data inquiry intents
            'data_summary': IntentTemplate(
                name='data_summary',
                category=IntentCategory.DATA_INQUIRY,
                description="User asking about their uploaded data",
                examples=[
                    "Show me my data",
                    "Data summary",
                    "What variables do I have?",
                    "Check my uploaded files",
                    "Review my data"
                ],
                keywords=['data', 'summary', 'variables', 'files', 'uploaded', 'review'],
                patterns=[
                    r'(show|view).*data',
                    r'data.*summary',
                    r'what.*variables',
                    r'(check|review).*data'
                ],
                semantic_anchors=[
                    "show my data",
                    "data summary",
                    "what variables",
                    "review uploaded data"
                ]
            ),
            
            # Help requests
            'general_help': IntentTemplate(
                name='general_help',
                category=IntentCategory.HELP_REQUEST,
                description="General help request",
                examples=[
                    "Help",
                    "I need help",
                    "How to use this?",
                    "Instructions",
                    "Getting started"
                ],
                keywords=['help', 'instructions', 'how', 'use', 'getting', 'started'],
                patterns=[
                    r'^help$',
                    r'i need help',
                    r'how to use',
                    r'instructions',
                    r'getting started'
                ],
                semantic_anchors=[
                    "help",
                    "I need help",
                    "how to use",
                    "instructions"
                ]
            ),
            
            # General knowledge questions
            'general_knowledge': IntentTemplate(
                name='general_knowledge',
                category=IntentCategory.UNKNOWN,  # Will be handled as general knowledge
                description="General knowledge questions not related to the specific analysis workflow",
                examples=[
                    "Tell me about the history of malaria in west africa",
                    "What is malaria?",
                    "How does malaria spread?",
                    "What are the symptoms of malaria?",
                    "When was malaria discovered?",
                    "What causes malaria?",
                    "How do you prevent malaria?",
                    "What is the epidemiology of malaria?",
                    "What are malaria interventions?",
                    "Tell me about malaria research"
                ],
                keywords=['history', 'what is', 'how does', 'when was', 'what causes', 'symptoms', 'prevent', 'epidemiology', 'research', 'discover', 'spread'],
                patterns=[
                    r'tell me about.*malaria',
                    r'what is (the )?malaria',
                    r'how does.*malaria',
                    r'when (was|did).*malaria',
                    r'what (causes|are).*malaria',
                    r'history of.*malaria',
                    r'tell me about.*history',
                    r'what (is|are|were).*history',
                    r'how (does|do|did|can|to).*prevent',
                    r'(symptoms|treatment|intervention).*malaria',
                    r'malaria.*(symptoms|treatment|intervention|prevention|research|epidemiology)'
                ],
                semantic_anchors=[
                    "tell me about malaria",
                    "history of malaria",
                    "what is malaria", 
                    "malaria symptoms",
                    "malaria prevention",
                    "malaria research",
                    "malaria epidemiology"
                ]
            ),
            
            # Conversation intents
            'greeting': IntentTemplate(
                name='greeting',
                category=IntentCategory.CONVERSATION,
                description="User greeting",
                examples=[
                    "Hello", "Hi", "Hey", "Good morning",
                    "Greetings", "How are you?"
                ],
                keywords=['hello', 'hi', 'hey', 'good', 'morning', 'greetings'],
                patterns=[
                    r'^(hello|hi|hey|greetings)',
                    r'good (morning|afternoon|evening)',
                    r'how are you'
                ],
                semantic_anchors=["hello", "hi", "good morning", "greetings"]
            ),
            
            'thanks': IntentTemplate(
                name='thanks',
                category=IntentCategory.CONVERSATION,
                description="User expressing gratitude",
                examples=[
                    "Thank you", "Thanks", "Thank you very much",
                    "I appreciate it", "That's helpful"
                ],
                keywords=['thank', 'thanks', 'appreciate', 'helpful'],
                patterns=[
                    r'thank (you|s)',
                    r'appreciate',
                    r'helpful'
                ],
                semantic_anchors=["thank you", "thanks", "appreciate it"]
            )
        }
    
    def _compute_intent_embeddings(self):
        """Pre-compute embeddings for all intent semantic anchors"""
        if not self.use_embeddings:
            return
        
        self.intent_embeddings = {}
        all_anchors = []
        anchor_to_intent = {}
        
        # Collect all semantic anchors
        for intent_name, template in self.intent_templates.items():
            for anchor in template.semantic_anchors:
                all_anchors.append(anchor)
                anchor_to_intent[anchor] = intent_name
        
        try:
            # Compute embeddings for all anchors at once
            embeddings = self.embedding_model.encode(all_anchors)
            
            # Group embeddings by intent
            for i, anchor in enumerate(all_anchors):
                intent_name = anchor_to_intent[anchor]
                if intent_name not in self.intent_embeddings:
                    self.intent_embeddings[intent_name] = []
                self.intent_embeddings[intent_name].append(embeddings[i])
            
            # Average embeddings for each intent
            for intent_name in self.intent_embeddings:
                self.intent_embeddings[intent_name] = np.mean(
                    self.intent_embeddings[intent_name], axis=0
                )
            
            logger.info(f"Computed embeddings for {len(self.intent_embeddings)} intents")
            
        except Exception as e:
            logger.error(f"Error computing intent embeddings: {e}")
            self.use_embeddings = False
    
    def recognize_intent(self, message: str, context_state: Optional[SessionState] = None) -> IntentResult:
        """
        Recognize intent using multiple approaches and return best result
        
        Args:
            message: User message
            context_state: Current session state for context
            
        Returns:
            IntentResult with best intent classification
        """
        if not message or not message.strip():
            return IntentResult(
                intent='clarification_needed',
                category=IntentCategory.UNKNOWN,
                confidence=0.0,
                entities={},
                method_used='default',
                alternative_intents=[],
                context_factors={}
            )
        
        message = message.strip()
        results = []
        
        # 1. Rule-based classification
        rule_result = self._classify_with_rules(message, context_state)
        results.append(('rules', rule_result))
        
        # 2. Semantic similarity classification
        if self.use_embeddings:
            semantic_result = self._classify_with_semantics(message, context_state)
            results.append(('semantics', semantic_result))
        
        # 3. LLM-based classification
        if self.llm_manager:
            llm_result = self._classify_with_llm(message, context_state)
            results.append(('llm', llm_result))
        
        # 4. Fusion of results
        final_result = self._fuse_results(results, message, context_state)
        
        return final_result
    
    def _classify_with_rules(self, message: str, context_state: Optional[SessionState]) -> Tuple[str, float, Dict]:
        """Classify using rule-based patterns"""
        message_lower = message.lower()
        best_intent = 'unknown'
        best_confidence = 0.0
        entities = {}
        
        for intent_name, template in self.intent_templates.items():
            confidence = 0.0
            
            # Check exact patterns
            for pattern in template.patterns:
                if re.search(pattern, message_lower):
                    confidence = max(confidence, 0.9)
                    break
            
            # Check keywords
            if confidence < 0.5:
                keyword_matches = sum(1 for keyword in template.keywords if keyword in message_lower)
                if keyword_matches > 0:
                    confidence = max(confidence, keyword_matches / len(template.keywords) * 0.7)
            
            # Apply context requirements
            if confidence > 0 and template.context_requirements and context_state:
                if not self._check_context_requirements(template.context_requirements, context_state):
                    confidence *= 0.3  # Reduce confidence if context doesn't match
            
            if confidence > best_confidence:
                best_confidence = confidence
                best_intent = intent_name
        
        return best_intent, best_confidence, entities
    
    def _classify_with_semantics(self, message: str, context_state: Optional[SessionState]) -> Tuple[str, float, Dict]:
        """Classify using semantic similarity"""
        if not self.use_embeddings:
            return 'unknown', 0.0, {}
        
        try:
            # Encode the user message
            message_embedding = self.embedding_model.encode([message])[0]
            
            best_intent = 'unknown'
            best_similarity = 0.0
            
            # Compare with all intent embeddings
            for intent_name, intent_embedding in self.intent_embeddings.items():
                # Compute cosine similarity
                similarity = np.dot(message_embedding, intent_embedding) / (
                    np.linalg.norm(message_embedding) * np.linalg.norm(intent_embedding)
                )
                
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_intent = intent_name
            
            # Apply context adjustments
            template = self.intent_templates.get(best_intent)
            if template and template.context_requirements and context_state:
                if not self._check_context_requirements(template.context_requirements, context_state):
                    best_similarity *= 0.5
            
            return best_intent, float(best_similarity), {}
            
        except Exception as e:
            logger.error(f"Error in semantic classification: {e}")
            return 'unknown', 0.0, {}
    
    def _classify_with_llm(self, message: str, context_state: Optional[SessionState]) -> Tuple[str, float, Dict]:
        """Classify using LLM with improved prompting"""
        if not self.llm_manager:
            return 'unknown', 0.0, {}
        
        # Create context-aware prompt
        context_info = ""
        if context_state:
            context_info = f"""
            
Current user context:
- Workflow stage: {context_state.workflow_stage.value}
- Data state: {context_state.data_state.value}  
- Analysis state: {context_state.analysis_state.value}
"""
        
        # List available intents with descriptions
        intent_descriptions = []
        for intent_name, template in self.intent_templates.items():
            intent_descriptions.append(f"- {intent_name}: {template.description}")
        
        system_message = f"""You are an expert intent classifier for a malaria risk analysis tool. 
        
Available intents:
{chr(10).join(intent_descriptions)}

{context_info}

Classify the user's message and return a JSON response with:
{{
    "intent": "most_likely_intent_name",
    "confidence": 0.95,
    "reasoning": "brief explanation",
    "entities": {{"key": "value"}}
}}

Consider the user's current context when classifying. If they ask to perform an action they can't do yet, still classify the intent correctly but note the context mismatch."""
        
        try:
            response = self.llm_manager.generate_response(
                f"Classify this message: '{message}'",
                context=context_state,
                system_message=system_message,
                temperature=0.1
            )
            
            # Parse JSON response
            if response and '{' in response:
                start = response.find('{')
                end = response.rfind('}') + 1
                json_str = response[start:end]
                
                result = json.loads(json_str)
                intent = result.get('intent', 'unknown')
                confidence = float(result.get('confidence', 0.0))
                entities = result.get('entities', {})
                
                return intent, confidence, entities
                
        except Exception as e:
            logger.error(f"Error in LLM classification: {e}")
        
        return 'unknown', 0.0, {}
    
    def _fuse_results(self, results: List[Tuple[str, Tuple[str, float, Dict]]], 
                     message: str, context_state: Optional[SessionState]) -> IntentResult:
        """Fuse results from multiple classification methods"""
        
        # Collect all predictions
        predictions = {}
        method_weights = {'rules': 0.4, 'semantics': 0.3, 'llm': 0.3}
        
        for method, (intent, confidence, entities) in results:
            weight = method_weights.get(method, 0.1)
            weighted_confidence = confidence * weight
            
            if intent not in predictions:
                predictions[intent] = {
                    'total_confidence': 0.0,
                    'methods': [],
                    'entities': {}
                }
            
            predictions[intent]['total_confidence'] += weighted_confidence
            predictions[intent]['methods'].append((method, confidence))
            predictions[intent]['entities'].update(entities)
        
        # Find best prediction
        if not predictions:
            best_intent = 'unknown'
            best_confidence = 0.0
            best_entities = {}
            methods_used = []
        else:
            best_intent = max(predictions.keys(), key=lambda k: predictions[k]['total_confidence'])
            best_confidence = predictions[best_intent]['total_confidence']
            best_entities = predictions[best_intent]['entities']
            methods_used = predictions[best_intent]['methods']
        
        # Get alternative intents
        alternatives = [(intent, data['total_confidence']) 
                       for intent, data in predictions.items() 
                       if intent != best_intent]
        alternatives.sort(key=lambda x: x[1], reverse=True)
        
        # Determine category
        template = self.intent_templates.get(best_intent)
        category = template.category if template else IntentCategory.UNKNOWN
        
        # Add context factors
        context_factors = {}
        if context_state:
            context_factors = {
                'workflow_stage': context_state.workflow_stage.value,
                'data_state': context_state.data_state.value,
                'analysis_state': context_state.analysis_state.value,
                'can_perform_action': self._can_perform_intent(best_intent, context_state)
            }
        
        return IntentResult(
            intent=best_intent,
            category=category,
            confidence=best_confidence,
            entities=best_entities,
            method_used=', '.join([m[0] for m in methods_used]),
            alternative_intents=alternatives[:3],
            context_factors=context_factors
        )
    
    def _check_context_requirements(self, requirements: Dict[str, Any], 
                                  context_state: SessionState) -> bool:
        """Check if context state meets intent requirements"""
        for req_key, req_values in requirements.items():
            if req_key == 'data_state':
                if context_state.data_state not in req_values:
                    return False
            elif req_key == 'analysis_state':
                if context_state.analysis_state not in req_values:
                    return False
            elif req_key == 'workflow_stage':
                if context_state.workflow_stage not in req_values:
                    return False
        
        return True
    
    def _can_perform_intent(self, intent: str, context_state: SessionState) -> bool:
        """Check if intent can be performed in current context"""
        template = self.intent_templates.get(intent)
        if not template or not template.context_requirements:
            return True
        
        return self._check_context_requirements(template.context_requirements, context_state)
    
    def add_custom_intent(self, intent_template: IntentTemplate):
        """Add a custom intent template"""
        self.intent_templates[intent_template.name] = intent_template
        
        # Recompute embeddings if using semantic classification
        if self.use_embeddings:
            self._compute_intent_embeddings()
    
    def get_intent_info(self, intent_name: str) -> Optional[IntentTemplate]:
        """Get information about a specific intent"""
        return self.intent_templates.get(intent_name) 