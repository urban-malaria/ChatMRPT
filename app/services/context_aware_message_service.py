"""
Context-Aware Message Service for ChatMRPT

This service integrates the enhanced state management system with message processing
to provide context-intelligent responses and tool self-awareness.
"""

import logging
import json
import re
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

from ..core import (
    StateManager, ContextChecker, GuidanceGenerator,
    WorkflowStage, DataState, AnalysisState, SessionState
)

# Import the advanced intent recognition system
from .advanced_intent_recognition import (
    AdvancedIntentRecognizer, IntentResult, IntentCategory
)

logger = logging.getLogger(__name__)


class ContextAwareMessageService:
    """
    Enhanced message service with context awareness and tool self-knowledge
    """
    
    def __init__(self, base_message_service, llm_manager=None):
        """
        Initialize context-aware message service
        
        Args:
            base_message_service: Original MessageService instance
            llm_manager: LLM manager for AI operations
        """
        self.base_service = base_message_service
        self.llm_manager = llm_manager
        self.context_checker = ContextChecker()
        self.guidance_generator = GuidanceGenerator()
        
        # Initialize advanced intent recognizer
        self.intent_recognizer = AdvancedIntentRecognizer(
            llm_manager=llm_manager,
            use_embeddings=True
        )
        
        # Tool knowledge base
        self._initialize_tool_knowledge()
        
        # Keep backward compatibility with simple patterns as fallback
        self._initialize_fallback_patterns()
    
    def _initialize_tool_knowledge(self):
        """Initialize comprehensive tool knowledge base"""
        self.tool_knowledge = {
            'capabilities': {
                'data_processing': [
                    "Accept CSV files with malaria risk variables",
                    "Process shapefiles for geographic boundaries", 
                    "Validate data quality and compatibility",
                    "Handle missing values and data cleaning",
                    "Normalize variables to comparable scales"
                ],
                'analysis': [
                    "Comprehensive malaria risk analysis",
                    "Composite vulnerability scoring",
                    "Multi-variable risk assessment",
                    "Geographic risk distribution analysis",
                    "Vulnerability ranking generation"
                ],
                'visualization': [
                    "Interactive vulnerability maps",
                    "Variable distribution maps",
                    "Composite risk visualizations", 
                    "Color-coded risk level mapping",
                    "Geographic pattern identification"
                ],
                'reporting': [
                    "Comprehensive analysis reports",
                    "Vulnerability ranking summaries",
                    "Methodology documentation",
                    "Intervention recommendations",
                    "Downloadable result formats"
                ]
            },
            
            'file_formats': {
                'csv_requirements': {
                    'description': "CSV file containing malaria risk variables",
                    'required_columns': ['WardName'],
                    'optional_variables': [
                        'rainfall', 'temperature', 'elevation', 'population_density',
                        'housing_quality', 'distance_to_water', 'ndvi', 'urban_extent'
                    ],
                    'format_notes': [
                        "First row should contain column headers",
                        "WardName column must match shapefile ward names",
                        "Numeric variables preferred for analysis",
                        "Missing values are acceptable and will be handled"
                    ]
                },
                'shapefile_requirements': {
                    'description': "Shapefile defining geographic boundaries",
                    'format': "ZIP file containing .shp, .shx, .dbf, .prj files",
                    'requirements': [
                        "Must contain polygon features for each ward",
                        "Ward names must match CSV WardName column",
                        "Coordinate reference system recommended",
                        "Standard shapefile format compliance"
                    ]
                }
            },
            
            'workflow_stages': {
                'data_preparation': {
                    'description': "Upload and validate your data files",
                    'steps': [
                        "Upload CSV file with malaria risk variables",
                        "Upload shapefile with geographic boundaries",
                        "Validate data quality and compatibility",
                        "Review data summary and fix any issues"
                    ]
                },
                'analysis': {
                    'description': "Run comprehensive malaria risk analysis",
                    'steps': [
                        "Data cleaning and preprocessing", 
                        "Variable normalization to 0-1 scale",
                        "Composite vulnerability score calculation",
                        "Ward ranking from highest to lowest risk"
                    ]
                },
                'visualization': {
                    'description': "Create maps and visualizations",
                    'options': [
                        "Vulnerability map showing overall risk levels",
                        "Variable maps showing individual risk factors",
                        "Composite maps with customizable weighting",
                        "Urban/rural risk pattern analysis"
                    ]
                }
            },
            
            'about_tool': {
                'name': "ChatMRPT - Conversational Malaria Risk Prediction Tool",
                'purpose': "AI-powered spatial analysis tool for malaria risk assessment",
                'key_features': [
                    "Conversational interface for easy interaction",
                    "Multi-variable malaria risk assessment",
                    "Interactive geographic visualizations", 
                    "Context-aware guidance and help",
                    "Comprehensive reporting capabilities"
                ],
                'methodology': [
                    "Evidence-based variable selection",
                    "Statistical normalization techniques",
                    "Weighted composite scoring models",
                    "Geographic information system integration"
                ]
            }
        }
    
    def _initialize_fallback_patterns(self):
        """Initialize simple patterns as fallback for backward compatibility"""
        self.fallback_patterns = {
            'tool_capabilities': [
                r'what can you do',
                r'what are your (capabilities|features)',
                r'how do you work',
                r'what is this tool',
                r'tell me about (yourself|this system)',
                r'what kind of analysis'
            ],
            'file_requirements': [
                r'what (files|data) do (i|you) need',
                r'what format(s?)',
                r'(csv|shapefile) requirements',
                r'how to prepare (data|files)',
                r'what should my (csv|data) contain'
            ],
            'workflow_help': [
                r'how to (start|begin)',
                r'what (are the|is the) steps?',
                r'how does the (process|workflow) work',
                r'what do i do (next|first)',
                r'guide me through'
            ],
            'current_status': [
                r'where am i',
                r'what (is my|\'s the) (status|progress)',
                r'what can i do now',
                r'what\'s next',
                r'am i ready'
            ],
            'general_help': [
                r'help',
                r'how to use',
                r'instructions',
                r'getting started',
                r'documentation'
            ]
        }
    
    def process_message_with_context(self, user_message: str, session_dict: Dict[str, Any], 
                                   data_handler=None, **kwargs) -> Dict[str, Any]:
        """
        Process message with full context awareness using advanced intent recognition
        
        Args:
            user_message: User's message
            session_dict: Flask session dictionary
            data_handler: Data handler instance
            **kwargs: Additional arguments
            
        Returns:
            Enhanced response with context awareness
        """
        try:
            # Initialize state management
            state_manager = StateManager(session_dict)
            current_state = state_manager.get_current_state()
            
            # Use advanced intent recognition
            intent_result = self.intent_recognizer.recognize_intent(user_message, current_state)
            
            logger.info(f"Advanced intent recognition result: {intent_result.intent} "
                       f"(confidence: {intent_result.confidence:.2f}, "
                       f"category: {intent_result.category.value}, "
                       f"method: {intent_result.method_used})")
            
            # Handle based on intent category
            if intent_result.category == IntentCategory.META_TOOL:
                return self._handle_meta_question(intent_result, current_state, state_manager)
            
            elif intent_result.category == IntentCategory.ACTION_REQUEST:
                # Check if action can be performed
                can_perform = intent_result.context_factors.get('can_perform_action', True)
                
                if not can_perform:
                    return self._generate_blocked_action_response(
                        intent_result, current_state, state_manager
                    )
                else:
                    # Delegate to base service for actual action execution
                    return self._handle_action_request(
                        intent_result, user_message, current_state, state_manager, 
                        data_handler, **kwargs
                    )
            
            elif intent_result.category == IntentCategory.CONVERSATION:
                return self._handle_conversation(intent_result, current_state)
            
            elif intent_result.category == IntentCategory.HELP_REQUEST:
                return self._provide_general_help(current_state)
            
            elif intent_result.category == IntentCategory.DATA_INQUIRY:
                return self._handle_data_inquiry(intent_result, current_state, data_handler)
            
            else:
                # For unclear intents or low confidence, delegate to base service
                if intent_result.confidence < 0.5:
                    logger.info(f"Low confidence intent ({intent_result.confidence:.2f}), "
                               f"delegating to base service")
                
                base_response = self.base_service.process_message(
                    user_message, 
                    session_dict.get('session_id'),
                    self._convert_state_to_legacy_format(current_state),
                    data_handler,
                    **kwargs
                )
                
                # Enhance response with contextual guidance
                enhanced_response = self._enhance_response_with_context(
                    base_response, current_state, state_manager, intent_result
                )
                
                return enhanced_response
            
        except Exception as e:
            logger.error(f"Error in context-aware message processing: {str(e)}", exc_info=True)
            # Fallback to base service
            return self.base_service.process_message(
                user_message,
                session_dict.get('session_id', ''),
                session_dict,
                data_handler,
                **kwargs
            )
    
    def _handle_meta_question(self, intent_result: IntentResult, 
                            current_state: SessionState, state_manager: StateManager) -> Dict[str, Any]:
        """Handle questions about the tool itself using advanced intent result"""
        
        if intent_result.intent == 'tool_capabilities':
            return self._explain_tool_capabilities(current_state, intent_result)
        
        elif intent_result.intent == 'file_requirements':
            return self._explain_file_requirements(current_state, intent_result)
        
        elif intent_result.intent == 'workflow_help':
            return self._explain_workflow(current_state, intent_result)
        
        elif intent_result.intent == 'current_status':
            return self._explain_current_status(current_state, state_manager, intent_result)
        
        else:
            return self._provide_general_help(current_state)
    
    def _handle_action_request(self, intent_result: IntentResult, user_message: str,
                             current_state: SessionState, state_manager: StateManager,
                             data_handler=None, **kwargs) -> Dict[str, Any]:
        """Handle action requests that are allowed in current context"""
        
        # Delegate to base service but with enhanced response
        base_response = self.base_service.process_message(
            user_message, 
            kwargs.get('session_id', ''),
            self._convert_state_to_legacy_format(current_state),
            data_handler,
            **kwargs
        )
        
        # Enhance with intent context
        enhanced_response = self._enhance_response_with_context(
            base_response, current_state, state_manager, intent_result
        )
        
        # Add action-specific enhancements
        if intent_result.intent == 'start_analysis':
            enhanced_response['intent_confidence'] = intent_result.confidence
            enhanced_response['predicted_action'] = 'start_analysis'
        
        elif intent_result.intent == 'create_map':
            enhanced_response['intent_confidence'] = intent_result.confidence
            enhanced_response['predicted_action'] = 'create_visualization'
        
        return enhanced_response
    
    def _handle_conversation(self, intent_result: IntentResult, 
                           current_state: SessionState) -> Dict[str, Any]:
        """Handle conversational intents like greetings and thanks"""
        
        if intent_result.intent == 'greeting':
            welcome_message = self.guidance_generator.get_welcome_message(current_state)
            response = f"👋 Hello! {welcome_message}"
            
        elif intent_result.intent == 'thanks':
            response = """🙏 **You're very welcome!** 
            
I'm here to help you with malaria risk analysis whenever you need it. Feel free to ask me anything about:
• Uploading and preparing your data
• Running the analysis
• Creating visualizations and maps
• Understanding the results

What would you like to do next?"""
            
        else:
            response = "I'm here to help you with malaria risk analysis! What can I do for you?"
        
        return {
            "status": "success",
            "response": response,
            "action": "conversation",
            "suggestions": self.guidance_generator.get_next_step_suggestions(current_state),
            "intent_result": {
                "intent": intent_result.intent,
                "confidence": intent_result.confidence,
                "category": intent_result.category.value
            }
        }
    
    def _handle_data_inquiry(self, intent_result: IntentResult, 
                           current_state: SessionState, data_handler=None) -> Dict[str, Any]:
        """Handle data-related inquiries"""
        
        if intent_result.intent == 'data_summary':
            if current_state.data_state == DataState.NO_DATA:
                response = """📊 **No Data Uploaded Yet**
                
You haven't uploaded any data files yet. To get started with malaria risk analysis, you'll need:

**Required Files:**
1. **CSV file** - Contains your malaria risk variables
2. **Shapefile** - Provides geographic boundaries

Would you like me to explain the file requirements in detail?"""
            
            else:
                # Show data summary
                summary_parts = []
                
                if current_state.data_summary.csv_info:
                    summary_parts.append(f"**CSV Data:** {current_state.data_summary.csv_info.filename}")
                    summary_parts.append(f"• Wards: {current_state.data_summary.ward_count}")
                    if current_state.data_summary.available_variables:
                        summary_parts.append(f"• Variables: {', '.join(current_state.data_summary.available_variables[:5])}")
                
                if current_state.data_summary.shapefile_info:
                    summary_parts.append(f"**Shapefile:** {current_state.data_summary.shapefile_info.filename}")
                
                response = "📊 **Your Data Summary**\n\n" + "\n".join(summary_parts)
            
            return {
                "status": "success",
                "response": response,
                "action": "data_summary",
                "suggestions": self.guidance_generator.get_next_step_suggestions(current_state)
            }
        
        else:
            return self._provide_general_help(current_state)
    
    def _generate_blocked_action_response(self, intent_result: IntentResult, 
                                        current_state: SessionState, 
                                        state_manager: StateManager) -> Dict[str, Any]:
        """Generate helpful response when action is blocked, using intent result"""
        
        guidance = self.context_checker.generate_helpful_message(intent_result.intent, current_state)
        missing_prereqs = self.context_checker.get_missing_prerequisites(intent_result.intent, current_state)
        next_steps = self.context_checker.get_next_steps(intent_result.intent, current_state)
        
        # Add intent-specific context
        confidence_note = ""
        if intent_result.confidence > 0.8:
            confidence_note = f"\n\n**I understood your request clearly** (confidence: {intent_result.confidence:.0%})"
        elif intent_result.alternative_intents:
            alt_intent = intent_result.alternative_intents[0][0]
            confidence_note = f"\n\n**Note:** Did you mean '{alt_intent}'? Let me know if I misunderstood."
        
        response = f"**🤔 I'd love to help with that{confidence_note}**\n\n{guidance}"
        
        if missing_prereqs:
            response += "\n\n**📋 What's needed first:**\n"
            response += "\n".join(f"• {prereq}" for prereq in missing_prereqs)
        
        if next_steps:
            response += "\n\n**🎯 Suggested next steps:**\n"
            response += "\n".join(f"• {step}" for step in next_steps)
        
        return {
            "status": "blocked_action",
            "response": response,
            "action": f"blocked_{intent_result.intent}",
            "missing_prerequisites": missing_prereqs,
            "suggestions": next_steps,
            "blocked_action": intent_result.intent,
            "intent_result": {
                "intent": intent_result.intent,
                "confidence": intent_result.confidence,
                "category": intent_result.category.value,
                "method_used": intent_result.method_used,
                "alternatives": intent_result.alternative_intents
            }
        }
    
    def _explain_tool_capabilities(self, state: SessionState, intent_result: Optional[IntentResult] = None) -> Dict[str, Any]:
        """Explain what the tool can do"""
        
        about = self.tool_knowledge['about_tool']
        capabilities = self.tool_knowledge['capabilities']
        
        response = f"""🧠 **About {about['name']}**

{about['purpose']}

**🎯 What I can do for you:**

**📊 Data Processing:**
""" + "\n".join(f"• {cap}" for cap in capabilities['data_processing']) + """

**🔬 Analysis:**  
""" + "\n".join(f"• {cap}" for cap in capabilities['analysis']) + """

**🗺️ Visualization:**
""" + "\n".join(f"• {cap}" for cap in capabilities['visualization']) + """

**📋 Reporting:**
""" + "\n".join(f"• {cap}" for cap in capabilities['reporting']) + f"""

**🔬 Methodology:**
""" + "\n".join(f"• {method}" for method in about['methodology']) + f"""

**Current Stage:** {state.workflow_stage.value.replace('_', ' ').title()}
"""
        
        # Add current capabilities
        current_caps = self.guidance_generator.get_current_capabilities(state)
        response += f"\n**💡 What you can do right now:**\n"
        response += "\n".join(f"• {cap}" for cap in current_caps)
        
        result = {
            "status": "success",
            "response": response,
            "action": "tool_explanation",
            "suggestions": self.guidance_generator.get_next_step_suggestions(state)
        }
        
        # Add intent result if available
        if intent_result:
            result['intent_result'] = {
                "intent": intent_result.intent,
                "confidence": intent_result.confidence,
                "method_used": intent_result.method_used
            }
        
        return result
    
    def _explain_file_requirements(self, state: SessionState, intent_result: Optional[IntentResult] = None) -> Dict[str, Any]:
        """Explain file format requirements"""
        
        file_formats = self.tool_knowledge['file_formats']
        
        response = """📁 **File Requirements for Malaria Risk Analysis**

**1. CSV Data File:**
"""
        
        csv_req = file_formats['csv_requirements']
        response += f"{csv_req['description']}\n\n"
        response += f"**Required:** {', '.join(csv_req['required_columns'])}\n"
        response += f"**Useful Variables:** {', '.join(csv_req['optional_variables'][:4])}...\n\n"
        response += "**Format Notes:**\n" + "\n".join(f"• {note}" for note in csv_req['format_notes'])
        
        response += "\n\n**2. Shapefile:**\n"
        shp_req = file_formats['shapefile_requirements']
        response += f"{shp_req['description']}\n"
        response += f"**Format:** {shp_req['format']}\n\n"
        response += "**Requirements:**\n" + "\n".join(f"• {req}" for req in shp_req['requirements'])
        
        # Add current status context
        if state.data_state == DataState.NO_DATA:
            response += "\n\n**📤 Ready to upload:** You can start by uploading either file first!"
        elif state.data_state == DataState.CSV_ONLY:
            response += "\n\n**✅ CSV uploaded** - Now upload your shapefile to complete setup!"
        elif state.data_state == DataState.SHAPEFILE_ONLY:
            response += "\n\n**✅ Shapefile uploaded** - Now upload your CSV data file!"
        else:
            response += "\n\n**✅ Both files uploaded** - Ready for analysis!"
        
        result = {
            "status": "success", 
            "response": response,
            "action": "file_requirements_explanation",
            "suggestions": self.guidance_generator.get_next_step_suggestions(state)
        }
        
        # Add intent result if available
        if intent_result:
            result['intent_result'] = {
                "intent": intent_result.intent,
                "confidence": intent_result.confidence,
                "method_used": intent_result.method_used
            }
        
        return result
    
    def _explain_workflow(self, state: SessionState, intent_result: Optional[IntentResult] = None) -> Dict[str, Any]:
        """Explain the analysis workflow"""
        
        workflow = self.tool_knowledge['workflow_stages']
        progress = self.guidance_generator.get_progress_update(state)
        
        response = """🚀 **Malaria Risk Analysis Workflow**

""" + progress + """

**📋 Complete Process:**

**Step 1: Data Preparation**
""" + "\n".join(f"• {step}" for step in workflow['data_preparation']['steps']) + """

**Step 2: Analysis**  
""" + "\n".join(f"• {step}" for step in workflow['analysis']['steps']) + """

**Step 3: Visualization**
""" + "\n".join(f"• {option}" for option in workflow['visualization']['options']) + """

**🎯 Your Next Steps:**
"""
        
        suggestions = self.guidance_generator.get_next_step_suggestions(state)
        response += "\n".join(f"• {suggestion}" for suggestion in suggestions)
        
        result = {
            "status": "success",
            "response": response, 
            "action": "workflow_explanation",
            "suggestions": suggestions
        }
        
        # Add intent result if available
        if intent_result:
            result['intent_result'] = {
                "intent": intent_result.intent,
                "confidence": intent_result.confidence,
                "method_used": intent_result.method_used
            }
        
        return result
    
    def _explain_current_status(self, state: SessionState, state_manager: StateManager, intent_result: Optional[IntentResult] = None) -> Dict[str, Any]:
        """Explain current status and what user can do"""
        
        welcome = self.guidance_generator.get_welcome_message(state)
        suggestions = self.guidance_generator.get_next_step_suggestions(state)
        capabilities = state_manager.get_available_capabilities()
        
        response = welcome + "\n\n**🎯 What you can do now:**\n"
        
        if capabilities.can_upload_csv:
            response += "• ✅ Upload CSV data file\n"
        if capabilities.can_upload_shapefile:
            response += "• ✅ Upload shapefile\n"
        if capabilities.can_start_analysis:
            response += "• ✅ Start malaria risk analysis\n"
        if capabilities.can_create_maps:
            response += "• ✅ Create vulnerability maps\n"
        if capabilities.can_view_rankings:
            response += "• ✅ View vulnerability rankings\n"
        if capabilities.can_generate_reports:
            response += "• ✅ Generate comprehensive reports\n"
        
        response += "\n**💡 Suggested next steps:**\n"
        response += "\n".join(f"• {suggestion}" for suggestion in suggestions)
        
        result = {
            "status": "success",
            "response": response,
            "action": "status_explanation", 
            "suggestions": suggestions,
            "capabilities": capabilities.__dict__
        }
        
        # Add intent result if available
        if intent_result:
            result['intent_result'] = {
                "intent": intent_result.intent,
                "confidence": intent_result.confidence,
                "method_used": intent_result.method_used
            }
        
        return result
    
    def _provide_general_help(self, state: SessionState) -> Dict[str, Any]:
        """Provide general help and guidance"""
        
        help_topics = self.guidance_generator.get_help_topics(state)
        tips = self.guidance_generator.get_contextual_tips(state)
        
        response = """❓ **How can I help you?**

**🔍 Common Questions:**
""" + "\n".join(f"• {topic}" for topic in help_topics) + """

**💡 Helpful Tips:**
""" + "\n".join(f"• {tip}" for tip in tips) + """

**🚀 Quick Actions:**
• Ask "What can you do?" to learn about my capabilities
• Ask "What files do I need?" to understand data requirements  
• Ask "Where am I?" to see your current progress
• Ask "What's next?" for step-by-step guidance

**💬 Just ask me anything about:**
• Malaria risk analysis methodology
• File format requirements
• How to interpret results
• What to do at each step
"""
        
        return {
            "status": "success",
            "response": response,
            "action": "general_help",
            "suggestions": self.guidance_generator.get_next_step_suggestions(state)
        }
    
    def _enhance_response_with_context(self, base_response: Dict[str, Any], 
                                     state: SessionState, state_manager: StateManager,
                                     intent_result: Optional[IntentResult] = None) -> Dict[str, Any]:
        """Enhance base response with contextual information and intent insights"""
        
        # Add contextual suggestions if not already present
        if 'suggestions' not in base_response:
            base_response['suggestions'] = self.guidance_generator.get_next_step_suggestions(state)
        
        # Add progress information for relevant actions
        if base_response.get('action') in ['analysis_started', 'analysis_complete', 'data_uploaded']:
            progress = self.guidance_generator.get_progress_update(state)
            base_response['progress'] = progress
        
        # Add contextual tips
        base_response['contextual_tips'] = self.guidance_generator.get_contextual_tips(state)
        
        # Add current capabilities
        base_response['current_capabilities'] = state_manager.get_available_capabilities().__dict__
        
        # Add intent recognition insights
        if intent_result:
            base_response['intent_insights'] = {
                "recognized_intent": intent_result.intent,
                "confidence": intent_result.confidence,
                "category": intent_result.category.value,
                "method_used": intent_result.method_used,
                "alternatives": intent_result.alternative_intents[:2],  # Top 2 alternatives
                "context_match": intent_result.context_factors.get('can_perform_action', True)
            }
        
        return base_response
    
    def _convert_state_to_legacy_format(self, state: SessionState) -> Dict[str, Any]:
        """Convert new state format to legacy session format for base service"""
        return {
            'csv_loaded': state.data_state in [DataState.CSV_ONLY, DataState.BOTH_LOADED, DataState.VALIDATED],
            'shapefile_loaded': state.data_state in [DataState.SHAPEFILE_ONLY, DataState.BOTH_LOADED, DataState.VALIDATED],
            'analysis_complete': state.analysis_state == AnalysisState.COMPLETE,
            'available_variables': state.data_summary.available_variables,
            'csv_rows': state.data_summary.ward_count
        }
    
    # Keep backward compatibility methods
    def _detect_meta_intent(self, message: str) -> Optional[str]:
        """Backward compatibility method using fallback patterns"""
        message_lower = message.lower()
        
        for intent_type, patterns in self.fallback_patterns.items():
            for pattern in patterns:
                if re.search(pattern, message_lower):
                    return intent_type
        
        return None
    
    def _detect_action_intent(self, message: str) -> Optional[str]:
        """Backward compatibility method"""
        message_lower = message.lower()
        
        action_patterns = {
            'start_analysis': [r'start analysis', r'run analysis', r'analyze', r'begin analysis'],
            'create_map': [r'create map', r'show map', r'make map', r'visualiz', r'map'],
            'view_rankings': [r'ranking', r'rank', r'most vulnerable', r'highest risk'],
            'generate_report': [r'report', r'generate report', r'create report'],
            'upload_csv': [r'upload csv', r'load data', r'add csv'],
            'upload_shapefile': [r'upload shape', r'add shape', r'load shape']
        }
        
        for action, patterns in action_patterns.items():
            for pattern in patterns:
                if re.search(pattern, message_lower):
                    return action
        
        return None 