"""
TPR Intent Classifier

Classifies user intent during TPR workflow to enable flexible interactions.
Determines if user is:
- Asking for help/clarification
- Making a selection
- Navigating (back, skip, restart)
- Asking a general question
"""

import re
import logging
from typing import Literal, Optional, Dict, List
from enum import Enum

logger = logging.getLogger(__name__)


class TPRIntent(Enum):
    """Intent types for TPR workflow interactions."""
    HELP_REQUEST = "help_request"  # User needs clarification
    SELECTION = "selection"  # User is making a choice
    NAVIGATION = "navigation"  # User wants to navigate (back, skip, etc.)
    QUESTION = "question"  # General question
    UNCLEAR = "unclear"  # Cannot determine intent


class TPRIntentClassifier:
    """Classifies user intent during TPR workflow for flexible routing."""

    # Help and clarification triggers
    HELP_TRIGGERS = [
        "what is", "what's", "what does", "whats",
        "explain", "tell me about", "help", "?",
        "i don't understand", "confused", "clarify",
        "how does", "why", "what are", "define",
        "more info", "more information", "details"
    ]

    # Navigation commands
    NAVIGATION_COMMANDS = {
        'back': ['back', 'previous', 'go back', 'change', 'undo', 'return'],
        'skip': ['skip', 'next', 'continue', 'default', 'use default', 'auto'],
        'restart': ['restart', 'start over', 'reset', 'begin again', 'start again'],
        'status': ['where am i', 'status', 'current', 'what have i selected', 'show selections'],
        'exit': ['exit', 'quit', 'stop', 'cancel', 'done', 'leave', 'end']
    }

    # Selection patterns based on stage
    STAGE_PATTERNS = {
        'state_selection': {
            'patterns': [],  # Will be populated with actual state names
            'numbers': True  # Allow number selection
        },
        'facility_selection': {
            'patterns': ['primary', 'secondary', 'tertiary', 'all', 'all levels', 'combined'],
            'numbers': True
        },
        'age_selection': {
            'patterns': ['under-5', 'under 5', '5-15', '5 to 15', 'above-15', 'above 15',
                        'all ages', 'all', 'combined', '<5', '>15'],
            'numbers': True
        }
    }

    def __init__(self):
        """Initialize the intent classifier."""
        self.last_classification = None

    def classify(self,
                user_query: str,
                current_stage: str,
                context: Optional[Dict] = None) -> TPRIntent:
        """
        Classify user intent based on query and current TPR stage.

        Args:
            user_query: The user's input message
            current_stage: Current TPR workflow stage
            context: Optional context with state names, etc.

        Returns:
            TPRIntent enum value
        """
        query_lower = user_query.lower().strip()

        # Store for debugging
        logger.debug(f"Classifying: '{user_query}' at stage: {current_stage}")

        # Priority 1: Check for help/clarification requests
        if self._is_help_request(query_lower):
            logger.info(f"Classified as HELP_REQUEST: '{user_query}'")
            return TPRIntent.HELP_REQUEST

        # Priority 2: Check for navigation commands
        nav_command = self._detect_navigation(query_lower)
        if nav_command:
            logger.info(f"Classified as NAVIGATION ({nav_command}): '{user_query}'")
            return TPRIntent.NAVIGATION

        # Priority 3: Check if it's a selection for current stage
        if self._is_selection(query_lower, current_stage, context):
            logger.info(f"Classified as SELECTION: '{user_query}'")
            return TPRIntent.SELECTION

        # Priority 4: Check if it's a general question
        if self._is_question(query_lower):
            logger.info(f"Classified as QUESTION: '{user_query}'")
            return TPRIntent.QUESTION

        # Default: If very short (1-2 words), likely a selection attempt
        if len(query_lower.split()) <= 2:
            logger.info(f"Classified as SELECTION (short input): '{user_query}'")
            return TPRIntent.SELECTION

        # Cannot determine intent
        logger.info(f"Classified as UNCLEAR: '{user_query}'")
        return TPRIntent.UNCLEAR

    def _is_help_request(self, query: str) -> bool:
        """Check if query is asking for help or clarification."""
        # Check for help triggers
        for trigger in self.HELP_TRIGGERS:
            if trigger in query:
                # But exclude if it's clearly a selection
                # e.g., "what is primary" during facility selection is help
                # but "primary" alone is a selection
                if len(query.split()) > 1:
                    return True

        # Question mark at end usually means help
        if query.strip().endswith('?'):
            return True

        return False

    def _detect_navigation(self, query: str) -> Optional[str]:
        """Detect navigation commands."""
        for command, triggers in self.NAVIGATION_COMMANDS.items():
            for trigger in triggers:
                if trigger in query:
                    return command
        return None

    def _is_selection(self, query: str, stage: str, context: Optional[Dict] = None) -> bool:
        """Check if query is a selection for the current stage."""
        # Map stage names to our patterns
        stage_key = None
        if 'state' in stage.lower():
            stage_key = 'state_selection'
        elif 'facility' in stage.lower():
            stage_key = 'facility_selection'
        elif 'age' in stage.lower():
            stage_key = 'age_selection'

        if not stage_key or stage_key not in self.STAGE_PATTERNS:
            return False

        patterns = self.STAGE_PATTERNS[stage_key]

        # For state selection, use context if available
        if stage_key == 'state_selection' and context and 'available_states' in context:
            # Check if query matches any state name
            for state in context['available_states']:
                if state.lower() in query or query in state.lower():
                    return True

        # Check pattern matches
        for pattern in patterns.get('patterns', []):
            if pattern in query:
                return True

        # Check if it's a number selection
        if patterns.get('numbers', False):
            # Check for simple number input (1, 2, 3, etc.)
            if re.match(r'^[1-9]\d?$', query.strip()):
                return True

        return False

    def _is_question(self, query: str) -> bool:
        """Check if query is a general question."""
        question_words = ['how', 'what', 'why', 'when', 'where', 'who', 'which', 'is', 'are', 'can', 'should']

        # Check if starts with question word
        first_word = query.split()[0] if query.split() else ''
        if first_word in question_words:
            return True

        # Has question mark
        if '?' in query:
            return True

        return False

    def get_navigation_type(self, query: str) -> Optional[str]:
        """Get the specific navigation command from query."""
        query_lower = query.lower().strip()
        return self._detect_navigation(query_lower)

    def extract_selection(self, query: str, stage: str) -> Optional[str]:
        """
        Extract the actual selection from user query.

        Args:
            query: User input
            stage: Current TPR stage

        Returns:
            Extracted selection or None
        """
        query_lower = query.lower().strip()

        # For number inputs, return as-is
        if re.match(r'^[1-9]\d?$', query_lower):
            return query_lower

        # For facility selection
        if 'facility' in stage.lower():
            if 'primary' in query_lower:
                return 'primary'
            elif 'secondary' in query_lower:
                return 'secondary'
            elif 'tertiary' in query_lower:
                return 'tertiary'
            elif 'all' in query_lower:
                return 'all'

        # For age selection
        if 'age' in stage.lower():
            if 'under' in query_lower and '5' in query_lower:
                return 'under-5'
            elif '5' in query_lower and '15' in query_lower:
                return '5-15'
            elif 'above' in query_lower and '15' in query_lower:
                return 'above-15'
            elif 'all' in query_lower:
                return 'all_ages'

        # Return cleaned query for state names
        return query_lower