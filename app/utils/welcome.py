"""
Welcome Helper Module

Provides first-time user onboarding and initial guidance.
Detects new users and offers quick-start options.
"""

import logging
from typing import Dict, Any, Optional
from flask import session

logger = logging.getLogger(__name__)


class WelcomeHelper:
    """Helper class for welcoming and onboarding new users."""

    def __init__(self):
        """Initialize the welcome helper."""
        self.welcome_shown_key = 'welcome_shown'
        self.user_experience_key = 'user_experience_level'

    def should_show_welcome(self, session_data: Dict[str, Any]) -> bool:
        """
        Check if welcome message should be shown to user.

        Args:
            session_data: Current session data

        Returns:
            True if welcome should be shown, False otherwise
        """
        # Show welcome if never shown before
        if not session_data.get(self.welcome_shown_key, False):
            return True

        # Also show if user has no data and hasn't completed any analysis
        has_data = session_data.get('csv_loaded', False) or \
                   session_data.get('shapefile_loaded', False)
        has_analysis = session_data.get('analysis_complete', False)

        if not has_data and not has_analysis:
            # User might have cleared session, show help
            return True

        return False

    def mark_welcome_shown(self, session_data: Dict[str, Any]):
        """Mark that welcome has been shown to user."""
        session_data[self.welcome_shown_key] = True
        if hasattr(session, 'modified'):
            session.modified = True

    def get_welcome_message(self, session_id: str) -> Dict[str, Any]:
        """
        Generate welcome message with quick-start options.

        Args:
            session_id: Current session ID

        Returns:
            Welcome message with structured options
        """
        message = {
            'type': 'welcome',
            'title': '🎯 Welcome to ChatMRPT - Your AI Malaria Risk Assistant!',
            'content': self._get_welcome_content(),
            'quick_actions': self._get_quick_actions(),
            'tips': self._get_starter_tips()
        }

        logger.info(f"Generated welcome message for session {session_id}")
        return message

    def _get_welcome_content(self) -> str:
        """Get the main welcome message content."""
        return """I'm here to help you analyze malaria risk data and plan interventions.

I can assist you with:
• **Data Analysis**: Upload CSV/Excel files with ward-level data
• **Risk Assessment**: Identify high-risk areas using advanced algorithms
• **Visualization**: Create interactive maps and charts
• **ITN Planning**: Optimize bed net distribution
• **Report Generation**: Create comprehensive PDF reports

Let's get started! Choose an option below or just tell me what you'd like to do."""

    def _get_quick_actions(self) -> list:
        """Get quick action buttons for new users."""
        return [
            {
                'id': 'upload_data',
                'label': '📎 Upload My Data',
                'action': 'show_upload_dialog',
                'description': 'Upload CSV and shapefile for analysis',
                'primary': True
            },
            {
                'id': 'load_sample',
                'label': '🔬 Try Sample Data',
                'action': 'load_sample_data',
                'description': 'Explore with example dataset',
                'primary': False
            },
            {
                'id': 'learn_more',
                'label': '📚 Learn What I Can Do',
                'action': 'show_capabilities',
                'description': 'See all available features',
                'primary': False
            },
            {
                'id': 'data_requirements',
                'label': '📋 Data Requirements',
                'action': 'show_data_requirements',
                'description': 'Learn what data format you need',
                'primary': False
            }
        ]

    def _get_starter_tips(self) -> list:
        """Get helpful tips for new users."""
        return [
            "💡 **Tip**: Your data should include ward names, population, and health indicators",
            "💡 **Tip**: Shapefiles must be uploaded as ZIP files containing .shp, .shx, and .dbf files",
            "💡 **Tip**: You can ask me questions in natural language - just type what you need!",
            "💡 **Tip**: After analysis, you can export results as PDF reports or interactive dashboards"
        ]

    def get_experience_level(self, session_data: Dict[str, Any]) -> str:
        """
        Determine user's experience level based on session history.

        Args:
            session_data: Current session data

        Returns:
            Experience level: 'new', 'beginner', 'intermediate', 'advanced'
        """
        # Check various indicators of experience
        analyses_completed = session_data.get('analyses_completed', 0)
        uploads_successful = session_data.get('uploads_successful', 0)
        tools_used = len(session_data.get('tools_used', []))

        if analyses_completed == 0 and uploads_successful == 0:
            return 'new'
        elif analyses_completed < 2:
            return 'beginner'
        elif analyses_completed < 5 or tools_used < 5:
            return 'intermediate'
        else:
            return 'advanced'

    def get_contextual_greeting(self, session_data: Dict[str, Any]) -> Optional[str]:
        """
        Get a contextual greeting based on user's current state.

        Args:
            session_data: Current session data

        Returns:
            Contextual greeting message or None
        """
        exp_level = self.get_experience_level(session_data)

        # Check current state
        has_data = session_data.get('csv_loaded', False)
        has_analysis = session_data.get('analysis_complete', False)

        if exp_level == 'new':
            return "Welcome! I see this is your first time using ChatMRPT. How can I help you today?"

        elif exp_level == 'beginner' and not has_data:
            return "Welcome back! Ready to upload some data for analysis?"

        elif has_data and not has_analysis:
            return "Great! Your data is loaded. Would you like me to run the malaria risk analysis?"

        elif has_analysis:
            options = [
                "view your results",
                "generate a report",
                "plan ITN distribution",
                "explore different visualizations"
            ]
            return f"Your analysis is complete! You can {', '.join(options)}."

        return None

    def format_for_display(self, message_data: Dict[str, Any]) -> str:
        """
        Format welcome message for display in chat interface.

        Args:
            message_data: Message data from get_welcome_message()

        Returns:
            Formatted string for display
        """
        parts = []

        # Title
        parts.append(f"## {message_data['title']}")
        parts.append("")

        # Main content
        parts.append(message_data['content'])
        parts.append("")

        # Quick actions as a formatted list
        parts.append("### 🚀 Quick Start Options:")
        for action in message_data['quick_actions']:
            emoji = "🔵" if action['primary'] else "⚪"
            parts.append(f"{emoji} **{action['label']}** - {action['description']}")

        parts.append("")

        # Tips
        parts.append("### 💡 Getting Started Tips:")
        for tip in message_data['tips']:
            parts.append(tip)

        parts.append("")
        parts.append("---")
        parts.append("**Ready to begin?** Just click an option above or type your request!")

        return "\n".join(parts)