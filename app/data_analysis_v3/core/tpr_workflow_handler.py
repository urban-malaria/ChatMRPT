"""
TPR Workflow Handler

Handles all TPR (Test Positivity Rate) workflow logic including state selection,
facility selection, age group selection, and calculation triggering.
"""

import os
import logging
import shutil
import uuid
from typing import Dict, Any, Optional, List
from pathlib import Path
from enum import Enum

from app.core.tpr_precompute_service import schedule_precompute

from .state_manager import DataAnalysisStateManager, ConversationStage
from .tpr_data_analyzer import TPRDataAnalyzer
from .tpr_language_interface import TPRLanguageInterface
from .encoding_handler import find_raw_data_file, read_raw_data
import plotly.express as px
import pandas as pd

logger = logging.getLogger(__name__)


class TPRWorkflowHandler:
    """Handles TPR workflow progression and state management."""

    def __init__(self, session_id: str, state_manager: DataAnalysisStateManager,
                 tpr_analyzer: TPRDataAnalyzer):
        """
        Initialize TPR workflow handler.

        Args:
            session_id: Session identifier
            state_manager: State management instance
            tpr_analyzer: TPR data analyzer instance
        """
        self.session_id = session_id
        self.state_manager = state_manager
        self.tpr_analyzer = tpr_analyzer
        self.language = TPRLanguageInterface(session_id)  # LLM-first intent classification
        self.tpr_selections = {}
        self.current_stage = ConversationStage.INITIAL
        self.uploaded_data = None
        self.session_folder = f"instance/uploads/{session_id}"
    
    def set_data(self, data):
        """Set the uploaded data for analysis."""
        self.uploaded_data = data
    
    def set_stage(self, stage: ConversationStage):
        """Update the current workflow stage."""
        self.current_stage = stage

    def load_state_from_manager(self):
        """Restore handler state from state manager. Used when handler is recreated."""
        self.current_stage = self.state_manager.get_workflow_stage()
        self.tpr_selections = self.state_manager.get_tpr_selections() or {}
        logger.info(f"Loaded state: stage={self.current_stage}, selections={self.tpr_selections}")

    # ---------- Small visualization helpers for selection steps ----------
    def _save_fig_as_html(self, fig, title: str) -> Optional[Dict[str, str]]:
        """Save a plotly figure into the session visualizations folder and return its web path."""
        try:
            viz_dir = os.path.join(self.session_folder, 'visualizations')
            os.makedirs(viz_dir, exist_ok=True)
            html_name = f"tpr_step_{uuid.uuid4()}.html"
            html_path = os.path.join(viz_dir, html_name)

            # Configure figure to be responsive
            config = {
                'responsive': True,
                'displayModeBar': True,
                'displaylogo': False
            }
            # Use include_plotlyjs=True to embed Plotly in HTML (bypasses CDN/CSP issues)
            html = fig.to_html(include_plotlyjs=True, config=config)
            with open(html_path, 'w') as f:
                f.write(html)
            web_path = f"/serve_viz_file/{self.session_id}/visualizations/{html_name}"
            return {
                "type": "iframe",  # Use iframe type for proper rendering
                "url": web_path,
                "title": title,
                "height": 600  # Provide a default height
            }
        except Exception as e:
            logger.error(f"Failed to save TPR step figure: {e}")
            return None

    def _build_facility_level_visualizations(self, analysis: Dict[str, Any]) -> List[Dict[str, str]]:
        """Create clean visualizations for facility level selection in original style."""
        try:
            levels = analysis.get('levels', {}) or {}
            rows = []
            test_rows = []
            for key, info in levels.items():
                if key == 'all':
                    continue
                name = str(info.get('name', key)).replace('facilities', '').strip().title()
                rows.append({
                    'Facility Level': name,
                    'Facilities': int(info.get('count', 0))
                })
                test_rows.append({
                    'Facility Level': name,
                    'RDT Tests': int(info.get('rdt_tests', 0)),
                    'Microscopy Tests': int(info.get('microscopy_tests', 0))
                })
            if not rows:
                return []

            df = pd.DataFrame(rows)
            test_df = pd.DataFrame(test_rows)

            import plotly.graph_objects as go
            from plotly.subplots import make_subplots

            # Create figure with subplots (2 charts that can be navigated)
            fig = make_subplots(rows=1, cols=1)

            # Chart 1: Facility mix by level (clean bar chart)
            fig1 = px.bar(df, x='Facility Level', y='Facilities',
                         title='Facility mix by level',
                         color_discrete_sequence=['#636EFA'])  # Uniform blue

            # Format text properly with k notation
            fig1.update_traces(
                text=df['Facilities'].apply(lambda x: f'{x/1000:.2f}k' if x >= 1000 else str(int(x))),
                textposition='inside',
                textfont=dict(size=14, color='white'),
                hovertemplate='<b>%{x}</b><br>Facilities: %{y:,.0f}<extra></extra>'
            )

            fig1.update_layout(
                showlegend=False,
                autosize=True,  # Let it fit naturally
                yaxis=dict(
                    title='Facilities',
                    showgrid=True,
                    gridcolor='rgba(0,0,0,0.1)',
                    gridwidth=1,
                    zeroline=True,
                    zerolinecolor='rgba(0,0,0,0.2)',
                    zerolinewidth=2
                ),
                xaxis=dict(
                    title='',
                    showgrid=False
                ),
                plot_bgcolor='rgba(240, 240, 240, 0.5)',  # Light gray background
                paper_bgcolor='white',
                margin=dict(l=60, r=30, t=80, b=60),
                font=dict(size=12)
            )

            # Chart 2: Test availability by level (stacked bar)
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                name='RDT Tests',
                x=test_df['Facility Level'],
                y=test_df['RDT Tests'],
                marker_color='#636EFA',  # Blue
                text=test_df['RDT Tests'].apply(lambda x: f'{x/1000:.1f}k' if x >= 1000 else str(int(x))),
                textposition='inside',
                textfont=dict(size=12, color='white'),
                hovertemplate='<b>%{x}</b><br>RDT Tests: %{y:,.0f}<extra></extra>'
            ))
            fig2.add_trace(go.Bar(
                name='Microscopy Tests',
                x=test_df['Facility Level'],
                y=test_df['Microscopy Tests'],
                marker_color='#EF553B',  # Red
                text=test_df['Microscopy Tests'].apply(lambda x: f'{x/1000:.1f}k' if x >= 1000 else str(int(x))),
                textposition='inside',
                textfont=dict(size=12, color='white'),
                hovertemplate='<b>%{x}</b><br>Microscopy: %{y:,.0f}<extra></extra>'
            ))
            fig2.update_layout(
                barmode='stack',
                title='Test availability by level',
                autosize=True,  # Let it fit naturally
                yaxis=dict(
                    title='Tests',
                    showgrid=True,
                    gridcolor='rgba(0,0,0,0.1)',
                    gridwidth=1,
                    zeroline=True,
                    zerolinecolor='rgba(0,0,0,0.2)',
                    zerolinewidth=2,
                    tickformat='.2s'  # Auto format with k, M notation
                ),
                xaxis=dict(
                    title='',
                    showgrid=False
                ),
                legend=dict(
                    title=dict(text='Test Type', font=dict(size=12)),
                    bgcolor='rgba(255,255,255,0.9)',
                    bordercolor='rgba(0,0,0,0.2)',
                    borderwidth=1,
                    yanchor="top",
                    y=0.98,
                    xanchor="right",
                    x=0.98
                ),
                plot_bgcolor='rgba(240, 240, 240, 0.5)',  # Light gray background
                paper_bgcolor='white',
                margin=dict(l=60, r=30, t=80, b=60),
                font=dict(size=12)
            )

            # Save both as separate visualizations
            viz1 = self._save_fig_as_html(fig1, 'Facility mix by level')
            viz2 = self._save_fig_as_html(fig2, 'Test availability by level')

            visualizations = [v for v in [viz1, viz2] if v]
            logger.info(f"Generated {len(visualizations)} facility visualizations")
            return visualizations
        except Exception as e:
            logger.error(f"Error building facility selection visuals: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []

    def _build_age_group_visualizations(self, analysis: Dict[str, Any]) -> List[Dict[str, str]]:
        """Create clean visualizations for age group selection in original style."""
        try:
            age_groups = analysis.get('age_groups', {}) or {}
            order = [('under_5', 'Under 5'), ('over_5', 'Over 5'), ('pregnant', 'Pregnant')]
            rows = []
            test_rows = []
            for key, label in order:
                if key in age_groups and age_groups[key].get('has_data'):
                    g = age_groups[key]
                    rows.append({
                        'Age Group': label,
                        'TPR (%)': float(g.get('positivity_rate', 0.0))
                    })
                    test_rows.append({
                        'Age Group': label,
                        'RDT Tests': int(g.get('rdt_tests', 0)),
                        'Microscopy Tests': int(g.get('microscopy_tests', 0))
                    })
            if not rows:
                return []

            df = pd.DataFrame(rows)
            test_df = pd.DataFrame(test_rows)

            import plotly.graph_objects as go

            # Chart 1: Test availability by age group (stacked bar)
            fig1 = go.Figure()
            fig1.add_trace(go.Bar(
                name='RDT Tests',
                x=test_df['Age Group'],
                y=test_df['RDT Tests'],
                marker_color='#636EFA',  # Blue
                text=test_df['RDT Tests'].apply(lambda x: f'{x/1000000:.1f}M' if x >= 1000000 else f'{x/1000:.0f}k' if x >= 1000 else str(int(x))),
                textposition='inside',
                textfont=dict(size=12, color='white'),
                hovertemplate='<b>%{x}</b><br>RDT Tests: %{y:,.0f}<extra></extra>'
            ))
            fig1.add_trace(go.Bar(
                name='Microscopy Tests',
                x=test_df['Age Group'],
                y=test_df['Microscopy Tests'],
                marker_color='#EF553B',  # Red
                text=test_df['Microscopy Tests'].apply(lambda x: f'{x/1000000:.1f}M' if x >= 1000000 else f'{x/1000:.0f}k' if x >= 1000 else str(int(x))),
                textposition='inside',
                textfont=dict(size=12, color='white'),
                hovertemplate='<b>%{x}</b><br>Microscopy: %{y:,.0f}<extra></extra>'
            ))
            fig1.update_layout(
                barmode='stack',
                title='Test availability by age group',
                autosize=True,  # Let it fit naturally
                yaxis=dict(
                    title='Tests',
                    showgrid=True,
                    gridcolor='rgba(0,0,0,0.1)',
                    gridwidth=1,
                    zeroline=True,
                    zerolinecolor='rgba(0,0,0,0.2)',
                    zerolinewidth=2,
                    tickformat='.2s'  # Auto format
                ),
                xaxis=dict(
                    title='',
                    showgrid=False
                ),
                legend=dict(
                    title=dict(text='Test Type', font=dict(size=12)),
                    bgcolor='rgba(255,255,255,0.9)',
                    bordercolor='rgba(0,0,0,0.2)',
                    borderwidth=1,
                    yanchor="top",
                    y=0.98,
                    xanchor="right",
                    x=0.98
                ),
                plot_bgcolor='rgba(240, 240, 240, 0.5)',  # Light gray background
                paper_bgcolor='white',
                margin=dict(l=60, r=30, t=80, b=60),
                font=dict(size=12)
            )

            # Chart 2: Positivity rates by age group (clean bar chart)
            fig2 = px.bar(df, x='Age Group', y='TPR (%)',
                         title='Test positivity rate by age group',
                         color_discrete_sequence=['#636EFA'])  # Uniform blue

            fig2.update_traces(
                text=df['TPR (%)'].apply(lambda x: f'{x:.1f}%'),
                textposition='outside',
                textfont=dict(size=14),
                hovertemplate='<b>%{x}</b><br>TPR: %{y:.1f}%<extra></extra>'
            )

            fig2.update_layout(
                showlegend=False,
                autosize=True,  # Let it fit naturally
                yaxis=dict(
                    title='TPR (%)',
                    range=[0, max(df['TPR (%)'].max() * 1.3, 40)],  # More headroom
                    showgrid=True,
                    gridcolor='rgba(0,0,0,0.1)',
                    gridwidth=1,
                    zeroline=True,
                    zerolinecolor='rgba(0,0,0,0.2)',
                    zerolinewidth=2
                ),
                xaxis=dict(
                    title='',
                    showgrid=False
                ),
                plot_bgcolor='rgba(240, 240, 240, 0.5)',  # Light gray background
                paper_bgcolor='white',
                margin=dict(l=60, r=30, t=80, b=80),  # Margins for text
                font=dict(size=12)
            )

            # Save both as separate visualizations
            viz1 = self._save_fig_as_html(fig1, 'Test availability by age group')
            viz2 = self._save_fig_as_html(fig2, 'Test positivity rate by age group')

            return [v for v in [viz1, viz2] if v]
        except Exception as e:
            logger.error(f"Error building age selection visuals: {e}")
            return []
    
    def execute_command(self, command: str, stage: ConversationStage) -> Dict[str, Any]:
        """
        Execute a command at a specific workflow stage.
        This is called by the routes when a selection is made.

        Args:
            command: The extracted command/selection (e.g., "primary", "u5", "adamawa")
            stage: Current conversation stage

        Returns:
            Response dictionary
        """
        logger.info(f"🎯 execute_command called: command='{command}', stage={stage}")

        # Route based on stage
        if stage == ConversationStage.TPR_STATE_SELECTION:
            logger.info(f"🟢 Executing state selection: {command}")
            return self.handle_state_selection(command)

        elif stage == ConversationStage.TPR_FACILITY_LEVEL:
            logger.info(f"🟢 Executing facility selection: {command}")
            return self.handle_facility_selection(command)

        elif stage == ConversationStage.TPR_AGE_GROUP:
            logger.info(f"🟢 Executing age group selection: {command}")
            return self.handle_age_group_selection(command)

        else:
            logger.warning(f"❌ execute_command called with unsupported stage: {stage}")
            return {
                "success": False,
                "message": f"Cannot execute command at stage {stage}",
                "session_id": self.session_id
            }

    def execute_confirmation(self) -> Dict[str, Any]:
        """
        Execute confirmation action (called from routes when user confirms).
        Currently handles risk analysis confirmation.

        Returns:
            Response dictionary
        """
        logger.info(f"🎯 execute_confirmation called")
        # The confirmation is for risk analysis transition
        return self.trigger_risk_analysis()

    def handle_workflow(self, user_query: str) -> Optional[Dict[str, Any]]:
        """
        Handle TPR workflow progression based on current stage with flexibility.

        Args:
            user_query: User's input message

        Returns:
            Response dictionary or None if not in TPR workflow
        """
        logger.info("="*60)
        logger.info("🔄 TPR: handle_workflow called")
        logger.info(f"  📝 Query: {user_query[:100]}...")
        logger.info(f"  🎯 Current Stage: {self.current_stage}")
        logger.info(f"  🆔 Session ID: {self.session_id}")
        logger.info(f"  📊 Selections: {self.tpr_selections}")
        logger.info("="*60)

        # Import intent classifier for navigation detection
        from .tpr_intent_classifier import TPRIntentClassifier

        # Check for navigation commands first (back, status, exit)
        classifier = TPRIntentClassifier()
        nav_type = classifier.get_navigation_type(user_query)

        if nav_type:
            return self.handle_navigation(nav_type)

        # Continue with stage-specific handling
        if self.current_stage == ConversationStage.TPR_STATE_SELECTION:
            logger.info("🟢 Routing to handle_state_selection")
            return self.handle_state_selection(user_query)

        elif self.current_stage == ConversationStage.TPR_FACILITY_LEVEL:
            logger.info("🟢 Routing to handle_facility_selection")
            return self.handle_facility_selection(user_query)

        elif self.current_stage == ConversationStage.TPR_AGE_GROUP:
            logger.info("🟢 Routing to handle_age_group_selection")
            return self.handle_age_group_selection(user_query)

        # ✅ REMOVED TPR_COMPLETED_AWAITING_CONFIRMATION HANDLER
        # The workflow now auto-transitions after TPR completes (no confirmation needed)
        # This stage should never be reached since calculate_tpr() transitions immediately

        return None

    def handle_navigation(self, nav_type: str) -> Dict[str, Any]:
        """
        Handle navigation commands during TPR workflow.

        Args:
            nav_type: Type of navigation (back, status, exit, etc.)

        Returns:
            Response dictionary
        """
        logger.info(f"Handling navigation command: {nav_type}")

        if nav_type == 'back':
            # Go back one stage
            if self.current_stage == ConversationStage.TPR_FACILITY_LEVEL:
                # Go back to state selection
                self.current_stage = ConversationStage.TPR_STATE_SELECTION
                self.state_manager.update_workflow_stage(self.current_stage)
                # Clear facility selection
                if 'facility_level' in self.tpr_selections:
                    del self.tpr_selections['facility_level']

                # Re-show state selection
                state_analysis = self.tpr_analyzer.analyze_states(self.uploaded_data)
                from .formatters import MessageFormatter
                formatter = MessageFormatter(self.session_id)
                message = formatter.format_state_selection(state_analysis)

                return {
                    "success": True,
                    "message": "Let's go back to state selection.\n\n" + message,
                    "session_id": self.session_id,
                    "workflow": "tpr",
                    "stage": "state_selection"
                }

            elif self.current_stage == ConversationStage.TPR_AGE_GROUP:
                # Go back to facility selection
                self.current_stage = ConversationStage.TPR_FACILITY_LEVEL
                self.state_manager.update_workflow_stage(self.current_stage)
                # Clear age selection
                if 'age_group' in self.tpr_selections:
                    del self.tpr_selections['age_group']

                # Re-show facility selection
                selected_state = self.tpr_selections.get('state', '')
                facility_analysis = self.tpr_analyzer.analyze_facility_levels(self.uploaded_data, selected_state)
                from .formatters import MessageFormatter
                formatter = MessageFormatter(self.session_id)
                message = formatter.format_facility_selection(selected_state, facility_analysis)

                return {
                    "success": True,
                    "message": "Let's go back to facility selection.\n\n" + message,
                    "session_id": self.session_id,
                    "workflow": "tpr",
                    "stage": "facility_selection"
                }

            else:
                return {
                    "success": True,
                    "message": "You're at the beginning of the TPR workflow. Please make your selection or say **'exit'** to leave.",
                    "session_id": self.session_id
                }

        elif nav_type == 'status':
            # Show current selections
            status = "## Your TPR workflow status\n\n"
            if self.tpr_selections:
                for key, value in self.tpr_selections.items():
                    status += f"- **{key.replace('_', ' ').title()}**: {value}\n"
            else:
                status += "No selections made yet.\n"

            status += f"\n**Current stage**: {str(self.current_stage).replace('ConversationStage.', '').replace('_', ' ').title()}"

            return {
                "success": True,
                "message": status + "\n\nPlease continue with your selection.",
                "session_id": self.session_id
            }

        elif nav_type == 'exit':
            # Exit TPR workflow
            self.state_manager.mark_tpr_workflow_complete()
            return {
                "success": True,
                "message": "Exiting TPR workflow. Your selections have been saved. You can restart anytime by saying 'run TPR analysis'.",
                "session_id": self.session_id,
                "workflow": "exit"
            }

        else:
            return {
                "success": True,
                "message": f"Navigation command '{nav_type}' recognized. Please continue with your selection.",
                "session_id": self.session_id
            }

    def start_workflow(self) -> Dict[str, Any]:
        """Start the TPR workflow with progressive disclosure."""
        logger.info("Starting TPR workflow")

        # CRITICAL: Mark TPR workflow as active
        self.state_manager.mark_tpr_workflow_active()
        logger.info("🔴 CRITICAL: Marked TPR workflow as ACTIVE")

        # Reset TPR selections
        self.tpr_selections = {}

        # Clear any pending visualizations from previous runs
        self.state_manager.update_state({'pending_visualizations': None})

        # Determine user expertise level for progressive disclosure
        user_expertise = self._determine_user_expertise()

        # Analyze available states
        state_analysis = self.tpr_analyzer.analyze_states(self.uploaded_data)
        
        # Check if there's only one state - if so, skip state selection
        if state_analysis.get('total_states') == 1:
            # Auto-select the single state
            single_state = list(state_analysis['states'].keys())[0]
            logger.info(f"Single state detected: {single_state}, skipping state selection")
            
            self.tpr_selections['state'] = single_state
            self.state_manager.save_tpr_selection('state', single_state)
            
            # Move directly to facility selection
            self.current_stage = ConversationStage.TPR_FACILITY_LEVEL
            self.state_manager.update_workflow_stage(self.current_stage)
            
            # Analyze facility levels
            facility_analysis = self.tpr_analyzer.analyze_facility_levels(self.uploaded_data, single_state)

            # Create workflow introduction with single state
            intro_message = "## Welcome to TPR Analysis\n\n"
            intro_message += "I'll guide you through calculating Test Positivity Rates to identify high-burden areas for intervention.\n\n"
            intro_message += "### The process has 3 simple steps\n\n"
            intro_message += "1. ~~Select State~~ (Auto-selected: " + single_state + ")\n"
            intro_message += "2. Choose Facility Level\n"
            intro_message += "3. Pick Age Group\n\n"
            intro_message += "At each step, I have data visualizations ready to help inform your decision. Just ask if you'd like to see them!\n\n"
            intro_message += "Let's begin!\n\n"

            # Import formatter
            from .formatters import MessageFormatter
            formatter = MessageFormatter(self.session_id)
            facility_message = formatter.format_facility_selection_only(facility_analysis)

            # Combine introduction with facility selection
            message = intro_message + facility_message
            
            # Build visual summaries for facility selection BUT DON'T AUTO-SHOW
            facility_viz = self._build_facility_level_visualizations(facility_analysis)

            # Store visualizations in state for agent to access on demand
            if facility_viz:
                self.state_manager.update_state({
                    'pending_visualizations': {
                        'facility_level': facility_viz,
                        'stage': 'facility_selection'
                    }
                })
                logger.info(f"📦 Stored {len(facility_viz)} facility visualizations for on-demand access")

            return {
                "success": True,
                "message": message,
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "facility_selection",
                "visualizations": None  # Don't auto-display visualizations
            }
        
        # Multiple states - show state selection
        self.current_stage = ConversationStage.TPR_STATE_SELECTION
        self.state_manager.update_workflow_stage(self.current_stage)

        from .formatters import MessageFormatter
        formatter = MessageFormatter(self.session_id)

        # Create conversational workflow introduction
        intro_message = "## Welcome to TPR Analysis\n\n"
        intro_message += "I'll guide you through calculating Test Positivity Rates to identify high-burden areas for intervention.\n\n"
        intro_message += "### The process has 3 simple steps\n\n"
        intro_message += "1. Select State (if multiple)\n"
        intro_message += "2. Choose Facility Level\n"
        intro_message += "3. Pick Age Group\n\n"
        intro_message += "At each step, I have data visualizations ready to help inform your decision. Just ask if you'd like to see them!\n\n"
        intro_message += "Let's begin!\n\n"

        # Get state selection message
        state_message = formatter.format_state_selection(state_analysis)

        # Combine introduction with state selection
        message = intro_message + state_message

        return {
            "success": True,
            "message": message,
            "session_id": self.session_id,
            "workflow": "tpr",
            "stage": "state_selection"
        }
    
    def handle_state_selection(self, user_query: str) -> Dict[str, Any]:
        """Handle state selection in TPR workflow."""
        # Extract selected state
        selected_state = self.extract_state_from_query(user_query)
        
        if not selected_state:
            return {
                "success": True,
                "message": "I didn't catch which state you'd like to analyze. Please specify the state name or number from the list.",
                "session_id": self.session_id
            }
        
        # Save selection
        self.tpr_selections['state'] = selected_state
        self.state_manager.save_tpr_selection('state', selected_state)

        # Move to facility level selection
        self.current_stage = ConversationStage.TPR_FACILITY_LEVEL
        self.state_manager.update_workflow_stage(self.current_stage)

        # Analyze facility levels for selected state
        facility_analysis = self.tpr_analyzer.analyze_facility_levels(self.uploaded_data, selected_state)

        # Add acknowledgment
        acknowledgment = f"Great choice! You've selected **{selected_state}**. "
        # Add contextual insight if available
        if facility_analysis.get('total_facilities'):
            total = facility_analysis['total_facilities']
            acknowledgment += f"I can see {total:,} health facilities in this state.\n\n"

        from .formatters import MessageFormatter
        formatter = MessageFormatter(self.session_id)
        message = acknowledgment + formatter.format_facility_selection(selected_state, facility_analysis)
        
        # Build visual summaries for facility selection BUT DON'T AUTO-SHOW
        facility_viz = self._build_facility_level_visualizations(facility_analysis)

        # Store visualizations in state for agent to access on demand
        if facility_viz:
            self.state_manager.update_state({
                'pending_visualizations': {
                    'facility_level': facility_viz,
                    'stage': 'facility_selection'
                }
            })
            logger.info(f"Stored {len(facility_viz)} facility visualizations for on-demand access")
            logger.info(f"Visualization URLs: {[v.get('url', 'no-url') for v in facility_viz]}")
        else:
            logger.warning("No facility visualizations generated to store")

        # Progressive disclosure for facility selection
        user_expertise = self._determine_user_expertise()
        if user_expertise == 'novice':
            # Add recommendation for new users
            message += "\n\n### Tip\n\nSecondary facilities usually provide the best balance of coverage and data quality."

        return {
            "success": True,
            "message": message,
            "session_id": self.session_id,
            "workflow": "tpr",
            "stage": "facility_selection",
            "visualizations": None  # Don't auto-display visualizations
        }
    
    def handle_facility_selection(self, user_query: str) -> Dict[str, Any]:
        """Handle facility level selection with LLM-first intent classification."""
        logger.info(f"🔵 Handling facility selection with query: '{user_query}'")

        # CRITICAL: Ensure state is loaded
        if 'state' not in self.tpr_selections or not self.tpr_selections['state']:
            saved_state = self.state_manager.get_tpr_selection('state')
            if saved_state:
                self.tpr_selections['state'] = saved_state
                logger.info(f"🔵 Loaded state from state_manager: {saved_state}")
            else:
                # Try to extract from data
                from app.core.tpr_utils import extract_state_from_data
                detected_state = extract_state_from_data(self.uploaded_data)
                if detected_state:
                    self.tpr_selections['state'] = detected_state
                    self.state_manager.save_tpr_selection('state', detected_state)
                    logger.info(f"🔵 Auto-detected state: {detected_state}")

        # Get facility analysis for context
        state_for_analysis = self.tpr_selections.get('state', '')
        facility_analysis = self.tpr_analyzer.analyze_facility_levels(
            self.uploaded_data,
            state_for_analysis
        )

        # LLM-FIRST: Classify intent with rich context
        intent_result = self.language.classify_intent(
            message=user_query,
            stage='facility_selection',
            context={
                'current_stage': 'facility_selection',
                'valid_options': ['primary', 'secondary', 'tertiary', 'all'],
                'state': state_for_analysis,
                'facility_analysis': {
                    'total_facilities': facility_analysis.get('total_facilities'),
                    'levels': list(facility_analysis.get('levels', {}).keys())
                },
                'data_columns': list(self.uploaded_data.columns) if self.uploaded_data is not None else [],
                'data_shape': {'rows': self.uploaded_data.shape[0], 'cols': self.uploaded_data.shape[1]} if self.uploaded_data is not None else None
            }
        )

        logger.info(f"🎯 Intent classified as: {intent_result.intent} (confidence={intent_result.confidence:.2f})")
        logger.info(f"   Rationale: {intent_result.rationale}")
        if intent_result.extracted_value:
            logger.info(f"   Extracted value: {intent_result.extracted_value}")

        # Route based on LLM-classified intent
        if intent_result.intent == 'information_request':
            logger.info("📚 User wants information about facility options")
            from .formatters import MessageFormatter
            formatter = MessageFormatter(self.session_id)
            explanation = formatter.format_facility_selection(state_for_analysis, facility_analysis)
            return {
                "success": True,
                "message": explanation,
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "facility_selection"
            }

        elif intent_result.intent == 'data_inquiry':
            logger.info("🔍 User asking about their data - handoff to agent")
            return self._handoff_to_agent(
                user_query,
                stage='facility_selection',
                valid_options=['primary', 'secondary', 'tertiary', 'all'],
                context_type='data_inquiry'
            )

        elif intent_result.intent == 'analysis_request':
            logger.info("📊 User wants analysis - handoff to agent")
            return self._handoff_to_agent(
                user_query,
                stage='facility_selection',
                valid_options=['primary', 'secondary', 'tertiary', 'all'],
                context_type='analysis_request'
            )

        elif intent_result.intent == 'selection':
            logger.info(f"✅ User making selection: {intent_result.extracted_value}")

            # LLM already extracted the value!
            selected_level = intent_result.extracted_value

            if not selected_level:
                # LLM couldn't extract - ask user to clarify
                return {
                    "success": True,
                    "message": "I didn't catch which facility level you want. Please type one of: **primary**, **secondary**, **tertiary**, or **all**",
                    "session_id": self.session_id,
                    "workflow": "tpr",
                    "stage": "facility_selection"
                }

            # Validate the selection
            if selected_level not in ['primary', 'secondary', 'tertiary', 'all']:
                return {
                    "success": True,
                    "message": f"I heard '{selected_level}' but that's not a valid option. Please choose: **primary**, **secondary**, **tertiary**, or **all**",
                    "session_id": self.session_id,
                    "workflow": "tpr",
                    "stage": "facility_selection"
                }

            # Save selection
            self.tpr_selections['facility_level'] = selected_level
            self.state_manager.save_tpr_selection('facility_level', selected_level)
            logger.info(f"🔵 Saved facility selection: {selected_level}")

            # Move to age group selection
            self.current_stage = ConversationStage.TPR_AGE_GROUP
            self.state_manager.update_workflow_stage(self.current_stage)
            logger.info(f"🔵 Updated stage to: {self.current_stage}")

            # Analyze age groups
            age_analysis = self.tpr_analyzer.analyze_age_groups(
                self.uploaded_data,
                state_for_analysis,
                selected_level
            )

            logger.info(f"🔵 Age analysis complete, formatting response")

            # Add acknowledgment
            level_display = selected_level.replace('_', ' ').title()
            acknowledgment = f"Perfect! You've selected **{level_display}** facilities. "
            if age_analysis.get('total_tests'):
                acknowledgment += f"These facilities conducted {age_analysis['total_tests']:,} tests.\n\n"

            from .formatters import MessageFormatter
            formatter = MessageFormatter(self.session_id)
            message = acknowledgment + formatter.format_age_group_selection(age_analysis)

            # Build age group visualizations BUT DON'T AUTO-SHOW
            age_viz = None
            try:
                age_viz = self._build_age_group_visualizations(age_analysis)
                logger.info(f"🔵 Created {len(age_viz) if age_viz else 0} age group visualizations")

                # Store visualizations in state for agent to access on demand
                if age_viz:
                    self.state_manager.update_state({
                        'pending_visualizations': {
                            'age_group': age_viz,
                            'stage': 'age_selection'
                        }
                    })
                    logger.info(f"📦 Stored {len(age_viz)} age visualizations for on-demand access")
            except Exception as e:
                logger.error(f"Could not create age selection visuals: {e}")

            logger.info(f"🔵 Returning age selection prompt (length: {len(message)})")

            return {
                "success": True,
                "message": message,
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "age_selection",
                "visualizations": None  # Don't auto-display visualizations
            }

        elif intent_result.intent == 'navigation':
            logger.info("🧭 User wants to navigate - handle navigation command")
            return self.handle_navigation(user_query)

        else:  # general or low confidence
            logger.warning(f"❓ Unclear intent (confidence={intent_result.confidence:.2f})")
            return {
                "success": True,
                "message": f"I'm not sure what you'd like to do. Are you:\n\n"
                          f"- **Asking about the options?** (Say 'explain' or 'show charts')\n"
                          f"- **Asking about your data?** (I can help with that too!)\n"
                          f"- **Making a selection?** (Type: primary, secondary, tertiary, or all)\n\n"
                          f"What would you like?",
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "facility_selection"
            }

    def _handoff_to_agent(
        self,
        user_query: str,
        stage: str,
        valid_options: List[str],
        context_type: str = 'general'
    ) -> Dict[str, Any]:
        """Hand off to agent with rich context about workflow state and data."""
        from app.data_analysis_v3.core.agent import DataAnalysisAgent
        import asyncio

        agent = DataAnalysisAgent(self.session_id)

        # Build rich workflow context
        workflow_context = {
            'in_tpr_workflow': True,
            'stage': stage,
            'valid_options': valid_options,
            'current_selections': self.tpr_selections,
            'context_type': context_type,  # 'data_inquiry', 'analysis_request', 'general'

            # Data context
            'data_loaded': self.uploaded_data is not None,
            'data_columns': list(self.uploaded_data.columns) if self.uploaded_data is not None else [],
            'data_shape': {'rows': self.uploaded_data.shape[0], 'cols': self.uploaded_data.shape[1]} if self.uploaded_data is not None else None,

            # Workflow reminder
            'workflow_reminder': f"After helping the user, gently remind them they're in the TPR workflow at the {stage} stage. They can continue by selecting: {', '.join(valid_options)}"
        }

        logger.info(f"🤝 Handing off to agent with context: {context_type}")
        logger.info(f"   Data available: {workflow_context['data_loaded']}")
        logger.info(f"   Columns: {len(workflow_context['data_columns'])}")

        # Call agent with rich context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                agent.analyze(user_query, workflow_context=workflow_context)
            )
        finally:
            loop.close()

        # Add workflow reminder to response
        if result.get('success') and result.get('message'):
            reminder = f"\n\n---\n\n💡 **Ready to continue the TPR workflow?** Select your facility level: {', '.join(valid_options)}"
            result['message'] += reminder
            result['workflow'] = 'tpr'
            result['stage'] = stage

        logger.info(f"🤝 Agent response received, length: {len(result.get('message', ''))}")

        return result

    def handle_age_group_selection(self, user_query: str) -> Dict[str, Any]:
        """Handle age group selection with LLM-first intent classification."""
        logger.info(f"🟣 Handling age group selection with query: '{user_query}'")
        logger.info(f"🟣 Current TPR selections: {self.tpr_selections}")

        # CRITICAL: Ensure state is loaded before any operations
        if 'state' not in self.tpr_selections or not self.tpr_selections['state']:
            saved_state = self.state_manager.get_tpr_selection('state')
            if saved_state:
                self.tpr_selections['state'] = saved_state
                logger.info(f"🟣 Loaded state from state_manager: {saved_state}")
            else:
                # Try to extract from data
                from app.core.tpr_utils import extract_state_from_data
                detected_state = extract_state_from_data(self.uploaded_data)
                if detected_state:
                    self.tpr_selections['state'] = detected_state
                    self.state_manager.save_tpr_selection('state', detected_state)
                    logger.info(f"🟣 Auto-detected state: {detected_state}")

        # Also ensure facility level is loaded
        if 'facility_level' not in self.tpr_selections or not self.tpr_selections['facility_level']:
            saved_facility = self.state_manager.get_tpr_selection('facility_level')
            if saved_facility:
                self.tpr_selections['facility_level'] = saved_facility
                logger.info(f"🟣 Loaded facility_level from state_manager: {saved_facility}")

        # Get analysis for context
        state_for_analysis = self.tpr_selections.get('state', '')
        facility_for_analysis = self.tpr_selections.get('facility_level', 'primary')
        age_analysis = self.tpr_analyzer.analyze_age_groups(
            self.uploaded_data,
            state_for_analysis,
            facility_for_analysis
        )

        # LLM-FIRST: Classify intent with rich context
        intent_result = self.language.classify_intent(
            message=user_query,
            stage='age_group',
            context={
                'current_stage': 'age_group',
                'valid_options': ['u5', 'o5', 'pw', 'all'],
                'state': state_for_analysis,
                'facility_level': facility_for_analysis,
                'age_analysis': {
                    'total_tests': age_analysis.get('total_tests'),
                    'age_groups': list(age_analysis.get('age_groups', {}).keys())
                },
                'data_columns': list(self.uploaded_data.columns) if self.uploaded_data is not None else [],
                'data_shape': {'rows': self.uploaded_data.shape[0], 'cols': self.uploaded_data.shape[1]} if self.uploaded_data is not None else None
            }
        )

        logger.info(f"🎯 Intent classified as: {intent_result.intent} (confidence={intent_result.confidence:.2f})")
        logger.info(f"   Rationale: {intent_result.rationale}")
        if intent_result.extracted_value:
            logger.info(f"   Extracted value: {intent_result.extracted_value}")

        # Route based on LLM-classified intent
        if intent_result.intent == 'information_request':
            logger.info("📚 User wants information about age group options")
            from .formatters import MessageFormatter
            formatter = MessageFormatter(self.session_id)
            explanation = formatter.format_age_group_selection(age_analysis)
            return {
                "success": True,
                "message": explanation,
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "age_selection"
            }

        elif intent_result.intent == 'data_inquiry':
            logger.info("🔍 User asking about their data - handoff to agent")
            return self._handoff_to_agent(
                user_query,
                stage='age_selection',
                valid_options=['u5', 'o5', 'pw', 'all'],
                context_type='data_inquiry'
            )

        elif intent_result.intent == 'analysis_request':
            logger.info("📊 User wants analysis - handoff to agent")
            return self._handoff_to_agent(
                user_query,
                stage='age_selection',
                valid_options=['u5', 'o5', 'pw', 'all'],
                context_type='analysis_request'
            )

        elif intent_result.intent == 'selection':
            logger.info(f"✅ User making selection: {intent_result.extracted_value}")

            # LLM already extracted the value!
            selected_age = intent_result.extracted_value

            if not selected_age:
                # LLM couldn't extract - ask user to clarify
                return {
                    "success": True,
                    "message": "I didn't catch which age group you want. Please type one of: **u5** (under 5), **o5** (over 5), **pw** (pregnant women), or **all**",
                    "session_id": self.session_id,
                    "workflow": "tpr",
                    "stage": "age_selection"
                }

            # Validate the selection
            if selected_age not in ['u5', 'o5', 'pw', 'all', 'all_ages']:
                return {
                    "success": True,
                    "message": f"I heard '{selected_age}' but that's not a valid option. Please choose: **u5**, **o5**, **pw**, or **all**",
                    "session_id": self.session_id,
                    "workflow": "tpr",
                    "stage": "age_selection"
                }

            # Normalize 'all' to 'all_ages' (backend format)
            if selected_age == 'all':
                selected_age = 'all_ages'

            # Save selection
            self.tpr_selections['age_group'] = selected_age
            self.state_manager.save_tpr_selection('age_group', selected_age)
            logger.info(f"🟣 Saved age_group selection: {selected_age}")

            # Move to calculation stage
            self.current_stage = ConversationStage.TPR_CALCULATING
            self.state_manager.update_workflow_stage(self.current_stage)
            logger.info(f"🟣 Updated stage to TPR_CALCULATING")

            # Add acknowledgment before calculation
            age_display = selected_age.replace('_', ' ').replace('-', ' to ').title()
            acknowledgment = f"Excellent! Analyzing **{age_display}** age group.\n\n"

            # Perform TPR calculation
            logger.info("🟣 About to call calculate_tpr()")
            result = self.calculate_tpr()

            # Prepend acknowledgment to result message
            if result and result.get('message'):
                result['message'] = acknowledgment + result['message']

            logger.info(f"🟣 calculate_tpr returned: success={result.get('success')}")
            return result

        elif intent_result.intent == 'navigation':
            logger.info("🧭 User wants to navigate - handle navigation command")
            return self.handle_navigation(user_query)

        else:  # general or low confidence
            logger.warning(f"❓ Unclear intent (confidence={intent_result.confidence:.2f})")
            return {
                "success": True,
                "message": f"I'm not sure what you'd like to do. Are you:\n\n"
                          f"- **Asking about the options?** (Say 'explain' or 'show charts')\n"
                          f"- **Asking about your data?** (I can help with that too!)\n"
                          f"- **Making a selection?** (Type: u5, o5, pw, or all)\n\n"
                          f"What would you like?",
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "age_selection"
            }

    def handle_risk_analysis_confirmation(self, user_query: str) -> Dict[str, Any]:
        """Handle confirmation to proceed to risk analysis after TPR completion.

        Uses keyword-first approach with gentle reminders.
        """
        logger.info(f"🟡 Handling risk analysis confirmation with query: '{user_query}'")

        # Keyword-first approach for confirmation
        query_lower = user_query.lower().strip()

        # Check for confirmation keywords
        confirmation_keywords = ['yes', 'y', 'continue', 'proceed', 'ok', 'okay', 'sure', 'go ahead', 'yeah']
        decline_keywords = ['no', 'n', 'stop', 'exit', 'cancel', 'not now', 'later']

        # Check if it's a confirmation
        if any(keyword == query_lower or keyword in query_lower.split() for keyword in confirmation_keywords):
            logger.info("🟡 User confirmed - triggering risk analysis transition")
            # Mark workflow as complete before transitioning
            self.state_manager.mark_tpr_workflow_complete()
            # Trigger risk analysis pipeline
            return self.trigger_risk_analysis()

        # Check if it's a decline
        elif any(keyword == query_lower or keyword in query_lower.split() for keyword in decline_keywords):
            # End workflow gracefully
            self.current_stage = ConversationStage.INITIAL
            self.state_manager.update_workflow_stage(self.current_stage)
            self.state_manager.mark_tpr_workflow_complete()
            logger.info("🔴 User declined risk analysis - marking TPR workflow as COMPLETE")
            return {
                "success": True,
                "message": "No problem! The TPR results have been saved. You can proceed to risk analysis anytime by saying 'analyze risk' or 'rank wards'.",
                "session_id": self.session_id
            }

        # Not a keyword - pass to AI with context and gentle reminder
        else:
            logger.info(f"🟡 No confirmation keyword detected in '{query_lower}', routing to AI with context")

            # Let the AI handle the question but add context about TPR completion
            # The question will be handled by the agent with a gentle reminder
            return {
                "success": True,
                "requires_ai": True,  # Signal that this needs AI processing
                "context": "TPR_COMPLETED_AWAITING_CONFIRMATION",
                "user_query": user_query,
                "session_id": self.session_id,
                "gentle_reminder": "\n\n💡 When you're ready to add environmental factors for comprehensive risk assessment, just say **'yes'** or **'continue'**."
            }

    def calculate_tpr(self) -> Dict[str, Any]:
        """Calculate TPR using the full-featured tool."""
        logger.info(f"🟣 calculate_tpr() method called with selections: {self.tpr_selections}")

        # CRITICAL: Validate state exists before proceeding
        if 'state' not in self.tpr_selections or not self.tpr_selections['state']:
            # Try to load from state manager
            saved_state = self.state_manager.get_tpr_selection('state')
            if saved_state:
                self.tpr_selections['state'] = saved_state
                logger.info(f"🟣 Loaded state from state_manager: {saved_state}")
            else:
                # Extract state from data as fallback
                from app.core.tpr_utils import extract_state_from_data
                detected_state = extract_state_from_data(self.uploaded_data)
                if detected_state:
                    self.tpr_selections['state'] = detected_state
                    self.state_manager.save_tpr_selection('state', detected_state)
                    logger.warning(f"🟣 State was missing, extracted from data: {detected_state}")
                else:
                    logger.error("🔴 CRITICAL: No state found in selections or data!")
                    return {
                        "success": False,
                        "message": "Error: State information is missing. Please restart the TPR workflow.",
                        "session_id": self.session_id
                    }

        import json
        import os
        import time
        from datetime import datetime
        from ..tools.tpr_analysis_tool import analyze_tpr_data

        # Start timing
        start_time = time.time()
        debug_info = {
            "timestamp": datetime.now().isoformat(),
            "session_id": self.session_id,
            "selections": self.tpr_selections,
            "stages": {}
        }

        logger.info("🟣 Imported analyze_tpr_data successfully")

        # Safely get state with fallback
        state_name = self.tpr_selections.get('state', 'Selected State')
        facility_level = self.tpr_selections.get('facility_level', 'all').replace('_', ' ').title()
        age_group = self.tpr_selections.get('age_group', 'all_ages').replace('_', ' ').title()

        message = f"""## ✅ Calculating TPR for {state_name}

### Settings

- Facility Level: {facility_level}
- Age Group: {age_group}

Analyzing test data and generating visualizations..."""
        
        try:
            # Prepare options for the tool
            options = {
                'age_group': self.tpr_selections['age_group'],
                'facility_level': self.tpr_selections['facility_level'],
                'test_method': 'both'  # Always use maximum of both methods (WHO standard)
            }
            
            # Create graph state for the tool
            graph_state = {
                'session_id': self.session_id,
                'data_loaded': True,
                'data_file': f"instance/uploads/{self.session_id}/uploaded_data.csv"
            }
            
            # Save data to CSV for tool to access (if not already saved)
            data_path = f"instance/uploads/{self.session_id}/uploaded_data.csv"
            if not os.path.exists(data_path):
                self.uploaded_data.to_csv(data_path, index=False)
                logger.info(f"✅ Saved data to {data_path} for TPR tool")
                debug_info["stages"]["data_save"] = {"success": True, "path": data_path}
            else:
                logger.info(f"📋 Data already exists at {data_path}")
                debug_info["stages"]["data_save"] = {"success": True, "exists": True}
            
            # Call the tool with calculate_tpr action
            logger.info(f"🎯 Calling TPR tool with options: {options}")
            logger.info("🔍 DEBUG: About to invoke analyze_tpr_data tool")
            logger.info(f"🔍 DEBUG: Tool input - action: calculate_tpr, options: {json.dumps(options)}")
            
            result = analyze_tpr_data.invoke({
                'thought': f"Calculating TPR for {self.tpr_selections['state']} with user selections",
                'action': "calculate_tpr",
                'options': json.dumps(options),
                'graph_state': graph_state
            })
            
            tool_time = time.time() - start_time
            logger.info(f"✅ Tool invocation completed in {tool_time:.2f}s, result type: {type(result)}")
            logger.info(f"📋 Tool result preview: {result[:500] if result else 'None'}")
            debug_info["stages"]["tpr_calculation"] = {"success": True, "time": tool_time}
            
            # Format the tool results
            from .formatters import MessageFormatter
            formatter = MessageFormatter(self.session_id)
            message = formatter.format_tool_tpr_results(result)
            
            # Check for various output files
            tpr_results_path = os.path.join(self.session_folder, 'tpr_results.csv')
            raw_data_path = find_raw_data_file(self.session_folder)
            shapefile_path = os.path.join(self.session_folder, 'raw_shapefile.zip')
            map_path = os.path.join(self.session_folder, 'tpr_distribution_map.html')

            files_status = {
                "tpr_results.csv": os.path.exists(tpr_results_path),
                "raw_data": raw_data_path is not None,
                "raw_shapefile.zip": os.path.exists(shapefile_path),
                "tpr_distribution_map.html": os.path.exists(map_path)
            }
            
            logger.info(f"📂 Files created status: {json.dumps(files_status, indent=2)}")
            debug_info["stages"]["file_creation"] = files_status
            
            # Check if map was created and add to visualizations
            visualizations = []
            logger.info(f"🔍 CRITICAL: Checking for TPR map at: {map_path}")
            logger.info(f"🔍 CRITICAL: map_path exists? {os.path.exists(map_path)}")

            if os.path.exists(map_path):
                logger.info(f"✅ TPR map found at {map_path}, adding to visualizations")
                file_size = os.path.getsize(map_path)
                logger.info(f"📊 Map file size: {file_size} bytes")
                debug_info["stages"]["map_creation"] = {"success": True, "size": file_size}

                # Create visualization object like production does
                visualization = {
                    'type': 'iframe',
                    'url': f'/serve_viz_file/{self.session_id}/tpr_distribution_map.html',
                    'title': f'TPR Distribution - {self.tpr_selections.get("state", "State")}',
                    'height': 600
                }
                visualizations.append(visualization)
                logger.info(f"🔍 CRITICAL: Added visualization to list. visualizations={visualizations}")

                # Remove iframe HTML from message if it exists
                if '<iframe' in message:
                    # Extract message before iframe tag
                    message = message.split('<iframe')[0].strip()
                    # Add note about map
                    if '📍' not in message:
                        message += "\n\n📍 TPR Map Visualization created (shown above)"
            else:
                logger.warning(f"❌ TPR map NOT found at {map_path}")
                logger.warning(f"❌ Session folder contents: {os.listdir(self.session_folder) if os.path.exists(self.session_folder) else 'folder does not exist'}")
                debug_info["stages"]["map_creation"] = {"success": False, "error": "File not created"}

            logger.info(f"🔍 CRITICAL: Final visualizations array length: {len(visualizations)}")
            
            # ✅ AUTO-TRANSITION: Instead of waiting for confirmation, transition immediately!
            # Users expect to immediately request risk analysis after TPR completes
            logger.info("✅ TPR complete - auto-transitioning to standard workflow for instant access")

            # Save TPR completion flag
            self.state_manager.update_state({'tpr_completed': True})

            # Start background pre-computation of all TPR combinations via queue
            user_combination = {
                'facility_level': self.tpr_selections.get('facility_level', 'all'),
                'age_group': self.tpr_selections.get('age_group', 'all_ages')
            }
            schedule_info = schedule_precompute(
                session_id=self.session_id,
                state=self.tpr_selections.get('state', ''),
                data_path=data_path,
                exclude_combination=user_combination
            )
            debug_info.setdefault('stages', {})['precompute'] = schedule_info

            # Calculate total time
            total_time = time.time() - start_time
            debug_info["total_time"] = total_time
            logger.info(f"⏱️ Total TPR calculation time: {total_time:.2f}s")

            # Multi-instance: sync session outputs to other instances so risk analysis can run anywhere
            try:
                from app.core.instance_sync import sync_session_after_upload
                sync_session_after_upload(self.session_id)
                logger.info(f"🔄 Synced TPR outputs for session {self.session_id} to all instances")
            except Exception as e:
                logger.warning(f"Could not sync session after TPR: {e}")

            # ✅ AUTO-TRANSITION: Trigger risk analysis transition immediately!
            logger.info("🚀 Calling trigger_risk_analysis() to transition to standard workflow")
            transition_result = self.trigger_risk_analysis()

            # Prepare final response based on transition status
            response: Dict[str, Any]
            if transition_result.get('success'):
                logger.info("✅ Auto-transition successful - combining TPR results with menu")
                logger.info(f"🔍 CRITICAL: visualizations before creating response: {visualizations}")
                logger.info(f"🔍 CRITICAL: visualizations length: {len(visualizations)}")

                # Combine TPR results message with transition menu
                combined_message = message + "\n\n" + transition_result['message']

                # Update the response to include transition flag and combined message
                response = {
                    "success": True,
                    "message": combined_message,
                    "session_id": self.session_id,
                    "workflow": "data_upload",  # Changed from "tpr" to "data_upload"
                    "stage": "complete",
                    "visualizations": visualizations,
                    "exit_data_analysis_mode": True,  # Signal frontend to exit Data Analysis mode
                    "debug": {
                        "selections": self.tpr_selections,
                        "files_created": files_status,
                        "total_time": total_time,
                        "has_map": len(visualizations) > 0,
                        "auto_transitioned": True,
                        "debug_file": "tpr_debug.json",
                        "visualization_count": len(visualizations)
                    }
                }

                logger.info(f"🔍 CRITICAL: response['visualizations'] = {response.get('visualizations')}")
                logger.info(f"🔍 CRITICAL: Complete response keys: {list(response.keys())}")
            else:
                logger.error(f"❌ Auto-transition failed: {transition_result.get('message')}")
                logger.warning("⚠️ Falling back to TPR results without transition")

                # Return TPR results without transition
                # CRITICAL FIX: Even if transition fails, we MUST exit Data Analysis mode
                response = {
                    "success": True,
                    "message": message,
                    "session_id": self.session_id,
                    "workflow": "tpr",
                    "stage": "complete",
                    "visualizations": visualizations,
                    "exit_data_analysis_mode": True,  # ✅ CRITICAL: Exit even if transition fails
                    "debug": {
                        "selections": self.tpr_selections,
                        "files_created": files_status,
                        "total_time": total_time,
                        "has_map": len(visualizations) > 0,
                        "auto_transition_failed": True,
                        "debug_file": "tpr_debug.json"
                    }
                }

            # Generate export documents for TPR results
            download_links: List[Dict[str, Any]] = []
            try:
                logger.info(f"Generating TPR export documents for session {self.session_id}")
                
                # Check if CSV and shapefile exist
                if os.path.exists(raw_data_path):
                    download_links.append({
                        'url': f'/export/download/{self.session_id}/raw_data.csv',
                        'filename': 'raw_data.csv',
                        'description': '📊 TPR Analysis Results (CSV)',
                        'type': 'csv'
                    })
                    logger.info(f"✅ Added TPR CSV download link")
                
                if os.path.exists(shapefile_path):
                    download_links.append({
                        'url': f'/export/download/{self.session_id}/raw_shapefile.zip',
                        'filename': 'raw_shapefile.zip',
                        'description': '🗺️ Ward Boundaries Shapefile (ZIP)',
                        'type': 'zip'
                    })
                    logger.info(f"✅ Added shapefile download link")
                
                # Generate comprehensive HTML dashboard
                try:
                    from pathlib import Path
                    import pandas as pd
                    
                    # Read TPR results
                    if os.path.exists(tpr_results_path):
                        tpr_df = pd.read_csv(tpr_results_path)
                    elif raw_data_path:
                        tpr_df = read_raw_data(self.session_folder)
                    else:
                        tpr_df = None
                    
                    if tpr_df is not None:
                        # Create HTML dashboard
                        dashboard_html = f"""
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <title>TPR Analysis Dashboard - {self.tpr_selections.get('state', 'State')}</title>
                            <style>
                                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                                h1 {{ color: #2c3e50; }}
                                .summary {{ background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }}
                                .metrics {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; }}
                                .metric {{ background: white; padding: 15px; border: 1px solid #dee2e6; border-radius: 5px; }}
                                .metric-value {{ font-size: 24px; font-weight: bold; color: #007bff; }}
                                table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
                                th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #dee2e6; }}
                                th {{ background: #f8f9fa; font-weight: bold; }}
                            </style>
                        </head>
                        <body>
                            <h1>TPR Analysis Dashboard</h1>
                            <div class="summary">
                                <h2>Analysis Summary</h2>
                                <div class="metrics">
                                    <div class="metric">
                                        <div>State</div>
                                        <div class="metric-value">{self.tpr_selections.get('state', 'N/A')}</div>
                                    </div>
                                    <div class="metric">
                                        <div>Facility Level</div>
                                        <div class="metric-value">{self.tpr_selections.get('facility_level', 'N/A')}</div>
                                    </div>
                                    <div class="metric">
                                        <div>Age Group</div>
                                        <div class="metric-value">{self.tpr_selections.get('age_group', 'N/A')}</div>
                                    </div>
                                    <div class="metric">
                                        <div>Total Wards</div>
                                        <div class="metric-value">{len(tpr_df) if 'WardName' in tpr_df.columns else 'N/A'}</div>
                                    </div>
                                    <div class="metric">
                                        <div>Average TPR</div>
                                        <div class="metric-value">{tpr_df['TPR'].mean():.2f}%</div>
                                    </div>
                                    <div class="metric">
                                        <div>Max TPR</div>
                                        <div class="metric-value">{tpr_df['TPR'].max():.2f}%</div>
                                    </div>
                                </div>
                            </div>
                            <h2>Top 10 Wards by TPR</h2>
                            <table>
                                <thead>
                                    <tr>
                                        <th>Ward Name</th>
                                        <th>TPR (%)</th>
                                        <th>Tests Positive</th>
                                        <th>Tests Examined</th>
                                    </tr>
                                </thead>
                                <tbody>
                        """
                        
                        # Add top 10 wards
                        top_wards = tpr_df.nlargest(10, 'TPR') if 'TPR' in tpr_df.columns else tpr_df.head(10)
                        for _, row in top_wards.iterrows():
                            dashboard_html += f"""
                                    <tr>
                                        <td>{row.get('WardName', 'N/A')}</td>
                                        <td>{row.get('TPR', 0):.2f}</td>
                                        <td>{row.get('Tests_Positive', 0):.0f}</td>
                                        <td>{row.get('Tests_Examined', 0):.0f}</td>
                                    </tr>
                            """
                        
                        dashboard_html += """
                                </tbody>
                            </table>
                        </body>
                        </html>
                        """
                        
                        # Save dashboard
                        dashboard_path = Path(self.session_folder) / 'tpr_dashboard.html'
                        dashboard_path.write_text(dashboard_html)
                        
                        download_links.append({
                            'url': f'/export/download/{self.session_id}/tpr_dashboard.html',
                            'filename': 'tpr_dashboard.html',
                            'description': '📈 Interactive TPR Dashboard (HTML)',
                            'type': 'html'
                        })
                        logger.info(f"✅ Generated TPR dashboard at {dashboard_path}")
                    
                except Exception as e:
                    logger.warning(f"Could not generate TPR dashboard: {e}")
                
                if download_links:
                    logger.info(f"✅ Generated {len(download_links)} export documents for TPR analysis")
                    
            except Exception as e:
                logger.error(f"Error generating TPR export documents: {e}")
                # Continue without exports - don't fail the main operation
            
            if download_links:
                response['download_links'] = download_links

            # Save debug info to file
            debug_file = os.path.join(self.session_folder, 'tpr_debug.json')
            with open(debug_file, 'w') as f:
                json.dump(debug_info, f, indent=2)
            logger.info(f"💾 Debug info saved to {debug_file}")

            logger.info("📤 Returning TPR response (success path)")
            return response

        except Exception as e:
            error_trace = traceback.format_exc()
            logger.error(f"❌ Error calculating TPR: {e}\n{error_trace}")

            debug_info["stages"]["error"] = {
                "message": str(e),
                "trace": error_trace
            }
            
            # Save error debug info
            debug_file = os.path.join(self.session_folder, 'tpr_error_debug.json')
            with open(debug_file, 'w') as f:
                json.dump(debug_info, f, indent=2)
            
            message = f"Error calculating TPR: {str(e)}"
            visualizations = []

            # Add debug info to response for browser console visibility
            # CRITICAL FIX: Even if there's an error, we MUST exit Data Analysis mode
            response = {
                "success": True,
                "message": message,
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "complete",
                "visualizations": visualizations,
                "exit_data_analysis_mode": True,  # ✅ CRITICAL: Exit even on error
                "debug": {
                    "selections": self.tpr_selections,
                    "files_created": files_status if 'files_status' in locals() else {},
                    "total_time": total_time if 'total_time' in locals() else 0,
                    "has_map": len(visualizations) > 0,
                    "debug_file": "tpr_debug.json",
                    "error_occurred": True
                }
            }

            logger.info(f"📤 Returning response with debug info: {json.dumps(response['debug'], indent=2)}")
            return response
    
    def trigger_risk_analysis(self) -> Dict[str, Any]:
        """
        Transition from TPR workflow to standard data upload workflow.
        This mimics the behavior of uploading data through the standard Upload tab.
        """
        logger.warning("[TPR SYNC] trigger_risk_analysis start session=%s", self.session_id)

        # Check if files are ready
        import os
        import pandas as pd
        session_folder = f"instance/uploads/{self.session_id}"
        raw_data_path = find_raw_data_file(session_folder)

        if not raw_data_path:
            return {
                "success": False,
                "message": "Error: TPR data file not found. Please re-run TPR calculation.",
                "session_id": self.session_id
            }

        try:
            self._ensure_active_session_has_outputs()

            # Load the data that TPR created
            df = read_raw_data(session_folder)
            logger.info(f"Loaded TPR output data: {len(df)} rows, {len(df.columns)} columns")
            
            # Concise focused menu - flows as ONE message with TPR completion
            message = "\n\nYou can now:\n"
            message += "- **Map variable distribution** - e.g., \"map rainfall distribution\"\n"
            message += "- **Run malaria risk analysis** - Rank wards for ITN distribution\n"
            message += "- **Ask me anything** about your data"
            
            # Reset conversation stage to INITIAL (like a fresh upload)
            self.current_stage = ConversationStage.INITIAL
            self.state_manager.update_workflow_stage(self.current_stage)
            
            # Mark TPR workflow as complete and ensure data_loaded is set
            self.state_manager.mark_tpr_workflow_complete()
            logger.info("🔴 User accepted risk analysis - marking TPR workflow as COMPLETE")
            
            # Check if analysis was completed before transition
            from pathlib import Path
            analysis_marker = Path(f"instance/uploads/{self.session_id}/.analysis_complete")
            analysis_was_complete = analysis_marker.exists()
            
            self.state_manager.update_state({
                'tpr_completed': True,
                'data_loaded': True,  # CRITICAL: Set this for main workflow to recognize data
                'csv_loaded': True,   # Also set this for compatibility
                'workflow_transitioned': True,
                'analysis_complete': analysis_was_complete  # PRESERVE analysis state!
            })

            if analysis_was_complete:
                logger.info(f"📌 Preserving analysis_complete=True during transition for {self.session_id}")

            # Use WorkflowStateManager for proper transition
            from app.core.workflow_state_manager import WorkflowStateManager, WorkflowSource, WorkflowStage
            workflow_manager = WorkflowStateManager(self.session_id)

            # Perform workflow transition with proper cleanup
            # CRITICAL FIX: Don't delete .analysis_complete marker - it's evidence!
            success = workflow_manager.transition_workflow(
                from_source=WorkflowSource.DATA_ANALYSIS_V3,
                to_source=WorkflowSource.STANDARD,
                new_stage=WorkflowStage.DATA_PREPARED,
                clear_markers=['.data_analysis_mode']  # Only clear V3-specific markers
            )

            if not success:
                logger.error(f"Failed to transition workflow for session {self.session_id}")

            # CRITICAL: Replicate EXACT session setup from upload_routes.py:371-396
            # This makes the system treat TPR transition EXACTLY like a standard upload
            # NOTE: This may fail if called outside Flask request context (e.g., from LangGraph)
            try:
                from flask import session
                session['upload_type'] = 'csv_shapefile'  # Same as standard upload
                session['raw_data_stored'] = True
                session['should_ask_analysis_permission'] = False
                session['csv_loaded'] = True
                session['shapefile_loaded'] = True
                session['data_loaded'] = True
                session['tpr_transition_complete'] = True
                session['previous_workflow'] = 'tpr'
                session.permanent = True
                session.modified = True
                logger.info(f"✅ Session flags set for {self.session_id}: csv_loaded=True, data_loaded=True")
            except RuntimeError as e:
                # Working outside of request context - this is OK, state is managed by state_manager
                logger.warning(f"⚠️ Could not set Flask session (no request context): {e}")
                logger.info("Session state is managed by DataAnalysisStateManager instead")

            # CRITICAL: Call same cross-instance sync as standard upload (upload_routes.py:392-396)
            try:
                from app.core.instance_sync import sync_session_after_upload
                sync_session_after_upload(self.session_id)
                logger.info(f"✅ Cross-instance sync completed for {self.session_id}")
            except Exception as e:
                logger.debug(f"Instance sync not performed: {e}")

            logger.info("Successfully transitioned to standard upload workflow")

            return {
                "success": True,
                "message": message,
                "session_id": self.session_id,
                "workflow": "data_upload",
                "stage": "complete",
                "transition": "tpr_to_upload",
                "exit_data_analysis_mode": True  # Signal frontend to exit Data Analysis mode
                # Removed redirect_message to give users autonomy to choose what they want to do
            }
                
        except Exception as e:
            logger.error(f"Error transitioning to upload workflow: {e}")
            return {
                "success": False,
                "message": f"Error transitioning to data analysis: {str(e)}",
                "session_id": self.session_id
            }

    def _ensure_active_session_has_outputs(self) -> None:
        """Copy freshly generated TPR assets into the scoped session folder."""
        try:
            from flask import current_app, session

            scoped_session_id = session.get('session_id')
            logger.warning(
                "[TPR SYNC] base_session=%s scoped_session=%s conversation_id=%s",
                self.session_id,
                scoped_session_id,
                getattr(session, 'conversation_id', None),
            )
            if not scoped_session_id or scoped_session_id == self.session_id:
                return

            upload_root = current_app.config.get('UPLOAD_FOLDER', 'instance/uploads')
            source_folder = os.path.join(upload_root, self.session_id)
            dest_folder = os.path.join(upload_root, scoped_session_id)
            logger.warning(
                "[TPR SYNC] source_folder=%s dest_folder=%s exists_source=%s",
                source_folder,
                dest_folder,
                os.path.isdir(source_folder),
            )

            if not os.path.isdir(source_folder):
                logger.warning(
                    "Source folder %s missing while syncing TPR outputs for session %s",
                    source_folder,
                    self.session_id,
                )
                return

            os.makedirs(dest_folder, exist_ok=True)

            critical_files = [
                'raw_data.csv',
                'raw_shapefile.zip',
                'tpr_results.csv',
                'tpr_distribution_map.html',
                '.agent_state.json',
                '.risk_ready',
                '.analysis_complete',
                '.tpr_waiting_confirmation',
                'tpr_debug.json',
            ]

            for filename in critical_files:
                source_path = os.path.join(source_folder, filename)
                if os.path.exists(source_path):
                    dest_path = os.path.join(dest_folder, filename)
                    try:
                        shutil.copy2(source_path, dest_path)
                        logger.warning(
                            "[TPR SYNC] copied=%s destination=%s scoped=%s",
                            filename,
                            dest_path,
                            scoped_session_id,
                        )
                    except Exception as copy_error:
                        logger.error(
                            "Failed to copy %s to scoped session %s: %s",
                            filename,
                            scoped_session_id,
                            copy_error,
                        )

            source_viz = os.path.join(source_folder, 'visualizations')
            if os.path.isdir(source_viz):
                dest_viz = os.path.join(dest_folder, 'visualizations')
                try:
                    shutil.copytree(source_viz, dest_viz, dirs_exist_ok=True)
                    logger.warning(
                        "[TPR SYNC] copied visualizations to scoped=%s",
                        scoped_session_id,
                    )
                except Exception as viz_error:
                    logger.error(
                        "Failed to sync visualizations to scoped session %s: %s",
                        scoped_session_id,
                        viz_error,
                    )

        except Exception as sync_error:
            logger.error(
                "Unexpected error while syncing TPR outputs for scoped session: %s",
                sync_error,
            )

    def _determine_user_expertise(self) -> str:
        """
        Determine user expertise level for progressive disclosure.

        Returns:
            'novice', 'intermediate', or 'expert'
        """
        try:
            # Check if user has run TPR before (would have files in session folder)
            session_files = os.listdir(self.session_folder)
            has_previous_tpr = any('tpr_results' in f for f in session_files)

            if has_previous_tpr:
                return 'intermediate'

            # Check if this is first time using ChatMRPT (no previous analyses)
            analysis_files = [f for f in session_files if 'analysis' in f or 'results' in f]
            if len(analysis_files) > 2:
                return 'intermediate'

            return 'novice'
        except Exception:
            # Default to novice for safety
            return 'novice'

    def extract_state_from_query(self, query: str) -> Optional[str]:
        """Extract state name from user query."""
        query_lower = query.lower().strip()
        
        # Check for number selection
        if query_lower in ['1', '1.', 'first']:
            # Would need to look up from analysis
            return None
        
        # Look for state names (simplified)
        return query.strip()
    
    def extract_facility_level(self, query: str) -> Optional[str]:
        """Extract facility level using STRICT keyword matching.

        Returns None if no exact keyword match is found.
        """
        query_clean = query.lower().strip()

        # Strict keyword mapping - only exact matches
        keyword_map = {
            'primary': 'primary',
            '1': 'primary',
            'secondary': 'secondary',
            '2': 'secondary',
            'tertiary': 'tertiary',
            '3': 'tertiary',
            'all': 'all',
            '4': 'all'
        }

        # Return exact match or None
        return keyword_map.get(query_clean, None)
    
    def extract_age_group(self, query: str) -> Optional[str]:
        """Extract age group using STRICT keyword matching.

        Returns None if no exact keyword match is found.
        """
        query_clean = query.lower().strip()

        # Strict keyword mapping - only exact matches
        keyword_map = {
            'u5': 'u5',
            'under5': 'u5',
            '1': 'u5',
            'o5': 'o5',
            'over5': 'o5',
            '2': 'o5',
            'pw': 'pw',
            'pregnant': 'pw',
            '3': 'pw',
            'all': 'all_ages',
            '4': 'all_ages'
        }

        # Return exact match or None
        return keyword_map.get(query_clean, None)

    def get_valid_keywords_for_stage(self, stage: ConversationStage) -> str:
        """Get valid keywords for the current workflow stage."""
        if stage == ConversationStage.TPR_STATE_SELECTION:
            return "State names from your data"
        elif stage == ConversationStage.TPR_FACILITY_LEVEL:
            return "'primary', 'secondary', 'tertiary', 'all' (or 1-4)"
        elif stage == ConversationStage.TPR_AGE_GROUP:
            return "'u5', 'o5', 'pw', 'all' (or 1-4)"
        else:
            return "No keywords needed at this stage"

    def get_pending_visualizations(self) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve pending visualizations stored for on-demand display.

        Returns:
            List of visualization dictionaries or None if no visualizations pending
        """
        try:
            pending = self.state_manager.get_field('pending_visualizations')
            logger.info(f"Retrieved pending_visualizations from state: {pending is not None}")

            if pending:
                logger.info(f"Pending viz keys: {pending.keys() if isinstance(pending, dict) else 'not-a-dict'}")

                # Get visualizations for the current stage
                current_stage_key = None
                if self.current_stage == ConversationStage.TPR_FACILITY_LEVEL:
                    current_stage_key = 'facility_level'
                elif self.current_stage == ConversationStage.TPR_AGE_GROUP:
                    current_stage_key = 'age_group'

                logger.info(f"Current stage: {self.current_stage}, looking for key: {current_stage_key}")

                if current_stage_key and current_stage_key in pending:
                    viz_list = pending[current_stage_key]
                    logger.info(f"Retrieved {len(viz_list)} pending visualizations for {current_stage_key}")
                    return viz_list
                else:
                    logger.warning(f"No visualizations found for stage key: {current_stage_key}")
            else:
                logger.warning("No pending_visualizations in state")
            return None
        except Exception as e:
            logger.error(f"Error retrieving pending visualizations: {e}")
            return None
