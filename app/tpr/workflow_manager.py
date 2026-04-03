"""
TPR Workflow Handler

Handles all TPR (Test Positivity Rate) workflow logic including state selection,
facility selection, age group selection, and calculation triggering.
"""

import os
import logging
import uuid
from typing import Dict, Any, Optional, List
from pathlib import Path
from enum import Enum


def reset_tpr_handler_cache(session_id: Optional[str] = None) -> None:
    """Clear any cached TPR workflow state for a session.

    Previously lived in app.runtime.tpr.workflow. Kept here so that
    chat_stream can import it from a single canonical location.
    TPRWorkflowHandler instances are created per-request and not cached,
    so this is effectively a no-op — it exists to avoid import errors.
    """
    logger = logging.getLogger(__name__)
    logger.debug("reset_tpr_handler_cache called for session %s (no-op)", session_id)

from app.agent.state_manager import DataAnalysisStateManager, ConversationStage
from .data_analyzer import TPRDataAnalyzer
from app.tpr.language import TPRLanguageInterface
try:
    from app.features.tpr.workflow.navigation import handle_navigation_command
except ImportError:  # pragma: no cover - legacy environments without features package
    def handle_navigation_command(*_args, **_kwargs):
        """Fallback navigation handler when feature package is unavailable."""
        return None
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
        self.tpr_selections = {}
        self.current_stage = ConversationStage.INITIAL
        self.uploaded_data = None
        self.session_folder = f"instance/uploads/{session_id}"
        self.language = TPRLanguageInterface(session_id)
        self.state_analysis = {}

    @staticmethod
    def _describe_selections(facility_level: str, age_group: str) -> str:
        facility_map = {
            'primary': 'primary facilities',
            'secondary': 'secondary facilities',
            'tertiary': 'tertiary facilities',
            'all': 'all facility levels',
        }
        age_map = {
            'u5': 'children under 5 years',
            'o5': 'people 5 years and older (excluding pregnant women)',
            'pw': 'pregnant women',
            'all_ages': 'all age groups',
        }
        # Population denominator description
        pop_map = {
            'u5': 'under-5 population',
            'o5': 'over-5 population',
            'pw': 'women 15-49 population',
            'all_ages': 'total population',
        }

        facility_text = facility_map.get(facility_level, 'all facility levels')
        age_text = age_map.get(age_group, 'all age groups')
        pop_text = pop_map.get(age_group, 'total population')

        return (
            f"Using {facility_text} and focusing on {age_text}, I calculated malaria burden "
            f"as cases per 1,000 {pop_text} using WorldPop estimates."
        )
    
    def set_data(self, data):
        """Set the uploaded data for analysis."""
        self.uploaded_data = data
        try:
            self.language.update_from_dataframe(data)
        except Exception:
            pass

    def load_data(self):
        """Load the uploaded data for TPR calculation."""
        return self.uploaded_data

    def set_stage(self, stage: ConversationStage):
        """Update the current workflow stage."""
        self.current_stage = stage

    def load_state_from_manager(self):
        """Restore handler state from state manager. Used when handler is recreated."""
        self.current_stage = self.state_manager.get_workflow_stage()
        self.tpr_selections = self.state_manager.get_tpr_selections() or {}
        logger.info(f"Loaded state: stage={self.current_stage}, selections={self.tpr_selections}")
        try:
            stored = self.state_manager.get_state()
            state_analysis = (stored or {}).get('state_analysis')
            if state_analysis and isinstance(state_analysis, dict):
                states = list((state_analysis.get('states') or {}).keys())
                if states:
                    self.state_analysis = state_analysis
                    self.language.update_available_states(states)
        except Exception:
            pass

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
            html = fig.to_html(include_plotlyjs='cdn', config=config)
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
            order = [('u5', 'Under 5'), ('o5', 'Over 5'), ('pw', 'Pregnant')]  # Fixed to use correct keys
            rows = []
            test_rows = []
            for key, label in order:
                if key in age_groups and age_groups[key].get('has_data'):
                    g = age_groups[key]
                    rows.append({
                        'Age Group': label,
                        'Positivity (%)': float(g.get('positivity_rate', 0.0))
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
            fig2 = px.bar(df, x='Age Group', y='Positivity (%)',
                         title='Test positivity rate by age group',
                         color_discrete_sequence=['#636EFA'])  # Uniform blue

            fig2.update_traces(
                text=df['Positivity (%)'].apply(lambda x: f'{x:.1f}%'),
                textposition='outside',
                textfont=dict(size=14),
                hovertemplate='<b>%{x}</b><br>Positivity: %{y:.1f}%<extra></extra>'
            )

            fig2.update_layout(
                showlegend=False,
                autosize=True,  # Let it fit naturally
                yaxis=dict(
                    title='Positivity (%)',
                    range=[0, max(df['Positivity (%)'].max() * 1.3, 40)],  # More headroom
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
    
    def handle_navigation(self, nav_type: str) -> Dict[str, Any]:
        """
        Handle navigation commands during TPR workflow.

        Args:
            nav_type: Type of navigation (back, status, exit, etc.)

        Returns:
            Response dictionary
        """
        logger.info(f"Handling navigation command: {nav_type}")

        response = handle_navigation_command(self, nav_type)
        if response is not None:
            return response

        return {
            "success": True,
            "message": f"Navigation command '{nav_type}' recognized. Please continue with your selection.",
            "session_id": self.session_id,
        }

    def handle_visualization_request(self, request_type: str) -> Dict[str, Any]:
        """
        Handle visualization requests via key phrases like 'show facility charts' or 'show age charts'.

        Args:
            request_type: Type of visualization requested ('facility' or 'age')

        Returns:
            Response dictionary with visualizations if available
        """
        logger.info(f"📊 Handling visualization request: {request_type}")

        current_state = self.state_manager.get_state() or {}
        pending_viz = current_state.get('pending_visualizations', {})

        if not pending_viz:
            return {
                "success": True,
                "message": "No visualizations are currently available. Please make a selection first.",
                "session_id": self.session_id
            }

        # Check if requested visualization type matches current stage
        if request_type == 'facility' and 'facility_level' in pending_viz:
            viz_list = pending_viz['facility_level']
            return {
                "success": True,
                "message": "Here are the facility-level visualizations to help inform your decision:",
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "facility_selection",
                "visualizations": viz_list
            }
        elif request_type == 'age' and 'age_group' in pending_viz:
            viz_list = pending_viz['age_group']
            return {
                "success": True,
                "message": "Here are the age group visualizations to help inform your decision:",
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "age_selection",
                "visualizations": viz_list
            }
        else:
            return {
                "success": True,
                "message": f"No {request_type} visualizations are available at this stage.",
                "session_id": self.session_id
            }

    def execute_command(self, command: str, stage: ConversationStage) -> Dict[str, Any]:
        """
        Execute a workflow command.

        Entry point for 2-route architecture. Commands are already extracted and validated.

        Args:
            command: The command to execute (e.g., 'primary', 'u5', 'back')
            stage: Current workflow stage

        Returns:
            Response dictionary
        """
        logger.info(f"🔧 Executing command: '{command}' at stage: {stage}")

        # Navigation commands
        if command in ['back', 'status', 'exit']:
            return self.handle_navigation(command)

        # Check if we're awaiting confirmation
        # LLM extraction already handled natural language → command mapping
        # So any non-navigation command during confirmation = user confirming
        current_state = self.state_manager.get_state() or {}
        if current_state.get('tpr_awaiting_confirmation'):
            logger.info(f"🔧 User confirmed workflow start (natural language supported)")
            return self.execute_confirmation()

        # Stage-specific commands
        if stage == ConversationStage.TPR_STATE_SELECTION:
            return self.execute_state_selection(command)
        elif stage == ConversationStage.TPR_FACILITY_LEVEL:
            return self.execute_facility_selection(command)
        elif stage == ConversationStage.TPR_AGE_GROUP:
            return self.execute_age_selection(command)
        else:
            logger.warning(f"⚠️ Unknown stage for command execution: {stage}")
            return {
                "success": True,
                "message": f"Command '{command}' received but I'm not sure how to process it at this stage.",
                "session_id": self.session_id
            }

    def execute_confirmation(self) -> Dict[str, Any]:
        """Execute workflow confirmation - auto-select state if only one exists."""
        logger.info(f"🔧 Executing workflow confirmation")

        # Clear confirmation flag
        self.state_manager.update_state({'tpr_awaiting_confirmation': False})

        # Get state analysis
        if not self.state_analysis:
            self.state_analysis = self.tpr_analyzer.analyze_states(self.uploaded_data)
            # Persist inferred schema so tpr_analysis_tool can reuse it without a second LLM call
            if self.tpr_analyzer._schema:
                self.state_manager.update_state({'column_schema': self.tpr_analyzer._schema})

        states = list(self.state_analysis.get('states', {}).keys())

        if len(states) == 1:
            # Auto-select single state
            selected_state = states[0]
            logger.info(f"🔧 Auto-selecting single state: {selected_state}")

            self.tpr_selections['state'] = selected_state
            self.state_manager.save_tpr_selection('state', selected_state)

            # Move to facility selection
            self.current_stage = ConversationStage.TPR_FACILITY_LEVEL
            self.state_manager.update_workflow_stage(self.current_stage)

            # Analyze facility levels
            facility_analysis = self.tpr_analyzer.analyze_facility_levels(self.uploaded_data, selected_state)

            # Format response
            from app.agent.formatters import MessageFormatter
            formatter = MessageFormatter(self.session_id)
            message = f"**State:** {selected_state} (auto-selected)\n\n" + formatter.format_facility_selection(selected_state, facility_analysis)

            # Store visualizations for on-demand access via key phrases
            facility_viz = self._build_facility_level_visualizations(facility_analysis)
            if facility_viz:
                self.state_manager.update_state({
                    'pending_visualizations': {
                        'facility_level': facility_viz,
                        'stage': 'facility_selection'
                    }
                })

            return {
                "success": True,
                "message": message,
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "facility_selection",
                "visualizations": None
            }
        else:
            # Multiple states - ask user to select
            from app.agent.formatters import MessageFormatter
            formatter = MessageFormatter(self.session_id)
            message = formatter.format_state_selection(self.state_analysis)

            return {
                "success": True,
                "message": message,
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "state_selection",
                "visualizations": None
            }

    def execute_state_selection(self, state: str) -> Dict[str, Any]:
        """Execute state selection command."""
        logger.info(f"🔧 Executing state selection: {state}")

        # Validate state exists in data
        states: List[str] = []
        if self.state_analysis:
            states = list((self.state_analysis.get('states') or {}).keys())
        if not states:
            try:
                self.state_analysis = self.tpr_analyzer.analyze_states(self.uploaded_data)
                states = list((self.state_analysis.get('states') or {}).keys())
                if self.tpr_analyzer._schema:
                    self.state_manager.update_state({'column_schema': self.tpr_analyzer._schema})
            except Exception:
                states = []

        # Check if state is valid
        if state not in states:
            return {
                "success": True,
                "message": f"I couldn't find '{state}' in your data. Available states: {', '.join(states)}",
                "session_id": self.session_id
            }

        # Save selection
        self.tpr_selections['state'] = state
        self.state_manager.save_tpr_selection('state', state)

        # Move to next stage
        self.current_stage = ConversationStage.TPR_FACILITY_LEVEL
        self.state_manager.update_workflow_stage(self.current_stage)

        # Analyze facility levels
        facility_analysis = self.tpr_analyzer.analyze_facility_levels(self.uploaded_data, state)

        # Format response
        acknowledgment = f"Great choice! You've selected **{state}**. "
        if facility_analysis.get('total_facilities'):
            total = facility_analysis['total_facilities']
            acknowledgment += f"I can see {total:,} health facilities in this state.\n\n"

        from app.agent.formatters import MessageFormatter
        formatter = MessageFormatter(self.session_id)
        message = acknowledgment + formatter.format_facility_selection(state, facility_analysis)

        # Store visualizations for on-demand access via key phrases
        facility_viz = self._build_facility_level_visualizations(facility_analysis)
        if facility_viz:
            self.state_manager.update_state({
                'pending_visualizations': {
                    'facility_level': facility_viz,
                    'stage': 'facility_selection'
                }
            })

        return {
            "success": True,
            "message": message,
            "session_id": self.session_id,
            "workflow": "tpr",
            "stage": "facility_selection",
            "visualizations": None
        }

    def execute_facility_selection(self, facility_level: str) -> Dict[str, Any]:
        """Execute facility level selection command."""
        logger.info(f"🔧 Executing facility selection: {facility_level}")

        # Save selection
        self.tpr_selections['facility_level'] = facility_level
        self.state_manager.save_tpr_selection('facility_level', facility_level)

        # Move to next stage
        self.current_stage = ConversationStage.TPR_AGE_GROUP
        self.state_manager.update_workflow_stage(self.current_stage)

        # Analyze age groups
        state_for_analysis = self.tpr_selections.get('state', '')
        age_analysis = self.tpr_analyzer.analyze_age_groups(
            self.uploaded_data,
            state_for_analysis,
            facility_level
        )

        # Format response
        level_display = facility_level.replace('_', ' ').title()
        acknowledgment = f"Perfect! You've selected **{level_display}** facilities. "
        if age_analysis.get('total_tests'):
            acknowledgment += f"These facilities conducted {age_analysis['total_tests']:,} tests.\n\n"

        from app.agent.formatters import MessageFormatter
        formatter = MessageFormatter(self.session_id)
        message = acknowledgment + formatter.format_age_group_selection(age_analysis)

        # Store visualizations for on-demand access via key phrases
        age_viz = self._build_age_group_visualizations(age_analysis)
        if age_viz:
            self.state_manager.update_state({
                'pending_visualizations': {
                    'age_group': age_viz,
                    'stage': 'age_selection'
                }
            })

        return {
            "success": True,
            "message": message,
            "session_id": self.session_id,
            "workflow": 'tpr',
            "stage": 'age_selection',
            "visualizations": None
        }

    def execute_age_selection(self, age_group: str) -> Dict[str, Any]:
        """Execute age group selection command."""
        logger.info(f"🔧 Executing age selection: {age_group}")

        # Save selection
        self.tpr_selections['age_group'] = age_group
        self.state_manager.save_tpr_selection('age_group', age_group)

        # Load data
        try:
            df = self.load_data()
        except Exception as exc:
            logger.error(f"Error loading data for TPR calculation: {exc}")
            return {
                "success": False,
                "message": "Error: Data not found",
                "session_id": self.session_id,
                "stage": "TPR_AGE_GROUP"
            }

        if df is None:
            return {
                "success": False,
                "message": "Error: Data not found",
                "session_id": self.session_id,
                "stage": "TPR_AGE_GROUP"
            }

        # Calculate TPR using the analyze_tpr_data tool (restored from backup)
        import json
        import os
        from app.tpr.analysis_tool import analyze_tpr_data

        facility_level = self.tpr_selections.get('facility_level', 'all')

        # Prepare options for the tool
        options = {
            'age_group': age_group,
            'facility_level': facility_level,
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
            df.to_csv(data_path, index=False)
            logger.info(f"✅ Saved data to {data_path} for TPR tool")

        try:
            # Call the tool
            logger.info(f"🎯 Calling TPR tool with options: {options}")
            result = analyze_tpr_data.invoke({
                'thought': f"Calculating TPR for {self.tpr_selections.get('state', 'state')} with user selections",
                'action': "calculate_tpr",
                'options': json.dumps(options),
                'graph_state': graph_state
            })

            logger.info(f"✅ TPR tool completed")

            # Format the tool results
            from app.agent.formatters import MessageFormatter
            formatter = MessageFormatter(self.session_id)
            message = formatter.format_tool_tpr_results(result)

            if message and message.startswith("## Malaria Burden Analysis Complete"):
                summary_line = self._describe_selections(facility_level, age_group)
                marker = "## Malaria Burden Analysis Complete\n\n"
                if message.startswith(marker):
                    message = message.replace(marker, f"{marker}{summary_line}\n\n", 1)

            # Mark workflow complete
            self.state_manager.mark_tpr_workflow_complete()
            self.state_manager.update_workflow_stage(ConversationStage.INITIAL)

            # Kick off background pre-computation of all other combinations
            try:
                from app.tpr.precompute_service import schedule_precompute
                schedule_precompute(
                    session_id=self.session_id,
                    state=self.tpr_selections.get('state', ''),
                    data_path=data_path,
                    exclude_combination={
                        'facility_level': facility_level,
                        'age_group': age_group,
                    },
                )
                logger.info("Scheduled TPR background pre-computation for session %s", self.session_id)
            except Exception as precompute_exc:
                logger.warning("Could not schedule TPR pre-computation: %s", precompute_exc)

            return {
                "success": True,
                "message": message,
                "session_id": self.session_id,
                "stage": "COMPLETE",
                "workflow": 'tpr'
            }

        except Exception as exc:
            logger.error(f"Error calculating TPR: {exc}", exc_info=True)
            return {
                "success": False,
                "message": f"Error calculating TPR: {str(exc)}",
                "session_id": self.session_id,
                "stage": "TPR_AGE_GROUP"
            }

    def start_workflow(self) -> Dict[str, Any]:
        """Start the TPR workflow with introduction and confirmation."""
        logger.info("Starting TPR workflow")

        # Analyze available states first (before marking active)
        state_analysis = self.tpr_analyzer.analyze_states(self.uploaded_data)
        if not state_analysis.get('state_column_detected'):
            logger.warning("TPR workflow aborted: no state column detected")
            return {
                "success": False,
                "error": "STATE_COLUMN_NOT_FOUND",
                "message": (
                    "I couldn't detect a column that lists Nigerian states in your dataset. "
                    "Please rename the column that contains state names (for example 'State' or 'StateName') "
                    "and try starting the TPR workflow again."
                ),
                "session_id": self.session_id,
                "workflow": "tpr",
            }

        self.state_analysis = state_analysis
        try:
            self.language.update_available_states(list(state_analysis.get('states', {}).keys()))
        except Exception:
            pass

        # Determine state info for introduction
        states = list(state_analysis['states'].keys())
        state_info = ""
        if len(states) == 1:
            state_info = f"**State:** {states[0]} (auto-detected)"
        else:
            state_info = f"**States detected:** {', '.join(states)}"

        # Create introduction message
        intro_message = f"""## Malaria Burden Analysis Workflow

Great! Let's analyze your malaria testing data to calculate ward-level Malaria Burden per 1,000 population.

**What This Workflow Does:**

Your uploaded data contains malaria testing records from health facilities. This workflow will:
- Calculate Malaria Burden for each ward: (Positive cases ÷ Ward Population) × 1,000
- Use WorldPop population data matched to your selected age group
- Help identify high-burden areas for targeted interventions

**Three Simple Steps:**

1. **State Selection** - Choose which state to analyze
2. **Facility Level** - Select Primary, Secondary, Tertiary, or All facilities
3. **Age Group** - Choose Under-5, Over-5, Pregnant Women, or All ages

{state_info}

**After Burden Calculation:**

Once we calculate ward-level malaria burden, the system will:
- Extract ward boundary shapefiles from our database for your state
- Add relevant environmental variables (rainfall, vegetation, water indices, etc.) specific to your geopolitical zone
- Create a unified dataset combining burden data with environmental factors
- Output: Ready-to-use dataset for comprehensive risk analysis and ward ranking for resource allocation

**Ready to begin?** Just type **'yes'** when you're ready, or ask me any questions about the workflow.
"""

        # Set stage to AWAITING_CONFIRMATION (we'll add this stage)
        # For now, just mark workflow as active and store intro state
        self.state_manager.mark_tpr_workflow_active()
        logger.info("TPR workflow marked as ACTIVE, awaiting user confirmation")

        # Store that we're awaiting confirmation
        self.state_manager.update_workflow_stage(ConversationStage.TPR_STATE_SELECTION)  # Temporary, will update on confirmation
        self.state_manager.update_state({
            'tpr_awaiting_confirmation': True,
            'state_analysis': state_analysis
        })

        return {
            "success": True,
            "message": intro_message,
            "session_id": self.session_id,
            "workflow": "tpr",
            "stage": "awaiting_confirmation"
        }
        
        # Multiple states - show state selection
        self.current_stage = ConversationStage.TPR_STATE_SELECTION
        self.state_manager.update_workflow_stage(self.current_stage)

        from app.agent.formatters import MessageFormatter
        formatter = MessageFormatter(self.session_id)

        # Create conversational workflow introduction
        intro_message = "## Welcome to Malaria Burden Analysis\n\n"
        intro_message += "I'll guide you through calculating Malaria Burden per 1,000 population to identify high-burden areas for intervention.\n\n"
        intro_message += "**The process has 3 simple steps:**\n"
        intro_message += "1. Select State (if multiple)\n"
        intro_message += "2. Choose Facility Level\n"
        intro_message += "3. Pick Age Group\n\n"
        intro_message += "At each step, I have data visualizations ready to help inform your decision.\n"
        intro_message += "Just ask if you'd like to see them!\n\n"
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
        states: List[str] = []
        if self.state_analysis:
            states = list((self.state_analysis.get('states') or {}).keys())
        if not states:
            try:
                self.state_analysis = self.tpr_analyzer.analyze_states(self.uploaded_data)
                states = list((self.state_analysis.get('states') or {}).keys())
                self.language.update_available_states(states)
            except Exception:
                states = []

        resolution = self.language.resolve_slot(
            slot_type='state',
            message=user_query,
            choices=states or ['']
        )

        if not resolution.value:
            options_text = ', '.join(states[:6]) if states else 'the states in your dataset'
            follow_up = "Could you rephrase which state you'd like to analyze?"
            if resolution.rationale:
                follow_up += f" ({resolution.rationale})"
            return {
                "success": True,
                "message": f"I want to be sure I understood you correctly. {follow_up} Options include: {options_text}.",
                "session_id": self.session_id
            }

        selected_state = resolution.value
        self.tpr_selections['state'] = selected_state
        self.state_manager.save_tpr_selection('state', selected_state)

        self.current_stage = ConversationStage.TPR_FACILITY_LEVEL
        self.state_manager.update_workflow_stage(self.current_stage)

        facility_analysis = self.tpr_analyzer.analyze_facility_levels(self.uploaded_data, selected_state)

        acknowledgment = f"Great choice! You've selected **{selected_state}**. "
        if facility_analysis.get('total_facilities'):
            total = facility_analysis['total_facilities']
            acknowledgment += f"I can see {total:,} health facilities in this state.\n\n"

        from app.agent.formatters import MessageFormatter
        formatter = MessageFormatter(self.session_id)
        message = acknowledgment + formatter.format_facility_selection(selected_state, facility_analysis)

        # Don't show rationale on success - it's annoying
        # if resolution.rationale:
        #     message += f"\n\n_I interpreted your selection as {selected_state}: {resolution.rationale}_"

        facility_viz = self._build_facility_level_visualizations(facility_analysis)

        if (facility_viz or []):
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

        if self._determine_user_expertise() == 'novice':
            message += "\n\n**Tip**: Secondary facilities usually provide the best balance of coverage and data quality."

        return {
            "success": True,
            "message": message,
            "session_id": self.session_id,
            "workflow": "tpr",
            "stage": "facility_selection",
            "visualizations": None
        }

    def _normalize_choice(self, value: Optional[str], allowed: List[str]) -> Optional[str]:
        if not value:
            return None
        value_clean = str(value).strip().lower()
        synonym_map = {
            'all ages': 'all',
            'all age': 'all',
            'allages': 'all',
            'all_age': 'all',
            'over5': 'o5',
            'under5': 'u5',
            'under 5': 'u5',
            'over 5': 'o5',
            'pregnant women': 'pw',
            'pregnant woman': 'pw'
        }
        if value_clean in synonym_map:
            value_clean = synonym_map[value_clean]
        for option in allowed:
            if value_clean == option.lower():
                return option
        return None

    def _set_workflow_pause(self, stage: str, options: List[str], context: Dict[str, Any]) -> None:
        self.state_manager.update_state({
            'workflow_paused': True,
            'paused_stage': stage,
            'paused_options': options,
            'paused_context': context
        })

    def _clear_workflow_pause(self) -> None:
        self.state_manager.update_state({
            'workflow_paused': False,
            'paused_stage': None,
            'paused_options': [],
            'paused_context': {}
        })

    def _format_tpr_results(self, results: Dict[str, Any], facility_level: str, age_group: str) -> str:
        message = results.get('message') or results.get('response')
        if message:
            return message

        selections_summary = f"Facility level: {facility_level}\nAge group: {age_group}"
        summary = results.get('summary') or results.get('data', {}).get('summary')
        if summary:
            return (
                "Malaria Burden Analysis Complete\n\n"
                f"{selections_summary}\n\n"
                f"{summary}"
            )

        return (
            "Malaria burden analysis completed successfully. "
            "If you'd like more detail, ask me to break down the results or visualize the distribution."
        )

    def handle_explanation_request(self) -> Dict[str, Any]:
        """
        Handle 'explain the differences' request with conceptual explanations.

        Provides educational context about facility levels or age groups
        based on the current workflow step, without repeating statistics.

        Returns:
            Dict with success status and explanation message
        """
        logger.info(f"[EXPLANATION] Handling explanation request at stage: {self.current_stage}")

        # Determine which explanation to provide based on current workflow step
        if self.current_stage == ConversationStage.TPR_FACILITY_LEVEL:
            message = self._explain_facility_levels()
        elif self.current_stage == ConversationStage.TPR_AGE_GROUP:
            message = self._explain_age_groups()
        else:
            # Not at a decision point - provide general help
            message = (
                "I can explain the differences when you're at a decision point in the TPR workflow.\n\n"
                "Currently, you can:\n"
                "- Type 'start the tpr workflow' to begin analyzing your malaria testing data\n"
                "- Ask me questions about your dataset\n"
                "- Or tell me what you'd like to do!"
            )

        return {
            "success": True,
            "message": message,
            "session_id": self.session_id,
            "workflow": "tpr",
            "stage": self.current_stage.value if hasattr(self.current_stage, 'value') else str(self.current_stage)
        }

    def _explain_facility_levels(self) -> str:
        """Provide conceptual explanation of facility levels."""
        return """Here's what these facility levels mean:

**Primary facilities** are community-level health centers - the first point of contact for most patients. They're typically small clinics in villages and neighborhoods, often with basic diagnostic tools like Rapid Diagnostic Tests (RDTs). This is where most people seek care when they first feel sick.

**Secondary facilities** are district or general hospitals with better equipment and trained staff. They receive referrals from primary facilities and can handle more complex cases. They usually have both RDTs and microscopy for more accurate diagnosis.

**Tertiary facilities** are specialized referral hospitals, usually found in major cities. They have advanced diagnostic capabilities including microscopy and can manage severe or complicated malaria cases. These are the highest level of care.

**Why choose one over the other?**
- **Primary**: Best for understanding community-level burden and routine care patterns. This shows you what's happening where most people first seek help.
- **Secondary/Tertiary**: Better for assessing severe cases or referral patterns. Useful if you want to focus on complicated cases.
- **All**: Gives you the complete picture across all levels of the health system.

Which level would you like to analyze? (**primary**, **secondary**, **tertiary**, or **all**)"""

    def _explain_age_groups(self) -> str:
        """Provide conceptual explanation of age groups."""
        return """Here's what these age groups represent:

**Under 5 (u5)** are young children who are most vulnerable to severe malaria. They have limited immunity because they haven't been exposed to malaria parasites enough times. This is the age group at highest risk of complications and death from malaria, making them a top priority for interventions.

**Over 5 (o5)** includes older children and adults who typically have developed some immunity from repeated exposure to malaria. They may still get infected, but usually have milder symptoms. In high-transmission areas, adults often carry the parasite without feeling very sick.

**Pregnant Women (pw)** are a special risk group - pregnancy naturally reduces immunity to malaria, putting both mother and baby at serious risk. Malaria in pregnancy can cause severe anemia, low birth weight, and pregnancy complications. These women are routinely tested at antenatal clinics.

**Why choose one over the other?**
- **U5**: Focus on the most vulnerable group for targeted interventions like ITN distribution and preventive treatment.
- **O5**: Understand the burden in the general population and transmission patterns.
- **PW**: Critical for maternal and child health programs - malaria in pregnancy requires special attention.
- **All**: Complete picture across all age groups to understand the full disease burden.

Which age group would you like to analyze? (**u5**, **o5**, **pw**, or **all**)**"""
