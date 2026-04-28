"""
Workflow Progress Helper Module

Tracks and displays progress through multi-step workflows.
Shows users what's completed, what's next, and prerequisites.
"""

import logging
from typing import Dict, Any, List, Optional
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)


class WorkflowStep(Enum):
    """Enum for workflow steps."""
    # Data preparation steps
    UPLOAD_CSV = "upload_csv"
    UPLOAD_SHAPEFILE = "upload_shapefile"
    VALIDATE_DATA = "validate_data"

    # Analysis steps
    RUN_ANALYSIS = "run_analysis"
    RUN_TPR = "run_tpr"

    # Results steps
    VIEW_RESULTS = "view_results"
    CREATE_VISUALIZATIONS = "create_visualizations"

    # Planning steps
    ITN_PLANNING = "itn_planning"
    REPRIORITIZATION = "reprioritization"

    # Export steps
    GENERATE_REPORT = "generate_report"
    EXPORT_DATA = "export_data"


class WorkflowProgressHelper:
    """Helper class for tracking workflow progress."""

    def __init__(self):
        """Initialize the workflow progress helper."""
        self.workflows = {
            'standard_analysis': self._get_standard_workflow(),
            'tpr_analysis': self._get_tpr_workflow(),
            'itn_planning': self._get_itn_workflow(),
            'quick_analysis': self._get_quick_workflow()
        }

    def _get_standard_workflow(self) -> Dict[str, Any]:
        """Define standard analysis workflow."""
        return {
            'name': 'Standard Malaria Risk Analysis',
            'description': 'Complete analysis with mapping and visualization',
            'steps': [
                {
                    'id': WorkflowStep.UPLOAD_CSV,
                    'name': 'Upload Data File',
                    'description': 'Upload CSV or Excel file with ward data',
                    'required': True,
                    'prerequisites': [],
                    'icon': '📎'
                },
                {
                    'id': WorkflowStep.UPLOAD_SHAPEFILE,
                    'name': 'Upload Shapefile',
                    'description': 'Upload ZIP file with geographic boundaries',
                    'required': True,
                    'prerequisites': [],
                    'icon': '🗺️'
                },
                {
                    'id': WorkflowStep.VALIDATE_DATA,
                    'name': 'Validate Data',
                    'description': 'Check data format and completeness',
                    'required': False,
                    'prerequisites': [WorkflowStep.UPLOAD_CSV],
                    'icon': '✅'
                },
                {
                    'id': WorkflowStep.RUN_ANALYSIS,
                    'name': 'Run Risk Analysis',
                    'description': 'Calculate vulnerability scores and rankings',
                    'required': True,
                    'prerequisites': [WorkflowStep.UPLOAD_CSV, WorkflowStep.UPLOAD_SHAPEFILE],
                    'icon': '🔬'
                },
                {
                    'id': WorkflowStep.VIEW_RESULTS,
                    'name': 'View Results',
                    'description': 'Explore analysis findings',
                    'required': False,
                    'prerequisites': [WorkflowStep.RUN_ANALYSIS],
                    'icon': '📊'
                },
                {
                    'id': WorkflowStep.CREATE_VISUALIZATIONS,
                    'name': 'Create Visualizations',
                    'description': 'Generate maps and charts',
                    'required': False,
                    'prerequisites': [WorkflowStep.RUN_ANALYSIS],
                    'icon': '📈'
                },
                {
                    'id': WorkflowStep.GENERATE_REPORT,
                    'name': 'Generate Report',
                    'description': 'Create PDF report with all findings',
                    'required': False,
                    'prerequisites': [WorkflowStep.RUN_ANALYSIS],
                    'icon': '📄'
                }
            ]
        }

    def _get_tpr_workflow(self) -> Dict[str, Any]:
        """Define TPR analysis workflow."""
        return {
            'name': 'TPR (Test Positivity Rate) Analysis',
            'description': 'Calculate test positivity rates from NMEP data',
            'steps': [
                {
                    'id': WorkflowStep.UPLOAD_CSV,
                    'name': 'Upload NMEP Excel File',
                    'description': 'Upload Excel file with test data',
                    'required': True,
                    'prerequisites': [],
                    'icon': '📎'
                },
                {
                    'id': WorkflowStep.RUN_TPR,
                    'name': 'Configure TPR Analysis',
                    'description': 'Select state, facility level, and age group',
                    'required': True,
                    'prerequisites': [WorkflowStep.UPLOAD_CSV],
                    'icon': '⚙️'
                },
                {
                    'id': WorkflowStep.VIEW_RESULTS,
                    'name': 'View TPR Results',
                    'description': 'See calculated TPR values',
                    'required': False,
                    'prerequisites': [WorkflowStep.RUN_TPR],
                    'icon': '📊'
                },
                {
                    'id': WorkflowStep.RUN_ANALYSIS,
                    'name': 'Run Risk Analysis',
                    'description': 'Continue to full risk assessment',
                    'required': False,
                    'prerequisites': [WorkflowStep.RUN_TPR],
                    'icon': '🔬'
                }
            ]
        }

    def _get_itn_workflow(self) -> Dict[str, Any]:
        """Define ITN planning workflow."""
        return {
            'name': 'ITN Distribution Planning',
            'description': 'Plan optimal bed net distribution',
            'steps': [
                {
                    'id': WorkflowStep.RUN_ANALYSIS,
                    'name': 'Complete Risk Analysis',
                    'description': 'Risk analysis must be completed first',
                    'required': True,
                    'prerequisites': [WorkflowStep.UPLOAD_CSV],
                    'icon': '🔬'
                },
                {
                    'id': WorkflowStep.ITN_PLANNING,
                    'name': 'Configure ITN Distribution',
                    'description': 'Set parameters for net allocation',
                    'required': True,
                    'prerequisites': [WorkflowStep.RUN_ANALYSIS],
                    'icon': '🛏️'
                },
                {
                    'id': WorkflowStep.VIEW_RESULTS,
                    'name': 'View Distribution Plan',
                    'description': 'See allocation across wards',
                    'required': False,
                    'prerequisites': [WorkflowStep.ITN_PLANNING],
                    'icon': '📊'
                },
                {
                    'id': WorkflowStep.EXPORT_DATA,
                    'name': 'Export Distribution List',
                    'description': 'Download allocation spreadsheet',
                    'required': False,
                    'prerequisites': [WorkflowStep.ITN_PLANNING],
                    'icon': '💾'
                }
            ]
        }

    def _get_quick_workflow(self) -> Dict[str, Any]:
        """Define quick analysis workflow (CSV only)."""
        return {
            'name': 'Quick Data Analysis',
            'description': 'Fast analysis without geographic mapping',
            'steps': [
                {
                    'id': WorkflowStep.UPLOAD_CSV,
                    'name': 'Upload Data File',
                    'description': 'Upload CSV or Excel file',
                    'required': True,
                    'prerequisites': [],
                    'icon': '📎'
                },
                {
                    'id': WorkflowStep.RUN_ANALYSIS,
                    'name': 'Run Analysis',
                    'description': 'Analyze data without mapping',
                    'required': True,
                    'prerequisites': [WorkflowStep.UPLOAD_CSV],
                    'icon': '🔬'
                },
                {
                    'id': WorkflowStep.VIEW_RESULTS,
                    'name': 'View Results',
                    'description': 'See rankings and scores',
                    'required': False,
                    'prerequisites': [WorkflowStep.RUN_ANALYSIS],
                    'icon': '📊'
                }
            ]
        }

    def get_workflow_status(self, workflow_type: str, session_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get current status of a workflow.

        Args:
            workflow_type: Type of workflow to check
            session_data: Current session data

        Returns:
            Workflow status with progress information
        """
        if workflow_type not in self.workflows:
            return {'error': f'Unknown workflow type: {workflow_type}'}

        workflow = self.workflows[workflow_type]
        steps_status = []

        for step in workflow['steps']:
            status = self._get_step_status(step, session_data)
            steps_status.append({
                'step': step,
                'status': status
            })

        # Calculate progress
        total_required = sum(1 for s in workflow['steps'] if s['required'])
        completed_required = sum(1 for s in steps_status if s['step']['required'] and s['status'] == 'completed')

        progress_percent = (completed_required / total_required * 100) if total_required > 0 else 0

        return {
            'workflow': workflow,
            'steps_status': steps_status,
            'progress_percent': progress_percent,
            'next_step': self._get_next_step(steps_status),
            'can_proceed': self._can_proceed_to_next(steps_status)
        }

    def _get_step_status(self, step: Dict[str, Any], session_data: Dict[str, Any]) -> str:
        """
        Determine status of a specific step.

        Returns: 'completed', 'available', 'blocked', or 'pending'
        """
        step_id = step['id']

        # Check if step is completed
        if step_id == WorkflowStep.UPLOAD_CSV:
            if session_data.get('csv_loaded', False):
                return 'completed'
        elif step_id == WorkflowStep.UPLOAD_SHAPEFILE:
            if session_data.get('shapefile_loaded', False):
                return 'completed'
        elif step_id == WorkflowStep.VALIDATE_DATA:
            if session_data.get('data_validated', False):
                return 'completed'
        elif step_id == WorkflowStep.RUN_ANALYSIS:
            if session_data.get('analysis_complete', False):
                return 'completed'
        elif step_id == WorkflowStep.RUN_TPR:
            if session_data.get('tpr_complete', False):
                return 'completed'
        elif step_id == WorkflowStep.ITN_PLANNING:
            if session_data.get('itn_planning_complete', False):
                return 'completed'
        elif step_id == WorkflowStep.GENERATE_REPORT:
            if session_data.get('report_generated', False):
                return 'completed'

        # Check if prerequisites are met
        prereqs_met = True
        for prereq in step['prerequisites']:
            prereq_status = self._get_step_status({'id': prereq, 'prerequisites': []}, session_data)
            if prereq_status != 'completed':
                prereqs_met = False
                break

        if prereqs_met:
            return 'available'
        else:
            return 'blocked'

    def _get_next_step(self, steps_status: List[Dict]) -> Optional[Dict[str, Any]]:
        """Find the next available step."""
        for step_status in steps_status:
            if step_status['status'] == 'available' and step_status['step']['required']:
                return step_status['step']

        # If no required steps, suggest optional ones
        for step_status in steps_status:
            if step_status['status'] == 'available':
                return step_status['step']

        return None

    def _can_proceed_to_next(self, steps_status: List[Dict]) -> bool:
        """Check if user can proceed to next step."""
        return any(s['status'] == 'available' for s in steps_status)

    def format_progress_display(self, workflow_status: Dict[str, Any]) -> str:
        """
        Format workflow progress for display.

        Args:
            workflow_status: Status from get_workflow_status()

        Returns:
            Formatted string for display
        """
        lines = []

        # Header
        workflow = workflow_status['workflow']
        lines.append(f"## 📊 {workflow['name']} Progress\n")
        lines.append(f"_{workflow['description']}_\n")

        # Progress bar
        progress = workflow_status['progress_percent']
        filled = int(progress / 10)
        bar = '█' * filled + '░' * (10 - filled)
        lines.append(f"**Progress:** [{bar}] {progress:.0f}%\n")

        # Steps list
        lines.append("### Steps:")
        for step_status in workflow_status['steps_status']:
            step = step_status['step']
            status = step_status['status']

            # Status icon
            if status == 'completed':
                icon = '✅'
            elif status == 'available':
                icon = '🔵'
            elif status == 'blocked':
                icon = '🔒'
            else:
                icon = '⭕'

            # Step line
            req = " *(Required)*" if step['required'] else ""
            lines.append(f"{icon} **{step['name']}**{req} - {step['description']}")

        # Next step suggestion
        next_step = workflow_status.get('next_step')
        if next_step:
            lines.append(f"\n### 👉 Next Step:")
            lines.append(f"**{next_step['name']}** - {next_step['description']}")

        return "\n".join(lines)

    def get_quick_status(self, session_data: Dict[str, Any]) -> str:
        """
        Get a quick one-line status of where the user is.

        Args:
            session_data: Current session data

        Returns:
            Quick status string
        """
        if not session_data.get('csv_loaded'):
            return "📎 Ready to upload data"
        elif not session_data.get('shapefile_loaded'):
            return "🗺️ CSV loaded - now upload shapefile for mapping"
        elif not session_data.get('analysis_complete'):
            return "🔬 Data ready - run analysis when ready"
        elif session_data.get('analysis_complete'):
            return "✅ Analysis complete - explore results or generate reports"
        else:
            return "💭 Ready to help with your analysis"

    def suggest_next_action(self, session_data: Dict[str, Any]) -> Optional[str]:
        """
        Suggest the next action based on current state.

        Args:
            session_data: Current session data

        Returns:
            Suggested action message or None
        """
        if not session_data.get('csv_loaded'):
            return "💡 **Start by uploading your data** - Click the 📎 icon or say 'upload data'"

        if session_data.get('csv_loaded') and not session_data.get('shapefile_loaded'):
            return "💡 **Add geographic boundaries** - Upload a shapefile for mapping capabilities"

        if session_data.get('csv_loaded') and not session_data.get('analysis_complete'):
            if session_data.get('tpr_workflow_active'):
                return "💡 **Continue TPR workflow** - Complete the state and facility selections"
            else:
                return "💡 **Run analysis** - Say 'analyze my data' or 'run risk analysis'"

        if session_data.get('analysis_complete'):
            if not session_data.get('report_generated'):
                return "💡 **Generate a report** - Say 'create report' to get a PDF summary"
            elif not session_data.get('itn_planning_complete'):
                return "💡 **Plan ITN distribution** - Say 'plan bed net distribution'"
            else:
                return "💡 **Explore your results** - Try different visualizations or export data"

        return None