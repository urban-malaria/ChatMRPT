"""
TPR Workflow LangGraph Tool

Converts the TPR workflow into a pausable/resumable LangGraph tool.
Preserves all Track A improvements:
- TPR data auto-detection
- Contextual welcome with facility/ward counts
- 3-level fuzzy keyword matching
- Proactive visualization offers
"""

import os
import logging
import uuid
from typing import Dict, Any, Optional, List
from langchain_core.tools import tool
from pathlib import Path

from ..core.state_manager import DataAnalysisStateManager, ConversationStage
from ..core.tpr_data_analyzer import TPRDataAnalyzer
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

logger = logging.getLogger(__name__)


class TPRWorkflowToolHandler:
    """
    Handler for TPR workflow tool execution.

    Encapsulates all TPR workflow logic extracted from TPRWorkflowHandler.
    Preserves Track A improvements while making workflow pausable/resumable.
    """

    def __init__(self, session_id: str):
        """
        Initialize TPR workflow tool handler.

        Args:
            session_id: Session identifier
        """
        self.session_id = session_id
        self.session_folder = f"instance/uploads/{session_id}"
        self.state_manager = DataAnalysisStateManager(session_id)
        self.tpr_analyzer = TPRDataAnalyzer(session_id)
        self.tpr_selections = {}
        self.uploaded_data = None

        # Load current state
        self.load_state()

    def load_state(self):
        """Load workflow state from state manager."""
        self.current_stage = self.state_manager.get_workflow_stage()
        self.tpr_selections = self.state_manager.get_tpr_selections() or {}
        logger.info(f"Loaded TPR state: stage={self.current_stage}, selections={self.tpr_selections}")

    def load_data(self) -> Optional[pd.DataFrame]:
        """Load uploaded data for analysis."""
        try:
            # Try multiple file name patterns
            file_patterns = [
                'data_analysis.csv',  # Data Analysis V3 upload endpoint
                'raw_data.csv',  # Legacy/standard upload
                'unified_dataset.csv',  # Post-analysis unified dataset
                'uploaded_data.csv'  # Alternative standard name
            ]

            for pattern in file_patterns:
                csv_path = os.path.join(self.session_folder, pattern)
                if os.path.exists(csv_path):
                    df = pd.read_csv(csv_path)
                    self.uploaded_data = df
                    logger.info(f"✅ Loaded data from {pattern}: {len(df)} rows, {len(df.columns)} columns")
                    return df

            # Try finding ANY data file in the folder (CSV or Excel)
            import glob
            data_files = glob.glob(os.path.join(self.session_folder, '*.csv')) + \
                        glob.glob(os.path.join(self.session_folder, '*.xlsx')) + \
                        glob.glob(os.path.join(self.session_folder, '*.xls'))
            if data_files:
                data_path = data_files[0]
                if data_path.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(data_path)
                else:
                    df = pd.read_csv(data_path)
                self.uploaded_data = df
                logger.info(f"✅ Loaded data from first file found: {os.path.basename(data_path)} ({len(df)} rows, {len(df.columns)} columns)")
                return df

            logger.warning(f"❌ No data found in {self.session_folder}")
            return None

        except Exception as e:
            logger.error(f"❌ Error loading data: {e}", exc_info=True)
            return None

    # ==================== CONVERSATIONAL ONBOARDING ====================

    def generate_data_overview(self, df: pd.DataFrame, filename: str = "your dataset") -> str:
        """
        Generate comprehensive, conversational data overview.

        This is shown to users BEFORE asking if they want TPR analysis,
        giving them context about their data.

        Args:
            df: Uploaded DataFrame
            filename: Name of uploaded file

        Returns:
            Markdown-formatted data overview
        """
        try:
            overview = "# Your Dataset Overview\n\n"

            # Basic stats
            overview += f"**File**: {filename}\n"
            overview += f"**Size**: {len(df):,} rows × {len(df.columns)} columns\n\n"

            overview += "### What You Have:\n\n"

            # Geographic coverage
            if 'State' in df.columns:
                overview += "**Geographic Coverage**:\n"
                states = df['State'].nunique()
                if states == 1:
                    overview += f"• State: {df['State'].iloc[0]}\n"
                else:
                    overview += f"• States: {states} states\n"

                if 'LGA' in df.columns:
                    lgas = df['LGA'].nunique()
                    overview += f"• LGAs: {lgas} local government areas\n"

                if 'WardName' in df.columns or 'Ward' in df.columns:
                    ward_col = 'WardName' if 'WardName' in df.columns else 'Ward'
                    wards = df[ward_col].nunique()
                    overview += f"• Wards: {wards} wards\n"

                if 'HealthFacility' in df.columns:
                    facilities = df['HealthFacility'].nunique()
                    overview += f"• Facilities: {facilities} health facilities\n"

            # Facility breakdown
            facility_col = None
            for col in ['FacilityLevel', 'FacilityType', 'Facility_Level', 'Facility_Type']:
                if col in df.columns:
                    facility_col = col
                    break

            if facility_col:
                facility_counts = df[facility_col].value_counts()
                overview += "\n**Facility Breakdown**:\n"
                total_facilities = len(df)
                for level, count in facility_counts.items():
                    pct = (count / total_facilities) * 100
                    level_str = str(level).capitalize()
                    overview += f"• {level_str} facilities: {count} ({pct:.1f}%)\n"

            # Test data detection
            test_cols = [col for col in df.columns if any(
                kw in col.lower() for kw in ['test', 'rdt', 'microscopy']
            )]

            if test_cols:
                overview += "\n**Test Data**:\n"

                # Try to calculate total tests
                total_tests = 0
                for col in test_cols:
                    if 'tested' in col.lower() and 'positive' not in col.lower():
                        try:
                            col_sum = df[col].sum()
                            if col_sum > 0:
                                total_tests += col_sum
                        except:
                            pass

                if total_tests > 0:
                    overview += f"• Total tests conducted: {int(total_tests):,}\n"

                # Test types
                test_types = []
                if any('rdt' in col.lower() for col in test_cols):
                    test_types.append("RDT")
                if any('microscopy' in col.lower() for col in test_cols):
                    test_types.append("Microscopy")

                if test_types:
                    overview += f"• Test types: {', '.join(test_types)}\n"

                # Coverage period (if available)
                for col in ['periodname', 'period_name', 'Period', 'Month']:
                    if col in df.columns:
                        periods = df[col].nunique()
                        overview += f"• Coverage periods: {periods}\n"
                        break

            # Age groups
            age_indicators = ['<5', '5yrs', 'pw', 'preg', 'u5', 'o5']
            has_age_groups = any(
                any(ind in col.lower() for ind in age_indicators)
                for col in df.columns
            )

            if has_age_groups:
                overview += "\n**Age Groups**:\n"
                if any('<5' in col or 'u5' in col.lower() for col in df.columns):
                    overview += "• Children under 5 years (U5)\n"
                if any('≥5' in col or '>5' in col or 'o5' in col.lower() or '5yrs' in col.lower() for col in df.columns):
                    overview += "• People 5 years and older (O5)\n"
                if any('pw' in col.lower() or 'preg' in col.lower() for col in df.columns):
                    overview += "• Pregnant women (PW)\n"

            overview += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

            # Ask user what they want to do (NO AUTO-DETECTION - user decides)
            overview += "**What would you like to do with this data?**\n\n"
            overview += "I can help you with:\n"
            overview += "• **TPR (Test Positivity Rate) analysis** - if this is malaria test data\n"
            overview += "• **General data exploration** - summary statistics, visualizations, insights\n"
            overview += "• **Custom analysis** - just ask me what you need!\n\n"
            overview += "What would you like to explore?"

            return overview

        except Exception as e:
            logger.error(f"Error generating data overview: {e}", exc_info=True)
            return f"# Your Dataset\n\nLoaded {len(df):,} rows × {len(df.columns)} columns.\n\nWhat would you like to do with this data?"

    def get_tpr_explanation(self) -> str:
        """
        Get comprehensive TPR explanation for onboarding.

        Returns:
            Markdown-formatted TPR explanation
        """
        return """# What is Test Positivity Rate (TPR) Analysis?

**TPR Definition**:
The percentage of malaria tests that come back positive. It's a key indicator for:
• Disease burden in the population
• Targeting intervention resources
• Monitoring malaria trends

**Example**:
If 100 people are tested and 15 test positive, TPR = 15%

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

**What We'll Calculate Together**:

1. **By Facility Type**: TPR for primary, secondary, and tertiary facilities
2. **By Age Group**: TPR for children <5, people ≥5, and pregnant women
3. **Geographic Patterns**: Which wards/LGAs have highest TPR

**The Process** (3-5 minutes):

**Step 1**: Choose facility type (primary, secondary, tertiary, or all)
**Step 2**: Choose age group (under 5, over 5, pregnant women, or all)
**Step 3**: System calculates TPR for your selection
**Step 4**: View results with visualizations
**Step 5**: Ready for risk analysis!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

**Questions?**
Feel free to ask me anything about:
• What TPR means
• Why we need facility and age group selections
• What the results will look like
• How this connects to risk analysis

Or say **"let's start"** when you're ready to begin!
"""

    def answer_tpr_question(self, question: str) -> str:
        """
        Answer common TPR-related questions during onboarding.

        Args:
            question: User's question (lowercase)

        Returns:
            Answer to the question
        """
        question_lower = question.lower()

        # What is TPR?
        if any(kw in question_lower for kw in ['what is tpr', 'what does tpr mean', 'define tpr']):
            return """TPR (Test Positivity Rate) is the percentage of malaria tests that return positive.

**Formula**: (Positive tests ÷ Total tests) × 100

**What it tells us**:
• High TPR (>20%) = High disease burden, more cases than testing capacity
• Medium TPR (10-20%) = Moderate burden, adequate testing
• Low TPR (<10%) = Low burden OR over-testing

It helps health officials decide where to focus malaria control efforts."""

        # Why facility type?
        elif any(kw in question_lower for kw in ['why facility', 'facility type', 'why different facilities']):
            return """Great question!

**Why Facility Type Matters**:

Different facility types serve different populations:

• **Primary facilities** (health centers):
  - Community baseline TPR
  - First point of contact
  - Higher patient volume, lower complexity

• **Secondary facilities** (district hospitals):
  - Referral cases (often higher TPR)
  - Handle more complex cases
  - More diagnostic capacity

• **Tertiary facilities** (specialist hospitals):
  - Severe cases (highest TPR)
  - Serve entire states/regions
  - Highest diagnostic accuracy

Analyzing separately gives you a **complete picture** across the health system."""

        # Why age group?
        elif any(kw in question_lower for kw in ['why age', 'age group', 'why different ages']):
            return """Excellent question!

**Why Age Groups Matter**:

Malaria affects different age groups differently:

• **Children <5**: Highest risk, most vulnerable, limited immunity
• **Adults ≥5**: Lower risk, developed immunity
• **Pregnant women**: Vulnerable, affects mother + baby

Different interventions target different groups, so we need separate TPR for:
• Targeting bed nets (children vs. pregnant women)
• Drug distribution strategies
• Vaccination priorities
• Resource allocation"""

        # What will results show?
        elif any(kw in question_lower for kw in ['what results', 'what will i see', 'what do i get']):
            return """**You'll receive**:

1. **Overall TPR** - e.g., "18.7% of children <5 tested positive"

2. **Breakdown by Facility Type** - TPR for primary, secondary, tertiary

3. **Geographic Hotspots** - Top wards/LGAs with highest TPR

4. **Interactive Visualizations** - Maps showing TPR by ward

5. **Downloadable Data** - CSV files with ward-level and facility-level TPR

6. **Next Steps** - Your data will be ready for risk analysis!"""

        # How long does it take?
        elif any(kw in question_lower for kw in ['how long', 'how much time', 'duration']):
            return """**Time Estimate**: 3-5 minutes total

**Breakdown**:
• Step 1 (Facility selection): 30 seconds
• Step 2 (Age group selection): 30 seconds
• Step 3 (Calculation): 1-2 minutes
• Step 4 (Review results): 1-2 minutes

The actual calculation is fast - most time is you reviewing the results!"""

        # Default response
        else:
            return """I can answer questions about:
• What TPR means and why it matters
• Why we need to select facility types and age groups
• What the results will show
• How long the process takes
• How this connects to risk analysis

What would you like to know?"""

    # ==================== TRACK A: FUZZY MATCHING ====================

    def fuzzy_match_facility(self, query: str) -> Optional[str]:
        """
        Extract facility level using 3-level fuzzy matching (Track A).

        Level 1: Exact keyword match (fast path ~20ms)
        Level 2: Fuzzy string matching for typos (~50ms)
        Level 3: Pattern/phrase matching for natural language (~100ms)

        Args:
            query: User's input text

        Returns:
            Matched facility level: 'primary', 'secondary', 'tertiary', 'all', or None
        """
        query_clean = query.lower().strip()

        # Level 1: Exact keyword match
        exact_keywords = {
            'primary': 'primary', '1': 'primary', 'one': 'primary',
            'secondary': 'secondary', '2': 'secondary', 'two': 'secondary',
            'tertiary': 'tertiary', '3': 'tertiary', 'three': 'tertiary',
            'all': 'all', '4': 'all', 'four': 'all', 'every': 'all'
        }

        if query_clean in exact_keywords:
            logger.info(f"✓ Exact match: '{query_clean}' → {exact_keywords[query_clean]}")
            return exact_keywords[query_clean]

        # Level 2: Fuzzy string matching for typos
        from difflib import get_close_matches
        close_matches = get_close_matches(query_clean, exact_keywords.keys(), n=1, cutoff=0.75)

        if close_matches:
            matched_key = close_matches[0]
            result = exact_keywords[matched_key]
            logger.info(f"✓ Fuzzy match: '{query_clean}' → '{matched_key}' → {result}")
            return result

        # Level 3: Pattern/phrase matching for natural language
        patterns = {
            'primary': [
                'primary', 'basic', 'community', 'first level', 'phc',
                'health center', 'clinic', 'local', 'ward level'
            ],
            'secondary': [
                'secondary', 'district', 'general hospital', 'second level',
                'cottage hospital', 'comprehensive', 'lga'
            ],
            'tertiary': [
                'tertiary', 'specialist', 'teaching hospital', 'third level',
                'referral', 'federal medical', 'university hospital'
            ],
            'all': [
                'all', 'every', 'combined', 'everything', 'total',
                'across all', 'all levels', 'complete'
            ]
        }

        # Check if ANY pattern keyword appears in query
        for level, keywords in patterns.items():
            for keyword in keywords:
                if keyword in query_clean:
                    logger.info(f"✓ Pattern match: '{query_clean}' contains '{keyword}' → {level}")
                    return level

        # No match found
        logger.info(f"✗ No facility match for: '{query_clean}'")
        return None

    def fuzzy_match_age_group(self, query: str) -> Optional[str]:
        """
        Extract age group using 3-level fuzzy matching (Track A).

        Args:
            query: User's input text

        Returns:
            Matched age group: 'u5', 'o5', 'pw', 'all_ages', or None
        """
        query_clean = query.lower().strip()

        # Level 1: Exact keyword match
        exact_keywords = {
            'u5': 'u5', 'under5': 'u5', '1': 'u5', 'one': 'u5',
            'o5': 'o5', 'over5': 'o5', '2': 'o5', 'two': 'o5',
            'pw': 'pw', 'pregnant': 'pw', '3': 'pw', 'three': 'pw',
            'all': 'all_ages', '4': 'all_ages', 'four': 'all_ages'
        }

        if query_clean in exact_keywords:
            logger.info(f"✓ Exact match: '{query_clean}' → {exact_keywords[query_clean]}")
            return exact_keywords[query_clean]

        # Level 2: Fuzzy string matching for typos
        from difflib import get_close_matches
        close_matches = get_close_matches(query_clean, exact_keywords.keys(), n=1, cutoff=0.75)

        if close_matches:
            matched_key = close_matches[0]
            result = exact_keywords[matched_key]
            logger.info(f"✓ Fuzzy match: '{query_clean}' → '{matched_key}' → {result}")
            return result

        # Level 3: Pattern/phrase matching for natural language
        patterns = {
            'u5': [
                'under 5', 'under five', 'u5', 'under-5', 'children',
                'kids', 'infant', 'toddler', 'young', 'pediatric', 'child'
            ],
            'o5': [
                'over 5', 'over five', 'o5', 'over-5', 'adult',
                'older', 'above 5', 'above five', 'grown'
            ],
            'pw': [
                'pregnant', 'pregnancy', 'maternal', 'mother',
                'antenatal', 'expecting', 'gravid', 'prenatal', 'women'
            ],
            'all_ages': [
                'all', 'every', 'combined', 'everything', 'total',
                'all ages', 'everyone', 'complete', 'all groups'
            ]
        }

        # Check if ANY pattern keyword appears in query
        for group, keywords in patterns.items():
            for keyword in keywords:
                if keyword in query_clean:
                    logger.info(f"✓ Pattern match: '{query_clean}' contains '{keyword}' → {group}")
                    return group

        # No match found
        logger.info(f"✗ No age group match for: '{query_clean}'")
        return None

    # ==================== TRACK A: AUTO-DETECTION ====================

    def detect_tpr_data(self, df: pd.DataFrame) -> bool:
        """
        Auto-detect if uploaded data is TPR data (Track A).

        Checks for:
        - Facility columns
        - Test data columns (RDT, Microscopy, etc.)
        - TPR-specific indicators

        Args:
            df: DataFrame to analyze

        Returns:
            True if TPR data detected, False otherwise
        """
        if df is None or df.empty:
            return False

        columns_lower = ' '.join(df.columns).lower()

        # Check for facility columns
        has_facility = any(keyword in columns_lower for keyword in [
            'facility', 'health_facility', 'healthfacility'
        ])

        # Check for test data columns
        has_test = any(keyword in columns_lower for keyword in [
            'rdt', 'microscopy', 'tested', 'positive'
        ])

        # Check for TPR-specific indicators
        has_tpr_indicators = sum([
            'tpr' in columns_lower,
            'positivity' in columns_lower,
            'age' in columns_lower or 'u5' in columns_lower or 'o5' in columns_lower,
            'facility_type' in columns_lower or 'facility_level' in columns_lower,
        ])

        is_tpr = has_facility and has_test and has_tpr_indicators >= 1

        if is_tpr:
            logger.info("✓ TPR data auto-detected")
        else:
            logger.info("✗ Not TPR data")

        return is_tpr

    # ==================== TRACK A: CONTEXTUAL WELCOME ====================

    def start_workflow(self) -> Dict[str, Any]:
        """
        Start TPR workflow with conversational onboarding.

        NEW APPROACH: Show data overview FIRST, no auto-detection.
        User decides if they want TPR analysis.

        Returns:
            Dict with response, status, stage
        """
        try:
            logger.info("🎬 Starting conversational TPR onboarding")
            logger.info(f"📂 Session folder: {self.session_folder}")

            # Load data
            df = self.load_data()
            if df is None:
                logger.error("❌ No data found in session")
                return {
                    "response": "No data found. Please upload your data first.",
                    "status": "error",
                    "stage": "INITIAL"
                }

            logger.info(f"✓ Data loaded: {len(df)} rows, {len(df.columns)} columns")

            # Get filename from session
            filename = "your dataset"
            try:
                import os
                csv_files = [f for f in os.listdir(self.session_folder) if f.endswith('.csv')]
                if csv_files:
                    filename = csv_files[0]
                    logger.info(f"📄 Using filename: {filename}")
            except Exception as e:
                logger.warning(f"Could not get filename: {e}")

            # Generate data overview (NO auto-detection check)
            logger.info("📊 Generating data overview...")
            overview = self.generate_data_overview(df, filename)
            logger.info(f"✓ Overview generated ({len(overview)} chars)")

            # Update stage to data overview shown
            self.state_manager.update_workflow_stage(ConversationStage.DATA_OVERVIEW_SHOWN)
            self.current_stage = ConversationStage.DATA_OVERVIEW_SHOWN
            logger.info("✓ Stage updated to DATA_OVERVIEW_SHOWN")

            return {
                "response": overview,
                "status": "success",
                "stage": "DATA_OVERVIEW_SHOWN",
                "workflow": "tpr_onboarding"
            }

        except Exception as e:
            logger.error(f"❌ Error in start_workflow: {e}", exc_info=True)
            return {
                "response": f"Error starting workflow: {str(e)}",
                "status": "error",
                "stage": "INITIAL"
            }

    def show_tpr_explanation(self) -> Dict[str, Any]:
        """
        Show TPR explanation after user confirms interest.

        Returns:
            Dict with explanation, status, stage
        """
        logger.info("📚 Showing TPR explanation")

        explanation = self.get_tpr_explanation()

        # Update stage to TPR explained
        self.state_manager.update_workflow_stage(ConversationStage.TPR_EXPLAINED)
        self.current_stage = ConversationStage.TPR_EXPLAINED

        return {
            "response": explanation,
            "status": "success",
            "stage": "TPR_EXPLAINED",
            "workflow": "tpr_onboarding"
        }

    def handle_tpr_question(self, user_query: str) -> Dict[str, Any]:
        """
        Answer user's question about TPR during onboarding.

        Args:
            user_query: User's question

        Returns:
            Dict with answer, status, stage
        """
        logger.info(f"❓ Answering TPR question: {user_query[:50]}...")

        answer = self.answer_tpr_question(user_query)

        # Add prompt to continue
        answer += "\n\nAny other questions, or ready to start?"

        # Stay in current stage
        return {
            "response": answer,
            "status": "success",
            "stage": str(self.current_stage.value) if hasattr(self, 'current_stage') else "TPR_EXPLAINED",
            "workflow": "tpr_onboarding"
        }

    def confirm_workflow_start(self) -> Dict[str, Any]:
        """
        User confirmed ready to start - show facility selection.

        Returns:
            Dict with facility prompt, status, stage
        """
        logger.info("✅ User confirmed - starting actual TPR workflow")

        # Load data for contextual facility prompt
        df = self.load_data()

        # Mark workflow as active
        self.state_manager.mark_tpr_workflow_active()

        # Generate facility selection prompt (use existing method)
        welcome_message = self._generate_contextual_welcome(df)

        # Advance to facility selection stage
        self.state_manager.update_workflow_stage(ConversationStage.TPR_FACILITY_LEVEL)
        self.current_stage = ConversationStage.TPR_FACILITY_LEVEL

        return {
            "response": welcome_message,
            "status": "success",
            "stage": "TPR_FACILITY_LEVEL",
            "workflow": "tpr"
        }

    def _generate_contextual_welcome(self, df: pd.DataFrame) -> str:
        """
        Generate contextual welcome message with data summary (Track A).

        Args:
            df: Uploaded data

        Returns:
            Welcome message with facility/ward counts
        """
        try:
            # Get facility counts by level
            facility_counts = {}
            if 'facility_level' in df.columns:
                facility_counts = df['facility_level'].value_counts().to_dict()
            elif 'FacilityType' in df.columns:
                facility_counts = df['FacilityType'].value_counts().to_dict()

            total_facilities = len(df)

            # Get ward count
            ward_count = 0
            for col in ['ward', 'Ward', 'WardName', 'ward_name']:
                if col in df.columns:
                    ward_count = df[col].nunique()
                    break

            # Get test counts
            total_tests = 0
            for col in ['Total_Tested', 'total_tested', 'RDT_Tested', 'Microscopy_Tested']:
                if col in df.columns:
                    total_tests = int(df[col].sum())
                    break

            # Build welcome message
            welcome = "# Welcome to ChatMRPT - Test Positivity Rate Analysis\n\n"
            welcome += f"**Detected:** TPR data from your facilities\n"

            if total_facilities:
                welcome += f"**Coverage:** {total_facilities:,} facilities"
                if ward_count:
                    welcome += f", {ward_count} wards"
                if total_tests:
                    welcome += f", {total_tests:,} tests conducted"
                welcome += "\n\n"

            welcome += "**What we'll do together** (3-5 minutes):\n"
            welcome += "1. 📊 Calculate TPR by facility type and age group\n"
            welcome += "2. 📈 Visualize test positivity rates\n"
            welcome += "3. 🎯 Identify high-risk populations\n\n"
            welcome += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            welcome += "**Let's start!** Which health facilities should we analyze?\n\n"
            welcome += "**Your options:**\n"

            # Show facility counts if available
            if facility_counts:
                for level, count in facility_counts.items():
                    level_name = str(level).lower().strip()
                    if level_name in ['primary', 'secondary', 'tertiary']:
                        desc = {
                            'primary': 'Community health centers',
                            'secondary': 'District hospitals',
                            'tertiary': 'Specialist centers'
                        }.get(level_name, '')
                        welcome += f"• **{level_name}** ({count:,} facilities) - {desc}\n"
            else:
                # Default options
                welcome += "• **primary** (or 1) - Community health centers\n"
                welcome += "• **secondary** (or 2) - District hospitals\n"
                welcome += "• **tertiary** (or 3) - Specialist centers\n"

            welcome += "• **all** (or 4) - Combined analysis\n\n"
            welcome += "💡 *Want to explore data first? Say 'show summary' or ask questions.*"

            return welcome

        except Exception as e:
            logger.error(f"Error generating contextual welcome: {e}")
            # Fallback to simple welcome
            return ("Welcome! I'll help you calculate Test Positivity Rates.\n\n"
                   "Which facilities? **primary**, **secondary**, **tertiary**, or **all**")

    # ==================== WORKFLOW STEPS ====================

    def handle_facility_selection(self, facility_level: str) -> Dict[str, Any]:
        """
        Handle facility level selection step.

        Args:
            facility_level: Selected facility level (primary/secondary/tertiary/all)

        Returns:
            Dict with response, status, stage
        """
        logger.info(f"🔵 Facility selected: {facility_level}")

        # Store selection
        self.tpr_selections['facility_level'] = facility_level
        self.state_manager.update_tpr_selections(self.tpr_selections)

        # Count facilities
        df = self.load_data()
        facility_count = 0
        if df is not None:
            if facility_level == 'all':
                facility_count = len(df)
            else:
                # Filter by facility level
                for col in ['facility_level', 'FacilityType', 'facility_type']:
                    if col in df.columns:
                        facility_count = len(df[df[col].str.lower() == facility_level.lower()])
                        break

        # Advance to age group selection
        self.state_manager.update_workflow_stage(ConversationStage.TPR_AGE_GROUP)
        self.current_stage = ConversationStage.TPR_AGE_GROUP

        # Build response
        level_names = {
            'primary': 'Primary',
            'secondary': 'Secondary',
            'tertiary': 'Tertiary',
            'all': 'All'
        }
        level_display = level_names.get(facility_level, facility_level.title())

        response = f"✓ **{level_display} facilities selected**"
        if facility_count > 0:
            response += f" ({facility_count:,} facilities)"
        response += "\n\n"
        response += "**Which age group should we analyze?**\n\n"
        response += "**Your options:**\n"
        response += "• **u5** (or 1) - Children under 5 years\n"
        response += "• **o5** (or 2) - People over 5 years\n"
        response += "• **pw** (or 3) - Pregnant women\n"
        response += "• **all** (or 4) - All age groups combined\n\n"
        response += "💡 *Not sure? Say 'what's the difference?' or ask questions.*"

        return {
            "response": response,
            "status": "success",
            "stage": "TPR_AGE_GROUP",
            "workflow": "tpr"
        }

    def handle_age_group_selection(self, age_group: str) -> Dict[str, Any]:
        """
        Handle age group selection and calculate TPR.

        Args:
            age_group: Selected age group (u5/o5/pw/all_ages)

        Returns:
            Dict with response, status, results, visualizations
        """
        logger.info(f"🟣 Age group selected: {age_group}")

        # Store selection
        self.tpr_selections['age_group'] = age_group
        self.state_manager.update_tpr_selections(self.tpr_selections)

        # Run TPR calculation
        try:
            df = self.load_data()
            if df is None:
                return {
                    "response": "Error: Data not found",
                    "status": "error",
                    "stage": "TPR_AGE_GROUP"
                }

            facility_level = self.tpr_selections.get('facility_level', 'all')

            # Calculate TPR
            results = self.tpr_analyzer.calculate_tpr(
                df=df,
                facility_level=facility_level,
                age_group=age_group
            )

            if not results or results.get('status') == 'error':
                return {
                    "response": f"Error calculating TPR: {results.get('message', 'Unknown error')}",
                    "status": "error",
                    "stage": "TPR_AGE_GROUP"
                }

            # Mark workflow as complete
            self.state_manager.mark_tpr_workflow_inactive()
            self.state_manager.update_workflow_stage(ConversationStage.INITIAL)

            # Format results
            response = self._format_tpr_results(results, facility_level, age_group)

            return {
                "response": response,
                "status": "success",
                "stage": "COMPLETE",
                "workflow": "tpr",
                "results": results,
                "download_links": results.get('download_links', [])
            }

        except Exception as e:
            logger.error(f"Error calculating TPR: {e}")
            return {
                "response": f"Error calculating TPR: {str(e)}",
                "status": "error",
                "stage": "TPR_AGE_GROUP"
            }

    def _format_tpr_results(self, results: Dict, facility_level: str, age_group: str) -> str:
        """Format TPR results for display."""
        response = "## Test Positivity Rate Analysis Complete\n\n"

        # Selection summary
        level_names = {
            'primary': 'Primary',
            'secondary': 'Secondary',
            'tertiary': 'Tertiary',
            'all': 'All'
        }
        age_names = {
            'u5': 'Under-5 children',
            'o5': 'Over-5 population',
            'pw': 'Pregnant women',
            'all_ages': 'All age groups'
        }

        response += f"**Analysis:** {level_names.get(facility_level, facility_level)} facilities, "
        response += f"{age_names.get(age_group, age_group)}\n\n"

        # Key findings
        if 'summary' in results:
            summary = results['summary']
            response += "### Key Findings\n\n"

            if 'overall_tpr' in summary:
                response += f"**Overall TPR:** {summary['overall_tpr']:.1f}%\n"

            if 'total_tested' in summary:
                response += f"**Tests Conducted:** {summary['total_tested']:,}\n"

            if 'total_positive' in summary:
                response += f"**Positive Cases:** {summary['total_positive']:,}\n"

            response += "\n"

        # Download links
        if 'download_links' in results and results['download_links']:
            response += "### Download Results\n\n"
            for link in results['download_links']:
                response += f"• [{link['label']}]({link['url']})\n"
            response += "\n"

        response += "💡 *Want to explore further? Ask questions or request visualizations.*"

        return response


# ==================== LANGGRAPH TOOL ====================

@tool
async def tpr_workflow_step(
    session_id: str,
    action: str,
    value: Optional[str] = None
) -> Dict[str, Any]:
    """
    Execute TPR workflow for malaria test positivity rate analysis.

    **WHEN TO CALL THIS TOOL**:
    - User says "yes" when asked if data is TPR data → action="show_explanation"
    - User says "yes" to proceed with TPR after seeing explanation → action="confirm_start"
    - User asks questions about TPR → action="answer_question" with their question as value
    - User wants to "calculate tpr", "run tpr", "analyze tpr", "tpr analysis", "test positivity"
    - User selects facility level: "primary", "secondary", "tertiary", "1", "2", "3"
    - User selects age group: "under 5", "u5", "over 5", "o5", "pregnant", "pw", "children", "women"
    - You are in TPR workflow and user provides ANY selection input
    - User mentions facility types or age groups in their message

    **CRITICAL**: Always call this tool for TPR-related requests, even if the user's
    phrasing is ambiguous. Natural language like "pregnant women" or "children" should
    trigger this tool when TPR workflow is active. DO NOT try to describe data columns -
    instead call this tool to SELECT the option.

    **WORKFLOW STAGES**:
    1. Start: action="start" - Begins workflow with contextual welcome showing facility counts
    2. Facility: action="select_facility" value="primary|secondary|tertiary|all"
       - Fuzzy matching handles typos: "secndary facilitys" → "secondary"
       - Natural language: "community health centers" → "primary"
    3. Age: action="select_age" value="u5|o5|pw|all"
       - Fuzzy matching: "children under 5" → "u5", "pregnant women" → "pw"
       - Natural language fully supported

    **STATE AWARENESS**: This tool manages workflow state. When workflow is active:
    - Facility stage: ANY user input should be treated as facility selection
    - Age stage: ANY user input should be treated as age group selection
    - Do NOT explore data - SELECT options instead

    Args:
        session_id: Current session identifier
        action: Workflow action to perform
            - "show_explanation": Show TPR workflow explanation (when user says yes to "Is this TPR data?")
            - "answer_question": Answer user's question about TPR (value = their question)
            - "confirm_start": Start TPR workflow after explanation (when user confirms)
            - "start": Begin TPR workflow directly
            - "select_facility": Select facility level (primary/secondary/tertiary/all)
            - "select_age": Select age group (u5/o5/pw/all - fuzzy matching enabled)
        value: The selection value, question, or empty string depending on action

    Returns:
        Dict with:
            - response: Message to display to user
            - status: "success", "error", or "awaiting_input"
            - stage: Current workflow stage
            - workflow: "tpr"
            - results: (optional) TPR calculation results
            - download_links: (optional) Download links for results

    Examples:
        # User: "calculate tpr" or "run tpr analysis" or "test positivity rate"
        tpr_workflow_step(session_id="abc", action="start")

        # User: "primary facilities" or "1" or "community health centers" or "phc"
        tpr_workflow_step(session_id="abc", action="select_facility", value="primary")

        # User: "pregnant women" or "maternal" or "pw" or "antenatal"
        tpr_workflow_step(session_id="abc", action="select_age", value="pregnant women")

        # User: "children under 5" or "kids" or "u5" or "pediatric"
        tpr_workflow_step(session_id="abc", action="select_age", value="children under 5")

        # User: "secondary" or "2" or "district hospital" (during workflow)
        tpr_workflow_step(session_id="abc",
            action="select_facility",
            value="primary"  # Tool will fuzzy match this
        )

        # User says "u5" or "children under 5"
        result = await tpr_workflow_step(
            session_id="abc123",
            action="select_age",
            value="children under 5"  # Tool will fuzzy match this
        )
    """
    import sys
    print(f"=== tpr_workflow_step CALLED: action={action}, value={value} ===", file=sys.stderr, flush=True)
    logger.error(f"🔧 TPR WORKFLOW TOOL CALLED: action={action}, value={value}, session={session_id}")

    handler = TPRWorkflowToolHandler(session_id)

    # CONVERSATIONAL ONBOARDING ACTIONS
    if action == "start":
        # Show data overview (NEW conversational approach)
        return handler.start_workflow()

    elif action == "show_explanation":
        # User wants TPR explanation
        return handler.show_tpr_explanation()

    elif action == "answer_question":
        # User asked a question about TPR
        return handler.handle_tpr_question(value or "")

    elif action == "confirm_start":
        # User confirmed ready to start workflow
        return handler.confirm_workflow_start()

    # WORKFLOW ACTIONS (existing)
    elif action == "select_facility":
        if not value:
            return {
                "response": "Please specify a facility type: primary, secondary, tertiary, or all",
                "status": "awaiting_input",
                "stage": "TPR_FACILITY_LEVEL"
            }

        # Use fuzzy matching (Track A improvement)
        matched_facility = handler.fuzzy_match_facility(value)

        if matched_facility:
            return handler.handle_facility_selection(matched_facility)
        else:
            return {
                "response": ("I didn't understand that facility type. Please choose:\n"
                           "• **primary** - Community health centers\n"
                           "• **secondary** - District hospitals\n"
                           "• **tertiary** - Specialist centers\n"
                           "• **all** - All facilities"),
                "status": "awaiting_input",
                "stage": "TPR_FACILITY_LEVEL"
            }

    elif action == "select_age":
        if not value:
            return {
                "response": "Please specify an age group: u5, o5, pw, or all",
                "status": "awaiting_input",
                "stage": "TPR_AGE_GROUP"
            }

        # Use fuzzy matching (Track A improvement)
        matched_age = handler.fuzzy_match_age_group(value)

        if matched_age:
            return handler.handle_age_group_selection(matched_age)
        else:
            return {
                "response": ("I didn't understand that age group. Please choose:\n"
                           "• **u5** - Children under 5 years\n"
                           "• **o5** - People over 5 years\n"
                           "• **pw** - Pregnant women\n"
                           "• **all** - All age groups"),
                "status": "awaiting_input",
                "stage": "TPR_AGE_GROUP"
            }

    else:
        return {
            "response": f"Unknown action: {action}",
            "status": "error",
            "stage": "UNKNOWN"
        }
