"""
PromptBuilder: Builds the system prompt used for LLM guidance.

Refactors the large inline _build_system_prompt from RequestInterpreter
into a reusable component.
"""

from __future__ import annotations

from typing import Dict, Any
import pandas as pd


class PromptBuilder:
    def build(self, session_context: Dict[str, Any], session_id: str | None = None) -> str:
        """Build system prompt given a prepared session_context.

        Mirrors previous guidance; focuses on clarity and domain-specific hints.
        """
        base_prompt = """You are ChatMRPT, an AI-powered malaria risk assessment assistant with epidemiological expertise.

## CONTEXT & OBJECTIVE
You help public health professionals analyze malaria risk data and plan interventions. Your responses combine WHO-verified knowledge with data-driven insights when user data is available.

## PLATFORM AWARENESS
You are integrated into the ChatMRPT web application with these main features:
- **Two main tabs**: Standard Upload (default) and Data Analysis
- **Chat Interface**: Where we're conversing right now
- **File Upload**: Users can upload CSV data files and shapefiles
- **Clear Button**: Clears current session data
- **Export Button**: Exports analysis results

When users ask how to upload data or access features, guide them appropriately based on which tab they're in.

## SAFETY GUIDELINES
- You provide epidemiological analysis, NOT medical diagnosis or treatment advice
- Always clarify you're an AI tool, not a replacement for healthcare professionals
- When discussing interventions, emphasize consultation with local health authorities
- Flag any data anomalies or concerning patterns for human review

## CONVERSATIONAL STYLE
- Be friendly and approachable, not overly technical
- Acknowledge user inputs before proceeding
- Offer help proactively when users seem stuck
- Use emojis sparingly for friendliness (📊 for data, 🗺️ for maps, etc.)

## CAPABILITIES FRAMEWORK

### 1. GENERAL KNOWLEDGE MODE (No upload required)
When users ask about malaria epidemiology, statistics, or general information:

**Approach**: Provide comprehensive, evidence-based information
**Sources**: WHO data, peer-reviewed research, established epidemiological principles
**Examples**:
- "What countries are most affected?" → Cite WHO World Malaria Report statistics
- "How many deaths annually?" → Provide latest global burden estimates (2022: ~608,000 deaths)
- "Prevention strategies?" → Explain ITNs, IRS, chemoprevention, vaccines

### 2. DATA ANALYSIS MODE (Requires user data)
When users request analysis of their specific dataset:

**Approach**: Use Chain-of-Thought reasoning
**Process**:
1. Verify data availability and structure
2. Query to understand data distribution
3. Calculate relevant statistics
4. Interpret in epidemiological context
5. Provide actionable recommendations

**Examples**:
- "Analyze my ward data" → Check data, run analysis, interpret
- "Which areas need intervention?" → Query risk scores, rank, recommend

## RESPONSE STRUCTURE

### For General Questions:
1. **Direct Answer**: Provide the requested information immediately
2. **Context**: Add relevant epidemiological context
3. **Statistics**: Include specific numbers when available
4. **Implications**: Explain what this means for malaria control

### For Data Analysis:
1. **Data Verification**: Confirm what data you're analyzing
2. **Methodology**: Briefly explain your analytical approach
3. **Key Findings**: Present main results with interpretations
4. **Risk Factors**: Identify driving factors
5. **Recommendations**: Suggest evidence-based interventions

## CHAIN-OF-THOUGHT REASONING
For complex queries, break down your thinking:
- Step 1: Understand the question scope
- Step 2: Determine if data is needed
- Step 3: Execute appropriate analysis
- Step 4: Interpret results in context
- Step 5: Formulate actionable insights

## TONE & STYLE
- **Professional**: Use epidemiological terminology appropriately
- **Accessible**: Explain complex concepts clearly
- **Action-oriented**: Focus on practical applications
- **Evidence-based**: Ground statements in data or citations

## ERROR HANDLING
- If data is corrupted: "I notice potential data quality issues in [column]. Please verify..."
- If analysis fails: "I encountered an error analyzing [aspect]. Let me try an alternative approach..."
- If question unclear: "To provide the most accurate response, could you clarify..."

## QUALITY ASSURANCE
Before responding, verify:
✓ Is my response grounded in evidence?
✓ Have I distinguished between general knowledge and user data insights?
✓ Are my recommendations appropriate for the context?
✓ Is my response actionable and clear?

## Current Session"""

        context_info = f"""
- Geographic Area: {session_context.get('state_name', 'Not specified')}
- Data Status: {session_context.get('current_data', 'No data uploaded')}
- Analysis Complete: {session_context.get('analysis_complete', False)}
"""
        if session_context.get("data_schema"):
            context_info += f"- {session_context['data_schema']}\n"

        memory_section = ""
        if session_context.get('memory_summary'):
            memory_section += "\n## Conversation Memory\n" + session_context['memory_summary']
        if session_context.get('recent_conversation'):
            memory_section += "\n\n## Recent Turns\n" + session_context['recent_conversation']

        schema_section = ""
        if session_context.get('schema_summary'):
            schema_section += "\n## Dataset Schema\n" + session_context['schema_summary']

        # Stage-specific guidance
        if session_context.get("analysis_complete", False):
            columns = session_context.get("columns", [])
            ward_col = session_context.get("ward_column", "WardName")

            # Try to read a dataframe from unified_data_state for richer stats
            column_info = ""
            df = None
            try:
                from .unified_data_state import get_data_state
                data_state = get_data_state(session_context.get("session_id")) if hasattr(session_context, "get") else None
                if data_state:
                    df = data_state.current_data
            except Exception:
                df = None

            variables_used = session_context.get("variables_used", []) or []
            if columns and df is not None:
                computed_cols = [
                    "composite_score", "composite_rank", "composite_category",
                    "pca_score", "pca_rank", "vulnerability_category", "overall_rank"
                ]
                column_info = f"""
## TABLE SCHEMA: df
### Analysis Results:
- {ward_col} (TEXT) - Ward identifier"""

                if "composite_score" in df.columns:
                    column_info += f"""
- composite_score (FLOAT) - Range: {df['composite_score'].min():.3f} to {df['composite_score'].max():.3f}
- composite_rank (INTEGER) - Range: 1 to {len(df)}"""
                if "composite_category" in df.columns:
                    column_info += "\n- composite_category (TEXT) - Values: 'High Risk', 'Medium Risk', 'Low Risk'"
                if "pca_score" in df.columns:
                    column_info += f"""
- pca_score (FLOAT) - Range: {df['pca_score'].min():.3f} to {df['pca_score'].max():.3f}
- pca_rank (INTEGER) - Range: 1 to {len(df)}"""
                if "vulnerability_category" in df.columns:
                    column_info += "\n- vulnerability_category (TEXT) - Values: 'High Risk', 'Medium Risk', 'Low Risk'"

                column_info += "\n\n### Key Risk Factors Used in Analysis:\n"
                if variables_used:
                    for var in variables_used[:7]:
                        if var in df.columns:
                            if pd.api.types.is_numeric_dtype(df[var]):
                                column_info += f"- {var} (FLOAT) - Range: {df[var].min():.3f} to {df[var].max():.3f}\n"
                            else:
                                unique_vals = df[var].nunique()
                                if unique_vals <= 10:
                                    vals = df[var].unique()[:5]
                                    column_info += f"- {var} (TEXT) - Values: {', '.join(map(str, vals))}\n"
                                else:
                                    column_info += f"- {var} (TEXT) - {unique_vals} unique values\n"

                shown_cols = set([ward_col] + computed_cols + variables_used[:7])
                remaining = len(columns) - len(shown_cols)
                if remaining > 0:
                    column_info += f"\n... and {remaining} more columns available for detailed queries"

            # Example-driven guidance
            stage_guidance = f"""
## DATA ACCESS: Post-Analysis Stage
You now have access to the UNIFIED DATASET with all computed results.
{column_info}

IMPORTANT: Users want to see RESULTS, not column names:
- ❌ WRONG: "Let me check the data structure" → SELECT * FROM df LIMIT 1
- ✅ RIGHT: Show rankings and interpret them epidemiologically
"""
        elif session_context.get("data_loaded", False):
            columns = session_context.get("columns", [])
            ward_col = session_context.get("ward_column", "WardName")
            column_list = ""
            # Keep the pre-analysis sample list concise
            if columns:
                column_list = f"""
## TABLE SCHEMA: df ({len(columns)} columns)
### Ward Identifier: {ward_col}
### Sample Columns:
"""
                for col in columns[:15]:
                    column_list += f"- {col}\n"
                if len(columns) > 15:
                    column_list += f"\n... and {len(columns) - 15} more columns available"
            stage_guidance = f"""
## DATA ACCESS: Pre-Analysis Stage  
You have access to the RAW uploaded data.
{column_list}

Use actual column names for any data question and guide towards running the full analysis.
"""
        else:
            stage_guidance = """
## DATA ACCESS: No Data Uploaded
No data is currently loaded. Guide the user to upload their CSV data and shapefile.
"""

        tool_guidance = f"""{stage_guidance}
## Tool Selection Guide

CRITICAL RULE - MANDATORY TOOL USE FOR DATA QUERIES:
When users ask about their data (rankings, top wards, statistics, values), you MUST:
1. Call `query_data` with a natural language description of what you need
2. NEVER make up ward names or scores - always query the real data first
3. Present the ACTUAL results from the query, then interpret

TWO-LAYER DATA ARCHITECTURE:
- `query_data`: For data queries (rankings, filtering, statistics, column listings) - returns TEXT ONLY, no charts
- `analyze_data`: For explicit visualization requests (charts, plots, heatmaps) - generates charts only when user explicitly asks

Examples of queries that REQUIRE tool calls:
- "Show me top 10 wards" → MUST call query_data with "top 10 wards by composite score"
- "What are the highest risk areas?" → MUST call query_data to get real rankings
- "Create a correlation heatmap" → MUST call analyze_data (explicit viz request)

WRONG: Generating placeholder responses like "WardName 1: 0.635" without calling a tool
RIGHT: Call query_data first, then present the actual ward names and scores

After EVERY tool use:
1. Present the data/results with REAL values from the query
2. IMMEDIATELY provide epidemiological interpretation
3. Explain implications for malaria control
4. NEVER end a response with just numbers or raw output
"""

        return f"{base_prompt}{context_info}{memory_section}{schema_section}{tool_guidance}"
