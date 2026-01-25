"""Routing logic for analysis chat requests."""

from __future__ import annotations

import os
import logging

from . import logger

__all__ = ["route_with_mistral"]


async def route_with_mistral(message: str, session_context: dict) -> str:
    """Use Mistral to decide whether to call tools, Arena, or request clarification."""
    message_lower = message.lower().strip()
    common_greetings = [
        'hi',
        'hello',
        'hey',
        'greetings',
        'good morning',
        'good afternoon',
        'good evening',
        'howdy',
    ]

    if message_lower in common_greetings or any(message_lower.startswith(g) for g in common_greetings):
        return "can_answer"

    if message_lower in ['thanks', 'thank you', 'bye', 'goodbye', 'ok', 'okay', 'sure', 'yes', 'no']:
        return "can_answer"

    if session_context.get('use_data_analysis_v3', False) or session_context.get('data_analysis_active', False):
        logger.info("🎯 Data Analysis V3 mode detected - routing to agent with tools")
        return "needs_tools"

    explicit_analysis_triggers = [
        'run the malaria risk analysis',
        'run malaria risk analysis',
        'malaria risk analysis',
        'run risk analysis',
        'risk analysis',
        'run complete analysis',
    ]
    if any(trigger in message_lower for trigger in explicit_analysis_triggers):
        logger.info("Tool detection: Explicit analysis trigger found (ungated) - routing to tools")
        return "needs_tools"

    if session_context.get('has_uploaded_files', False):
        analysis_triggers = [
            'run the malaria risk analysis',
            'run malaria risk analysis',
            'malaria risk analysis',
            'risk analysis',
            'malaria analysis',
            'run malaria analysis',
            'run vulnerability analysis',
            'run complete analysis',
            'run analysis',
            'perform analysis',
            'analyze the data',
            'analyze my data',
            'start analysis',
            'complete analysis',
            'run the analysis',
            'rank wards',
        ]
        if any(trigger in message_lower for trigger in analysis_triggers) or (
            'risk' in message_lower and 'analysis' in message_lower
        ):
            logger.info("Tool detection: Analysis trigger found - routing to tools")
            return "needs_tools"

        visualization_keywords = ['plot', 'map', 'chart', 'visualize', 'graph', 'show me the', 'display']
        if any(keyword in message_lower for keyword in visualization_keywords):
            if 'vulnerability' in message_lower and ('map' in message_lower or 'plot' in message_lower):
                logger.info("Tool detection: Vulnerability map trigger - routing to tools")
                return "needs_tools"
            if 'distribution' in message_lower:
                logger.info("Tool detection: Distribution visualization trigger - routing to tools")
                return "needs_tools"
            if any(word in message_lower for word in ['box plot', 'boxplot', 'histogram', 'scatter', 'heatmap', 'bar chart']):
                logger.info("Tool detection: Chart type trigger - routing to tools")
                return "needs_tools"
            if any(keyword in message_lower for keyword in ['plot', 'map', 'chart', 'visualize']):
                logger.info("Tool detection: General visualization trigger - routing to tools")
                return "needs_tools"

        ranking_triggers = [
            'top',
            'highest',
            'lowest',
            'rank',
            'list wards',
            'worst',
            'best',
            'most at risk',
            'least at risk',
            'high risk wards',
            'low risk wards',
        ]
        if any(trigger in message_lower for trigger in ranking_triggers) and 'ward' in message_lower:
            logger.info("Tool detection: Ranking query trigger - routing to tools")
            return "needs_tools"

        data_query_triggers = [
            'check data quality',
            'data quality',
            'check quality',
            'summarize the data',
            'summary of data',
            'data summary',
            'describe the data',
            'what variables',
            'available variables',
        ]
        if any(trigger in message_lower for trigger in data_query_triggers):
            logger.info("Tool detection: Data query trigger - routing to tools")
            return "needs_tools"

        intervention_triggers = [
            'bed net',
            'bednet',
            'bed-net',
            'itn',
            'intervention',
            'plan distribution',
            'insecticide',
            'spray',
            'irs',
            'treatment',
            'mosquito net',
            'llin',
            'distribute net',
            'distributing net',
            'net distribution',
            'nets distribution',
            'distribution of net',
            'allocate net',
            'allocation of net',
            'plan itn',
            'itn planning',
            'itn distribution',
            'distribute itn',
            'plan high trend',
            'high trend distribution',
            'trend distribution',
        ]
        if any(trigger in message_lower for trigger in intervention_triggers):
            logger.info("Tool detection: Intervention planning trigger (ITN) - routing to tools")
            return "needs_tools"

        itn_param_patterns = [
            ('have' in message_lower and 'net' in message_lower and any(char.isdigit() for char in message)),
            ('household size' in message_lower and any(char.isdigit() for char in message)),
            ('average household' in message_lower and any(char.isdigit() for char in message)),
            (any(word in message_lower for word in ['million', 'thousand', 'hundred']) and 'net' in message_lower),
            ('i have' in message_lower and any(word in message_lower for word in ['nets', 'bed nets', 'bednets']) and any(char.isdigit() for char in message)),
        ]
        if any(pattern for pattern in itn_param_patterns):
            logger.info("Tool detection: ITN parameter response detected - routing to tools")
            return "needs_tools"

        if 'why' in message_lower and 'ward' in message_lower and (
            'rank' in message_lower or 'high' in message_lower or 'risk' in message_lower
        ):
            logger.info("Tool detection: Ward analysis explanation trigger - routing to tools")
            return "needs_tools"

    try:
        from app.core.llm_adapter import LLMAdapter
    except ImportError as exc:  # pragma: no cover - defensive
        logger.warning("LLM adapter unavailable: %s", exc)
        return "needs_clarification"

    files_info = []
    if session_context.get('has_uploaded_files'):
        if session_context.get('csv_loaded'):
            files_info.append("CSV data")
        if session_context.get('shapefile_loaded'):
            files_info.append("Shapefile")
        if session_context.get('analysis_complete'):
            files_info.append("Analysis completed")
    files_str = (
        f"Uploaded files: {', '.join(files_info)}" if files_info else "No files uploaded"
    )

    prompt = f"""You are a routing assistant for ChatMRPT, a malaria risk analysis system.

AVAILABLE CAPABILITIES:

1. TOOLS (require uploaded data to function):
   - Analysis Tools: RunMalariaRiskAnalysis
     Purpose: Analyze uploaded malaria data, calculate risk scores, identify high-risk areas
   - Visualization Tools: CreateVulnerabilityMap, CreateBoxPlot, CreateHistogram, CreateHeatmap
     Purpose: Generate maps and charts from uploaded data
   - Export Tools: ExportResults, GenerateReport
     Purpose: Export analysis results to PDF/Excel
   - Data Query Tools: CheckDataQuality, GetSummaryStatistics
     Purpose: Query and examine uploaded data

2. KNOWLEDGE RESPONSES (no data needed):
   - Explain malaria concepts (transmission, epidemiology, prevention)
   - Describe analysis methodologies (PCA, composite scoring, risk assessment)
   - ChatMRPT help and guidance
   - General public health information
   - Answer "what is", "how does", "explain" type questions

Context:
- User has uploaded data: {session_context.get('has_uploaded_files', False)}
- {files_str}

User message: "{message}"

ROUTING DECISION PROCESS:

1. Does the user want to PERFORM AN ACTION on their uploaded data?
   Keywords: analyze, plot, visualize, calculate, generate, create, run, export, check, perform
   → If YES and data exists: Reply NEEDS_TOOLS
   
2. Does the user want INFORMATION or EXPLANATION?
   Keywords: what is, how does, explain, tell me about, describe, why
   → Reply CAN_ANSWER (even if data exists - they want knowledge, not action)

3. Is the message explicitly about their uploaded data?
   Phrases: "my data", "the data", "my file", "the csv", "uploaded"
   → If asking for action: Reply NEEDS_TOOLS
   → If asking for explanation: Reply CAN_ANSWER

Reply ONLY: NEEDS_TOOLS, CAN_ANSWER, or NEEDS_CLARIFICATION"""

    if os.getenv('CHATMRPT_MISTRAL_ROUTER', '0') == '0':
        if session_context.get('use_data_analysis_v3', False) or session_context.get('data_analysis_active', False):
            return "needs_tools"
        if session_context.get('has_uploaded_files', False):
            return "needs_tools"
        return "can_answer"

    try:
        # Use Groq for fast routing (synchronous call in async context)
        adapter = LLMAdapter(backend='groq', model='llama-3.3-70b-versatile')
        response = adapter.generate(
            prompt=prompt,
            max_tokens=20,
            temperature=0.1,
        )
        decision = response.strip().upper()
        if decision == "NEEDS_TOOLS":
            return "needs_tools"
        if decision == "CAN_ANSWER":
            return "can_answer"
        if decision == "NEEDS_CLARIFICATION":
            return "needs_clarification"
        logger.warning("Unexpected Mistral response: %s. Using fallback logic.", decision)
    except Exception as exc:  # pragma: no cover - network failure
        logger.error("Error in Mistral routing: %s. Using neutral fallback.", exc)
        return "needs_clarification"

    message_lower = message.lower().strip()
    data_references = [
        'my data',
        'the data',
        'uploaded',
        'my file',
        'the file',
        'my csv',
        'the csv',
        'analyze this',
        'plot my',
        'visualize the',
    ]
    if any(ref in message_lower for ref in data_references):
        return "needs_tools"
    return "can_answer"
