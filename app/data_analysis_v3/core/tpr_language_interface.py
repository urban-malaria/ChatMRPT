"""
Simplified language interface for TPR workflow command extraction.

Uses LLM to extract workflow commands from natural language while preserving
flexibility. Supports variations like "primary", "Let's go with primary", etc.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

logger = logging.getLogger(__name__)


@dataclass
class SlotResolution:
    value: Optional[str]
    confidence: float
    rationale: Optional[str] = None

    @property
    def is_confident(self) -> bool:
        return self.value is not None and self.confidence >= 0.6


@dataclass
class IntentResult:
    """Lightweight container for workflow intent classification."""

    intent: str
    confidence: float
    rationale: str
    extracted_value: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "intent": self.intent,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "extracted_value": self.extracted_value,
        }

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)


class TPRLanguageInterface:
    """Helper for extracting workflow commands from natural language."""

    def __init__(self, session_id: str, *, model: str = "gpt-4o-mini") -> None:
        self.session_id = session_id
        self.model_name = model
        self._llm = self._init_llm()
        self._command_prompt: Optional[ChatPromptTemplate] = None
        self._slot_prompt: Optional[ChatPromptTemplate] = None
        self._intent_prompt: Optional[ChatPromptTemplate] = None

    def _init_llm(self) -> Optional[ChatOpenAI]:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.warning("OPENAI_API_KEY not found; TPR language interface will operate without LLM")
            return None
        try:
            return ChatOpenAI(
                model=self.model_name,
                api_key=api_key,
                temperature=0,
                timeout=30,
                max_tokens=300,
            )
        except Exception as exc:
            logger.error(f"Failed to initialise TPR language model: {exc}")
            return None

    # ------------------------------------------------------------------
    # Command extraction (replaces 7-intent classification)
    # ------------------------------------------------------------------

    def extract_command(
        self,
        message: str,
        stage: str,
        valid_options: List[str],
        context: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Extract workflow command from natural language.

        Returns command token (e.g., 'primary', 'o5') or None if not a command.
        Uses LLM to handle variations like "Let's go with primary" → 'primary'.

        Args:
            message: User's input message
            stage: Current workflow stage
            valid_options: List of valid command options for this stage
            context: Optional context dict

        Returns:
            Command token or None
        """
        if not message or not valid_options:
            return None

        message_lower = message.lower().strip()

        # Detect negations (e.g., "NOT primary")
        negation_patterns = [
            'not ', "don't", 'dont', "doesn't", 'no ', 'nope',
            'anything except', 'anything but', 'exclude'
        ]
        has_negation = any(pattern in message_lower for pattern in negation_patterns)

        if has_negation:
            logger.info(f"🚫 Negation detected in '{message}' - not extracting command")
            return None

        # Fast path: Check for exact matches (~20ms)
        exact_match = self._check_exact_match(message, valid_options)
        if exact_match:
            logger.info(f"⚡ Exact match: '{message}' → '{exact_match}' (stage={stage})")
            return exact_match

        # Fuzzy fallback even without LLM (handles "primarry" → "primary")
        fuzzy_match = self._fuzzy_match_option(message_lower, valid_options)
        if fuzzy_match:
            logger.info(f"🎯 Fuzzy match without LLM: '{message}' → '{fuzzy_match}'")
            return fuzzy_match

        # Flexible path: Use LLM to extract command from natural language (~2s)
        if not self._llm:
            logger.warning(f"❌ No LLM available and no exact match for '{message}'")
            return None

        return self._llm_extract_command(message, stage, valid_options, context)

    def _check_exact_match(self, message: str, valid_options: List[str]) -> Optional[str]:
        """Fast path: Check if message exactly matches a valid option."""
        clean = message.lower().strip()
        cleaned = re.sub(r'[^a-z0-9 ]', '', clean).strip()

        # Check exact matches
        for option in valid_options:
            if cleaned == option.lower():
                return option

        # Check common synonyms for age groups
        age_synonyms = {
            'under 5': 'u5',
            'under5': 'u5',
            'under five': 'u5',
            'over 5': 'o5',
            'over5': 'o5',
            'over five': 'o5',
            'pregnant': 'pw',
            'pregnant women': 'pw'
        }

        synonym_match = age_synonyms.get(cleaned)
        if synonym_match and synonym_match in valid_options:
            return synonym_match

        return None

    def _llm_extract_command(
        self,
        message: str,
        stage: str,
        valid_options: List[str],
        context: Optional[Dict[str, Any]]
    ) -> Optional[str]:
        """Use LLM to extract command from natural language."""

        if self._command_prompt is None:
            self._command_prompt = ChatPromptTemplate.from_messages([
                (
                    "system",
                    """You extract workflow commands from user messages in a Test Positivity Rate (TPR) analysis workflow.

The user is making a selection from valid options. Your job is to extract the EXACT option code from their natural language message.

**Option codes and their meanings:**
- **u5** = under 5 years old, under-fives, children under five, under 5
- **o5** = over 5 years old, over-fives, above five years, over 5
- **pw** = pregnant women, pregnancy, pregnant
- **primary** = primary health facilities, primary level, PHC
- **secondary** = secondary health facilities, secondary level, general hospitals
- **tertiary** = tertiary health facilities, tertiary level, teaching hospitals
- **all** = all options, everything, complete, total

**Task:** Extract the option code from the user's message. Return the CODE (like "u5"), not the full phrase (not "under 5 years").

**Examples:**
- "primary" → "primary"
- "Let's go with primary" → "primary"
- "I want primary facilities" → "primary"
- "under 5 years" → "u5"
- "let's go with the recommended under 5 years" → "u5"
- "over five years" → "o5"
- "pregnant women" → "pw"
- "Can we do secondary?" → "secondary"
- "all of them" → "all"
- "explain the differences" → null (question, not selection)
- "tell me about variables" → null (information request)
- "plot distribution" → null (analysis request)

**Valid options for this request:** {valid_options}
**Current stage:** {stage}
**User message:** {message}

Respond with JSON: {{"command": str or null, "rationale": short explanation}}"""
                ),
                (
                    "user",
                    "Stage: {stage}\nValid options: {valid_options}\nMessage: {message}"
                ),
            ])

        try:
            logger.info(f"🤖 LLM extracting command from: '{message}' (stage={stage})")
            logger.info(f"   Valid options: {valid_options}")

            llm_with_json = self._llm.bind(response_format={"type": "json_object"})
            reply = llm_with_json.invoke(
                self._command_prompt.format_messages(
                    stage=stage or "unknown",
                    message=message or "",
                    valid_options=valid_options,
                    context=json.dumps(context or {}, ensure_ascii=False)[:500]
                )
            )

            payload = json.loads(reply.content)
            proposed_command = payload.get("command")
            rationale = payload.get("rationale", "")

            if not proposed_command:
                logger.info(f"🤖 LLM determined not a command: {rationale}")
                return None

            # Normalize and validate against valid_options
            proposed_lower = str(proposed_command).strip().lower()

            # STEP 1: Try exact match first (fast path)
            for option in valid_options:
                if proposed_lower == option.lower():
                    logger.info(f"✅ LLM extracted command (exact): '{message}' → '{option}' ({rationale})")
                    return option

            # STEP 2: Try fuzzy match for typos (handles "primarry" → "primary")
            logger.info(f"🔍 No exact match for '{proposed_command}', trying fuzzy matching...")
            best_match = None
            best_similarity = 0.0

            for option in valid_options:
                similarity = SequenceMatcher(None, proposed_lower, option.lower()).ratio()
                logger.debug(f"   '{proposed_lower}' vs '{option.lower()}': {similarity:.2%} similar")
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = option

            # Accept if similarity >= 80%
            if best_similarity >= 0.8:
                logger.info(f"✅ LLM extracted command (fuzzy): '{message}' → '{best_match}' (similarity={best_similarity:.2%}, {rationale})")
                return best_match

            logger.warning(f"⚠️ LLM returned '{proposed_command}' but closest match '{best_match}' is only {best_similarity:.2%} similar (threshold: 80%)")
            return None

        except Exception as exc:
            logger.error(f"❌ Command extraction failed: {exc}", exc_info=True)
            return None

    def _fuzzy_match_option(self, message_lower: str, valid_options: List[str], threshold: float = 0.8) -> Optional[str]:
        """Fuzzy match helper for typo-tolerant command detection."""
        if not message_lower or not valid_options:
            return None

        tokens = re.findall(r'[a-z0-9]+', message_lower)
        search_space = tokens + [message_lower]

        best_match = None
        best_score = 0.0

        for candidate in search_space:
            for option in valid_options:
                score = SequenceMatcher(None, candidate, option.lower()).ratio()
                if score > best_score:
                    best_score = score
                    best_match = option

        if best_match and best_score >= threshold:
            logger.debug(f"Fuzzy matched '{message_lower}' → '{best_match}' (score={best_score:.2%})")
            return best_match

        return None

    # ------------------------------------------------------------------
    # Context update methods (kept for API compatibility)
    # ------------------------------------------------------------------

    def update_available_states(self, states: Iterable[str]) -> None:
        """Update available states for better slot resolution."""
        pass

    def update_from_dataframe(self, df: Any) -> None:
        """Extract available states from dataframe."""
        pass

    def update_from_metadata(self, metadata: Dict[str, Any]) -> None:
        """Extract available states from metadata."""
        pass

    # ------------------------------------------------------------------
    # Slot resolution (kept - still useful for fuzzy matching)
    # ------------------------------------------------------------------

    def resolve_slot(self, *, slot_type: str, message: str, choices: Iterable[str]) -> SlotResolution:
        """Normalise user input for a specific slot (state, facility level, age group)."""
        choices = list(dict.fromkeys(str(choice).strip() for choice in choices if choice))
        if not choices or not message:
            return SlotResolution(value=None, confidence=0.0)

        # KEYWORD-FIRST: Check for exact matches before calling LLM (fast path ~20ms)
        keyword_set = {choice.lower().strip() for choice in choices}
        message_clean = message.lower().strip()
        cleaned = re.sub(r'[^a-z0-9 ]', '', message_clean).strip()
        if cleaned and cleaned in keyword_set:
            matched = next(choice for choice in choices if choice.lower() == cleaned)
            logger.info(f"✅ Exact keyword match: '{message}' → '{matched}' (confidence=1.0)")
            return SlotResolution(value=matched, confidence=1.0, rationale='Exact match')

        # No exact match, try LLM resolution (flexible path ~2-5s)
        logger.info(f"🤖 No exact match for '{message}', attempting LLM resolution...")
        logger.info(f"   Slot type: {slot_type}")
        logger.info(f"   Choices: {choices}")

        if not self._llm:
            logger.warning(f"❌ No LLM available and no exact match for '{message}' in {choices}")
            return SlotResolution(value=None, confidence=0.0)

        if self._slot_prompt is None:
            self._slot_prompt = ChatPromptTemplate.from_messages([
                (
                    "system",
                    "You map user replies to canonical workflow choices. "
                    "The user's message may contain the choice word somewhere in their sentence. "
                    "Extract the most likely choice from their message. "
                    "Respond with JSON {{\"choice\": str or null, \"confidence\": float 0-1, \"rationale\": short text}}."
                ),
                (
                    "user",
                    "Slot: {slot_type}\nChoices: {choices}\nMessage: {message}"
                ),
            ])

        try:
            logger.info(f"🔄 Calling LLM with model: {self.model_name}")
            llm_with_json = self._llm.bind(response_format={"type": "json_object"})
            reply = llm_with_json.invoke(
                self._slot_prompt.format_messages(
                    slot_type=slot_type,
                    choices=choices,
                    message=message
                )
            )
            logger.info(f"📥 LLM raw response: {reply.content[:500]}")

            payload = json.loads(reply.content)
            proposed = payload.get("choice")
            confidence = float(payload.get("confidence", 0.0) or 0.0)
            rationale = payload.get("rationale")

            logger.info(f"📊 LLM parsed result:")
            logger.info(f"   Proposed choice: {proposed}")
            logger.info(f"   Confidence: {confidence}")
            logger.info(f"   Rationale: {rationale}")

            value = None
            if proposed:
                proposed_norm = str(proposed).strip().lower()
                logger.info(f"🔍 Normalizing proposed '{proposed}' → '{proposed_norm}'")
                for opt in choices:
                    if proposed_norm == str(opt).strip().lower():
                        value = opt
                        logger.info(f"✅ LLM resolution successful: '{message}' → '{value}' (confidence={confidence})")
                        break
                if not value:
                    logger.warning(f"⚠️ LLM proposed '{proposed}' but it doesn't match any choice in {choices}")
            else:
                logger.warning(f"⚠️ LLM returned null choice (confidence={confidence})")

            return SlotResolution(value=value, confidence=confidence, rationale=rationale)
        except Exception as exc:
            logger.error(f"❌ Slot resolution exception ({slot_type}): {exc}", exc_info=True)
            return SlotResolution(value=None, confidence=0.0)

    # ------------------------------------------------------------------
    # Intent classification (NEW - for intent-first architecture)
    # ------------------------------------------------------------------

    def classify_intent(
        self,
        message: str,
        stage: str,
        valid_options: List[str],
        context: Optional[Dict[str, Any]] = None
    ) -> IntentResult:
        """Classify user intent for the TPR workflow."""

        def _result(intent: str, confidence: float, rationale: str, extracted: Optional[str] = None) -> IntentResult:
            return IntentResult(
                intent=intent,
                confidence=max(0.0, min(confidence, 1.0)),
                rationale=rationale,
                extracted_value=extracted,
            )

        if not message:
            return _result("question", 1.0, "Empty message")

        message_clean = message.strip()
        message_lower = message_clean.lower()
        valid_set = {opt.lower(): opt for opt in (valid_options or [])}

        logger.info(f"🎯 Classifying intent for: '{message_clean}' (stage={stage})")
        logger.info(f"   Valid options: {valid_options}")

        # CRITICAL: Check non-selection intents FIRST before calling extract_command()
        # This prevents false positives like "List all columns" → extracting "all" as selection

        # Step 1: Check navigation keywords
        navigation_keywords = {
            "back", "go back", "previous", "prev", "exit", "quit", "cancel", "menu",
            "restart", "start over", "main menu", "status", "summary"
        }

        if any(message_lower == kw or message_lower.startswith(f"{kw} ") for kw in navigation_keywords):
            return _result("navigation", 0.9, "Detected navigation keyword")

        # Step 2: Define intent keyword lists (before extract_command!)
        question_starters = (
            "what", "why", "how", "when", "where", "which", "who", "can",
            "could", "would", "should", "do ", "does", "did"
        )

        analysis_keywords = (
            "analy", "analysis", "analyze", "trend", "chart", "plot", "graph", "visual",
            "calculate", "compute", "comparison", "correl", "distribution", "histogram",
            "heatmap", "statistics", "stats"
        )
        data_keywords = (
            "data", "dataset", "column", "row", "value", "table", "record", "entries",
            "fields", "variables", "list", "show", "display", "view"
        )
        info_keywords = (
            "explain", "info", "information", "detail", "difference", "options", "describe",
            "meaning", "definition", "what is", "what are"
        )

        # Step 3: Check for non-selection intents BEFORE trying to extract commands
        # This fixes "List all columns" being misinterpreted as selecting "all"

        if any(kw in message_lower for kw in analysis_keywords):
            logger.info(f"🔍 Detected analysis keyword in: '{message_clean}'")
            return _result("analysis_request", 0.75, "Detected analysis keyword")

        if any(kw in message_lower for kw in data_keywords):
            logger.info(f"🔍 Detected data keyword in: '{message_clean}'")
            return _result("data_inquiry", 0.7, "Detected data exploration keyword")

        if any(kw in message_lower for kw in info_keywords):
            logger.info(f"🔍 Detected info keyword in: '{message_clean}'")
            return _result("information_request", 0.7, "Detected information request keyword")

        # Step 3b: Check question phrasing BEFORE extract_command so that messages like
        # "What does o5 mean?" never reach LLM extraction and get mis-classified as selections.
        if message_lower.endswith("?") or message_lower.startswith(question_starters):
            logger.info(f"🔍 Detected question phrasing before extraction: '{message_clean}'")
            return _result("question", 0.8, "Detected question phrasing before extraction")

        # Step 4: ONLY NOW try to extract selection commands
        # This happens AFTER checking for data/analysis/info intents
        extracted = None
        if valid_options:
            extracted = self.extract_command(
                message=message_clean,
                stage=stage,
                valid_options=valid_options,
                context=context
            )

        if extracted:
            normalized = valid_set.get(extracted.lower(), extracted) if isinstance(extracted, str) else extracted
            logger.info(f"✅ Extracted selection after keyword checks: '{normalized}'")
            return _result("selection", 0.95, f"Detected selection '{normalized}'", normalized)

        if message_lower in valid_set:
            normalized = valid_set[message_lower]
            return _result("selection", 0.9, "Exact match to valid option", normalized)

        if message_lower.endswith("?") or message_lower.startswith(question_starters):
            return _result("question", 0.7, "Detected question phrasing")

        if not self._llm:
            logger.warning("❌ No LLM available for intent classification")
            return _result("question", 0.5, "No LLM available - defaulting to question")

        if self._intent_prompt is None:
            self._intent_prompt = ChatPromptTemplate.from_messages([
                (
                    "system",
                    """You classify messages during the Test Positivity Rate workflow.

Return the intent that best matches the user's goal:
- "selection" – choosing one of the valid options
- "information_request" – asking about options or workflow guidance
- "data_inquiry" – asking about the uploaded data (columns, values, etc.)
- "analysis_request" – requesting calculations, charts, or analytical work
- "navigation" – asking to go back, exit, restart, or view status
- "question" – other questions not covered above
- "general" – statements with unclear intent

If the message contains a valid selection, set intent to "selection" and include the selected option.

Respond with JSON: {{"intent": str, "confidence": 0-1, "rationale": str, "selection": str or null}}"""
                ),
                (
                    "user",
                    "Stage: {stage}\nValid options: {valid_options}\nContext: {context}\nMessage: {message}"
                ),
            ])

        try:
            llm_with_json = self._llm.bind(response_format={"type": "json_object"})
            reply = llm_with_json.invoke(
                self._intent_prompt.format_messages(
                    stage=stage,
                    valid_options=valid_options,
                    context=json.dumps(context or {}, ensure_ascii=False)[:500],
                    message=message_clean
                )
            )

            payload = json.loads(reply.content)
            intent = payload.get("intent", "question")
            confidence = float(payload.get("confidence", 0.0) or 0.0)
            rationale = payload.get("rationale", "")
            selection = payload.get("selection") or payload.get("extracted_value")

            if intent == "selection":
                if selection:
                    selection_lower = selection.lower()
                    if selection_lower in valid_set:
                        selection = valid_set[selection_lower]
                else:
                    logger.info("LLM classified selection without value; attempting extraction")
                    selection = self.extract_command(message_clean, stage, valid_options, context)

            logger.info("🎯 Intent classification result:")
            logger.info(f"   Intent: {intent}")
            logger.info(f"   Confidence: {confidence:.2f}")
            logger.info(f"   Rationale: {rationale}")
            if selection:
                logger.info(f"   Selection: {selection}")

            return _result(intent, confidence, rationale, selection)

        except Exception as exc:
            logger.error(f"❌ Intent classification failed: {exc}", exc_info=True)
            return _result("question", 0.5, f"Error: {str(exc)}")


__all__ = [
    "TPRLanguageInterface",
    "SlotResolution",
    "IntentResult",
]
