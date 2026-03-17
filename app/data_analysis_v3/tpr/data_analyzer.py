"""
TPR Data Analyzer

Analyzes TPR data to provide rich contextual information at each decision point.
Generates statistics for states, facilities, and age groups to help users make informed choices.

Schema inference is LLM-driven: one call per dataset reads the file raw (header=None),
asks the model to identify the header row and map ALL semantic columns (structural +
age/test-method), then re-reads with the correct header. No keyword fallbacks.
"""

import json
import logging
import os
import re

import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)


class TPRDataAnalyzer:
    """
    Analyzes TPR data to generate contextual statistics for workflow decisions.
    """

    def __init__(self):
        self.data = None
        self.analysis_cache = {}
        self._schema: Optional[Dict[str, Optional[str]]] = None  # set once per instance

    # ------------------------------------------------------------------
    # Public entry point: infer schema + read file correctly
    # ------------------------------------------------------------------

    def infer_schema_from_file(self, file_path: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Read the file raw (header=None), ask the LLM to identify the header row
        and map every semantic column, then re-read with the correct header.

        Returns (df, schema).  Sets self._schema so all analyze_* methods
        work without further LLM calls.

        Raises RuntimeError if inference fails (API down, no key, etc.) so the
        caller can surface a clear error rather than silently producing wrong results.
        """
        from app.data_analysis_v3.core.encoding_handler import EncodingHandler

        # Step 1 — raw read (no header assumptions)
        try:
            if file_path.lower().endswith('.csv'):
                raw = pd.read_csv(file_path, header=None, nrows=15,
                                  encoding='utf-8', on_bad_lines='skip')
            else:
                raw = pd.read_excel(file_path, header=None, nrows=15)
        except Exception as exc:
            raise RuntimeError(f"Cannot read file for schema inference: {exc}") from exc

        # Step 2 — fix encoding artifacts in raw cell values so the LLM sees the
        # same column names that EncodingHandler will produce after re-reading
        # (e.g. raw bytes show ">=5" but after fix it becomes "≥5")
        raw = raw.apply(
            lambda col: col.map(
                lambda x: EncodingHandler.fix_text_encoding(str(x)) if isinstance(x, str) else x
            )
        )

        # Step 3 — LLM determines structure + column mapping
        parsed = self._call_llm_schema(raw, file_path)

        # Step 4 — re-read with the correct header row
        header_row = int(parsed.get('header_row', 0))
        try:
            if file_path.lower().endswith('.csv'):
                df = EncodingHandler.read_csv_with_encoding(file_path)
            else:
                df = EncodingHandler.read_excel_with_encoding(file_path, header=header_row)
        except Exception as exc:
            raise RuntimeError(f"Cannot re-read file with header_row={header_row}: {exc}") from exc

        # Step 5 — validate every mapped column actually exists in df
        # Preserve header_row so callers can re-read the file with the same offset.
        validated: Dict[str, Any] = {'header_row': header_row}

        # Build a normalized lookup: replace ≥/≤ with >=/<=  so LLM output
        # "Persons tested positive... >=5" matches the actual column "...≥5".
        def _norm(s: str) -> str:
            return s.replace('≥', '>=').replace('≤', '<=') if s else s

        actual_cols_norm = {_norm(c): c for c in df.columns}

        for field, col in parsed.items():
            if field == 'header_row':
                continue
            if not col or col in ('null', 'None', None):
                validated[field] = None
            elif col in df.columns:
                validated[field] = col
            elif _norm(col) in actual_cols_norm:
                # LLM returned ">=" but actual column has "≥" (or vice-versa)
                actual = actual_cols_norm[_norm(col)]
                logger.info("Fuzzy-matched '%s' → '%s' (≥/>= normalisation)", col, actual)
                validated[field] = actual
            else:
                # Last resort: substring match — handles LLM truncating long column names.
                # Disambiguate using field semantics: o5 should prefer "excl PW";
                # pw should prefer "preg women"; if still ambiguous, drop.
                norm_col = _norm(col)
                candidates = [(norm_a, a) for norm_a, a in actual_cols_norm.items()
                              if norm_col in norm_a]
                if len(candidates) == 1:
                    logger.info("Substring-matched '%s' → '%s'", col, candidates[0][1])
                    validated[field] = candidates[0][1]
                elif len(candidates) > 1:
                    # Disambiguate by field type
                    if 'pw_' in field:
                        pref = [(na, a) for na, a in candidates if 'preg women' in na.lower()]
                    elif 'o5_' in field:
                        pref = [(na, a) for na, a in candidates if 'excl pw' in na.lower() or 'excl. pw' in na.lower()]
                    else:
                        pref = []
                    chosen = pref[0][1] if len(pref) == 1 else None
                    if chosen:
                        logger.info("Disambiguated substring-match '%s' → '%s' (field=%s)", col, chosen, field)
                        validated[field] = chosen
                    else:
                        logger.warning(
                            "Ambiguous substring match for field '%s' col '%s': %s — dropping",
                            field, col, [a for _, a in candidates]
                        )
                        validated[field] = None
                else:
                    logger.warning("Schema field '%s' → '%s' not found in columns — dropping", field, col)
                    validated[field] = None

        self._schema = validated
        logger.info(
            "Schema inferred from %s: %s",
            os.path.basename(file_path),
            {k: v for k, v in validated.items() if v},
        )
        return df, validated

    def _call_llm_schema(self, raw: pd.DataFrame, file_path: str) -> Dict[str, Any]:
        """Send the raw preview to gpt-4o-mini and get back the full schema dict."""
        api_key = self._get_openai_api_key()
        if not api_key:
            raise RuntimeError("No OpenAI API key — cannot infer column schema")

        import openai

        raw_preview = raw.to_string(max_cols=40, max_colwidth=80)

        # Build a full (untruncated) listing of values per row so the LLM can
        # read complete column names even when the table preview wraps long text.
        row_value_lines = []
        for i in range(len(raw)):
            vals = [str(v) for v in raw.iloc[i].tolist()
                    if pd.notna(v) and str(v).strip() and str(v).lower() != 'nan']
            if vals:
                row_value_lines.append(f"  Row {i}: {vals}")
        row_values_section = "\n".join(row_value_lines)

        prompt = (
            "You are analyzing a Nigerian malaria surveillance dataset.\n"
            "It may be a DHIS2 export, HMIS data, NMEP-processed Excel, or a custom format.\n\n"
            f"The file was read with header=None. Here are the first {len(raw)} rows (0-indexed):\n\n"
            f"{raw_preview}\n\n"
            "Full cell values per row (use these for EXACT column names — no truncation):\n"
            f"{row_values_section}\n\n"
            "TASK:\n"
            "1. Find the row index that contains the actual column headers "
            "(the row with recognizable field names, not blank, not data values).\n"
            "2. Using those column names, map every semantic field below to its EXACT column name.\n"
            "   Copy the column name character-for-character from the row values listed above.\n"
            "   Return null for fields not present. Never invent or rename columns.\n\n"
            "CRITICAL DISTINCTION — read carefully:\n"
            "  'tested' fields = TOTAL people who received a test (the DENOMINATOR).\n"
            "    Column names contain phrases like 'tested by RDT', 'presenting with fever & tested'.\n"
            "  'positive' fields = people who tested POSITIVE (the NUMERATOR).\n"
            "    Column names contain phrases like 'tested positive', 'positive by RDT'.\n"
            "  These are ALWAYS different columns. The tested count ≥ the positive count.\n"
            "  NEVER map the same column to both a _tested and a _positive field.\n\n"
            "Semantic fields:\n"
            "- header_row: (integer) 0-indexed row that is the column header row\n"
            "- state: Nigerian state name\n"
            "- lga: Local Government Area\n"
            "- ward: Ward name\n"
            "- facility_name: Health facility name\n"
            "- facility_level: Facility tier/type (Primary / Secondary / Tertiary / PHC)\n"
            "- period: Reporting period (year, month, date, or period code)\n"
            "- u5_rdt_tested: TOTAL people tested by RDT — Under-5 children (denominator)\n"
            "- u5_rdt_positive: People who tested POSITIVE by RDT — Under-5 children (numerator)\n"
            "- o5_rdt_tested: TOTAL people tested by RDT — Over-5 / Adults excl. pregnant women (denominator)\n"
            "- o5_rdt_positive: People who tested POSITIVE by RDT — Over-5 / Adults (numerator)\n"
            "- pw_rdt_tested: TOTAL pregnant women tested by RDT (denominator)\n"
            "- pw_rdt_positive: Pregnant women who tested POSITIVE by RDT (numerator)\n"
            "- u5_microscopy_tested: TOTAL tested by Microscopy — Under-5 (denominator)\n"
            "- u5_microscopy_positive: Tested POSITIVE by Microscopy — Under-5 (numerator)\n"
            "- o5_microscopy_tested: TOTAL tested by Microscopy — Over-5 / Adults (denominator)\n"
            "- o5_microscopy_positive: Tested POSITIVE by Microscopy — Over-5 / Adults (numerator)\n"
            "- pw_microscopy_tested: TOTAL pregnant women tested by Microscopy (denominator)\n"
            "- pw_microscopy_positive: Pregnant women tested POSITIVE by Microscopy (numerator)\n\n"
            "Return JSON only, no explanation:\n"
            '{"header_row": 1, "state": "...", "lga": "...", "ward": "...", '
            '"facility_name": "...", "facility_level": "...", "period": "...", '
            '"u5_rdt_tested": "...", "u5_rdt_positive": "...", '
            '"o5_rdt_tested": "...", "o5_rdt_positive": "...", '
            '"pw_rdt_tested": "...", "pw_rdt_positive": "...", '
            '"u5_microscopy_tested": "...", "u5_microscopy_positive": "...", '
            '"o5_microscopy_tested": "...", "o5_microscopy_positive": "...", '
            '"pw_microscopy_tested": "...", "pw_microscopy_positive": "..."}'
        )

        client = openai.OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=900,
            response_format={"type": "json_object"},
        )

        raw_resp = response.choices[0].message.content
        parsed = json.loads(raw_resp)
        logger.info("LLM schema response: %s", parsed)
        return parsed

    def _get_openai_api_key(self) -> Optional[str]:
        try:
            from flask import current_app
            return current_app.config.get('OPENAI_API_KEY')
        except Exception:
            return os.environ.get('OPENAI_API_KEY')

    # ------------------------------------------------------------------
    # Schema accessors
    # ------------------------------------------------------------------

    def ensure_schema(self, df: pd.DataFrame) -> None:
        """No-op if schema was already set by infer_schema_from_file. Kept for
        compatibility — the schema must be set before calling analyze_* methods."""
        if self._schema is None:
            logger.warning(
                "ensure_schema called but no schema set — "
                "call infer_schema_from_file() first"
            )

    def _get_column(self, df: pd.DataFrame, field: str) -> Optional[str]:
        """Resolve a semantic field name to a DataFrame column name via schema."""
        col = (self._schema or {}).get(field)
        return col if col and col in df.columns else None

    # ------------------------------------------------------------------
    # Analyze methods
    # ------------------------------------------------------------------

    def analyze_states(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze available states in the data."""
        try:
            self.ensure_schema(df)

            state_col = self._get_column(df, 'state')
            facility_col = self._get_column(df, 'facility_name')

            if not state_col:
                logger.warning("No state column in schema")
                return {
                    'states': {},
                    'total_states': 0,
                    'recommended': None,
                    'state_column': None,
                    'state_column_detected': False,
                    'error': 'STATE_COLUMN_NOT_FOUND',
                }

            states_info = {}
            for state in df[state_col].dropna().unique():
                state_data = df[df[state_col] == state]
                states_info[str(state)] = {
                    'name': str(state),
                    'display_name': self._strip_dhis2_prefix(state),
                    'total_records': len(state_data),
                    'facilities': (
                        state_data[facility_col].nunique()
                        if facility_col else len(state_data)
                    ),
                    'total_tests': self._count_total_tests(state_data),
                    'data_completeness': self._calculate_completeness(state_data),
                }

            sorted_states = sorted(
                states_info.items(),
                key=lambda x: x[1]['total_tests'],
                reverse=True,
            )

            return {
                'states': dict(sorted_states),
                'total_states': len(states_info),
                'recommended': sorted_states[0][0] if sorted_states else None,
                'state_column': state_col,
                'state_column_detected': True,
            }

        except Exception as exc:
            logger.error("Error analyzing states: %s", exc)
            return {'error': str(exc), 'states': {}}

    def analyze_facility_levels(self, df: pd.DataFrame, state: str) -> Dict[str, Any]:
        """Analyze facility level distribution for a state."""
        try:
            self.ensure_schema(df)
            df = self._filter_data(df, state, 'all')

            level_col = self._get_column(df, 'facility_level')
            if not level_col:
                return {
                    'levels': {
                        'all': {
                            'name': 'All Facilities',
                            'count': len(df),
                            'percentage': 100,
                            'description': 'All healthcare facilities',
                            'recommended': False,
                        }
                    },
                    'has_levels': False,
                }

            s = self._schema or {}
            rdt_tested_keys = ['u5_rdt_tested', 'o5_rdt_tested', 'pw_rdt_tested']
            micro_tested_keys = ['u5_microscopy_tested', 'o5_microscopy_tested', 'pw_microscopy_tested']

            levels_info = {}
            total_records = len(df)

            for level in df[level_col].dropna().unique():
                level_data = df[df[level_col] == level]
                level_key = level.lower().replace(' ', '_')

                rdt_tests = sum(
                    self._col_sum(level_data, s.get(k)) for k in rdt_tested_keys
                )
                microscopy_tests = sum(
                    self._col_sum(level_data, s.get(k)) for k in micro_tested_keys
                )

                levels_info[level_key] = {
                    'name': level,
                    'count': len(level_data),
                    'percentage': round((len(level_data) / total_records) * 100, 1),
                    'urban_percentage': 0,
                    'rural_percentage': 0,
                    'description': self._get_facility_description(level),
                    'rdt_tests': int(rdt_tests),
                    'microscopy_tests': int(microscopy_tests),
                }

            primary_found = False
            for key, info in levels_info.items():
                if 'primary' in info['name'].lower():
                    info['recommended'] = True
                    primary_found = True
                else:
                    info['recommended'] = False

            levels_info['all'] = {
                'name': 'All Facilities',
                'count': total_records,
                'percentage': 100,
                'description': 'Complete coverage across all facility types',
                'recommended': False,
            }

            return {
                'levels': levels_info,
                'has_levels': True,
                'total_facilities': total_records,
                'state_name': state,
            }

        except Exception as exc:
            logger.error("Error analyzing facility levels: %s", exc)
            return {'error': str(exc), 'levels': {}}

    def analyze_age_groups(self, df: pd.DataFrame, state: str, facility_level: str) -> Dict[str, Any]:
        """Analyze age group data availability and positivity rates."""
        try:
            df = self._filter_data(df, state, facility_level)
            s = self._schema or {}

            # Schema key mapping per age group
            age_meta = {
                'u5': {
                    'name': 'Under 5 Years',
                    'description': 'Highest risk group for severe malaria',
                    'icon': '👶',
                    'rdt_tested': 'u5_rdt_tested',
                    'rdt_positive': 'u5_rdt_positive',
                    'microscopy_tested': 'u5_microscopy_tested',
                    'microscopy_positive': 'u5_microscopy_positive',
                },
                'o5': {
                    'name': 'Over 5 Years',
                    'description': 'Community transmission patterns',
                    'icon': '👥',
                    'rdt_tested': 'o5_rdt_tested',
                    'rdt_positive': 'o5_rdt_positive',
                    'microscopy_tested': 'o5_microscopy_tested',
                    'microscopy_positive': 'o5_microscopy_positive',
                },
                'pw': {
                    'name': 'Pregnant Women',
                    'description': 'Special vulnerable population',
                    'icon': '🤰',
                    'rdt_tested': 'pw_rdt_tested',
                    'rdt_positive': 'pw_rdt_positive',
                    'microscopy_tested': 'pw_microscopy_tested',
                    'microscopy_positive': 'pw_microscopy_positive',
                },
            }

            age_groups_info = {}

            for key, meta in age_meta.items():
                rdt_tests = self._col_sum(df, s.get(meta['rdt_tested']))
                rdt_positives = self._col_sum(df, s.get(meta['rdt_positive']))
                micro_tests = self._col_sum(df, s.get(meta['microscopy_tested']))
                micro_positives = self._col_sum(df, s.get(meta['microscopy_positive']))

                total_tests = rdt_tests + micro_tests
                total_positives = rdt_positives + micro_positives

                positivity_rate = (total_positives / total_tests * 100) if total_tests > 0 else 0
                rdt_tpr = (rdt_positives / rdt_tests * 100) if rdt_tests > 0 else 0
                micro_tpr = (micro_positives / micro_tests * 100) if micro_tests > 0 else 0

                test_cols = [c for c in [s.get(meta['rdt_tested']), s.get(meta['microscopy_tested'])]
                             if c and c in df.columns]
                facilities_reporting = max(
                    (df[c].notna().sum() for c in test_cols), default=0
                )

                age_groups_info[key] = {
                    'name': meta['name'],
                    'total_tests': int(total_tests),
                    'total_positive': int(total_positives),
                    'positivity_rate': round(positivity_rate, 1),
                    'facilities_reporting': int(facilities_reporting),
                    'description': meta['description'],
                    'icon': meta['icon'],
                    'has_data': total_tests > 0,
                    'rdt_tests': int(rdt_tests),
                    'rdt_tpr': round(rdt_tpr, 1),
                    'microscopy_tests': int(micro_tests),
                    'microscopy_tpr': round(micro_tpr, 1),
                }

            # Mark recommended group
            if age_groups_info.get('u5', {}).get('has_data'):
                age_groups_info['u5']['recommended'] = True
            elif age_groups_info.get('o5', {}).get('has_data'):
                age_groups_info['o5']['recommended'] = True

            total_tests_all = sum(v.get('total_tests', 0) for v in age_groups_info.values())

            return {
                'age_groups': age_groups_info,
                'total_tests': total_tests_all,
                'state': state,
                'facility_level': facility_level,
            }

        except Exception as exc:
            logger.error("Error analyzing age groups: %s", exc)
            return {'error': str(exc), 'age_groups': {}}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _col_sum(df: pd.DataFrame, col: Optional[str]) -> float:
        """Sum a column if it exists and is numeric, else 0."""
        if col and col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            return float(df[col].fillna(0).sum())
        return 0.0

    def _count_total_tests(self, df: pd.DataFrame) -> int:
        """Sum all test columns from the schema."""
        s = self._schema or {}
        test_keys = [
            'u5_rdt_tested', 'o5_rdt_tested', 'pw_rdt_tested',
            'u5_microscopy_tested', 'o5_microscopy_tested', 'pw_microscopy_tested',
        ]
        return int(sum(self._col_sum(df, s.get(k)) for k in test_keys))

    def _filter_data(self, df: pd.DataFrame, state: str, facility_level: str) -> pd.DataFrame:
        """Filter data by state and facility level using schema columns."""
        result = df.copy()

        if state:
            state_col = self._get_column(df, 'state')
            if state_col and state in df[state_col].values:
                result = result[result[state_col] == state]

        if facility_level and facility_level != 'all':
            level_col = self._get_column(df, 'facility_level')
            if level_col:
                if facility_level in result[level_col].values:
                    result = result[result[level_col] == facility_level]
                elif result[level_col].astype(str).str.lower().eq(facility_level.lower()).any():
                    result = result[result[level_col].astype(str).str.lower() == facility_level.lower()]

        return result

    @staticmethod
    def _strip_dhis2_prefix(value: str) -> str:
        """Strip two-letter DHIS2 state prefixes (e.g. 'kw Kwara State' → 'Kwara')."""
        cleaned = re.sub(r'^[a-z]{2}\s+', '', str(value), flags=re.IGNORECASE)
        cleaned = re.sub(r'\s+State$', '', cleaned, flags=re.IGNORECASE)
        return cleaned.strip()

    def _calculate_completeness(self, df: pd.DataFrame) -> float:
        if df.empty:
            return 0.0
        total_cells = len(df) * len(df.columns)
        return round((df.count().sum() / total_cells) * 100, 1) if total_cells else 0.0

    def _get_facility_description(self, level: str) -> str:
        level_lower = level.lower()
        descriptions = {
            'primary': 'Community-level care, first point of contact',
            'secondary': 'District hospitals with broader services',
            'tertiary': 'Specialized teaching hospitals',
            'phc': 'Primary Health Centers serving local communities',
            'general': 'General hospitals with comprehensive services',
            'teaching': 'Teaching hospitals with specialized care',
            'clinic': 'Small health clinics',
            'dispensary': 'Basic health dispensaries',
        }
        for key, desc in descriptions.items():
            if key in level_lower:
                return desc
        return 'Healthcare facilities'

    def generate_recommendation(self, analysis: Dict[str, Any], stage: str) -> str:
        if stage == 'state' and 'recommended' in analysis:
            return f"💡 Tip: {analysis['recommended']} has the most complete data"
        elif stage == 'facility':
            return "💡 Tip: 'Primary' facilities are recommended for community-level insights"
        elif stage == 'age':
            return "💡 Tip: 'Under 5' is the recommended age group for highest malaria risk"
        return ""
