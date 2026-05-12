"""
Universal Visualization Explanation Service - Data-Driven Approach

Generates LLM-powered explanations for visualizations by reading the underlying
data files and sending rich statistical context to the LLM.  No headless browser
or image conversion required.

For each visualization type the service:
1. Reads the relevant CSV/state files from the session folder
2. Computes key statistics (means, extremes, distributions, top/bottom wards)
3. Builds a visualization-type-specific prompt with real numbers
4. Calls the LLM to produce a concrete, data-referenced explanation
"""

import json
import os
import logging
from typing import Dict, Any, Optional

import pandas as pd
from flask import current_app

logger = logging.getLogger(__name__)

UNSUPPORTED_CAUSAL_TERMS = {
    'sanitation': ['sanitation', 'latrine', 'waste'],
    'healthcare_access': ['healthcare access', 'health care access', 'facility access', 'infrastructure'],
    'stagnant_water': ['stagnant water', 'breeding site', 'mosquito breeding'],
    'socioeconomic': ['socioeconomic', 'poverty', 'income'],
}

# ---------------------------------------------------------------------------
# Visualization type metadata - what each type shows and which data to read
# ---------------------------------------------------------------------------
VIZ_TYPE_INFO = {
    'tpr_map': {
        'label': 'Malaria Burden Distribution Map',
        'description': 'a choropleth map showing ward-level malaria burden (cases per 1,000 population)',
        'data_files': ['raw_data.csv', 'tpr_results.csv'],
        'metric_col': 'Burden',
    },
    'vulnerability_map_composite': {
        'label': 'Composite Vulnerability Classification Map',
        'description': 'a map classifying wards by malaria vulnerability using a composite scoring method that combines multiple risk models',
        'data_files': ['unified_dataset.csv', 'raw_data.csv'],
        'metric_col': 'composite_score',
    },
    'vulnerability_map_pca': {
        'label': 'PCA Vulnerability Classification Map',
        'description': 'a map classifying wards by malaria vulnerability using Principal Component Analysis (PCA)',
        'data_files': ['unified_dataset.csv', 'raw_data.csv'],
        'metric_col': 'pca_score',
    },
    'composite_score_maps': {
        'label': 'Individual Model Score Maps',
        'description': 'a set of choropleth maps showing each individual risk model\'s scores across wards',
        'data_files': ['unified_dataset.csv'],
        'metric_col': 'composite_score',
    },
    'box_plot': {
        'label': 'Ward Vulnerability Box Plot Ranking',
        'description': 'horizontal box plots showing the distribution of model scores for each ward, ranked by overall vulnerability',
        'data_files': ['unified_dataset.csv'],
        'metric_col': 'composite_score',
    },
    'variable_distribution': {
        'label': 'Variable Distribution Map',
        'description': 'a choropleth map showing the spatial distribution of an environmental or risk variable across wards',
        'data_files': ['unified_dataset.csv', 'raw_data.csv'],
        'metric_col': None,  # determined from filename
    },
    'itn_map': {
        'label': 'ITN Distribution Planning Map',
        'description': 'a map showing insecticide-treated net (ITN) allocation across wards based on vulnerability ranking and population',
        'data_files': ['unified_dataset.csv'],
        'metric_col': 'composite_score',
    },
    'urban_extent_map': {
        'label': 'Urban Extent Map',
        'description': 'a map showing ward vulnerability filtered by urbanisation level, highlighting differences between urban and rural areas',
        'data_files': ['unified_dataset.csv'],
        'metric_col': 'urbanPercentage',
    },
    'settlement_map': {
        'label': 'Settlement Pattern Map',
        'description': 'a map showing settlement patterns and their relationship to malaria risk',
        'data_files': ['unified_dataset.csv', 'raw_data.csv'],
        'metric_col': None,
    },
    'decision_tree': {
        'label': 'Analysis Decision Tree',
        'description': 'a diagram showing how variables were selected and processed to produce the vulnerability ranking',
        'data_files': ['unified_dataset.csv'],
        'metric_col': None,
    },
}


class UniversalVisualizationExplainer:
    """
    Data-driven explanation service for visualizations.

    Instead of converting HTML to screenshots, reads the underlying data
    and sends rich statistical context to the LLM for explanation.
    """

    def __init__(self, llm_manager=None):
        self.llm_manager = llm_manager

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------
    def explain_visualization(
        self, viz_path: str, viz_type: str = None, session_id: str = None
    ) -> str:
        """Generate a data-driven explanation for a visualization."""
        if not self.llm_manager:
            return "ERROR: LLM manager not available for explanations"

        viz_type = self._normalize_viz_type(viz_type, viz_path)
        logger.info(
            "Explaining visualization: type=%s path=%s session=%s",
            viz_type, viz_path, session_id,
        )

        try:
            # 1. Build rich data context from session files
            ctx = self._build_data_context(session_id, viz_type, viz_path)

            # 2. Build a type-specific prompt with real data
            prompt = self._build_prompt(viz_type, ctx)
            hardened = current_app.config.get("ENABLE_HARDENED_EXPLAIN", True)

            # 3. Call LLM
            if hardened:
                system_message = (
                    "You are a malaria epidemiologist embedded in the ChatMRPT analysis system. "
                    "Explain only from the data context provided by ChatMRPT. "
                    "Separate observed evidence from interpretation. "
                    "Do not state causes or drivers unless the variable is explicitly listed in the provided evidence. "
                    "If a useful explanation would require field validation or external data, label it as a caveat. "
                    "Reference actual ward names, LGA names, ranks, scores, coverage, and counts when provided. "
                    "Use the requested structured headings and stay concise."
                )
            else:
                system_message = (
                    "You are a malaria epidemiologist embedded in the ChatMRPT analysis system. "
                    "Explain the visualization clearly and specifically using the data provided. "
                    "Reference actual ward names, LGA names, and numbers. "
                    "Be concise and focus on actionable insights for public health officials."
                )

            explanation = self.llm_manager.generate_response(
                prompt=prompt,
                system_message=system_message,
                session_id=session_id,
            )

            if not explanation or explanation.strip().upper().startswith('ERROR'):
                return "ERROR: LLM failed to generate explanation"

            if hardened:
                explanation = self._remove_unsupported_claims(explanation, ctx)
            return explanation

        except Exception as e:
            logger.error("Error explaining visualization: %s", e, exc_info=True)
            return f"ERROR: {e}"

    # ------------------------------------------------------------------
    # Data context builder
    # ------------------------------------------------------------------
    def _build_data_context(
        self, session_id: Optional[str], viz_type: str, viz_path: Optional[str]
    ) -> Dict[str, Any]:
        """Build rich context from session data files."""
        ctx: Dict[str, Any] = {
            'viz_type': viz_type,
            'source_file': os.path.basename(viz_path) if viz_path else None,
            'state_name': None,
            'facility_level': None,
            'age_group': None,
            'allowed_evidence': [],
            'variables_used': [],
        }

        if not session_id:
            return ctx

        sess_dir = self._get_session_dir(session_id)
        if not sess_dir:
            return ctx
        ctx['_session_dir'] = sess_dir

        # Read workflow metadata (state, facility, age group)
        self._read_workflow_metadata(sess_dir, ctx)

        # Read the primary data file for this viz type
        df = self._read_data_file(sess_dir, viz_type)
        if df is None or df.empty:
            ctx['error'] = 'No data file found for this session'
            return ctx

        ctx['total_wards'] = len(df)
        ctx['columns'] = list(df.columns)

        # Extract LGA info
        lga_col = self._find_column(df, ['LGA', 'LGAName', 'lga'])
        ward_col = self._find_column(df, ['WardName', 'ward_name', 'Ward'])
        if lga_col:
            ctx['lga_count'] = int(df[lga_col].nunique())
            ctx['lga_names'] = df[lga_col].dropna().unique().tolist()[:10]

        ctx['allowed_evidence'] = self._allowed_evidence_from_columns(df.columns)

        # Dispatch to viz-type-specific extractors
        extractors = {
            'tpr_map': self._extract_tpr_context,
            'vulnerability_map_composite': self._extract_vulnerability_context,
            'vulnerability_map_pca': self._extract_vulnerability_context,
            'composite_score_maps': self._extract_composite_models_context,
            'box_plot': self._extract_composite_models_context,
            'variable_distribution': self._extract_variable_context,
            'itn_map': self._extract_itn_context,
            'urban_extent_map': self._extract_urban_context,
            'decision_tree': self._extract_decision_tree_context,
        }

        extractor = extractors.get(viz_type, self._extract_generic_context)
        extractor(df, ctx, ward_col, lga_col, viz_path)

        return ctx

    # ------------------------------------------------------------------
    # Viz-type-specific extractors
    # ------------------------------------------------------------------
    def _extract_tpr_context(
        self, df: pd.DataFrame, ctx: dict, ward_col: str, lga_col: str,
        viz_path: str = None
    ):
        """Extract burden/TPR map context."""
        burden_col = self._find_column(df, ['Burden', 'burden', 'TPR', 'tpr'])
        pop_col = self._find_column(df, ['Population', 'population'])
        pos_col = self._find_column(df, ['Total_Positive', 'total_positive'])

        if burden_col:
            series = pd.to_numeric(df[burden_col], errors='coerce')
            ctx['metric_name'] = 'Malaria Burden (per 1,000)'
            ctx['mean'] = round(float(series.mean(skipna=True)), 1)
            ctx['median'] = round(float(series.median(skipna=True)), 1)
            ctx['min'] = round(float(series.min(skipna=True)), 1)
            ctx['max'] = round(float(series.max(skipna=True)), 1)

            if ward_col:
                top5 = df.nlargest(5, burden_col)
                ctx['top_5_wards'] = [
                    {'ward': r[ward_col], 'value': round(float(r[burden_col]), 1)}
                    for _, r in top5.iterrows()
                ]
                bottom5 = df[series > 0].nsmallest(5, burden_col)
                ctx['bottom_5_wards'] = [
                    {'ward': r[ward_col], 'value': round(float(r[burden_col]), 1)}
                    for _, r in bottom5.iterrows()
                ]

            if lga_col:
                lga_avg = df.groupby(lga_col)[burden_col].mean().sort_values(ascending=False)
                ctx['top_3_lgas'] = [
                    {'lga': name, 'avg_burden': round(float(val), 1)}
                    for name, val in lga_avg.head(3).items()
                ]

        if pop_col:
            ctx['total_population'] = int(pd.to_numeric(df[pop_col], errors='coerce').sum())
        if pos_col:
            ctx['total_positive'] = int(pd.to_numeric(df[pos_col], errors='coerce').sum())

    def _extract_vulnerability_context(
        self, df: pd.DataFrame, ctx: dict, ward_col: str, lga_col: str,
        viz_path: str = None
    ):
        """Extract vulnerability map context (composite or PCA)."""
        is_pca = 'pca' in (ctx.get('viz_type') or '')
        ctx['map_scope'] = self._infer_map_scope(viz_path)

        score_col = self._find_column(
            df, ['pca_score', 'pca_rank'] if is_pca else ['composite_score', 'composite_rank']
        )
        cat_col = self._find_column(
            df, ['pca_category'] if is_pca else ['composite_category']
        )
        rank_col = self._find_column(
            df, ['pca_rank'] if is_pca else ['composite_rank']
        )

        ctx['method'] = 'PCA' if is_pca else 'Composite'
        ctx['ranking_basis'] = (
            'PCA score/rank' if is_pca else 'Composite score/rank from selected malaria risk variables'
        )
        risk_vars = self._risk_factor_columns(df)
        ctx['variables_used'] = risk_vars[:12]

        if cat_col:
            counts = df[cat_col].value_counts().to_dict()
            ctx['category_distribution'] = counts

        if rank_col and ward_col:
            ranked = df.dropna(subset=[rank_col]).sort_values(rank_col)
            ctx['most_vulnerable'] = [
                {'ward': r[ward_col], 'rank': int(r[rank_col])}
                for _, r in ranked.head(5).iterrows()
            ]
            ctx['least_vulnerable'] = [
                {'ward': r[ward_col], 'rank': int(r[rank_col])}
                for _, r in ranked.tail(5).iterrows()
            ]
            ctx['total_ranked'] = len(ranked)

        if score_col:
            scores = pd.to_numeric(df[score_col], errors='coerce')
            ctx['score_mean'] = round(float(scores.mean(skipna=True)), 3)
            ctx['score_range'] = {
                'min': round(float(scores.min()), 3),
                'max': round(float(scores.max()), 3),
            }

        if lga_col and rank_col:
            lga_risk = df.groupby(lga_col)[rank_col].mean().sort_values()
            ctx['highest_risk_lgas'] = [
                {'lga': name, 'avg_rank': round(float(val), 1)}
                for name, val in lga_risk.head(3).items()
            ]

    def _extract_composite_models_context(
        self, df: pd.DataFrame, ctx: dict, ward_col: str, lga_col: str,
        viz_path: str = None
    ):
        """Extract context for composite score maps and box plots."""
        model_cols = [c for c in df.columns if c.startswith('model_')]
        ctx['model_count'] = len(model_cols)
        ctx['model_names'] = model_cols[:10]

        if model_cols:
            model_stats = {}
            for col in model_cols[:8]:
                series = pd.to_numeric(df[col], errors='coerce')
                model_stats[col] = {
                    'mean': round(float(series.mean(skipna=True)), 3),
                    'min': round(float(series.min(skipna=True)), 3),
                    'max': round(float(series.max(skipna=True)), 3),
                }
            ctx['model_stats'] = model_stats

        # Also extract overall vulnerability for box plot
        self._extract_vulnerability_context(df, ctx, ward_col, lga_col, viz_path)

    def _extract_variable_context(
        self, df: pd.DataFrame, ctx: dict, ward_col: str, lga_col: str,
        viz_path: str = None
    ):
        """Extract context for variable distribution maps."""
        # Try to identify the variable from the viz filename
        variable = None
        if viz_path:
            basename = os.path.basename(viz_path).lower()
            # Common variable names in filenames
            for var_name in [
                'rainfall', 'ndvi', 'ndwi', 'elevation', 'housing_quality',
                'urban_percentage', 'soil_wetness', 'distance_to_waterbodies',
                'urban_extent', 'burden', 'tpr', 'population',
            ]:
                if var_name in basename:
                    variable = var_name
                    break

        if variable:
            col = self._find_column(df, [variable, variable.title(), variable.replace('_', ' ').title()])
            if col:
                series = pd.to_numeric(df[col], errors='coerce')
                ctx['variable_name'] = variable.replace('_', ' ').title()
                ctx['variables_used'] = [col]
                ctx['mean'] = round(float(series.mean(skipna=True)), 2)
                ctx['median'] = round(float(series.median(skipna=True)), 2)
                ctx['min'] = round(float(series.min(skipna=True)), 2)
                ctx['max'] = round(float(series.max(skipna=True)), 2)
                ctx['std'] = round(float(series.std(skipna=True)), 2)

                if ward_col:
                    top3 = df.nlargest(3, col)
                    ctx['highest_wards'] = [
                        {'ward': r[ward_col], 'value': round(float(r[col]), 2)}
                        for _, r in top3.iterrows()
                    ]
        else:
            ctx['variable_name'] = 'Unknown variable'
            self._extract_generic_context(df, ctx, ward_col, lga_col, viz_path)

    def _extract_itn_context(
        self, df: pd.DataFrame, ctx: dict, ward_col: str, lga_col: str,
        viz_path: str = None
    ):
        """Extract ITN distribution map context."""
        nets_col = self._find_column(df, ['nets_allocated', 'Nets_Allocated'])
        coverage_col = self._find_column(df, ['coverage_percent', 'Coverage_Percent'])
        phase_col = self._find_column(df, ['allocation_phase', 'Allocation_Phase'])

        if phase_col:
            phase_counts = df[phase_col].value_counts().to_dict()
            ctx['allocation_phases'] = phase_counts
            ctx['wards_covered'] = int(df[phase_col].eq('Allocated').sum()) if 'Allocated' in df[phase_col].values else 0

        if nets_col:
            nets = pd.to_numeric(df[nets_col], errors='coerce')
            ctx['total_nets'] = int(nets.sum())
            ctx['avg_nets_per_ward'] = round(float(nets[nets > 0].mean()), 0) if (nets > 0).any() else 0

        if coverage_col:
            cov = pd.to_numeric(df[coverage_col], errors='coerce')
            ctx['avg_coverage'] = round(float(cov.mean(skipna=True)), 1)
            ctx['coverage_range'] = {
                'min': round(float(cov.min()), 1),
                'max': round(float(cov.max()), 1),
            }

        self._extract_saved_itn_results(ctx)

        # Also get vulnerability context
        self._extract_vulnerability_context(df, ctx, ward_col, lga_col, viz_path)

    def _extract_urban_context(
        self, df: pd.DataFrame, ctx: dict, ward_col: str, lga_col: str,
        viz_path: str = None
    ):
        """Extract urban extent map context."""
        urban_col = self._find_column(df, ['urbanPercentage', 'urban_percentage', 'Urban_Percentage'])
        if urban_col:
            ctx['variables_used'] = [urban_col]
            urban = pd.to_numeric(df[urban_col], errors='coerce')
            ctx['urban_mean'] = round(float(urban.mean(skipna=True)), 1)
            ctx['urban_median'] = round(float(urban.median(skipna=True)), 1)
            ctx['urban_wards'] = int((urban > 50).sum())
            ctx['rural_wards'] = int((urban <= 50).sum())

        # Also get vulnerability ranking
        self._extract_vulnerability_context(df, ctx, ward_col, lga_col, viz_path)

    def _extract_decision_tree_context(
        self, df: pd.DataFrame, ctx: dict, ward_col: str, lga_col: str,
        viz_path: str = None
    ):
        """Extract decision tree context."""
        model_cols = [c for c in df.columns if c.startswith('model_')]
        env_cols = [
            c for c in df.columns
            if c not in ['WardCode', 'StateCode', 'LGACode', 'WardName', 'LGA', 'State',
                         'GeopoliticalZone', 'geometry']
            and not c.startswith('model_')
            and not c.endswith('_rank') and not c.endswith('_category') and not c.endswith('_score')
        ]
        ctx['total_variables'] = len(env_cols)
        ctx['variables_used'] = env_cols[:10]
        ctx['model_count'] = len(model_cols)
        self._extract_vulnerability_context(df, ctx, ward_col, lga_col, viz_path)

    def _extract_generic_context(
        self, df: pd.DataFrame, ctx: dict, ward_col: str, lga_col: str,
        viz_path: str = None
    ):
        """Fallback: extract basic stats from whatever data is available."""
        numeric_cols = df.select_dtypes(include='number').columns.tolist()
        if numeric_cols:
            primary = numeric_cols[0]
            series = df[primary]
            ctx['primary_metric'] = primary
            ctx['mean'] = round(float(series.mean(skipna=True)), 2)
            ctx['min'] = round(float(series.min(skipna=True)), 2)
            ctx['max'] = round(float(series.max(skipna=True)), 2)

    def _extract_saved_itn_results(self, ctx: dict):
        """Add allocation-specific context from saved ITN results JSON."""
        sess_dir = ctx.get('_session_dir')
        if not sess_dir:
            return
        path = os.path.join(sess_dir, 'itn_distribution_results.json')
        if not os.path.exists(path):
            return

        try:
            with open(path, 'r') as handle:
                saved = json.load(handle)
        except Exception as exc:
            logger.debug("Could not read ITN result context %s: %s", path, exc)
            return

        stats = saved.get('stats') or saved.get('summary') or {}
        if stats:
            ctx['itn_stats'] = {
                'total_nets': stats.get('total_nets'),
                'allocated_nets': stats.get('allocated_nets') or stats.get('total_nets_allocated'),
                'remaining_nets': stats.get('remaining_nets'),
                'covered_population': stats.get('covered_population') or stats.get('population_covered'),
                'coverage_percent': stats.get('coverage_percent'),
                'prioritized_wards': stats.get('prioritized_wards'),
                'reprioritized_wards': stats.get('reprioritized_wards'),
            }

        prioritized = saved.get('prioritized') or saved.get('distribution') or []
        if prioritized:
            frame = pd.DataFrame(prioritized)
            ward_col = self._find_column(frame, ['WardName', 'ward_name', 'ward'])
            nets_col = self._find_column(frame, ['nets_allocated', 'Nets_Allocated'])
            rank_col = self._find_column(frame, ['overall_rank', 'composite_rank', 'rank'])
            pop_col = self._find_column(frame, ['Population', 'population'])
            coverage_col = self._find_column(frame, ['coverage_percent', 'Coverage_Percent'])

            sort_col = rank_col or nets_col
            if ward_col and sort_col:
                ranked = frame.sort_values(sort_col, ascending=True if rank_col else False).head(5)
                top_allocations = []
                for _, row in ranked.iterrows():
                    item = {'ward': row.get(ward_col)}
                    if rank_col and pd.notna(row.get(rank_col)):
                        item['rank'] = int(row.get(rank_col))
                    if nets_col and pd.notna(row.get(nets_col)):
                        item['nets_allocated'] = int(row.get(nets_col))
                    if pop_col and pd.notna(row.get(pop_col)):
                        item['population'] = int(row.get(pop_col))
                    if coverage_col and pd.notna(row.get(coverage_col)):
                        item['coverage_percent'] = round(float(row.get(coverage_col)), 1)
                    top_allocations.append(item)
                ctx['top_itn_allocations'] = top_allocations

    # ------------------------------------------------------------------
    # Prompt builder
    # ------------------------------------------------------------------
    def _build_prompt(self, viz_type: str, ctx: Dict[str, Any]) -> str:
        """Build a visualization-type-specific prompt with real data."""
        info = VIZ_TYPE_INFO.get(viz_type, {})
        label = info.get('label', viz_type.replace('_', ' ').title() if viz_type else 'Visualization')
        description = info.get('description', 'a data visualization from the malaria risk analysis')

        state = ctx.get('state_name') or 'the selected state'
        lines = [
            f"This is a **{label}** for **{state}** — {description}.",
            "",
            "**Grounding rules for your answer:**",
            "- Use only the evidence listed below.",
            "- Do not invent causes, drivers, field conditions, or service-access explanations.",
            "- If a causal claim would require data not listed below, put it under Caveats as needing field validation.",
            "- Use these exact headings: Key finding, Evidence from the data, Interpretation, Caveats, Suggested next actions.",
            "- Keep the explanation concise and action-oriented.",
        ]

        # Add facility/age context for TPR maps
        if viz_type == 'tpr_map':
            fac = ctx.get('facility_level')
            age = ctx.get('age_group')
            if fac or age:
                lines.append(f"Facility level: {fac or 'all'}, Age group: {age or 'all ages'}.")

        if ctx.get('error'):
            lines.append("")
            lines.append(f"Data availability note: {ctx['error']}")

        lines.append("")
        lines.append("**Key statistics from the underlying data:**")

        total = ctx.get('total_wards', 0)
        lga_count = ctx.get('lga_count', 0)
        if total:
            lines.append(f"- {total} wards analyzed across {lga_count} LGAs")

        if ctx.get('map_scope'):
            lines.append(f"- Map scope: {ctx['map_scope']}")
        if ctx.get('ranking_basis'):
            lines.append(f"- Ranking basis: {ctx['ranking_basis']}")
        if ctx.get('variables_used'):
            lines.append(f"- Variables available for interpretation: {', '.join(map(str, ctx['variables_used']))}")
        elif ctx.get('allowed_evidence'):
            lines.append(f"- Available evidence columns: {', '.join(map(str, ctx['allowed_evidence'][:12]))}")

        # Metric stats
        metric_name = ctx.get('metric_name', ctx.get('variable_name'))
        if metric_name and 'mean' in ctx:
            lines.append(
                f"- {metric_name}: mean {ctx['mean']}, median {ctx.get('median', 'N/A')}, "
                f"range {ctx.get('min', '?')} - {ctx.get('max', '?')}"
            )

        # Top/bottom wards
        for key, header in [
            ('top_5_wards', 'Highest burden wards'),
            ('bottom_5_wards', 'Lowest burden wards (non-zero)'),
            ('most_vulnerable', 'Most vulnerable wards'),
            ('least_vulnerable', 'Least vulnerable wards'),
            ('highest_wards', 'Highest value wards'),
        ]:
            items = ctx.get(key)
            if items:
                ward_strs = [
                    f"{w.get('ward', '?')} ({w.get('value', w.get('rank', '?'))})"
                    for w in items[:5]
                ]
                lines.append(f"- {header}: {', '.join(ward_strs)}")

        # LGA breakdown
        for key, header in [
            ('top_3_lgas', 'Highest burden LGAs (avg)'),
            ('highest_risk_lgas', 'Highest risk LGAs (avg rank)'),
        ]:
            items = ctx.get(key)
            if items:
                lga_strs = [
                    f"{l.get('lga', '?')} ({l.get('avg_burden', l.get('avg_rank', '?'))})"
                    for l in items
                ]
                lines.append(f"- {header}: {', '.join(lga_strs)}")

        # Category distribution
        cat_dist = ctx.get('category_distribution')
        if cat_dist:
            cat_str = ', '.join(f"{k}: {v}" for k, v in cat_dist.items())
            lines.append(f"- Vulnerability categories: {cat_str}")

        # Population / positives
        if ctx.get('total_population'):
            lines.append(f"- Total population: {ctx['total_population']:,}")
        if ctx.get('total_positive'):
            lines.append(f"- Total positive cases: {ctx['total_positive']:,}")

        # Urban context
        if ctx.get('urban_wards') is not None:
            lines.append(f"- Urban wards (>50% urbanised): {ctx['urban_wards']}, Rural wards: {ctx['rural_wards']}")

        # ITN context
        if ctx.get('total_nets'):
            lines.append(f"- Total nets allocated: {ctx['total_nets']:,}")
            lines.append(f"- Average nets per covered ward: {ctx.get('avg_nets_per_ward', 0):,.0f}")
            lines.append(f"- Wards receiving nets: {ctx.get('wards_covered', 0)}")
        if ctx.get('itn_stats'):
            stats = {k: v for k, v in ctx['itn_stats'].items() if v is not None}
            if stats:
                stat_str = ', '.join(f"{k}: {v:,}" if isinstance(v, int) else f"{k}: {v}" for k, v in stats.items())
                lines.append(f"- ITN allocation summary: {stat_str}")
        if ctx.get('top_itn_allocations'):
            alloc_strs = []
            for item in ctx['top_itn_allocations'][:5]:
                parts = [str(item.get('ward', '?'))]
                if item.get('rank') is not None:
                    parts.append(f"rank {item['rank']}")
                if item.get('nets_allocated') is not None:
                    parts.append(f"{item['nets_allocated']:,} nets")
                if item.get('coverage_percent') is not None:
                    parts.append(f"{item['coverage_percent']}% coverage")
                alloc_strs.append(" (".join([parts[0], ", ".join(parts[1:]) + ")"]) if len(parts) > 1 else parts[0])
            lines.append(f"- Top ITN allocations: {', '.join(alloc_strs)}")

        # Model info
        if ctx.get('model_count'):
            lines.append(f"- Risk models used: {ctx['model_count']}")

        # Method
        if ctx.get('method'):
            lines.append(f"- Analysis method: {ctx['method']}")

        lines.append("")
        lines.append("**Unsupported-claim guardrail:**")
        lines.append(
            "Unless these are explicitly listed in the evidence columns or variables above, do not mention "
            "sanitation, stagnant water, mosquito breeding sites, poverty, socioeconomic status, healthcare access, "
            "healthcare infrastructure, or facility access as factual drivers."
        )
        lines.append("")
        lines.append(
            "Now write the explanation using exactly these headings:\n"
            "## Key finding\n"
            "## Evidence from the data\n"
            "## Interpretation\n"
            "## Caveats\n"
            "## Suggested next actions"
        )

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_viz_type(viz_type: str | None, viz_path: str | None = None) -> str:
        """Normalize frontend/legacy visualization type aliases."""
        raw = (viz_type or '').lower().replace(' ', '_')
        path = (viz_path or '').lower()
        if raw in {'vulnerability_map', 'vulnerability', 'composite_map'}:
            return 'vulnerability_map_pca' if 'pca' in path else 'vulnerability_map_composite'
        if raw in {'pca_map', 'pca_vulnerability_map'}:
            return 'vulnerability_map_pca'
        if raw in {'itn_distribution', 'itn_distribution_map'}:
            return 'itn_map'
        if raw in {'variable_map', 'variable_distribution_map'}:
            return 'variable_distribution'
        return raw

    @staticmethod
    def _infer_map_scope(viz_path: str | None) -> str:
        name = os.path.basename(viz_path or '').lower()
        if 'multi_year' in name or 'all_years' in name:
            return (
                'Tabbed multi-year map. All Years is aggregate environmental vulnerability; '
                'individual year tabs blend environmental vulnerability with burden prioritization.'
            )
        if any(str(year) in name for year in range(2020, 2031)):
            return 'Single-year map; interpret as year-specific prioritization.'
        return 'Current analysis map for the active session.'

    @staticmethod
    def _allowed_evidence_from_columns(columns) -> list[str]:
        keep = []
        for col in columns:
            text = str(col)
            low = text.lower()
            if any(token in low for token in [
                'ward', 'lga', 'state', 'burden', 'tpr', 'positive', 'population',
                'rainfall', 'ndvi', 'ndwi', 'elevation', 'housing', 'urban',
                'score', 'rank', 'category', 'nets', 'coverage', 'allocation',
                'water', 'temperature', 'humidity', 'soil',
            ]):
                keep.append(text)
        return keep[:25]

    @staticmethod
    def _risk_factor_columns(df: pd.DataFrame) -> list[str]:
        excluded = {
            'wardcode', 'statecode', 'lgacode', 'wardname', 'lga', 'state',
            'geopoliticalzone', 'geometry',
        }
        risk_vars = []
        for col in df.columns:
            low = str(col).lower()
            if low in excluded:
                continue
            if low.startswith('model_') or low.endswith('_rank') or low.endswith('_category') or low.endswith('_score'):
                continue
            if low in {'overall_rank', 'composite_rank', 'pca_rank'}:
                continue
            if pd.api.types.is_numeric_dtype(df[col]):
                risk_vars.append(str(col))
        return risk_vars

    def _remove_unsupported_claims(self, explanation: str, ctx: Dict[str, Any]) -> str:
        """Remove common unsupported causal sentences from generated explanations."""
        evidence_text = ' '.join(
            str(value).lower()
            for value in (ctx.get('variables_used') or []) + (ctx.get('allowed_evidence') or [])
        )
        blocked_phrases = []
        for evidence_key, phrases in UNSUPPORTED_CAUSAL_TERMS.items():
            if evidence_key.replace('_', ' ') in evidence_text:
                continue
            if any(part in evidence_text for part in evidence_key.split('_')):
                continue
            blocked_phrases.extend(phrases)

        if not blocked_phrases:
            return explanation

        sentences = []
        removed = False
        for sentence in explanation.replace('\n', '\n ').split('. '):
            low = sentence.lower()
            if any(phrase in low for phrase in blocked_phrases):
                removed = True
                continue
            sentences.append(sentence.strip())

        cleaned = '. '.join(s for s in sentences if s)
        if removed:
            cleaned = cleaned.rstrip()
            if cleaned and not cleaned.endswith('.'):
                cleaned += '.'
            cleaned += (
                "\n\n## Caveats\n"
                "I removed unsupported causal language from the generated explanation. "
                "Drivers such as sanitation, stagnant water, socioeconomic conditions, or healthcare access "
                "should only be stated when those variables are present in the uploaded data or verified externally."
            )
            logger.warning(
                "explain_unsupported_claims_removed",
                extra={"viz_type": ctx.get("viz_type"), "source_file": ctx.get("source_file")},
            )
        return cleaned or explanation

    def _get_session_dir(self, session_id: str) -> Optional[str]:
        """Get the session upload directory path."""
        try:
            upload_folder = current_app.config.get('UPLOAD_FOLDER', 'instance/uploads')
            sess_dir = os.path.join(upload_folder, session_id)
            if os.path.isdir(sess_dir):
                return sess_dir
            # Also try instance path
            sess_dir = os.path.join(current_app.instance_path, 'uploads', session_id)
            if os.path.isdir(sess_dir):
                return sess_dir
        except RuntimeError:
            # Outside app context
            sess_dir = os.path.join('instance', 'uploads', session_id)
            if os.path.isdir(sess_dir):
                return sess_dir
        return None

    def _read_workflow_metadata(self, sess_dir: str, ctx: dict):
        """Read .agent_state.json and tpr_debug.json for workflow context."""
        for filename in ['.agent_state.json', 'tpr_debug.json']:
            path = os.path.join(sess_dir, filename)
            if os.path.exists(path):
                try:
                    with open(path, 'r') as f:
                        data = json.load(f)
                    # Extract common fields
                    selections = data.get('tpr_selections', data.get('selections', {}))
                    if selections:
                        ctx['state_name'] = ctx.get('state_name') or selections.get('state_name', selections.get('state'))
                        ctx['facility_level'] = ctx.get('facility_level') or selections.get('facility_level', selections.get('facility'))
                        ctx['age_group'] = ctx.get('age_group') or selections.get('age_group')
                except Exception as e:
                    logger.debug("Could not read %s: %s", filename, e)

    def _read_data_file(self, sess_dir: str, viz_type: str) -> Optional[pd.DataFrame]:
        """Read the most relevant data file for this visualization type."""
        info = VIZ_TYPE_INFO.get(viz_type, {})
        candidates = info.get('data_files', ['unified_dataset.csv', 'raw_data.csv'])

        for filename in candidates:
            path = os.path.join(sess_dir, filename)
            if os.path.exists(path):
                try:
                    if filename.endswith(('.xlsx', '.xls')):
                        return pd.read_excel(path)
                    return pd.read_csv(path)
                except Exception as e:
                    logger.debug("Could not read %s: %s", path, e)

        # Fallback: try any CSV in the session
        import glob
        csvs = glob.glob(os.path.join(sess_dir, '*.csv'))
        if csvs:
            try:
                return pd.read_csv(max(csvs, key=os.path.getctime))
            except Exception:
                pass

        return None

    @staticmethod
    def _find_column(df: pd.DataFrame, candidates: list) -> Optional[str]:
        """Find the first matching column name from a list of candidates."""
        for col in candidates:
            if col in df.columns:
                return col
        # Case-insensitive fallback
        lower_map = {c.lower(): c for c in df.columns}
        for col in candidates:
            if col.lower() in lower_map:
                return lower_map[col.lower()]
        return None


# Alias for backward compatibility (request_interpreter imports this name)
UniversalVizExplainer = UniversalVisualizationExplainer


def get_universal_viz_explainer(llm_manager=None):
    """Factory function to create universal visualization explainer."""
    return UniversalVisualizationExplainer(llm_manager=llm_manager)
