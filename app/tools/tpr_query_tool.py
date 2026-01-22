"""
TPR Query Tool

Allows users to query pre-computed TPR data for any facility level and age group
combination after completing the initial TPR workflow.
"""

import logging
from typing import Optional
from pydantic import Field

from .base import DataAnalysisTool, ToolExecutionResult, ToolCategory

logger = logging.getLogger(__name__)


class QueryTPRData(DataAnalysisTool):
    """
    Query pre-computed TPR (Test Positivity Rate) data for different facility levels and age groups.

    Use this tool when users want to see TPR results for a different combination than
    they originally selected during the TPR workflow. All 16 combinations are pre-computed
    and available for instant retrieval.

    Examples:
    - "Show me TPR for pregnant women at secondary facilities"
    - "What's the TPR for under 5s at primary facilities?"
    - "Show top 10 wards by TPR for over 5 age group"
    """

    facility_level: str = Field(
        default='all',
        description="Facility level to filter by: 'primary', 'secondary', 'tertiary', or 'all'"
    )

    age_group: str = Field(
        default='all_ages',
        description="Age group to filter by: 'u5' (under 5), 'o5' (over 5), 'pw' (pregnant women), or 'all_ages'"
    )

    top_n: Optional[int] = Field(
        default=None,
        description="Limit results to top N wards (by TPR). If not specified, returns all wards."
    )

    lga: Optional[str] = Field(
        default=None,
        description="Filter by specific LGA (Local Government Area)"
    )

    sort_by: str = Field(
        default='tpr',
        description="Column to sort by: 'tpr', 'total_tested', or 'ward_name'"
    )

    @classmethod
    def get_tool_name(cls) -> str:
        return "query_tpr_data"

    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS

    @classmethod
    def get_description(cls) -> str:
        return (
            "Query pre-computed TPR data for any facility level and age group combination. "
            "Use after completing TPR workflow to explore different combinations without re-uploading data."
        )

    @classmethod
    def get_examples(cls) -> list:
        return [
            "Show TPR for pregnant women at secondary facilities",
            "What's the TPR for under 5s at all facilities?",
            "Show top 20 wards by TPR for over 5 age group at primary facilities",
            "TPR data for tertiary facilities, all ages"
        ]

    def execute(self, session_id: str, **kwargs) -> ToolExecutionResult:
        """Execute the TPR query and return formatted results."""
        from app.core.tpr_precompute import query_precomputed_tpr, is_tpr_precomputed

        # Check if pre-computed data exists
        if not is_tpr_precomputed(session_id):
            return self._create_error_result(
                message="No pre-computed TPR data found for this session. "
                        "Please complete the TPR workflow first by uploading TPR data "
                        "and selecting your state, facility level, and age group.",
                error_details="TPR pre-computation not available"
            )

        # Query the data
        result = query_precomputed_tpr(
            session_id=session_id,
            facility_level=self.facility_level,
            age_group=self.age_group,
            lga=self.lga,
            top_n=self.top_n,
            sort_by=self.sort_by,
            sort_desc=True
        )

        if not result['success']:
            return self._create_error_result(
                message=result.get('error', 'Failed to query TPR data'),
                error_details=result.get('error')
            )

        data = result['data']
        summary = result['summary']

        if not data:
            return self._create_success_result(
                message=f"No TPR data found for {self._format_age_group(self.age_group)} "
                        f"at {self._format_facility_level(self.facility_level)} facilities.",
                data={'summary': summary}
            )

        # Format the response message
        message = self._format_response(data, summary)

        return self._create_success_result(
            message=message,
            data={
                'wards': data,
                'summary': summary,
                'query_params': {
                    'facility_level': self.facility_level,
                    'age_group': self.age_group,
                    'top_n': self.top_n,
                    'lga': self.lga
                }
            }
        )

    def _format_response(self, data: list, summary: dict) -> str:
        """Format the query results into a readable message."""
        age_label = self._format_age_group(summary.get('age_group', self.age_group))
        facility_label = self._format_facility_level(summary.get('facility_level', self.facility_level))

        lines = [
            f"## TPR Results: {age_label} at {facility_label} Facilities",
            ""
        ]

        # Add summary
        if summary.get('total_wards', 0) > 0:
            lines.extend([
                "### Summary",
                f"- **Wards analyzed:** {summary['total_wards']}",
                f"- **Average TPR:** {summary['avg_tpr']}%",
                f"- **Range:** {summary['min_tpr']}% - {summary['max_tpr']}%",
                f"- **Total tested:** {summary['total_tested']:,}",
                f"- **Total positive:** {summary['total_positive']:,}",
                ""
            ])

        # Add table header
        lines.extend([
            "### Ward-Level Results",
            "",
            "| Ward | LGA | TPR (%) | Tested | Positive |",
            "|------|-----|---------|--------|----------|"
        ])

        # Limit display to first 20 rows for readability
        display_data = data[:20]
        for row in display_data:
            ward = row.get('ward_name', 'Unknown')[:25]
            lga = row.get('lga', '')[:15]
            tpr = row.get('tpr', 0)
            tested = row.get('total_tested', 0)
            positive = row.get('total_positive', 0)
            lines.append(f"| {ward} | {lga} | {tpr:.1f} | {tested:,} | {positive:,} |")

        if len(data) > 20:
            lines.append(f"\n*Showing top 20 of {len(data)} wards. Use `top_n` parameter to see more.*")

        return "\n".join(lines)

    def _format_age_group(self, age_group: str) -> str:
        """Convert age group code to readable label."""
        labels = {
            'u5': 'Under 5 Years',
            'o5': 'Over 5 Years',
            'pw': 'Pregnant Women',
            'all_ages': 'All Age Groups'
        }
        return labels.get(age_group.lower(), age_group.title())

    def _format_facility_level(self, facility_level: str) -> str:
        """Convert facility level to readable label."""
        labels = {
            'primary': 'Primary',
            'secondary': 'Secondary',
            'tertiary': 'Tertiary',
            'all': 'All'
        }
        return labels.get(facility_level.lower(), facility_level.title())


class SwitchTPRCombination(DataAnalysisTool):
    """
    Switch to a different TPR combination (facility level + age group) and regenerate outputs.

    This tool rebuilds raw_data.csv, raw_shapefile.zip, and the TPR map for a new combination
    using cached ward data. Enables risk analysis and ITN planning on different combinations
    without re-uploading data.

    Examples:
    - "Switch to TPR for pregnant women at secondary facilities"
    - "Show TPR map for under 5s at primary facilities"
    - "Use tertiary facility TPR for all ages"
    """

    facility_level: str = Field(
        default='all',
        description="Facility level: 'primary', 'secondary', 'tertiary', or 'all'"
    )

    age_group: str = Field(
        default='all_ages',
        description="Age group: 'u5' (under 5), 'o5' (over 5), 'pw' (pregnant women), or 'all_ages'"
    )

    generate_map: bool = Field(
        default=True,
        description="Whether to generate a new TPR distribution map"
    )

    @classmethod
    def get_tool_name(cls) -> str:
        return "switch_tpr_combination"

    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.DATA_ANALYSIS

    @classmethod
    def get_description(cls) -> str:
        return (
            "Switch to a different TPR combination and regenerate the analysis files. "
            "This updates raw_data.csv, the shapefile, and TPR map for risk analysis on the new combination."
        )

    @classmethod
    def get_examples(cls) -> list:
        return [
            "Switch to TPR for pregnant women at secondary facilities",
            "Show TPR map for under 5s at all facilities",
            "Use primary facility TPR for over 5 age group",
            "Change to tertiary facilities, pregnant women"
        ]

    def execute(self, session_id: str, **kwargs) -> ToolExecutionResult:
        """Switch to a new TPR combination and regenerate outputs."""
        import os
        import pandas as pd
        import geopandas as gpd

        from app.core.tpr_ward_cache import load_ward_cache, is_ward_cache_available
        from app.core.tpr_precompute import query_precomputed_tpr, is_tpr_precomputed

        session_folder = f"instance/uploads/{session_id}"

        # Check prerequisites
        if not is_ward_cache_available(session_id):
            return self._create_error_result(
                message="Ward cache not found. Please complete the TPR workflow first.",
                error_details="Ward cache not available - run TPR analysis first"
            )

        if not is_tpr_precomputed(session_id):
            return self._create_error_result(
                message="Pre-computed TPR data not found. Please complete the TPR workflow first.",
                error_details="TPR pre-computation not available"
            )

        # Normalize inputs
        facility_level = self._normalize_facility_level(self.facility_level)
        age_group = self._normalize_age_group(self.age_group)

        logger.info(f"Switching to TPR combination: {facility_level}/{age_group}")

        # Load ward cache
        cache = load_ward_cache(session_id)
        if not cache:
            return self._create_error_result(
                message="Failed to load ward cache.",
                error_details="Cache load failed"
            )

        state_gdf = cache['state_gdf']
        env_data = cache['env_data']
        state_name = cache['state_name']

        # Query TPR for requested combination
        tpr_result = query_precomputed_tpr(
            session_id=session_id,
            facility_level=facility_level,
            age_group=age_group
        )

        if not tpr_result['success']:
            return self._create_error_result(
                message=f"No TPR data found for {self._format_age_group(age_group)} at {self._format_facility_level(facility_level)} facilities.",
                error_details=tpr_result.get('error')
            )

        tpr_data = tpr_result['data']
        if not tpr_data:
            return self._create_error_result(
                message=f"No TPR data available for this combination.",
                error_details="Empty TPR result"
            )

        # Convert to DataFrame
        tpr_df = pd.DataFrame(tpr_data)
        tpr_df.rename(columns={
            'ward_name': 'WardName',
            'lga': 'LGA',
            'tpr': 'TPR',
            'total_tested': 'Total_Tested',
            'total_positive': 'Total_Positive'
        }, inplace=True)

        # Merge TPR with ward geometries
        merged_gdf = self._merge_tpr_with_wards(tpr_df, state_gdf)

        # Build final dataset
        final_df = self._build_final_dataset(merged_gdf, env_data, state_name)

        # Save raw_data.csv
        raw_data_path = os.path.join(session_folder, 'raw_data.csv')
        final_df.to_csv(raw_data_path, index=False)
        logger.info(f"Updated raw_data.csv with {len(final_df)} wards")

        # Save shapefile
        try:
            from app.data_analysis_v3.tools.tpr_analysis_tool import create_shapefile_package

            # Add final_df columns to merged_gdf for shapefile
            for col in final_df.columns:
                if col not in merged_gdf.columns:
                    merged_gdf[col] = final_df[col].values

            create_shapefile_package(merged_gdf, session_folder)
            logger.info("Updated raw_shapefile.zip")
        except Exception as e:
            logger.warning(f"Could not update shapefile: {e}")

        # Generate TPR map
        visualization = None
        if self.generate_map:
            try:
                from app.data_analysis_v3.tools.tpr_analysis_tool import create_tpr_map

                map_created = create_tpr_map(tpr_df, session_folder, state_name)
                if map_created:
                    visualization = {
                        'type': 'iframe',
                        'url': f'/serve_viz_file/{session_id}/tpr_distribution_map.html',
                        'title': f'TPR Distribution - {state_name} ({self._format_age_group(age_group)}, {self._format_facility_level(facility_level)} Facilities)',
                        'height': 600
                    }
                    logger.info("TPR map regenerated successfully")
            except Exception as e:
                logger.warning(f"Could not regenerate TPR map: {e}")

        # Prepare response
        summary = tpr_result.get('summary', {})
        age_label = self._format_age_group(age_group)
        facility_label = self._format_facility_level(facility_level)

        message = f"""## TPR Combination Switched

**{state_name}**: Now using **{age_label}** at **{facility_label}** facilities

### Summary
- **Wards analyzed:** {summary.get('total_wards', len(tpr_data))}
- **Average TPR:** {summary.get('avg_tpr', 0):.1f}%
- **Range:** {summary.get('min_tpr', 0):.1f}% - {summary.get('max_tpr', 0):.1f}%
- **Total tested:** {summary.get('total_tested', 0):,}
- **Total positive:** {summary.get('total_positive', 0):,}

Files updated:
- `raw_data.csv` - Ward data with new TPR values
- `raw_shapefile.zip` - Shapefile with new TPR values
- TPR distribution map

You can now run **risk analysis** or **ITN planning** using this combination."""

        result = self._create_success_result(
            message=message,
            data={
                'facility_level': facility_level,
                'age_group': age_group,
                'state': state_name,
                'summary': summary
            }
        )

        # Add visualization if created
        if visualization:
            result.visualizations = [visualization]

        return result

    def _normalize_facility_level(self, level: str) -> str:
        """Normalize facility level input."""
        level = level.lower().replace(' ', '_').replace('-', '_')
        mappings = {
            'primary': 'primary', 'phc': 'primary', 'primary_health': 'primary',
            'secondary': 'secondary', 'shc': 'secondary',
            'tertiary': 'tertiary', 'thc': 'tertiary',
            'all': 'all', 'all_facilities': 'all', 'combined': 'all'
        }
        return mappings.get(level, 'all')

    def _normalize_age_group(self, group: str) -> str:
        """Normalize age group input."""
        group = group.lower().replace(' ', '_').replace('-', '_')
        mappings = {
            'u5': 'u5', 'under_5': 'u5', 'under5': 'u5', '<5': 'u5', 'under_five': 'u5',
            'o5': 'o5', 'over_5': 'o5', 'over5': 'o5', '>5': 'o5', '5+': 'o5', 'over_five': 'o5',
            'pw': 'pw', 'pregnant': 'pw', 'pregnant_women': 'pw', 'anc': 'pw',
            'all_ages': 'all_ages', 'all': 'all_ages', 'combined': 'all_ages'
        }
        return mappings.get(group, 'all_ages')

    def _merge_tpr_with_wards(self, tpr_df: pd.DataFrame, state_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Merge TPR data with ward geometries."""
        from app.core.tpr_utils import normalize_ward_name

        # Normalize ward names
        tpr_df['WardName_norm'] = tpr_df['WardName'].apply(normalize_ward_name)
        state_gdf = state_gdf.copy()
        state_gdf['WardName_norm'] = state_gdf['WardName'].apply(normalize_ward_name)

        # Merge
        merged = state_gdf.merge(
            tpr_df[['WardName_norm', 'TPR', 'Total_Tested', 'Total_Positive']],
            on='WardName_norm',
            how='left'
        )

        # Fill missing TPR with 0
        merged['TPR'] = merged['TPR'].fillna(0)
        merged['Total_Tested'] = merged['Total_Tested'].fillna(0).astype(int)
        merged['Total_Positive'] = merged['Total_Positive'].fillna(0).astype(int)

        return merged

    def _build_final_dataset(self, merged_gdf: gpd.GeoDataFrame, env_data: pd.DataFrame, state_name: str) -> pd.DataFrame:
        """Build the final dataset for risk analysis."""
        from app.data_analysis_v3.tools.tpr_analysis_tool import get_geopolitical_zone

        final_df = pd.DataFrame()

        # Add identifiers
        final_df['WardCode'] = merged_gdf['WardCode'] if 'WardCode' in merged_gdf.columns else range(len(merged_gdf))
        final_df['StateCode'] = merged_gdf['StateCode'] if 'StateCode' in merged_gdf.columns else state_name[:2].upper()
        final_df['LGACode'] = merged_gdf['LGACode'] if 'LGACode' in merged_gdf.columns else ''
        final_df['WardName'] = merged_gdf['WardName']
        final_df['LGA'] = merged_gdf['LGAName'] if 'LGAName' in merged_gdf.columns else merged_gdf.get('LGA', '')
        final_df['State'] = state_name
        final_df['GeopoliticalZone'] = get_geopolitical_zone(state_name)

        # Add TPR metrics
        final_df['TPR'] = merged_gdf['TPR'].fillna(0)
        final_df['Total_Tested'] = merged_gdf['Total_Tested'].fillna(0).astype(int)
        final_df['Total_Positive'] = merged_gdf['Total_Positive'].fillna(0).astype(int)

        # Add environmental variables
        if not env_data.empty:
            for col in env_data.columns:
                if col != 'WardCode' and col not in final_df.columns:
                    final_df[col] = env_data[col].values

        return final_df

    def _format_age_group(self, age_group: str) -> str:
        """Convert age group code to readable label."""
        labels = {
            'u5': 'Under 5 Years',
            'o5': 'Over 5 Years',
            'pw': 'Pregnant Women',
            'all_ages': 'All Age Groups'
        }
        return labels.get(age_group.lower(), age_group.title())

    def _format_facility_level(self, facility_level: str) -> str:
        """Convert facility level to readable label."""
        labels = {
            'primary': 'Primary',
            'secondary': 'Secondary',
            'tertiary': 'Tertiary',
            'all': 'All'
        }
        return labels.get(facility_level.lower(), facility_level.title())
