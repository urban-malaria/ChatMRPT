"""
TPR Analysis Pipeline.

This module coordinates the complete TPR analysis workflow from
data processing through output generation.
"""

import logging
import pandas as pd
import geopandas as gpd
from typing import Dict, Any, Optional, List
from datetime import datetime

from ..data.column_mapper import ColumnMapper
from ..data.data_validator import DataValidator
from ..core.tpr_calculator import TPRCalculator
from ..services.facility_filter import FacilityFilter
from ..services.threshold_detector import ThresholdDetector
from ..services.raster_extractor import RasterExtractor
from ..output.output_generator import OutputGenerator
from ..data.geopolitical_zones import STATE_TO_ZONE, ZONE_VARIABLES

logger = logging.getLogger(__name__)

# Import population extraction for Burden calculation
try:
    from rasterstats import zonal_stats
    from app.config.data_paths import POP_TOTAL_RASTER, POP_U5_RASTER, POP_F15_49_RASTER
    POPULATION_EXTRACTION_AVAILABLE = True
except ImportError:
    POPULATION_EXTRACTION_AVAILABLE = False
    logger.warning("rasterstats or population rasters not available - Burden calculation disabled")

class TPRPipeline:
    """Coordinate the complete TPR analysis pipeline."""
    
    def __init__(self, 
                 nmep_parser=None,
                 state_selector=None,
                 output_generator=None):
        """
        Initialize the TPR pipeline.
        
        Args:
            nmep_parser: NMEP parser instance
            state_selector: State selector instance
            output_generator: Output generator instance
        """
        # Core components
        self.column_mapper = ColumnMapper()
        self.validator = DataValidator()
        self.calculator = TPRCalculator()
        self.facility_filter = FacilityFilter()
        self.threshold_detector = ThresholdDetector()
        self.raster_extractor = RasterExtractor()
        
        # Injected components
        self.nmep_parser = nmep_parser
        self.state_selector = state_selector
        self.output_generator = output_generator
        
        logger.info("TPR Pipeline initialized")

    def _extract_ward_populations(self, gdf: gpd.GeoDataFrame, age_group: str = 'all_ages') -> pd.Series:
        """
        Extract population from rasters for each ward using zonal statistics.

        Args:
            gdf: GeoDataFrame with ward geometries
            age_group: Age group for population ('all_ages', 'u5', 'o5', 'pw')

        Returns:
            pandas Series with population values, or None if extraction fails
        """
        if not POPULATION_EXTRACTION_AVAILABLE:
            logger.warning("Population extraction not available")
            return None

        import os

        # Select appropriate raster based on age group
        if age_group == 'u5':
            raster = POP_U5_RASTER
        elif age_group == 'pw':
            raster = POP_F15_49_RASTER
        elif age_group == 'o5':
            # O5 = Total - U5
            if not os.path.exists(POP_TOTAL_RASTER) or not os.path.exists(POP_U5_RASTER):
                logger.warning("Population rasters not found for O5 calculation")
                return None
            total_stats = zonal_stats(gdf, POP_TOTAL_RASTER, stats=['sum'])
            u5_stats = zonal_stats(gdf, POP_U5_RASTER, stats=['sum'])
            return pd.Series([(t['sum'] or 0) - (u['sum'] or 0) for t, u in zip(total_stats, u5_stats)])
        else:  # all_ages
            raster = POP_TOTAL_RASTER

        if not os.path.exists(raster):
            logger.warning(f"Population raster not found: {raster}")
            return None

        try:
            stats = zonal_stats(gdf, raster, stats=['sum'])
            return pd.Series([s['sum'] or 0 for s in stats])
        except Exception as e:
            logger.error(f"Error extracting ward populations: {e}")
            return None

    def run(self,
            nmep_data: pd.DataFrame,
            state_name: str,
            state_boundaries: gpd.GeoDataFrame,
            facility_level: str = 'all',
            age_group: str = 'all_ages',
            metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Run the complete TPR analysis pipeline.
        
        Args:
            nmep_data: Parsed NMEP data
            state_name: Selected state name
            state_boundaries: State boundary GeoDataFrame
            facility_level: Facility level filter
            age_group: Age group filter
            metadata: Additional metadata
            
        Returns:
            Pipeline results with output paths
        """
        try:
            logger.info(f"Starting TPR pipeline for {state_name}")
            start_time = datetime.now()
            
            # Step 1: Map columns to standard names
            logger.info("Step 1: Mapping columns")
            mapped_data = self.column_mapper.map_nmep_columns(nmep_data)
            
            # Step 2: Filter for selected state
            logger.info(f"Step 2: Filtering for {state_name}")
            state_data = self._filter_state_data(mapped_data, state_name)
            
            if state_data.empty:
                return {
                    'status': 'error',
                    'message': f'No data found for {state_name}'
                }
            
            logger.info(f"Found {len(state_data)} facilities in {state_name}")
            
            # Step 3: Apply facility level filter
            logger.info(f"Step 3: Applying facility filter: {facility_level}")
            if facility_level != 'all':
                filtered_data = self.facility_filter.filter_by_level(
                    state_data, facility_level
                )
            else:
                filtered_data = state_data
            
            logger.info(f"Filtered to {len(filtered_data)} facilities")
            
            # Step 4: Apply age group filter
            logger.info(f"Step 4: Applying age group filter: {age_group}")
            age_filtered_data = self._apply_age_filter(filtered_data, age_group)
            
            # Step 5: Calculate TPR
            logger.info("Step 5: Calculating TPR")
            tpr_results = self.calculator.calculate_tpr(age_filtered_data)
            
            # Step 6: Aggregate to ward level
            logger.info("Step 6: Aggregating to ward level")
            ward_results = self._aggregate_to_wards(tpr_results)
            
            # Step 7: Match with shapefile boundaries
            logger.info("Step 7: Matching with shapefile")
            matched_results = self._match_with_shapefile(
                ward_results, state_boundaries
            )
            
            # Step 7.5: Extract population and calculate Burden
            logger.info("Step 7.5: Extracting population and calculating Malaria Burden per 1,000")
            if isinstance(matched_results, gpd.GeoDataFrame) and 'geometry' in matched_results.columns:
                # Extract population based on age group
                pop_series = self._extract_ward_populations(matched_results, age_group)

                if pop_series is not None:
                    matched_results['Population'] = pop_series.values

                    # Get positive cases - from TPR calculation or raw data
                    if 'Total_Positive' in matched_results.columns:
                        positive_col = 'Total_Positive'
                    elif 'Positive_Cases' in matched_results.columns:
                        positive_col = 'Positive_Cases'
                    else:
                        # Calculate from TPR if we have it
                        # TPR = (positive / tested) * 100 => positive = TPR * tested / 100
                        if 'TPR' in matched_results.columns and 'Total_Tested' in matched_results.columns:
                            matched_results['Total_Positive'] = (matched_results['TPR'] * matched_results['Total_Tested'] / 100).fillna(0)
                            positive_col = 'Total_Positive'
                        else:
                            logger.warning("Could not find positive cases column for Burden calculation")
                            positive_col = None

                    if positive_col:
                        # Calculate Burden = (positive / population) × 1000
                        matched_results['Burden'] = matched_results.apply(
                            lambda r: round((r[positive_col] / r['Population']) * 1000, 2)
                            if pd.notna(r.get(positive_col)) and r['Population'] > 0 else 0,
                            axis=1
                        )
                        logger.info(f"Calculated Malaria Burden for {len(matched_results)} wards")
                        logger.info(f"Burden range: {matched_results['Burden'].min():.1f} - {matched_results['Burden'].max():.1f} per 1,000")
                    else:
                        matched_results['Burden'] = 0
                        matched_results['Population'] = 0
                else:
                    logger.warning("Population extraction failed, setting Burden to 0")
                    matched_results['Burden'] = 0
                    matched_results['Population'] = 0
            else:
                logger.warning("No geometry available for population extraction")
                matched_results['Burden'] = 0
                matched_results['Population'] = 0

            # Step 8: Detect thresholds and recommendations
            logger.info("Step 8: Detecting thresholds")
            threshold_results = self.threshold_detector.detect_thresholds(
                matched_results
            )

            # Apply recommendations
            matched_results = self.threshold_detector.apply_recommendations(
                matched_results, threshold_results
            )
            
            # Step 9: Extract environmental variables
            logger.info("Step 9: Extracting environmental variables")
            zone = STATE_TO_ZONE.get(state_name, 'North_Central')
            zone_variables = ZONE_VARIABLES.get(zone, [])
            
            if zone_variables and not matched_results.empty:
                # Get year from metadata
                year = metadata.get('year') if metadata else None
                
                final_results = self.raster_extractor.extract_zone_variables(
                    matched_results,
                    zone,
                    ZONE_VARIABLES,
                    year
                )
            else:
                final_results = matched_results
            
            # Step 10: Generate outputs
            logger.info("Step 10: Generating output files")
            output_paths = self.output_generator.generate_outputs(
                final_results,
                state_name,
                {
                    'facility_level': facility_level,
                    'age_group': age_group,
                    'source_file': metadata.get('source_file') if metadata else 'NMEP',
                    'year': metadata.get('year') if metadata else None,
                    'month': metadata.get('month') if metadata else None
                }
            )
            
            # Calculate summary statistics
            summary_stats = self._calculate_summary_stats(final_results)
            
            # Log pipeline completion
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"TPR pipeline completed in {duration:.2f} seconds")
            
            return {
                'status': 'success',
                'output_paths': output_paths,
                'wards_analyzed': len(final_results),
                'mean_tpr': summary_stats['mean_tpr'],
                'high_tpr_wards': summary_stats['high_tpr_wards'],
                'summary_stats': summary_stats,
                'threshold_results': threshold_results,
                'pipeline_duration': duration
            }
            
        except Exception as e:
            logger.error(f"Error in TPR pipeline: {str(e)}")
            import traceback
            traceback.print_exc()
            
            return {
                'status': 'error',
                'message': f'Pipeline failed: {str(e)}'
            }
    
    def _filter_state_data(self, data: pd.DataFrame, state_name: str) -> pd.DataFrame:
        """Filter data for selected state."""
        # Normalize state name
        normalized_state = state_name.upper().replace(' STATE', '').strip()
        
        # Filter by state column
        state_data = data[
            data['State'].str.upper().str.replace(' STATE', '').str.strip() == normalized_state
        ].copy()
        
        return state_data
    
    def _apply_age_filter(self, data: pd.DataFrame, age_group: str) -> pd.DataFrame:
        """Apply age group filtering."""
        if age_group == 'all_ages':
            return data
        
        # Map age groups to column patterns
        age_mappings = {
            'under_5': ['<5', 'Under 5', 'Under_5', '0-4'],
            'over_5': ['>=5', 'Over 5', 'Over_5', '5+', '≥5'],
            '5_14': ['5-14', '5_14'],
            '15_plus': ['15+', '>=15', '≥15', '15_plus']
        }
        
        if age_group not in age_mappings:
            logger.warning(f"Unknown age group: {age_group}, using all ages")
            return data
        
        # Filter columns based on age group
        age_patterns = age_mappings[age_group]
        filtered_data = data.copy()
        
        # Create new columns with age-specific data
        for col_type in ['RDT_Tested', 'RDT_Positive', 'Microscopy_Tested', 'Microscopy_Positive']:
            # Look for age-specific columns
            age_col_found = False
            for pattern in age_patterns:
                for col in data.columns:
                    if col_type in col and pattern in col:
                        filtered_data[col_type] = data[col]
                        age_col_found = True
                        break
                if age_col_found:
                    break
            
            if not age_col_found:
                logger.warning(f"No age-specific column found for {col_type} and {age_group}")
        
        return filtered_data
    
    def _aggregate_to_wards(self, facility_data: pd.DataFrame) -> pd.DataFrame:
        """Aggregate facility-level data to ward level."""
        # Check if WardCode is available for unique aggregation
        if 'WardCode' in facility_data.columns and facility_data['WardCode'].notna().any():
            # Group by WardCode for unique ward identification
            groupby_cols = ['WardCode']
            # Keep LGA and Ward for reference
            first_cols = {'LGA': 'first', 'Ward': 'first', 'State': 'first'}
            logger.info("Aggregating by WardCode to ensure unique wards")
        else:
            # Fallback to LGA and Ward grouping
            groupby_cols = ['LGA', 'Ward']
            first_cols = {'State': 'first'}
            logger.info("Aggregating by LGA and Ward (WardCode not available)")
        
        ward_groups = facility_data.groupby(groupby_cols)
        
        # Aggregate numeric columns
        numeric_cols = [
            'RDT_Tested', 'RDT_Positive',
            'Microscopy_Tested', 'Microscopy_Positive',
            'Total_Tested', 'Total_Positive',
            'Outpatient_Attendance'
        ]
        
        # Build aggregation dictionary
        agg_dict = {col: 'sum' for col in numeric_cols if col in facility_data.columns}
        # Add first value aggregations
        agg_dict.update(first_cols)
        # Count facilities
        agg_dict['Facility'] = 'count'
        
        # Add other identifier columns if present
        for col in ['StateCode', 'LGACode', 'Urban', 'AMAPCODE']:
            if col in facility_data.columns:
                agg_dict[col] = 'first'
        
        # Aggregate
        ward_data = ward_groups.agg(agg_dict).reset_index()
        
        # Rename facility count column
        ward_data.rename(columns={'Facility': 'Facility_Count'}, inplace=True)
        
        # Recalculate TPR at ward level
        ward_tpr = []
        methods = []
        
        for _, ward in ward_data.iterrows():
            result = self.calculator._calculate_single_tpr(ward)
            ward_tpr.append(result['tpr'])
            methods.append(result['method'])
        
        ward_data['TPR'] = ward_tpr
        ward_data['Method'] = methods
        
        # Add state column
        ward_data['State'] = facility_data['State'].iloc[0]
        
        # Log ward count after aggregation
        logger.info(f"Aggregated to {len(ward_data)} wards")
        if 'WardCode' in ward_data.columns:
            unique_codes = ward_data['WardCode'].nunique()
            logger.info(f"Unique WardCodes: {unique_codes}")
        
        return ward_data
    
    def _match_with_shapefile(self, 
                             ward_data: pd.DataFrame,
                             shapefile_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Match ward data with shapefile boundaries."""
        # Identify ward column in shapefile
        ward_col_shp = None
        for col in ['Ward', 'ward', 'WardName', 'ward_name', 'WARD_NAME']:
            if col in shapefile_data.columns:
                ward_col_shp = col
                break
        
        if not ward_col_shp:
            logger.warning("Could not identify ward column in shapefile")
            # Convert ward data to GeoDataFrame without geometry
            return gpd.GeoDataFrame(ward_data)
        
        # Normalize ward names for matching
        # Check which ward column exists in ward_data
        ward_col_data = None
        for col in ['WardName', 'Ward', 'ward', 'ward_name']:
            if col in ward_data.columns:
                ward_col_data = col
                break
        
        if not ward_col_data:
            logger.warning("Could not identify ward column in TPR data")
            return gpd.GeoDataFrame(ward_data)
        
        ward_data['ward_norm'] = ward_data[ward_col_data].str.upper().str.strip()
        shapefile_data['ward_norm'] = shapefile_data[ward_col_shp].str.upper().str.strip()
        
        # Merge data - keep all TPR wards and add shapefile data
        merged = ward_data.merge(
            shapefile_data,
            on='ward_norm',
            how='left',
            suffixes=('', '_shp')
        )
        
        # Clean up
        merged.drop(columns=['ward_norm'], inplace=True)
        
        # Log matching statistics
        matched = merged['TPR'].notna().sum()
        total = len(merged)
        logger.info(f"Matched {matched}/{total} wards ({matched/total*100:.1f}%)")
        
        return merged
    
    def _calculate_summary_stats(self, results: pd.DataFrame) -> Dict[str, Any]:
        """Calculate summary statistics for Malaria Burden per 1,000."""
        stats = {}

        # Check for Burden column first (new metric)
        if 'Burden' in results.columns:
            burden_values = results['Burden'].dropna()

            stats['mean_burden'] = round(burden_values.mean(), 2) if len(burden_values) > 0 else 0
            stats['median_burden'] = round(burden_values.median(), 2) if len(burden_values) > 0 else 0
            stats['min_burden'] = round(burden_values.min(), 2) if len(burden_values) > 0 else 0
            stats['max_burden'] = round(burden_values.max(), 2) if len(burden_values) > 0 else 0
            stats['std_burden'] = round(burden_values.std(), 2) if len(burden_values) > 0 else 0

            # Count high burden wards (per 1,000 - values typically 5-100)
            stats['high_burden_wards'] = int((burden_values > 50).sum())
            stats['very_high_burden_wards'] = int((burden_values > 100).sum())

            # Keep backward compatibility with old keys
            stats['mean_tpr'] = stats['mean_burden']
            stats['high_tpr_wards'] = stats['high_burden_wards']

            # Population stats
            if 'Population' in results.columns:
                stats['total_population'] = int(results['Population'].sum())
            if 'Total_Positive' in results.columns:
                stats['total_positive'] = int(results['Total_Positive'].sum())

        # Fallback to TPR if Burden not available
        elif 'TPR' in results.columns:
            tpr_values = results['TPR'].dropna()

            stats['mean_tpr'] = tpr_values.mean()
            stats['median_tpr'] = tpr_values.median()
            stats['min_tpr'] = tpr_values.min()
            stats['max_tpr'] = tpr_values.max()
            stats['std_tpr'] = tpr_values.std()

            # Count high TPR wards
            stats['high_tpr_wards'] = (tpr_values > 50).sum()
            stats['very_high_tpr_wards'] = (tpr_values > 70).sum()

            # Method breakdown
            if 'Method' in results.columns:
                method_counts = results['Method'].value_counts().to_dict()
                stats['methods_used'] = method_counts
        else:
            stats = {
                'mean_burden': 0,
                'median_burden': 0,
                'min_burden': 0,
                'max_burden': 0,
                'std_burden': 0,
                'high_burden_wards': 0,
                'very_high_burden_wards': 0,
                'mean_tpr': 0,
                'high_tpr_wards': 0
            }

        return stats


def create_tpr_pipeline(session_id: str) -> TPRPipeline:
    """
    Create a TPR pipeline instance with proper dependencies.
    
    Args:
        session_id: Session ID for output generation
        
    Returns:
        Configured TPR pipeline
    """
    from ..data.nmep_parser import NMEPParser
    from ..services.state_selector import StateSelector
    from ..output.output_generator import OutputGenerator
    
    return TPRPipeline(
        nmep_parser=NMEPParser(),
        state_selector=StateSelector(),
        output_generator=OutputGenerator(session_id)
    )