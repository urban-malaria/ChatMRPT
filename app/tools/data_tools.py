"""
Data Analysis Tools for ChatMRPT - Unified Dataset Access

These tools provide access to the unified dataset with smart metadata,
categorized columns, and comprehensive analysis capabilities.
Properly accesses the GeoParquet unified dataset structure.
"""

import logging
from typing import Dict, Any, Optional, List
from flask import current_app
import pandas as pd
import geopandas as gpd

logger = logging.getLogger(__name__)


def _get_unified_dataset(session_id: str) -> Optional[gpd.GeoDataFrame]:
    """
    Get the unified dataset from session - NO FALLBACKS, ONLY UNIFIED DATASET.
    
    This forces us to see real issues instead of masking them with fallbacks.
    """
    try:
        # ONLY try unified dataset - no fallbacks
        from ..data.unified_dataset_builder import load_unified_dataset
        unified_gdf = load_unified_dataset(session_id)
        if unified_gdf is not None:
            logger.debug(f"✅ Unified dataset loaded: {len(unified_gdf)} rows, {len(unified_gdf.columns)} columns")
            return unified_gdf
        else:
            logger.error(f"❌ Unified dataset not found for session {session_id}")
            return None
        
    except Exception as e:
        logger.error(f"❌ Error accessing unified dataset: {e}")
        return None


def _get_column_metadata(session_id: str) -> Dict[str, Dict[str, str]]:
    """Get column metadata from unified dataset."""
    try:
        import os
        import json
        
        session_folder = f"instance/uploads/{session_id}"
        metadata_path = os.path.join(session_folder, 'column_metadata.json')
        
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                return json.load(f)
        
        return {}
        
    except Exception as e:
        logger.error(f"Error loading column metadata: {e}")
        return {}


def _get_columns_by_category(session_id: str, category: str) -> List[str]:
    """Get columns by category using smart metadata."""
    metadata = _get_column_metadata(session_id)
    return [col for col, meta in metadata.items() if meta.get('category') == category]


def run_composite_analysis(session_id: str) -> Dict[str, Any]:
    """
    Run complete composite scoring analysis using existing analysis service.
    
    Args:
        session_id: Session identifier
        
    Returns:
        Dict with status, message, and analysis results
    """
    try:
        # Get services
        analysis_service = current_app.services.analysis_service
        
        # Create session-specific DataHandler for this analysis
        import os
        session_folder = os.path.join(current_app.instance_path, "uploads", session_id)
        
        if not os.path.exists(session_folder):
            return {
                'status': 'error',
                'message': 'Session folder not found. Please upload data first.'
            }
        
        # Create new DataHandler for session data
        from ..data import DataHandler
        session_data_handler = DataHandler(session_folder)
        
        if session_data_handler.csv_data is None or session_data_handler.shapefile_data is None:
            return {
                'status': 'error',
                'message': 'No data available in session. Please upload CSV and shapefile first.'
            }
        
        # Run analysis using existing service with session-specific handler
        analysis_result = analysis_service.run_standard_analysis(session_data_handler, session_id)
        
        if analysis_result['status'] != 'success':
            return analysis_result
        
        # Check if unified dataset was created
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is not None:
            # Extract composite analysis results from unified dataset
            composite_columns = [col for col in unified_gdf.columns if 'composite' in col.lower()]
            
            return {
                'status': 'success',
                'message': 'Composite analysis completed successfully',
                'analysis_result': analysis_result,
                'composite_columns': composite_columns,
                'variables_used': analysis_result.get('variables_used', []),
                'unified_dataset_available': True
            }
        
        return analysis_result
        
    except Exception as e:
        logger.error(f"Error in run_composite_analysis: {e}")
        return {
            'status': 'error',
            'message': f'Error running composite analysis: {str(e)}'
        }


def run_pca_analysis(session_id: str) -> Dict[str, Any]:
    """
    Run PCA analysis using the independent PCA pipeline.
    
    Args:
        session_id: Session identifier
        
    Returns:
        Dict with status, message, and PCA results
    """
    try:
        # Import the independent PCA analysis function
        from app.analysis.pca_pipeline import run_independent_pca_analysis
        
        # Create session-specific DataHandler for this analysis
        import os
        session_folder = os.path.join(current_app.instance_path, "uploads", session_id)
        
        if not os.path.exists(session_folder):
            return {
                'status': 'error',
                'message': 'Session folder not found. Please upload data first.'
            }
        
        # Create DataHandler to load the session data
        from app.data import DataHandler
        data_handler = DataHandler(session_folder)
        
        # Check if data is available
        if not hasattr(data_handler, 'csv_data') or data_handler.csv_data is None:
            return {
                'status': 'error',
                'message': 'No CSV data available. Please upload data first.'
            }
        
        logger.info(f"🔬 PCA TOOL: Starting independent PCA analysis for session {session_id}")
        
        # Run the independent PCA analysis
        pca_result = run_independent_pca_analysis(
            data_handler=data_handler,
            selected_variables=None,  # Let PCA auto-select variables
            session_id=session_id
        )
        
        if pca_result.get('status') == 'success':
            pca_data = pca_result.get('data', {})
            ward_count = pca_data.get('ward_count', 0)
            variables_used = pca_data.get('variables_used', [])
            explained_variance = pca_data.get('explained_variance', {})
            variable_importance = pca_data.get('variable_importance', {})
            
            # Get top variables by importance
            top_variables = list(variable_importance.keys())[:3] if variable_importance else []
            total_variance_explained = explained_variance.get('total_explained', 0) * 100
            
            message = f"""🔬 **PCA Analysis Complete!**

📊 **PCA Results Summary:**
- **Method:** Principal Component Analysis (Independent)
- **Wards Analyzed:** {ward_count}
- **Variables Used:** {len(variables_used)}
- **Total Variance Explained:** {total_variance_explained:.1f}%

🎯 **Key Findings:**
- **Most Important Variables:** {', '.join(top_variables)}
- **Components Generated:** {pca_data.get('n_components', 'N/A')}
- **Analysis Status:** ✅ Complete

🏆 **Top Risk Areas Identified:** Using PCA-based vulnerability scoring
🗺️ **Geographic Mapping:** Ready for visualization

**PCA Analysis provides a statistical approach to identify the most important risk patterns in your data, complementing the composite scoring method.**"""

            return {
                'status': 'success',
                'message': message,
                'analysis_type': 'pca',
                'results': {
                    'method': 'pca',
                    'ward_count': ward_count,
                    'variables_used': variables_used,
                    'explained_variance': explained_variance,
                    'variable_importance': variable_importance,
                    'n_components': pca_data.get('n_components'),
                    'summary': pca_data.get('summary')
                }
            }
        else:
            return {
                'status': 'error',
                'message': f"PCA analysis failed: {pca_result.get('message', 'Unknown error')}"
            }
            
    except Exception as e:
        logger.error(f"Error in PCA analysis tool: {e}")
        return {
            'status': 'error',
            'message': f'PCA analysis failed: {str(e)}'
        }


def get_composite_rankings(session_id: str, top_n: int = 20, location_filter: str = None) -> Dict[str, Any]:
    """
    Get composite score rankings from unified dataset with optional location filtering.
    
    Args:
        session_id: Session identifier
        top_n: Number of top/bottom wards to return
        location_filter: Optional filter by state, LGA, or ward name (e.g., "Kano", "Lagos")
        
    Returns:
        Dict with ranked wards by composite score
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No unified dataset available - analysis may not have been run or unified dataset not created'
            }
        
        # Find composite score column
        score_columns = [col for col in unified_gdf.columns 
                        if any(term in col.lower() for term in ['composite_score', 'composite_rank'])]
        
        if not score_columns:
            return {
                'status': 'error',
                'message': f'No composite scores found in dataset. Available columns: {list(unified_gdf.columns)[:10]}...'
            }
        
        score_column = next((col for col in score_columns if 'score' in col.lower()), score_columns[0])
        
        # Find ward identifier column
        ward_columns = [col for col in unified_gdf.columns 
                       if any(term in col.lower() for term in ['ward', 'name']) and unified_gdf[col].dtype == 'object']
        
        if not ward_columns:
            return {
                'status': 'error',
                'message': f'No ward identifier column found. Available object columns: {[col for col in unified_gdf.columns if unified_gdf[col].dtype == "object"]}'
            }
        
        ward_column = ward_columns[0]
        
        # Apply location filtering if specified
        filtered_gdf = unified_gdf.copy()
        if location_filter:
            location_filter = location_filter.upper()
            # Find location columns (state, LGA, ward)
            location_columns = []
            for col in unified_gdf.columns:
                if any(term in col.lower() for term in ['state', 'lga', 'ward']):
                    location_columns.append(col)
            
            # Create filter mask
            mask = pd.Series([False] * len(unified_gdf))
            for col in location_columns:
                if col in unified_gdf.columns:
                    mask |= unified_gdf[col].astype(str).str.contains(location_filter, case=False, na=False)
            
            filtered_gdf = unified_gdf[mask]
            
            if len(filtered_gdf) == 0:
                return {
                    'status': 'error',
                    'message': f'No wards found matching location filter: {location_filter}'
                }
        
        # Get rankings
        ranked_df = filtered_gdf.sort_values(score_column, ascending=False)
        
        top_wards = ranked_df.head(top_n)[[ward_column, score_column]].to_dict('records')
        bottom_wards = ranked_df.tail(top_n)[[ward_column, score_column]].to_dict('records')
        
        return {
            'status': 'success',
            'message': f'Retrieved composite rankings for {len(filtered_gdf)} wards' + (f' in {location_filter}' if location_filter else ''),
            'score_column': score_column,
            'ward_column': ward_column,
            'top_wards': top_wards,
            'bottom_wards': bottom_wards,
            'total_wards': len(filtered_gdf),
            'location_filter': location_filter,
            'total_available_wards': len(unified_gdf)
        }
        
    except Exception as e:
        logger.error(f"Error getting composite rankings: {e}")
        return {
            'status': 'error',
            'message': f'Error retrieving rankings: {str(e)}'
        }


def get_pca_rankings(session_id: str, top_n: int = 20) -> Dict[str, Any]:
    """
    Get PCA-based vulnerability rankings from unified dataset.
    
    Args:
        session_id: Session identifier
        top_n: Number of top/bottom wards to return (FIXED to match LLM parameter)
        
    Returns:
        Dict with PCA-based vulnerability rankings
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No unified dataset available - analysis may not have been run or unified dataset not created'
            }
        
        # Find ward column first
        ward_columns = [col for col in unified_gdf.columns 
                       if any(term in col.lower() for term in ['ward', 'name']) and unified_gdf[col].dtype == 'object']
        
        if not ward_columns:
            return {'status': 'error', 'message': f'No ward identifier found. Available object columns: {[col for col in unified_gdf.columns if unified_gdf[col].dtype == "object"]}'}
        
        ward_column = ward_columns[0]
        
        # Look for PCA categories first
        pca_category_cols = [col for col in unified_gdf.columns 
                            if 'pca' in col.lower() and 'category' in col.lower()]
        
        if pca_category_cols:
            category_col = pca_category_cols[0]
            
            # Get all unique category values to understand the data better
            unique_categories = unified_gdf[category_col].unique()
            logger.info(f"PCA category values found: {list(unique_categories)}")
            
            # Try different possible category naming patterns
            high_risk_wards = []
            medium_risk_wards = []
            low_risk_wards = []
            
            # Pattern 1: Standard High/Medium/Low
            high_risk = unified_gdf[unified_gdf[category_col] == 'High Risk'][ward_column].tolist()[:top_n]
            medium_risk = unified_gdf[unified_gdf[category_col] == 'Medium Risk'][ward_column].tolist()[:top_n]
            low_risk = unified_gdf[unified_gdf[category_col] == 'Low Risk'][ward_column].tolist()[:top_n]
            
            # Pattern 2: Alternative naming - High/Medium/Low without "Risk"
            if not high_risk:
                high_risk = unified_gdf[unified_gdf[category_col] == 'High'][ward_column].tolist()[:top_n]
            if not medium_risk:
                medium_risk = unified_gdf[unified_gdf[category_col] == 'Medium'][ward_column].tolist()[:top_n]
            if not low_risk:
                low_risk = unified_gdf[unified_gdf[category_col] == 'Low'][ward_column].tolist()[:top_n]
            
            # Pattern 3: Check for numeric categories (1=High, 2=Medium, 3=Low or similar)
            if not high_risk and not medium_risk and not low_risk:
                # Try to infer from the values
                for cat_value in unique_categories:
                    cat_str = str(cat_value).lower()
                    if 'high' in cat_str or cat_value == 1:
                        high_risk = unified_gdf[unified_gdf[category_col] == cat_value][ward_column].tolist()[:top_n]
                    elif 'medium' in cat_str or cat_value == 2:
                        medium_risk = unified_gdf[unified_gdf[category_col] == cat_value][ward_column].tolist()[:top_n]
                    elif 'low' in cat_str or cat_value == 3:
                        low_risk = unified_gdf[unified_gdf[category_col] == cat_value][ward_column].tolist()[:top_n]
            
            # If still no categories found, create them from ranking
            if not high_risk and not medium_risk and not low_risk:
                # Use PCA rank or score to create categories
                if 'pca_rank' in unified_gdf.columns:
                    total_wards = len(unified_gdf)
                    high_cutoff = total_wards // 3
                    low_cutoff = (2 * total_wards) // 3
                    
                    # Sort by rank (lower rank = higher risk)
                    sorted_df = unified_gdf.sort_values('pca_rank')
                    high_risk = sorted_df.head(high_cutoff)[ward_column].tolist()[:top_n]
                    medium_risk = sorted_df.iloc[high_cutoff:low_cutoff][ward_column].tolist()[:top_n]
                    low_risk = sorted_df.tail(total_wards - low_cutoff)[ward_column].tolist()[:top_n]
                elif 'pca_score' in unified_gdf.columns:
                    # Sort by score (higher score = higher risk)
                    sorted_df = unified_gdf.sort_values('pca_score', ascending=False)
                    total_wards = len(unified_gdf)
                    high_cutoff = total_wards // 3
                    low_cutoff = (2 * total_wards) // 3
                    
                    high_risk = sorted_df.head(high_cutoff)[ward_column].tolist()[:top_n]
                    medium_risk = sorted_df.iloc[high_cutoff:low_cutoff][ward_column].tolist()[:top_n]
                    low_risk = sorted_df.tail(total_wards - low_cutoff)[ward_column].tolist()[:top_n]
            
            return {
                'status': 'success',
                'message': f'Retrieved PCA vulnerability rankings for {len(unified_gdf)} wards',
                'category_column': category_col,
                'high_risk_wards': high_risk,
                'medium_risk_wards': medium_risk,
                'low_risk_wards': low_risk,
                'total_wards': len(unified_gdf),
                'category_values_found': list(unique_categories)
            }
        
        # Look for PCA scores as fallback
        pca_score_cols = [col for col in unified_gdf.columns 
                         if 'pca' in col.lower() and 'score' in col.lower()]
        
        if pca_score_cols:
            score_col = pca_score_cols[0]
            
            # Create rankings from scores and generate categories
            ranked_df = unified_gdf.sort_values(score_col, ascending=False)
            
            # Create top/bottom wards for backward compatibility
            top_wards = ranked_df.head(top_n)[[ward_column, score_col]].to_dict('records')
            bottom_wards = ranked_df.tail(top_n)[[ward_column, score_col]].to_dict('records')
            
            # Also create high/medium/low categories from the scores
            total_wards = len(unified_gdf)
            high_cutoff = total_wards // 3
            low_cutoff = (2 * total_wards) // 3
            
            high_risk = ranked_df.head(high_cutoff)[ward_column].tolist()[:top_n]
            medium_risk = ranked_df.iloc[high_cutoff:low_cutoff][ward_column].tolist()[:top_n]
            low_risk = ranked_df.tail(total_wards - low_cutoff)[ward_column].tolist()[:top_n]
            
            return {
                'status': 'success',
                'message': f'Retrieved PCA score rankings for {len(unified_gdf)} wards',
                'score_column': score_col,
                'top_wards': top_wards,
                'bottom_wards': bottom_wards,
                'high_risk_wards': high_risk,
                'medium_risk_wards': medium_risk,
                'low_risk_wards': low_risk,
                'total_wards': len(unified_gdf)
            }
        
        return {
            'status': 'error',
            'message': f'No PCA analysis results found in dataset. Available columns: {list(unified_gdf.columns)[:10]}...'
        }
        
    except Exception as e:
        logger.error(f"Error getting PCA rankings: {e}")
        return {
            'status': 'error',
            'message': f'Error retrieving PCA rankings: {str(e)}'
        }


def create_composite_score_maps(session_id: str) -> Dict[str, Any]:
    """
    Create composite score maps using existing visualization functions.
    
    Args:
        session_id: Session identifier
        
    Returns:
        Dict with visualization result
    """
    try:
        from ..data import DataHandler
        
        # Create a proper DataHandler instance for the session
        data_handler = DataHandler(session_id)
        
        if not data_handler or not hasattr(data_handler, 'df') or data_handler.df is None:
            return {
                'status': 'error',
                'message': 'No data available for session'
            }
        
        from ..services.agents.visualizations import create_agent_composite_score_maps
        result = create_agent_composite_score_maps(data_handler, session_id=session_id)
        return result
        
    except Exception as e:
        logger.error(f"Error creating composite score maps: {e}")
        return {
            'status': 'error',
            'message': f'Error creating maps: {str(e)}'
        }


def create_vulnerability_map(session_id: str, method: str = 'auto') -> Dict[str, Any]:
    """
    Create vulnerability map using existing visualization functions.
    Intelligently chooses between composite and PCA methods.
    
    Args:
        session_id: Session identifier
        method: 'auto', 'composite', or 'pca' - auto detects best method
        
    Returns:
        Dict with visualization result
    """
    try:
        from ..services.agents.visualizations import (
            create_agent_vulnerability_map, 
            create_agent_pca_vulnerability_map
        )
        
        # Get unified dataset
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No unified dataset available for session'
            }
        
        # Auto-detect method based on available columns
        has_composite = 'composite_score' in unified_gdf.columns
        has_pca = 'pca_score' in unified_gdf.columns and 'pca_rank' in unified_gdf.columns
        
        if method == 'auto':
            if has_composite and has_pca:
                # Both available - create composite by default but mention PCA
                logger.info("Both composite and PCA data available, using composite method")
                result = create_agent_vulnerability_map(unified_gdf, session_id=session_id)
                if result.get('status') == 'success':
                    result['message'] += f" (Both composite and PCA methods available)"
                    result['available_methods'] = ['composite', 'pca']
                return result
            elif has_pca:
                logger.info("Only PCA data available, using PCA method")
                return create_agent_pca_vulnerability_map(unified_gdf, session_id=session_id)
            elif has_composite:
                logger.info("Only composite data available, using composite method")
                return create_agent_vulnerability_map(unified_gdf, session_id=session_id)
            else:
                return {
                    'status': 'error',
                    'message': 'No vulnerability analysis data found. Please run composite or PCA analysis first.',
                    'available_columns': list(unified_gdf.columns)
                }
        
        # Explicit method requested
        elif method == 'composite':
            if not has_composite:
                return {
                    'status': 'error',
                    'message': 'Composite vulnerability data not found. Run composite analysis first.'
                }
            return create_agent_vulnerability_map(unified_gdf, session_id=session_id)
            
        elif method == 'pca':
            if not has_pca:
                return {
                    'status': 'error',
                    'message': 'PCA vulnerability data not found. Run PCA analysis first.'
                }
            return create_agent_pca_vulnerability_map(unified_gdf, session_id=session_id)
            
        else:
            return {
                'status': 'error',
                'message': f'Invalid method "{method}". Use "auto", "composite", or "pca".'
            }
        
    except Exception as e:
        logger.error(f"Error creating vulnerability map: {e}")
        return {
            'status': 'error',
            'message': f'Error creating vulnerability map: {str(e)}'
        }


def create_decision_tree(session_id: str) -> Dict[str, Any]:
    """
    Create decision tree visualization using existing functions.
    
    Args:
        session_id: Session identifier
        
    Returns:
        Dict with visualization result
    """
    try:
        from ..services.agents.visualizations import create_agent_decision_tree
        
        # Get unified dataset instead of passing data_handler directly
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No unified dataset available for session'
            }
        
        result = create_agent_decision_tree(unified_gdf, session_id=session_id)
        return result
        
    except Exception as e:
        logger.error(f"Error creating decision tree: {e}")
        return {
            'status': 'error',
            'message': f'Error creating decision tree: {str(e)}'
        }


def create_urban_extent_map(session_id: str, threshold: float = 50.0) -> Dict[str, Any]:
    """
    Create urban extent map using existing visualization functions.
    
    Args:
        session_id: Session identifier
        threshold: Threshold for urban classification (0-100)
        
    Returns:
        Dict with visualization result
    """
    try:
        from ..services.agents.visualizations import create_agent_urban_extent_map
        
        # Get unified dataset instead of passing data_handler directly
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No unified dataset available for session'
            }
        
        # Note: removed threshold_percentile parameter as agent function doesn't accept it
        result = create_agent_urban_extent_map(unified_gdf, session_id=session_id, threshold=threshold)
        return result
        
    except Exception as e:
        logger.error(f"Error creating urban extent map: {e}")
        return {
            'status': 'error',
            'message': f'Error creating urban extent map: {str(e)}'
        }


def filter_wards_by_risk(session_id: str, risk_level: str, limit: int = 10, location_filter: str = None) -> Dict[str, Any]:
    """
    Filter wards by risk level using unified dataset with optional location filtering.
    
    Args:
        session_id: Session identifier
        risk_level: Risk level to filter by ('High', 'Medium', 'Low')
        limit: Maximum number of wards to return
        location_filter: Optional filter by state, LGA, or ward name (e.g., "Kano", "Lagos")
        
    Returns:
        Dict with filtered wards
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No unified dataset available'
            }
        
        # Apply location filtering first if specified
        filtered_gdf = unified_gdf.copy()
        if location_filter:
            location_filter = location_filter.upper()
            # Find location columns (state, LGA, ward)
            location_columns = []
            for col in unified_gdf.columns:
                if any(term in col.lower() for term in ['state', 'lga', 'ward']):
                    location_columns.append(col)
            
            # Create filter mask
            mask = pd.Series([False] * len(unified_gdf))
            for col in location_columns:
                if col in unified_gdf.columns:
                    mask |= unified_gdf[col].astype(str).str.contains(location_filter, case=False, na=False)
            
            filtered_gdf = unified_gdf[mask]
            
            if len(filtered_gdf) == 0:
                return {
                    'status': 'error',
                    'message': f'No wards found matching location filter: {location_filter}'
                }
        
        # Find category columns
        category_columns = [col for col in filtered_gdf.columns if 'category' in col.lower()]
        
        if category_columns:
            # Use the first category column found
            category_col = category_columns[0]
            filtered_wards = filtered_gdf[filtered_gdf[category_col] == risk_level]
            
            # Find ward identifier
            ward_columns = [col for col in filtered_gdf.columns 
                           if any(term in col.lower() for term in ['ward', 'name']) and filtered_gdf[col].dtype == 'object']
            
            if not ward_columns:
                return {'status': 'error', 'message': 'No ward identifier found'}
            
            ward_column = ward_columns[0]
            wards = filtered_wards[ward_column].tolist()[:limit]
            
            return {
                'status': 'success',
                'message': f'Found {len(wards)} {risk_level.lower()} risk wards' + (f' in {location_filter}' if location_filter else ''),
                'risk_level': risk_level,
                'wards': wards,
                'location_filter': location_filter,
                'source': f'category_column_{category_col}',
                'total_matches': len(filtered_wards),
                'total_available_wards': len(filtered_gdf)
            }
        
        # Fallback to score-based filtering
        score_columns = [col for col in unified_gdf.columns if 'score' in col.lower()]
        
        if score_columns:
            score_col = score_columns[0]
            
            # Create risk categories based on percentiles
            high_threshold = unified_gdf[score_col].quantile(0.67)
            low_threshold = unified_gdf[score_col].quantile(0.33)
            
            if risk_level == 'High':
                filtered_gdf = unified_gdf[unified_gdf[score_col] >= high_threshold]
            elif risk_level == 'Medium':
                filtered_gdf = unified_gdf[(unified_gdf[score_col] >= low_threshold) & (unified_gdf[score_col] < high_threshold)]
            else:  # Low
                filtered_gdf = unified_gdf[unified_gdf[score_col] < low_threshold]
            
            ward_columns = [col for col in unified_gdf.columns 
                           if any(term in col.lower() for term in ['ward', 'name']) and unified_gdf[col].dtype == 'object']
            
            if not ward_columns:
                return {'status': 'error', 'message': 'No ward identifier found'}
            
            ward_column = ward_columns[0]
            wards = filtered_gdf[ward_column].tolist()[:limit]
            
            return {
                'status': 'success',
                'message': f'Found {len(wards)} {risk_level.lower()} risk wards',
                'risk_level': risk_level,
                'wards': wards,
                'source': f'score_based_{score_col}',
                'total_matches': len(filtered_gdf)
            }
        
        return {
            'status': 'error',
            'message': 'No risk analysis data found in unified dataset'
        }
        
    except Exception as e:
        logger.error(f"Error filtering wards by risk: {e}")
        return {
            'status': 'error',
            'message': f'Error filtering wards: {str(e)}'
        }


def filter_wards_by_criteria(session_id: str, criteria: Dict[str, Any], limit: int = 20) -> Dict[str, Any]:
    """
    Filter wards by custom criteria using unified dataset.
    
    Args:
        session_id: Session identifier
        criteria: Dict with filtering criteria (e.g., {'settlement_type': 'Urban'})
        limit: Maximum number of wards to return
        
    Returns:
        Dict with filtered wards
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No unified dataset available'
            }
        
        # Find ward identifier
        ward_columns = [col for col in unified_gdf.columns 
                       if any(term in col.lower() for term in ['ward', 'name']) and unified_gdf[col].dtype == 'object']
        
        if not ward_columns:
            return {'status': 'error', 'message': 'No ward identifier found'}
        
        ward_column = ward_columns[0]
        
        # Apply filters
        filtered_gdf = unified_gdf.copy()
        applied_filters = []
        
        for column, value in criteria.items():
            if column in unified_gdf.columns:
                if isinstance(value, str):
                    filtered_gdf = filtered_gdf[filtered_gdf[column].astype(str).str.contains(value, case=False, na=False)]
                else:
                    filtered_gdf = filtered_gdf[filtered_gdf[column] == value]
                applied_filters.append(f"{column} = {value}")
        
        wards = filtered_gdf[ward_column].tolist()[:limit]
        
        return {
            'status': 'success',
            'message': f'Found {len(wards)} wards matching criteria',
            'criteria': criteria,
            'applied_filters': applied_filters,
            'wards': wards,
            'total_matches': len(filtered_gdf)
        }
        
    except Exception as e:
        logger.error(f"Error filtering wards by criteria: {e}")
        return {
            'status': 'error',
            'message': f'Error filtering wards: {str(e)}'
        }


def get_session_data_summary(session_id: str) -> Dict[str, Any]:
    """
    Get comprehensive summary of session data from unified dataset.
    
    Args:
        session_id: Session identifier
        
    Returns:
        Dict with comprehensive data summary
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        column_metadata = _get_column_metadata(session_id)
        
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No unified dataset available in session'
            }
        
        # Basic dataset info
        summary = {
            'status': 'success',
            'message': 'Unified dataset summary retrieved',
            'total_wards': len(unified_gdf),
            'total_columns': len(unified_gdf.columns),
            'has_spatial_data': 'geometry' in unified_gdf.columns,
            'memory_usage_mb': round(unified_gdf.memory_usage(deep=True).sum() / 1024 / 1024, 2)
        }
        
        # Column categorization using metadata
        if column_metadata:
            categories = {}
            for col, meta in column_metadata.items():
                cat = meta.get('category', 'other')
                if cat not in categories:
                    categories[cat] = []
                categories[cat].append(col)
            
            summary['column_categories'] = categories
            summary['category_counts'] = {cat: len(cols) for cat, cols in categories.items()}
        
        # Analysis completeness
        composite_columns = [col for col in unified_gdf.columns if 'composite' in col.lower()]
        pca_columns = [col for col in unified_gdf.columns if 'pca' in col.lower() or col.startswith('PC')]
        model_columns = [col for col in unified_gdf.columns if col.startswith('model_')]
        
        summary.update({
            'analysis_complete': len(composite_columns) > 0 or len(pca_columns) > 0,
            'composite_analysis_available': len(composite_columns) > 0,
            'pca_analysis_available': len(pca_columns) > 0,
            'individual_models_available': len(model_columns) > 0,
            'composite_columns': composite_columns,
            'pca_columns': pca_columns,
            'model_columns': model_columns
        })
        
        # Data quality metrics
        completeness = (1 - unified_gdf.isnull().sum().sum() / (len(unified_gdf) * len(unified_gdf.columns))) * 100
        summary['data_quality'] = {
            'overall_completeness_percent': round(completeness, 2),
            'missing_values_total': unified_gdf.isnull().sum().sum(),
            'duplicate_rows': unified_gdf.duplicated().sum()
        }
        
        # Variable information
        numeric_vars = list(unified_gdf.select_dtypes(include=['number']).columns)
        categorical_vars = list(unified_gdf.select_dtypes(include=['object']).columns)
        
        summary.update({
            'numeric_variables': numeric_vars,
            'categorical_variables': categorical_vars,
            'numeric_variable_count': len(numeric_vars),
            'categorical_variable_count': len(categorical_vars)
        })
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting session data summary: {e}")
        return {
            'status': 'error',
            'message': f'Error retrieving data summary: {str(e)}'
        }


 