"""
Spatial Analysis Tools for ChatMRPT

This module provides advanced spatial analysis capabilities including:
- Spatial autocorrelation (Moran's I)
- Local spatial clustering (LISA)
- Hot spot analysis (Getis-Ord Gi*)
- Spatial similarity analysis
- Neighborhood analysis

All functions work with the unified dataset and maintain privacy-first approach.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple
from flask import current_app
import pandas as pd
import geopandas as gpd
import numpy as np
from scipy import stats
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import euclidean_distances

logger = logging.getLogger(__name__)


def _get_unified_dataset(session_id: str) -> Optional[gpd.GeoDataFrame]:
    """Get the unified dataset from session."""
    try:
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


def _create_spatial_weights_matrix(gdf: gpd.GeoDataFrame, method: str = 'queen', k: int = 8) -> np.ndarray:
    """
    Create spatial weights matrix using different methods.
    
    Args:
        gdf: GeoDataFrame with geometry
        method: 'queen', 'rook', 'knn', or 'distance'
        k: number of neighbors for KNN method
        
    Returns:
        Spatial weights matrix (n x n)
    """
    n = len(gdf)
    W = np.zeros((n, n))
    
    if method in ['queen', 'rook']:
        # Contiguity-based weights
        for i, geom_i in enumerate(gdf.geometry):
            for j, geom_j in enumerate(gdf.geometry):
                if i != j:
                    if method == 'queen':
                        # Queen contiguity: shared vertex or edge
                        if geom_i.touches(geom_j) or geom_i.intersects(geom_j):
                            W[i, j] = 1
                    elif method == 'rook':
                        # Rook contiguity: shared edge only
                        intersection = geom_i.intersection(geom_j)
                        if hasattr(intersection, 'length') and intersection.length > 0:
                            W[i, j] = 1
    
    elif method == 'knn':
        # K-nearest neighbors based on centroids
        centroids = np.array([[geom.centroid.x, geom.centroid.y] for geom in gdf.geometry])
        nbrs = NearestNeighbors(n_neighbors=k+1).fit(centroids)
        distances, indices = nbrs.kneighbors(centroids)
        
        for i, neighbors in enumerate(indices):
            for j in neighbors[1:]:  # Skip first (self)
                W[i, j] = 1
    
    elif method == 'distance':
        # Distance-based weights (inverse distance)
        centroids = np.array([[geom.centroid.x, geom.centroid.y] for geom in gdf.geometry])
        distances = euclidean_distances(centroids)
        
        # Use inverse distance, set diagonal to 0
        with np.errstate(divide='ignore'):
            W = 1.0 / distances
            np.fill_diagonal(W, 0)
            
        # Threshold very small weights
        W[W < 0.001] = 0
    
    # Row-standardize weights
    row_sums = W.sum(axis=1)
    row_sums[row_sums == 0] = 1  # Avoid division by zero
    W = W / row_sums[:, np.newaxis]
    
    return W


def _calculate_morans_i(values: np.ndarray, W: np.ndarray) -> Dict[str, float]:
    """
    Calculate Moran's I spatial autocorrelation statistic.
    
    Args:
        values: Variable values
        W: Spatial weights matrix
        
    Returns:
        Dictionary with Moran's I, expected value, variance, and z-score
    """
    n = len(values)
    if n < 3:
        return {'morans_i': 0, 'expected': 0, 'variance': 0, 'z_score': 0, 'p_value': 1.0}
    
    # Remove missing values
    valid_mask = ~np.isnan(values)
    if valid_mask.sum() < 3:
        return {'morans_i': 0, 'expected': 0, 'variance': 0, 'z_score': 0, 'p_value': 1.0}
    
    values = values[valid_mask]
    W = W[np.ix_(valid_mask, valid_mask)]
    n = len(values)
    
    # Standardize values
    y = values - np.mean(values)
    
    # Calculate Moran's I
    W_sum = np.sum(W)
    if W_sum == 0:
        return {'morans_i': 0, 'expected': 0, 'variance': 0, 'z_score': 0, 'p_value': 1.0}
    
    numerator = np.sum(W * np.outer(y, y))
    denominator = np.sum(y**2)
    
    if denominator == 0:
        return {'morans_i': 0, 'expected': 0, 'variance': 0, 'z_score': 0, 'p_value': 1.0}
    
    I = (n / W_sum) * (numerator / denominator)
    
    # Expected value and variance
    E_I = -1 / (n - 1)
    
    # Simplified variance calculation
    S0 = W_sum
    S1 = 0.5 * np.sum((W + W.T)**2)
    S2 = np.sum(np.sum(W + W.T, axis=1)**2)
    
    b2 = n * np.sum(y**4) / (np.sum(y**2)**2)
    
    var_I = ((n * ((n**2 - 3*n + 3) * S1 - n*S2 + 3*S0**2)) - 
             (b2 * ((n**2 - n) * S1 - 2*n*S2 + 6*S0**2))) / ((n-1) * (n-2) * (n-3) * S0**2)
    
    if var_I <= 0:
        var_I = 1e-10
    
    # Z-score and p-value
    z_score = (I - E_I) / np.sqrt(var_I)
    p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))
    
    return {
        'morans_i': float(I),
        'expected': float(E_I),
        'variance': float(var_I),
        'z_score': float(z_score),
        'p_value': float(p_value)
    }


def _calculate_local_morans_i(values: np.ndarray, W: np.ndarray) -> Dict[str, np.ndarray]:
    """
    Calculate Local Moran's I (LISA) statistics.
    
    Args:
        values: Variable values
        W: Spatial weights matrix
        
    Returns:
        Dictionary with local I values, z-scores, and cluster types
    """
    n = len(values)
    if n < 3:
        return {
            'local_i': np.zeros(n),
            'z_scores': np.zeros(n),
            'p_values': np.ones(n),
            'cluster_types': ['Not Significant'] * n
        }
    
    # Standardize values
    y = values - np.mean(values)
    y_std = y / np.std(y) if np.std(y) > 0 else y
    
    # Calculate local Moran's I for each location
    local_i = np.zeros(n)
    for i in range(n):
        if not np.isnan(y_std[i]):
            neighbors = W[i, :] > 0
            if neighbors.sum() > 0:
                local_i[i] = y_std[i] * np.sum(W[i, neighbors] * y_std[neighbors])
    
    # Calculate z-scores (simplified)
    E_Ii = -np.sum(W, axis=1) / (n - 1)
    var_Ii = np.var(local_i) if np.var(local_i) > 0 else 1e-10
    z_scores = (local_i - E_Ii) / np.sqrt(var_Ii)
    p_values = 2 * (1 - stats.norm.cdf(np.abs(z_scores)))
    
    # Classify cluster types
    cluster_types = []
    y_mean = np.mean(values)
    
    for i in range(n):
        if p_values[i] < 0.05:  # Significant
            neighbors = W[i, :] > 0
            if neighbors.sum() > 0:
                neighbor_mean = np.mean(values[neighbors])
                
                if values[i] > y_mean and neighbor_mean > y_mean:
                    cluster_types.append('High-High')
                elif values[i] < y_mean and neighbor_mean < y_mean:
                    cluster_types.append('Low-Low')
                elif values[i] > y_mean and neighbor_mean < y_mean:
                    cluster_types.append('High-Low')
                elif values[i] < y_mean and neighbor_mean > y_mean:
                    cluster_types.append('Low-High')
                else:
                    cluster_types.append('Not Significant')
            else:
                cluster_types.append('Not Significant')
        else:
            cluster_types.append('Not Significant')
    
    return {
        'local_i': local_i,
        'z_scores': z_scores,
        'p_values': p_values,
        'cluster_types': cluster_types
    }


def spatial_autocorrelation_analysis(session_id: str, variable: str, method: str = 'queen') -> Dict[str, Any]:
    """
    Perform spatial autocorrelation analysis using Moran's I.
    
    Args:
        session_id: Session identifier
        variable: Variable name to analyze
        method: Spatial weights method ('queen', 'rook', 'knn', 'distance')
        
    Returns:
        Dict with analysis results
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No dataset available for spatial analysis'
            }
        
        if variable not in unified_gdf.columns:
            available_vars = [col for col in unified_gdf.columns if unified_gdf[col].dtype in ['float64', 'int64']]
            return {
                'status': 'error',
                'message': f'Variable "{variable}" not found. Available numeric variables: {available_vars[:10]}'
            }
        
        # Simple spatial autocorrelation using coordinates
        if 'centroid_lat' in unified_gdf.columns and 'centroid_lon' in unified_gdf.columns:
            coords = unified_gdf[['centroid_lon', 'centroid_lat']].values
        else:
            return {
                'status': 'error',
                'message': 'No coordinate columns found for spatial analysis'
            }
        
        values = unified_gdf[variable].dropna()
        coords = coords[unified_gdf[variable].notna()]
        
        if len(values) < 5:
            return {
                'status': 'error',
                'message': 'Insufficient data points for spatial analysis'
            }
        
        # Simple KNN-based spatial weights
        k = min(8, len(values) - 1)
        nbrs = NearestNeighbors(n_neighbors=k).fit(coords)
        distances, indices = nbrs.kneighbors(coords)
        
        # Calculate simplified Moran's I
        n = len(values)
        mean_val = np.mean(values)
        
        numerator = 0
        denominator = np.sum((values - mean_val) ** 2)
        total_weights = 0
        
        for i in range(n):
            for j in range(1, k):  # Skip self (index 0)
                neighbor_idx = indices[i, j]
                weight = 1.0 / (distances[i, j] + 1e-6)  # Inverse distance weight
                numerator += weight * (values.iloc[i] - mean_val) * (values.iloc[neighbor_idx] - mean_val)
                total_weights += weight
        
        if denominator > 0 and total_weights > 0:
            morans_i = (n / total_weights) * (numerator / denominator)
        else:
            morans_i = 0
        
        # Expected value and simplified significance test
        expected_i = -1.0 / (n - 1)
        z_score = (morans_i - expected_i) / 0.1  # Simplified variance
        p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))
        
        # Interpretation
        interpretation = "No spatial autocorrelation"
        if p_value < 0.05:
            if morans_i > 0:
                interpretation = "Positive spatial autocorrelation (spatial clustering)"
            else:
                interpretation = "Negative spatial autocorrelation (spatial dispersion)"
        
        return {
            'status': 'success',
            'message': f'Spatial autocorrelation analysis completed for {variable}',
            'variable': variable,
            'method': method,
            'valid_observations': len(values),
            'morans_i': float(morans_i),
            'expected_i': float(expected_i),
            'z_score': float(z_score),
            'p_value': float(p_value),
            'interpretation': interpretation,
            'is_significant': p_value < 0.05,
            'spatial_weights_method': f'KNN (k={k}) inverse distance'
        }
        
    except Exception as e:
        logger.error(f"Error in spatial autocorrelation analysis: {e}")
        return {
            'status': 'error',
            'message': f'Error in spatial autocorrelation analysis: {str(e)}'
        }


def local_spatial_clustering_analysis(session_id: str, variable: str, method: str = 'queen') -> Dict[str, Any]:
    """
    Perform Local Indicators of Spatial Association (LISA) analysis.
    
    Args:
        session_id: Session identifier
        variable: Variable name to analyze
        method: Spatial weights method
        
    Returns:
        Dict with local clustering results
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No dataset available for local spatial analysis'
            }
        
        if variable not in unified_gdf.columns:
            return {
                'status': 'error',
                'message': f'Variable "{variable}" not found'
            }
        
        # Create spatial weights matrix
        logger.info(f"🗺️ Creating spatial weights for local analysis using {method}")
        W = _create_spatial_weights_matrix(unified_gdf, method=method)
        
        # Get variable values
        values = unified_gdf[variable].values
        
        # Calculate local Moran's I
        logger.info(f"📍 Calculating Local Moran's I (LISA) for {variable}")
        lisa_result = _calculate_local_morans_i(values, W)
        
        # Count cluster types
        cluster_counts = {}
        for cluster_type in lisa_result['cluster_types']:
            cluster_counts[cluster_type] = cluster_counts.get(cluster_type, 0) + 1
        
        # Find significant hot spots and cold spots
        hot_spots = []
        cold_spots = []
        outliers = []
        
        ward_col = 'WardName' if 'WardName' in unified_gdf.columns else unified_gdf.columns[0]
        
        for i, (cluster_type, p_val) in enumerate(zip(lisa_result['cluster_types'], lisa_result['p_values'])):
            if p_val < 0.05:  # Significant
                ward_name = unified_gdf.iloc[i][ward_col] if ward_col in unified_gdf.columns else f"Ward_{i}"
                ward_value = values[i] if not np.isnan(values[i]) else 0
                
                if cluster_type == 'High-High':
                    hot_spots.append({'ward': ward_name, 'value': float(ward_value), 'local_i': float(lisa_result['local_i'][i])})
                elif cluster_type == 'Low-Low':
                    cold_spots.append({'ward': ward_name, 'value': float(ward_value), 'local_i': float(lisa_result['local_i'][i])})
                elif cluster_type in ['High-Low', 'Low-High']:
                    outliers.append({'ward': ward_name, 'value': float(ward_value), 'type': cluster_type, 'local_i': float(lisa_result['local_i'][i])})
        
        return {
            'status': 'success',
            'message': f'Local spatial clustering analysis completed for {variable}',
            'variable': variable,
            'method': method,
            'total_wards': len(values),
            'significant_clusters': sum(1 for p in lisa_result['p_values'] if p < 0.05),
            'cluster_counts': cluster_counts,
            'hot_spots': sorted(hot_spots, key=lambda x: x['local_i'], reverse=True)[:10],  # Top 10
            'cold_spots': sorted(cold_spots, key=lambda x: x['local_i'])[:10],  # Bottom 10
            'spatial_outliers': outliers[:10],  # Top 10
            'interpretation': {
                'hot_spots_count': len(hot_spots),
                'cold_spots_count': len(cold_spots),
                'outliers_count': len(outliers),
                'dominant_pattern': max(cluster_counts.items(), key=lambda x: x[1])[0] if cluster_counts else 'None'
            }
        }
        
    except Exception as e:
        logger.error(f"Error in local spatial clustering analysis: {e}")
        return {
            'status': 'error',
            'message': f'Error in local spatial clustering analysis: {str(e)}'
        }


def spatial_similarity_analysis(session_id: str, ward_name: str, variables: List[str], top_n: int = 10) -> Dict[str, Any]:
    """
    Find wards with similar risk profiles to a reference ward.
    
    Args:
        session_id: Session identifier
        ward_name: Reference ward name
        variables: List of variables to compare
        top_n: Number of similar wards to return
        
    Returns:
        Dict with similar wards and similarity scores
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No dataset available for similarity analysis'
            }
        
        ward_col = 'WardName' if 'WardName' in unified_gdf.columns else None
        if not ward_col:
            return {
                'status': 'error',
                'message': 'No ward name column found'
            }
        
        # Find reference ward
        ref_ward_mask = unified_gdf[ward_col].str.contains(ward_name, case=False, na=False)
        if not ref_ward_mask.any():
            available_wards = unified_gdf[ward_col].dropna().unique()[:10]
            return {
                'status': 'error',
                'message': f'Ward "{ward_name}" not found. Available wards: {list(available_wards)}'
            }
        
        ref_ward_idx = ref_ward_mask.idxmax()
        
        # Check variables
        available_vars = [var for var in variables if var in unified_gdf.columns]
        if not available_vars:
            numeric_vars = [col for col in unified_gdf.columns if unified_gdf[col].dtype in ['float64', 'int64']]
            return {
                'status': 'error',
                'message': f'None of the specified variables found. Available numeric variables: {numeric_vars[:10]}'
            }
        
        # Prepare data for similarity analysis
        logger.info(f"🔍 Analyzing spatial similarity for {ward_name} using variables: {available_vars}")
        
        data_subset = unified_gdf[available_vars].copy()
        
        # Handle missing values
        data_subset = data_subset.fillna(data_subset.mean())
        
        # Standardize data
        scaler = StandardScaler()
        data_scaled = scaler.fit_transform(data_subset)
        
        # Calculate distances from reference ward
        ref_profile = data_scaled[ref_ward_idx].reshape(1, -1)
        distances = euclidean_distances(ref_profile, data_scaled)[0]
        
        # Convert distances to similarity scores (higher = more similar)
        max_distance = np.max(distances)
        if max_distance > 0:
            similarity_scores = 1 - (distances / max_distance)
        else:
            similarity_scores = np.ones_like(distances)
        
        # Get top similar wards (excluding the reference ward itself)
        similar_indices = np.argsort(similarity_scores)[::-1]
        
        similar_wards = []
        count = 0
        for idx in similar_indices:
            if idx != ref_ward_idx and count < top_n:
                ward_data = unified_gdf.iloc[idx]
                ward_info = {
                    'ward_name': ward_data[ward_col],
                    'similarity_score': float(similarity_scores[idx]),
                    'distance_score': float(distances[idx]),
                    'profile': {}
                }
                
                # Add variable values for comparison
                for var in available_vars:
                    ward_info['profile'][var] = float(ward_data[var]) if not pd.isna(ward_data[var]) else None
                
                similar_wards.append(ward_info)
                count += 1
        
        # Get reference ward profile
        ref_ward_data = unified_gdf.iloc[ref_ward_idx]
        ref_profile_dict = {}
        for var in available_vars:
            ref_profile_dict[var] = float(ref_ward_data[var]) if not pd.isna(ref_ward_data[var]) else None
        
        return {
            'status': 'success',
            'message': f'Spatial similarity analysis completed for {ward_name}',
            'reference_ward': {
                'name': ref_ward_data[ward_col],
                'profile': ref_profile_dict
            },
            'variables_used': available_vars,
            'similar_wards': similar_wards,
            'analysis_method': 'Euclidean distance with standardized variables',
            'total_comparisons': len(unified_gdf) - 1
        }
        
    except Exception as e:
        logger.error(f"Error in spatial similarity analysis: {e}")
        return {
            'status': 'error',
            'message': f'Error in spatial similarity analysis: {str(e)}'
        }


def spatial_hot_spot_analysis(session_id: str, variable: str, confidence_level: float = 0.95) -> Dict[str, Any]:
    """
    Identify statistically significant hot spots and cold spots using Getis-Ord Gi*.
    
    Args:
        session_id: Session identifier
        variable: Variable name to analyze
        confidence_level: Statistical confidence level (0.90, 0.95, 0.99)
        
    Returns:
        Dict with hot spot analysis results
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No dataset available for hot spot analysis'
            }
        
        if variable not in unified_gdf.columns:
            return {
                'status': 'error',
                'message': f'Variable "{variable}" not found'
            }
        
        # Create spatial weights matrix
        logger.info(f"🔥 Performing hot spot analysis for {variable}")
        W = _create_spatial_weights_matrix(unified_gdf, method='knn', k=8)
        
        values = unified_gdf[variable].values
        valid_mask = ~np.isnan(values)
        
        if valid_mask.sum() < 5:
            return {
                'status': 'error',
                'message': f'Insufficient valid data points for hot spot analysis'
            }
        
        # Calculate Getis-Ord Gi* statistic (simplified version)
        n = len(values)
        gi_stats = np.zeros(n)
        
        for i in range(n):
            if valid_mask[i]:
                # Include focal area in calculation (Gi*)
                neighbors = W[i, :] > 0
                neighbors[i] = True  # Include self
                
                if neighbors.sum() > 1:
                    neighbor_values = values[neighbors]
                    neighbor_weights = W[i, neighbors] if not neighbors[i] else np.concatenate([[1], W[i, neighbors & ~np.array([j == i for j in range(n)])]])
                    
                    if len(neighbor_weights) == len(neighbor_values):
                        weighted_sum = np.sum(neighbor_weights * neighbor_values)
                        sum_weights = np.sum(neighbor_weights)
                        
                        if sum_weights > 0:
                            gi_stats[i] = weighted_sum / sum_weights
        
        # Convert to z-scores
        mean_gi = np.nanmean(gi_stats[valid_mask])
        std_gi = np.nanstd(gi_stats[valid_mask])
        
        if std_gi > 0:
            z_scores = (gi_stats - mean_gi) / std_gi
        else:
            z_scores = np.zeros_like(gi_stats)
        
        # Determine significance thresholds
        alpha = 1 - confidence_level
        z_threshold = stats.norm.ppf(1 - alpha/2)
        
        # Classify hot spots and cold spots
        ward_col = 'WardName' if 'WardName' in unified_gdf.columns else unified_gdf.columns[0]
        
        hot_spots = []
        cold_spots = []
        
        for i in range(n):
            if valid_mask[i]:
                z_score = z_scores[i]
                p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))
                
                if abs(z_score) > z_threshold:  # Significant
                    ward_name = unified_gdf.iloc[i][ward_col] if ward_col in unified_gdf.columns else f"Ward_{i}"
                    ward_value = values[i]
                    
                    spot_info = {
                        'ward': ward_name,
                        'value': float(ward_value),
                        'z_score': float(z_score),
                        'p_value': float(p_value),
                        'gi_stat': float(gi_stats[i])
                    }
                    
                    if z_score > 0:
                        hot_spots.append(spot_info)
                    else:
                        cold_spots.append(spot_info)
        
        # Sort by z-score strength
        hot_spots = sorted(hot_spots, key=lambda x: x['z_score'], reverse=True)
        cold_spots = sorted(cold_spots, key=lambda x: x['z_score'])
        
        return {
            'status': 'success',
            'message': f'Hot spot analysis completed for {variable}',
            'variable': variable,
            'confidence_level': confidence_level,
            'z_threshold': float(z_threshold),
            'total_wards': int(valid_mask.sum()),
            'hot_spots': hot_spots[:15],  # Top 15 hot spots
            'cold_spots': cold_spots[:15],  # Top 15 cold spots
            'summary': {
                'significant_hot_spots': len(hot_spots),
                'significant_cold_spots': len(cold_spots),
                'percent_significant': round((len(hot_spots) + len(cold_spots)) / valid_mask.sum() * 100, 1)
            },
            'interpretation': {
                'hot_spots_description': f"Areas with significantly high {variable} values and high neighboring values",
                'cold_spots_description': f"Areas with significantly low {variable} values and low neighboring values",
                'method': "Getis-Ord Gi* statistic with KNN spatial weights"
            }
        }
        
    except Exception as e:
        logger.error(f"Error in spatial hot spot analysis: {e}")
        return {
            'status': 'error',
            'message': f'Error in spatial hot spot analysis: {str(e)}'
        }


def spatial_dependency_test(session_id: str, variable: str, state: str = None) -> Dict[str, Any]:
    """
    Test if the malaria burden shows spatial dependency patterns.
    
    Args:
        session_id: Session identifier
        variable: Variable to test (e.g., 'composite_score', 'u5_tpr_rdt')
        state: Optional state filter
        
    Returns:
        Dict with spatial dependency test results
    """
    try:
        unified_gdf = _get_unified_dataset(session_id)
        if unified_gdf is None:
            return {
                'status': 'error',
                'message': 'No dataset available for spatial dependency test'
            }
        
        # Filter by state if specified
        if state:
            state_col = 'state_name' if 'state_name' in unified_gdf.columns else 'StateCode'
            if state_col in unified_gdf.columns:
                state_mask = unified_gdf[state_col].str.contains(state, case=False, na=False)
                if state_mask.any():
                    unified_gdf = unified_gdf[state_mask].copy()
                    logger.info(f"🗺️ Filtered to {state}: {len(unified_gdf)} wards")
        
        # Test multiple spatial weight methods
        methods = ['knn']
        results = {}
        
        for method in methods:
            logger.info(f"🧪 Testing spatial dependency using {method} weights")
            result = spatial_autocorrelation_analysis(session_id, variable, method)
            
            if result['status'] == 'success':
                results[method] = {
                    'morans_i': result['morans_i'],
                    'p_value': result['p_value'],
                    'is_significant': result['is_significant'],
                    'interpretation': result['interpretation']
                }
        
        # Overall assessment
        significant_tests = sum(1 for r in results.values() if r['is_significant'])
        total_tests = len(results)
        
        spatial_dependency = "No"
        confidence = "Low"
        
        if significant_tests == total_tests:
            spatial_dependency = "Yes"
            confidence = "High"
        elif significant_tests > 0:
            spatial_dependency = "Possible"
            confidence = "Medium"
        
        # Get mean Moran's I across methods
        mean_morans_i = np.mean([r['morans_i'] for r in results.values()]) if results else 0
        
        return {
            'status': 'success',
            'message': f'Spatial dependency test completed for {variable}',
            'variable': variable,
            'state_filter': state,
            'spatial_dependency': spatial_dependency,
            'confidence_level': confidence,
            'mean_morans_i': float(mean_morans_i),
            'tests_significant': f"{significant_tests}/{total_tests}",
            'detailed_results': results,
            'interpretation': {
                'summary': f"Malaria burden ({variable}) shows {spatial_dependency.lower()} spatial dependency",
                'evidence': f"{significant_tests} out of {total_tests} spatial autocorrelation tests were significant",
                'implication': "Spatially clustered patterns" if spatial_dependency == "Yes" else "Random spatial distribution" if spatial_dependency == "No" else "Mixed spatial patterns"
            }
        }
        
    except Exception as e:
        logger.error(f"Error in spatial dependency test: {e}")
        return {
            'status': 'error',
            'message': f'Error in spatial dependency test: {str(e)}'
        } 