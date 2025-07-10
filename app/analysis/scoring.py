# app/analysis/scoring.py
import logging
import numpy as np
import pandas as pd
import itertools
import traceback
from typing import Dict, List, Optional, Any, Union
import concurrent.futures

# Set up logging
from app.services.variable_resolution_service import variable_resolver
logger = logging.getLogger(__name__)


def compute_composite_score_model(normalized_data, variables, metadata=None, step_id=None):
    """
    Calculate a single composite score model using selected normalized variables
    
    Args:
        normalized_data: DataFrame with normalized variables
        variables: List of normalized column names to use
        metadata: Optional AnalysisMetadata instance for logging
        step_id: Optional step ID for linking to metadata
        
    Returns:
        Series with composite scores
    """
    try:
        # Compute simple mean of normalized values
        composite_scores = normalized_data[variables].mean(axis=1)
        
        # Record the calculation if metadata is provided
        if metadata and step_id:
            variable_names = [var.replace('normalization_', '') for var in variables]
            metadata.record_calculation(
                step_id,
                'composite_score',
                'mean_aggregation',
                {'variables': variable_names, 'row_count': len(normalized_data)},
                {'min_score': float(composite_scores.min()), 
                 'max_score': float(composite_scores.max()),
                 'mean_score': float(composite_scores.mean())}
            )
        
        return composite_scores
    except Exception as e:
        logger.error("Error computing composite score model: {}".format(str(e)))
        # Return zeros as fallback
        return pd.Series(0, index=normalized_data.index)


def compute_composite_scores(normalized_data, selected_vars=None, method='mean', n_jobs=-1, metadata=None):
    """
    Calculate composite scores using selected normalized variables
    
    Note: This function now only supports 'mean' method. For PCA analysis, 
    use the independent PCA pipeline in app.analysis.pca_pipeline
    
    Args:
        normalized_data: DataFrame with normalized variables
        selected_vars: List of variables to use (if None, use all normalized variables)
        method: Aggregation method (only 'mean' supported now)
        n_jobs: Number of parallel jobs (-1 for all available cores)
        metadata: Optional AnalysisMetadata instance for logging
        
    Returns:
        Dict with scores DataFrame and model formulas
    """
    try:
        # Validate method
        if method != 'mean':
            logger.warning(f"Method '{method}' not supported. Using 'mean' method. For PCA analysis, use the independent PCA pipeline.")
            method = 'mean'
        
        # Record analysis step if metadata is provided
        step_id = None
        if metadata:
            input_summary = {
                'row_count': len(normalized_data),
                'selected_vars_count': len(selected_vars) if selected_vars else 0,
                'method': method
            }
            step_id = metadata.record_step(
                'compute_composite_scores',
                input_summary,
                None,  # Output summary will be updated later
                'composite_score_calculation',
                {'method': method, 'n_jobs': n_jobs}
            )
        
        # Make sure WardName column is present
        exists, resolved_col = variable_resolver.check_column_exists('WardName', list(normalized_data.columns))
        if not exists:
            raise ValueError("WardName column must be present in normalized data")
        
        # Get normalized columns (starting with "normalization_")
        norm_cols = [col for col in normalized_data.columns if col.startswith('normalization_')]
        
        # If specific variables are selected, filter columns
        if selected_vars:
            selected_norm_cols = []
            for var in selected_vars:
                norm_col = "normalization_{}".format(var.lower())
                if norm_col in norm_cols:
                    selected_norm_cols.append(norm_col)
                elif var in norm_cols:  # Allow already normalized column names
                    selected_norm_cols.append(var)
            norm_cols = selected_norm_cols
            
            if metadata:
                metadata.record_decision(
                    step_id,
                    'variable_selection',
                    options=[col.replace('normalization_', '') for col in normalized_data.columns 
                             if col.startswith('normalization_')],
                    criteria='user specified selection',
                    selected_option=[col.replace('normalization_', '') for col in norm_cols]
                )
        
        # Need at least 2 variables for composite score
        if len(norm_cols) < 2:
            raise ValueError("Need at least 2 normalized variables. Found {}.".format(len(norm_cols)))
        
        # Initialize result dataframe with WardName
        result = pd.DataFrame({'WardName': normalized_data['WardName']})
        
        # Generate LIMITED combinations to avoid exponential explosion
        model_formulas = []
        combinations = []
        
        # Strategy: Generate a reasonable number of models (max ~20 combinations)
        if len(norm_cols) <= 3:
            # For 2-3 variables: use all combinations
            for r in range(2, len(norm_cols) + 1):
                combinations.extend(list(itertools.combinations(norm_cols, r)))
        elif len(norm_cols) <= 5:
            # For 4-5 variables: use ALL combinations for accurate pagination
            for r in range(2, len(norm_cols) + 1):
                combinations.extend(list(itertools.combinations(norm_cols, r)))
        else:
            # For 6+ variables (fallback): heavily limit combinations
            logger.warning("Too many variables ({}), limiting to essential combinations".format(len(norm_cols)))
            # Use only first 5 variables to prevent explosion
            norm_cols = norm_cols[:5]
            # Add pairs from first 5
            combinations.extend(list(itertools.combinations(norm_cols, 2))[:10])  # Max 10 pairs
            # Add a few triplets
            combinations.extend(list(itertools.combinations(norm_cols, 3))[:5])   # Max 5 triplets
            # Add one model with all 5
            combinations.append(tuple(norm_cols))
        
        # Log combination count
        if metadata:
            metadata.record_calculation(
                step_id,
                'combinations_generation',
                'limited_combinations',
                {'variable_count': len(norm_cols)},
                {'combination_count': len(combinations)}
            )
        
        logger.info("Generating {} composite score models from {} variables".format(len(combinations), len(norm_cols)))
        
        # Define worker function for parallel processing
        def compute_model(i, combo):
            model_name = "model_{}".format(i+1)
            
            # Calculate composite score using mean method
            result_series = compute_composite_score_model(
                normalized_data, list(combo), metadata, step_id
            )
            formula = {
                'model': model_name,
                'variables': [col.replace('normalization_', '') for col in combo],
                'method': 'mean'
            }
            
            return model_name, result_series, formula
        
        # Calculate composite score for each combination
        # Use sequential processing for better reliability and debugging
        model_results = []
        for i, combo in enumerate(combinations):
            try:
                model_name, result_series, formula = compute_model(i, combo)
                model_results.append((model_name, result_series, formula))
            except Exception as e:
                logger.error("Error computing model {}: {}".format(i+1, str(e)))
                continue
        
        # Process results
        for model_name, result_series, formula in model_results:
            result[model_name] = result_series
            model_formulas.append(formula)
        
        # Update metadata with output summary
        if metadata:
            output_summary = {
                'model_count': len(model_formulas),
                'ward_count': len(result),
                'method_used': method
            }
            # Update the step with output information
            for step in metadata.steps:
                if step['step_id'] == step_id:
                    step['output_summary'] = output_summary
                    break
        
        logger.info("Successfully generated {} composite score models".format(len(model_formulas)))
        
        # Return simplified result for mean method only
        return {
            'scores': result,
            'formulas': model_formulas,
            'method': method
        }
    
    except Exception as e:
        logger.error("Error computing composite scores: {}".format(str(e)))
        traceback.print_exc()
        raise


def analyze_vulnerability(composite_scores, n_categories=3, metadata=None):
    """
    Analyze vulnerability based on composite scores
    
    Args:
        composite_scores: Dict with scores DataFrame and model formulas
        n_categories: Number of vulnerability categories
        metadata: Optional AnalysisMetadata instance for logging
        
    Returns:
        DataFrame with vulnerability rankings
    """
    print(f"\n🔍 VULNERABILITY DEBUG: Starting analyze_vulnerability function")
    print(f"📊 VULNERABILITY DEBUG: n_categories={n_categories}")
    print(f"🔧 VULNERABILITY DEBUG: composite_scores type: {type(composite_scores)}")
    
    try:
        # Record analysis step if metadata is provided
        step_id = None
        if metadata:
            input_summary = {
                'model_count': len([col for col in composite_scores['scores'].columns 
                                   if col.startswith('model_')]),
                'ward_count': len(composite_scores['scores']),
                'n_categories': n_categories
            }
            step_id = metadata.record_step(
                'analyze_vulnerability',
                input_summary,
                None,  # Output summary will be updated later
                'median_rank_calculation',
                {'n_categories': n_categories}
            )
        
        # Extract scores dataframe
        scores_df = composite_scores['scores']
        print(f"📈 VULNERABILITY DEBUG: Scores DataFrame shape: {scores_df.shape}")
        print(f"📋 VULNERABILITY DEBUG: Scores DataFrame columns: {list(scores_df.columns)}")
        
        # Get model columns
        model_cols = [col for col in scores_df.columns if col.startswith('model_')]
        print(f"🤖 VULNERABILITY DEBUG: Found {len(model_cols)} model columns: {model_cols}")
        
        if not model_cols:
            print("❌ VULNERABILITY DEBUG: No model scores found!")
            raise ValueError("No model scores found in composite scores")
        
        # Initialize results dataframe with WardName
        result = scores_df[['WardName']].copy()
        print(f"🏘️ VULNERABILITY DEBUG: Result DataFrame initialized with {len(result)} wards")
        
        # Calculate median score across all models
        result['median_score'] = scores_df[model_cols].median(axis=1)
        print(f"📊 VULNERABILITY DEBUG: Median scores calculated, range: {result['median_score'].min():.3f} to {result['median_score'].max():.3f}")
        
        if metadata:
            metadata.record_calculation(
                step_id,
                'vulnerability',
                'median_score_calculation',
                {'model_count': len(model_cols), 'ward_count': len(scores_df)},
                {'min_score': float(result['median_score'].min()), 
                 'max_score': float(result['median_score'].max()),
                 'mean_score': float(result['median_score'].mean())}
            )
        
        # Sort by median score (descending) to get overall rank
        result = result.sort_values('median_score', ascending=False)
        result['overall_rank'] = range(1, len(result) + 1)
        print(f"🏆 VULNERABILITY DEBUG: Ranking completed, top ward: {result.iloc[0]['WardName']} (score: {result.iloc[0]['median_score']:.3f})")
        
        # Reset index
        result = result.reset_index(drop=True)
        
        # SIMPLE RISK CATEGORIZATION: High, Medium, Low Risk (consistent with PCA)
        n_wards = len(result)
        
        # Simple 3-category system: High, Medium, Low Risk
        # Divide into thirds (approximately)
        high_risk_count = n_wards // 3
        low_risk_count = n_wards // 3
        medium_risk_count = n_wards - high_risk_count - low_risk_count  # Remainder goes to medium
        
        print(f"🎯 RISK CATEGORIZATION: Using simplified High/Medium/Low Risk system")
        print(f"📊 Total wards: {n_wards}")
        print(f"🚨 High Risk: {high_risk_count} wards")
        print(f"📋 Medium Risk: {medium_risk_count} wards")
        print(f"✅ Low Risk: {low_risk_count} wards")
        
        # Assign risk categories based on ranking (higher rank = higher vulnerability)
        result['vulnerability_category'] = 'Medium Risk'  # Default
        result.loc[:high_risk_count-1, 'vulnerability_category'] = 'High Risk'
        result.loc[n_wards-low_risk_count:, 'vulnerability_category'] = 'Low Risk'
        
        # Check the categorization results
        category_counts = result['vulnerability_category'].value_counts()
        print(f"✅ RISK CATEGORIZATION: Complete!")
        print(f"🎯 Risk distribution: {dict(category_counts)}")
        
        # Record category counts if metadata is provided
        if metadata:
            category_counts_dict = result['vulnerability_category'].value_counts().to_dict()
            metadata.record_calculation(
                step_id,
                'vulnerability',
                'risk_assignment',
                {'approach': 'simplified_risk_levels', 'high_risk_count': high_risk_count, 'low_risk_count': low_risk_count},
                {'category_counts': category_counts_dict}
            )
            
            # Identify the most important wards for decision making
            top_5_vulnerable = result.head(5)['WardName'].tolist()
            bottom_5_vulnerable = result.tail(5)['WardName'].tolist()
            high_risk_wards = result[result['vulnerability_category'] == 'High Risk']['WardName'].tolist()
            
            metadata.record_calculation(
                step_id,
                'vulnerability',
                'priority_wards_identification',
                {'criterion': 'simplified_risk_levels'},
                {
                    'top_5_most_vulnerable': top_5_vulnerable,
                    'bottom_5_least_vulnerable': bottom_5_vulnerable,
                    'high_risk_wards': high_risk_wards
                }
            )
        
        # Update metadata with output summary
        if metadata:
            output_summary = {
                'ward_count': len(result),
                'risk_categories': {cat: int(count) for cat, count in category_counts.items()},
                'top_5_vulnerable': top_5_vulnerable,
                'bottom_5_vulnerable': bottom_5_vulnerable,
                'high_risk_count': high_risk_count,
                'low_risk_count': low_risk_count
            }
            # Update the step with output information
            for step in metadata.steps:
                if step['step_id'] == step_id:
                    step['output_summary'] = output_summary
                    break
        
        print(f"🎉 VULNERABILITY DEBUG: Analysis complete, returning DataFrame with shape: {result.shape}")
        return result
    
    except Exception as e:
        print(f"💥 VULNERABILITY DEBUG: Error in analyze_vulnerability: {str(e)}")
        logger.error("Error analyzing vulnerability: {}".format(str(e)))
        traceback.print_exc()
        raise


def get_scoring_summary(composite_scores, vulnerability_analysis=None):
    """
    Generate a summary of the scoring analysis
    
    Args:
        composite_scores: Dict with scores DataFrame and model formulas
        vulnerability_analysis: Optional DataFrame from analyze_vulnerability()
        
    Returns:
        Dict: Summary of scoring results
    """
    try:
        scores_df = composite_scores['scores']
        formulas = composite_scores['formulas']
        
        # Get model columns
        model_cols = [col for col in scores_df.columns if col.startswith('model_')]
        
        # Calculate basic statistics
        summary = {
            'model_count': len(model_cols),
            'ward_count': len(scores_df),
            'variables_used': [],
            'score_statistics': {},
            'model_formulas': formulas
        }
        
        # Collect all variables used across models
        all_variables = set()
        for formula in formulas:
            all_variables.update(formula.get('variables', []))
        summary['variables_used'] = sorted(list(all_variables))
        
        # Calculate statistics for each model
        for col in model_cols:
            exists, resolved_col = variable_resolver.check_column_exists(col, list(scores_df.columns))
            if exists:
                summary['score_statistics'][col] = {
                    'min': float(scores_df[col].min()),
                    'max': float(scores_df[col].max()),
                    'mean': float(scores_df[col].mean()),
                    'median': float(scores_df[col].median()),
                    'std': float(scores_df[col].std())
                }
        
        # Add vulnerability analysis if provided
        if vulnerability_analysis is not None:
            vulnerability_summary = {
                'categories': vulnerability_analysis['vulnerability_category'].value_counts().to_dict(),
                'top_5_vulnerable': vulnerability_analysis.head(5)['WardName'].tolist(),
                'bottom_5_vulnerable': vulnerability_analysis.tail(5)['WardName'].tolist(),
                'median_score_range': {
                    'min': float(vulnerability_analysis['median_score'].min()),
                    'max': float(vulnerability_analysis['median_score'].max())
                }
            }
            summary['vulnerability_analysis'] = vulnerability_summary
        
        return summary
        
    except Exception as e:
        logger.error("Error generating scoring summary: {}".format(str(e)))
        return {
            'error': str(e),
            'model_count': 0,
            'ward_count': 0,
            'variables_used': [],
            'score_statistics': {},
            'model_formulas': []
        }


def validate_scoring_inputs(normalized_data, selected_vars=None):
    """
    Validate inputs for scoring functions
    
    Args:
        normalized_data: DataFrame with normalized variables
        selected_vars: List of variables to validate
        
    Returns:
        Dict: Validation results with warnings and recommendations
    """
    validation_results = {
        'is_valid': True,
        'warnings': [],
        'recommendations': [],
        'normalized_columns_found': [],
        'missing_columns': []
    }
    
    try:
        # Check if DataFrame is provided
        if normalized_data is None or normalized_data.empty:
            validation_results['is_valid'] = False
            validation_results['warnings'].append("No data provided or data is empty")
            return validation_results
        
        # Check for WardName column
        exists, resolved_col = variable_resolver.check_column_exists('WardName', list(normalized_data.columns))
        if not exists:
            validation_results['is_valid'] = False
            validation_results['warnings'].append("WardName column is required but not found")
        
        # Find normalized columns
        norm_cols = [col for col in normalized_data.columns if col.startswith('normalization_')]
        validation_results['normalized_columns_found'] = norm_cols
        
        if len(norm_cols) < 2:
            validation_results['is_valid'] = False
            validation_results['warnings'].append("At least 2 normalized variables are required for scoring")
            validation_results['recommendations'].append("Run normalization process first to create normalized variables")
        
        # Validate selected variables if provided
        if selected_vars:
            missing_vars = []
            for var in selected_vars:
                norm_col = "normalization_{}".format(var.lower())
                if norm_col not in norm_cols and var not in norm_cols:
                    missing_vars.append(var)
            
            validation_results['missing_columns'] = missing_vars
            if missing_vars:
                validation_results['warnings'].append("Some selected variables not found: {}".format(missing_vars))
                validation_results['recommendations'].append("Check variable names or run normalization for missing variables")
        
        # Check for missing values in normalized columns
        missing_data_cols = []
        for col in norm_cols:
            if normalized_data[col].isna().any():
                missing_data_cols.append(col)
        
        if missing_data_cols:
            validation_results['warnings'].append("Missing values found in normalized columns: {}".format(missing_data_cols))
            validation_results['recommendations'].append("Run imputation process to handle missing values")
        
        # Check data quality
        if len(normalized_data) < 3:
            validation_results['warnings'].append("Very small dataset (less than 3 rows) may produce unreliable scores")
        
        return validation_results
        
    except Exception as e:
        validation_results['is_valid'] = False
        validation_results['warnings'].append("Error during validation: {}".format(str(e)))
        return validation_results 