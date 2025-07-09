"""
Independent PCA Analysis Pipeline for ChatMRPT

This module provides a completely separate PCA implementation that:
1. Takes raw data after initial data cleaning
2. Handles missing values independently
3. Fixes ward name duplicates independently
4. Standardizes data for PCA analysis
5. Runs principal component analysis
6. Creates PCA-based vulnerability rankings
7. Generates PCA-specific visualizations

This is completely separate from the composite scoring system.
"""

import logging
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from typing import Dict, List, Optional, Any, Tuple
import warnings
import os
import json

logger = logging.getLogger(__name__)

class PCAAnalysisPipeline:
    """
    Complete PCA analysis pipeline independent of composite scoring.
    """
    
    def __init__(self, session_id: str):
        """
        Initialize PCA pipeline for a session.
        
        Args:
            session_id: Session identifier
        """
        self.session_id = session_id
        self.raw_data = None
        self.cleaned_data = None
        self.standardized_data = None
        self.pca_model = None
        self.pca_scores = None
        self.pca_rankings = None
        self.variable_importance = None
        self.explained_variance = None
        
        logger.info(f"🔬 PCA PIPELINE: Initialized for session {session_id}")
    
    def run_complete_pca_analysis(self, data_handler, selected_variables: Optional[List[str]] = None, 
                                 llm_manager=None) -> Dict[str, Any]:
        """
        Run the complete PCA analysis pipeline.
        
        Args:
            data_handler: DataHandler with loaded CSV data
            selected_variables: Optional list of variables to use for PCA
            llm_manager: Optional LLM manager for variable selection fallback
            
        Returns:
            Dict containing complete PCA analysis results
        """
        try:
            logger.info("🚀 PCA PIPELINE: Starting complete PCA analysis")
            
            # Step 1: Load and validate raw data
            if not self._load_raw_data(data_handler):
                return {
                    'status': 'error',
                    'message': 'Failed to load raw data for PCA analysis',
                    'data': None
                }
            
            # Step 1.5: Apply unified variable selection using the coordinator
            from .variable_selection_coordinator import get_variable_coordinator
            
            coordinator = get_variable_coordinator(self.session_id)
            
            if selected_variables is None:
                logger.info("🔄 PCA PIPELINE: Applying unified variable selection")
                
                selection_result = coordinator.get_unified_variable_selection(
                    self.raw_data, 
                    data_handler.shapefile_data,
                    None,  # No custom variables
                    llm_manager
                )
                
                if selection_result['status'] == 'success':
                    selected_variables = selection_result['variables']
                    logger.info(f"✅ PCA UNIFIED SELECTION: {selection_result['zone_detected']} zone, "
                               f"{len(selected_variables)} variables selected via {selection_result['selection_method']}")
                else:
                    logger.error(f"❌ PCA UNIFIED SELECTION FAILED: {selection_result.get('message', 'Unknown error')}")
                    return {
                        'status': 'error',
                        'message': f"PCA variable selection failed: {selection_result.get('message', 'Unknown error')}",
                        'data': None
                    }
            else:
                # User provided custom variables - validate through coordinator
                logger.info("🔄 PCA PIPELINE: Validating user-provided variables through coordinator")
                
                selection_result = coordinator.get_unified_variable_selection(
                    self.raw_data, 
                    data_handler.shapefile_data,
                    selected_variables,  # Custom variables
                    llm_manager
                )
                
                if selection_result['status'] == 'success':
                    # Update selected_variables with validated set
                    selected_variables = selection_result['variables']
                    logger.info(f"✅ PCA CUSTOM VARIABLES VALIDATED: {len(selected_variables)} variables approved")
                else:
                    logger.error(f"❌ PCA CUSTOM VARIABLES VALIDATION FAILED: {selection_result.get('message', 'Unknown error')}")
                    return {
                        'status': 'error',
                        'message': f"PCA custom variable validation failed: {selection_result.get('message', 'Unknown error')}",
                        'data': None
                    }
            
            # Step 2: Clean and prepare data for PCA
            if not self._clean_and_prepare_data(selected_variables):
                return {
                    'status': 'error',
                    'message': 'Failed to clean and prepare data for PCA',
                    'data': None
                }
            
            # Step 3: Standardize data for PCA
            if not self._standardize_data():
                return {
                    'status': 'error',
                    'message': 'Failed to standardize data for PCA',
                    'data': None
                }
            
            # Step 4: Run PCA analysis
            if not self._run_pca_analysis():
                return {
                    'status': 'error',
                    'message': 'PCA analysis failed',
                    'data': None
                }
            
            # Step 5: Create PCA-based vulnerability rankings
            if not self._create_pca_rankings():
                return {
                    'status': 'error',
                    'message': 'Failed to create PCA vulnerability rankings',
                    'data': None
                }
            
            # Step 6: Store results in data handler
            self._store_results_in_handler(data_handler)
            
            # Step 7: Create analysis summary
            summary = self._create_pca_summary()
            
            logger.info("✅ PCA PIPELINE: Complete PCA analysis finished successfully")
            
            return {
                'status': 'success',
                'message': 'PCA analysis completed successfully',
                'data': {
                    'method': 'pca',
                    'pca_rankings': self.pca_rankings,
                    'explained_variance': self.explained_variance,
                    'variable_importance': self.variable_importance,
                    'n_components': self.pca_model.n_components_,
                    'variables_used': list(self.cleaned_data.columns[:-1]),  # Exclude WardName
                    'ward_count': len(self.pca_rankings),
                    'summary': summary
                }
            }
            
        except Exception as e:
            logger.error(f"💥 PCA PIPELINE: Error in complete analysis: {e}")
            return {
                'status': 'error',
                'message': f'PCA analysis failed: {str(e)}',
                'data': None
            }
    
    def _load_raw_data(self, data_handler) -> bool:
        """Load cleaned data from data handler (updated for consistency)."""
        try:
            if not hasattr(data_handler, 'cleaned_data') or data_handler.cleaned_data is None:
                logger.error("No cleaned data available in data handler")
                return False
            
            self.raw_data = data_handler.cleaned_data.copy()
            logger.info(f"📊 PCA DATA: Loaded cleaned data with shape {self.raw_data.shape}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading raw data: {e}")
            return False
    
    def _clean_and_prepare_data(self, selected_variables: Optional[List[str]] = None) -> bool:
        """Clean data and prepare for PCA analysis."""
        try:
            logger.info("🧹 PCA CLEANING: Starting data cleaning and preparation")
            
            # Find ward column
            ward_column = self._find_ward_column()
            if not ward_column:
                logger.error("No ward column found in data")
                return False
            
            # Handle ward name duplicates
            self._handle_ward_duplicates(ward_column)
            
            # Select variables for PCA
            pca_variables = self._select_pca_variables(selected_variables)
            if not pca_variables:
                logger.error("No suitable variables found for PCA")
                return False
            
            # Create cleaned dataset with selected variables + ward names
            self.cleaned_data = self.raw_data[[ward_column] + pca_variables].copy()
            self.cleaned_data = self.cleaned_data.rename(columns={ward_column: 'WardName'})
            
            # Handle missing values
            self._handle_missing_values()
            
            logger.info(f"✅ PCA CLEANING: Prepared data with {len(pca_variables)} variables and {len(self.cleaned_data)} wards")
            logger.info(f"📋 PCA VARIABLES: {pca_variables}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in data cleaning: {e}")
            return False
    
    def _find_ward_column(self) -> Optional[str]:
        """Find the ward name column in the data."""
        possible_names = ['wardname', 'ward_name', 'ward', 'WardName', 'Ward_Name', 'Ward']
        
        for col in self.raw_data.columns:
            if col.lower() in [name.lower() for name in possible_names]:
                logger.info(f"🏘️ PCA WARD: Found ward column: {col}")
                return col
        
        # Fallback: look for columns containing 'ward'
        for col in self.raw_data.columns:
            if 'ward' in col.lower():
                logger.info(f"🏘️ PCA WARD: Using ward column (partial match): {col}")
                return col
        
        return None
    
    def _handle_ward_duplicates(self, ward_column: str):
        """Handle duplicate ward names using WardCode system."""
        duplicates = self.raw_data[ward_column].duplicated()
        if duplicates.any():
            n_duplicates = duplicates.sum()
            logger.warning(f"⚠️ PCA DUPLICATES: Found {n_duplicates} duplicate ward names")
            
            # Check if WardCode is available for proper duplicate handling
            ward_code_col = None
            for col in ['WardCode', 'ward_code', 'WardCode_x', 'WardCode_y']:
                if col in self.raw_data.columns:
                    ward_code_col = col
                    break
            
            if ward_code_col:
                # Use WardCode system - keep original ward names, rely on WardCode for uniqueness
                logger.info(f"✅ PCA DUPLICATES: Using {ward_code_col} for unique identification - preserving original ward names")
                logger.info(f"📝 PCA WARD POLICY: Duplicate ward names preserved as per WardCode system design")
            else:
                # Fallback: Remove duplicates but warn about potential data loss
                logger.warning(f"⚠️ PCA DUPLICATES: No WardCode found - removing {n_duplicates} duplicate entries")
                self.raw_data = self.raw_data.drop_duplicates(subset=[ward_column], keep='first')
                logger.info(f"✅ PCA DUPLICATES: Removed duplicates, {len(self.raw_data)} wards remaining")
    
    def _select_pca_variables(self, selected_variables: Optional[List[str]] = None) -> List[str]:
        """Select variables suitable for PCA analysis."""
        try:
            if selected_variables:
                # Use user-specified variables
                available_vars = [var for var in selected_variables if var in self.raw_data.columns]
                logger.info(f"📋 PCA SELECTION: Using {len(available_vars)} user-specified variables")
                return available_vars
            
            # Auto-select numeric variables suitable for PCA
            numeric_columns = self.raw_data.select_dtypes(include=[np.number]).columns.tolist()
            
            # Remove obviously non-analytical columns
            exclude_patterns = ['id', 'code', 'x.1', 'x', 'index', 'unnamed', 'urban_percentage', 'urbanpercentage', 'urban_percent', 'urbanpercent', 'pct_urban', 'pcturban']
            pca_variables = []
            
            for col in numeric_columns:
                # Skip if matches exclude patterns
                if any(pattern in col.lower() for pattern in exclude_patterns):
                    continue
                
                # Skip if too many missing values (>50%)
                missing_pct = self.raw_data[col].isna().sum() / len(self.raw_data)
                if missing_pct > 0.5:
                    continue
                
                # Skip if zero variance
                if self.raw_data[col].var() == 0:
                    continue
                
                pca_variables.append(col)
            
            logger.info(f"🎯 PCA AUTO-SELECT: Selected {len(pca_variables)} variables from {len(numeric_columns)} numeric columns")
            logger.info(f"🚫 PCA EXCLUSIONS: Automatically excluded urban percentage variables")
            return pca_variables
            
        except Exception as e:
            logger.error(f"Error selecting PCA variables: {e}")
            return []
    
    def _handle_missing_values(self):
        """Handle missing values in the dataset."""
        try:
            # Check for missing values
            missing_counts = self.cleaned_data.isna().sum()
            total_missing = missing_counts.sum()
            
            if total_missing > 0:
                logger.info(f"🔧 PCA MISSING: Found {total_missing} missing values across {(missing_counts > 0).sum()} variables")
                
                # Separate numeric columns for imputation
                numeric_cols = self.cleaned_data.select_dtypes(include=[np.number]).columns.tolist()
                
                if numeric_cols:
                    # Use median imputation for numeric variables
                    imputer = SimpleImputer(strategy='median')
                    self.cleaned_data[numeric_cols] = imputer.fit_transform(self.cleaned_data[numeric_cols])
                    logger.info("✅ PCA MISSING: Applied median imputation to numeric variables")
                
                # Final check
                remaining_missing = self.cleaned_data.isna().sum().sum()
                if remaining_missing > 0:
                    logger.warning(f"⚠️ PCA MISSING: {remaining_missing} missing values remain after imputation")
            else:
                logger.info("✅ PCA MISSING: No missing values found")
                
        except Exception as e:
            logger.error(f"Error handling missing values: {e}")
    
    def _standardize_data(self) -> bool:
        """Standardize data for PCA analysis."""
        try:
            logger.info("📊 PCA STANDARDIZATION: Applying Z-score standardization")
            
            # Get numeric columns (exclude WardName)
            numeric_cols = self.cleaned_data.select_dtypes(include=[np.number]).columns.tolist()
            
            if not numeric_cols:
                logger.error("No numeric columns found for standardization")
                return False
            
            # Apply standardization
            scaler = StandardScaler()
            standardized_values = scaler.fit_transform(self.cleaned_data[numeric_cols])
            
            # Create standardized dataframe
            self.standardized_data = pd.DataFrame(
                standardized_values,
                columns=numeric_cols,
                index=self.cleaned_data.index
            )
            
            # Add back ward names
            self.standardized_data['WardName'] = self.cleaned_data['WardName'].values
            
            # Log standardization statistics
            logger.info(f"✅ PCA STANDARDIZATION: Standardized {len(numeric_cols)} variables")
            logger.info(f"📊 PCA STATS: Mean ≈ {self.standardized_data[numeric_cols].mean().mean():.3f}, Std ≈ {self.standardized_data[numeric_cols].std().mean():.3f}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in standardization: {e}")
            return False
    
    def _run_pca_analysis(self) -> bool:
        """Run the actual PCA analysis."""
        try:
            logger.info("🔬 PCA ANALYSIS: Running principal component analysis")
            
            # Get numeric data for PCA (exclude WardName)
            numeric_cols = self.standardized_data.select_dtypes(include=[np.number]).columns.tolist()
            pca_data = self.standardized_data[numeric_cols].values
            
            # Determine number of components
            n_variables = len(numeric_cols)
            n_components = min(n_variables, max(2, int(n_variables * 0.8)))  # Use 80% of variables or minimum 2
            
            # Run PCA
            self.pca_model = PCA(n_components=n_components, random_state=42)
            pca_scores = self.pca_model.fit_transform(pca_data)
            
            # Store PCA scores
            self.pca_scores = pd.DataFrame(
                pca_scores,
                columns=[f'PC{i+1}' for i in range(n_components)],
                index=self.standardized_data.index
            )
            self.pca_scores['WardName'] = self.standardized_data['WardName'].values
            
            # Calculate explained variance
            self.explained_variance = {
                'per_component': self.pca_model.explained_variance_ratio_.tolist(),
                'cumulative': np.cumsum(self.pca_model.explained_variance_ratio_).tolist(),
                'total_explained': float(self.pca_model.explained_variance_ratio_.sum())
            }
            
            # Calculate variable importance
            self._calculate_variable_importance(numeric_cols)
            
            logger.info(f"✅ PCA ANALYSIS: Completed with {n_components} components explaining {self.explained_variance['total_explained']*100:.1f}% variance")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in PCA analysis: {e}")
            return False
    
    def _calculate_variable_importance(self, variable_names: List[str]):
        """Calculate variable importance from PCA loadings."""
        try:
            # Get loadings (components)
            loadings = self.pca_model.components_.T  # Variables x Components
            explained_variance_ratio = self.pca_model.explained_variance_ratio_
            
            # Calculate importance as weighted sum of absolute loadings
            importance_scores = np.zeros(len(variable_names))
            
            for i, var_name in enumerate(variable_names):
                # Weight loadings by explained variance of each component
                importance = sum(
                    abs(loadings[i, j]) * explained_variance_ratio[j] 
                    for j in range(self.pca_model.n_components_)
                )
                importance_scores[i] = importance
            
            # Normalize to sum to 1
            total_importance = importance_scores.sum()
            if total_importance > 0:
                importance_scores = importance_scores / total_importance
            
            self.variable_importance = {
                var_name: float(score) 
                for var_name, score in zip(variable_names, importance_scores)
            }
            
            # Sort by importance
            self.variable_importance = dict(
                sorted(self.variable_importance.items(), key=lambda x: x[1], reverse=True)
            )
            
            logger.info(f"📊 PCA IMPORTANCE: Top variables - {list(self.variable_importance.keys())[:3]}")
            
        except Exception as e:
            logger.error(f"Error calculating variable importance: {e}")
            self.variable_importance = {}
    
    def _create_pca_rankings(self) -> bool:
        """Create vulnerability rankings based on PCA scores."""
        try:
            logger.info("🏆 PCA RANKINGS: Creating vulnerability rankings from PCA scores")
            
            # Create composite score from first few principal components
            # Weight by explained variance ratio
            weights = self.pca_model.explained_variance_ratio_
            
            # Use first component primarily, with additional components if significant
            pc_cols = [col for col in self.pca_scores.columns if col.startswith('PC')]
            
            if len(pc_cols) == 1:
                composite_score = self.pca_scores[pc_cols[0]]
            else:
                # Weighted combination of first 2-3 components
                n_components_to_use = min(3, len(pc_cols))
                composite_score = np.zeros(len(self.pca_scores))
                
                for i in range(n_components_to_use):
                    composite_score += self.pca_scores[pc_cols[i]] * weights[i]
            
            # Create rankings dataframe
            self.pca_rankings = pd.DataFrame({
                'WardName': self.pca_scores['WardName'],
                'pca_score': composite_score,
                'pc1_score': self.pca_scores['PC1'] if 'PC1' in self.pca_scores.columns else 0
            })
            
            # Sort by PCA score (higher scores = higher vulnerability)
            self.pca_rankings = self.pca_rankings.sort_values('pca_score', ascending=False)
            self.pca_rankings['pca_rank'] = range(1, len(self.pca_rankings) + 1)
            
            # Create vulnerability categories
            self._create_pca_vulnerability_categories()
            
            # Reset index
            self.pca_rankings = self.pca_rankings.reset_index(drop=True)
            
            logger.info(f"✅ PCA RANKINGS: Created rankings for {len(self.pca_rankings)} wards")
            logger.info(f"🏆 PCA TOP 5: {self.pca_rankings.head(5)['WardName'].tolist()}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating PCA rankings: {e}")
            return False
    
    def _create_pca_vulnerability_categories(self):
        """Create vulnerability categories for PCA rankings."""
        n_wards = len(self.pca_rankings)
        
        # Simple 3-category system: High, Medium, Low Risk
        # Divide into thirds (approximately)
        high_risk_count = n_wards // 3
        low_risk_count = n_wards // 3
        medium_risk_count = n_wards - high_risk_count - low_risk_count  # Remainder goes to medium
        
        # Assign categories based on ranking (higher rank = higher vulnerability)
        self.pca_rankings['vulnerability_category'] = 'Medium Risk'  # Default
        self.pca_rankings.loc[:high_risk_count-1, 'vulnerability_category'] = 'High Risk'
        self.pca_rankings.loc[n_wards-low_risk_count:, 'vulnerability_category'] = 'Low Risk'
        
        logger.info(f"📊 PCA CATEGORIES: {high_risk_count} High Risk, {medium_risk_count} Medium Risk, {low_risk_count} Low Risk")
    
    def _store_results_in_handler(self, data_handler):
        """Store PCA results in the data handler."""
        try:
            # Store PCA-specific results
            data_handler.pca_scores = self.pca_scores
            data_handler.pca_model = self.pca_model
            data_handler.pca_explained_variance = self.explained_variance
            data_handler.pca_variable_importance = self.variable_importance
            
            # Store variables used for PCA (for comparison with composite method)
            data_handler.pca_variables_used = list(self.cleaned_data.columns[:-1])  # Exclude WardName
            
            # Transform PCA rankings to expected format for vulnerability maps
            # The map expects 'overall_rank' and 'vulnerability_category' columns
            vulnerability_rankings_pca = self.pca_rankings.copy()
            vulnerability_rankings_pca['overall_rank'] = vulnerability_rankings_pca['pca_rank']
            vulnerability_rankings_pca['median_score'] = vulnerability_rankings_pca['pca_score']
            vulnerability_rankings_pca['value'] = vulnerability_rankings_pca['pca_score']
            
            # Store in the expected attribute name for data handler
            data_handler.vulnerability_rankings_pca = vulnerability_rankings_pca
            
            # Save PCA results to CSV files for persistence and access by comprehensive summary
            try:
                session_folder = getattr(data_handler, 'session_folder', None)
                if session_folder and os.path.exists(session_folder):
                    # Save PCA vulnerability rankings
                    pca_rankings_file = os.path.join(session_folder, 'analysis_vulnerability_rankings_pca.csv')
                    vulnerability_rankings_pca.to_csv(pca_rankings_file, index=False)
                    logger.info(f"💾 PCA SAVE: Saved PCA rankings to {pca_rankings_file}")
                    
                    # Save PCA scores
                    pca_scores_file = os.path.join(session_folder, 'analysis_pca_scores.csv')
                    self.pca_scores.to_csv(pca_scores_file, index=False)
                    
                    # Save PCA variable importance
                    pca_variables_file = os.path.join(session_folder, 'pca_variable_importance.json')
                    with open(pca_variables_file, 'w') as f:
                        json.dump(self.variable_importance, f, indent=2)
                    
                    # Save PCA explained variance
                    pca_variance_file = os.path.join(session_folder, 'pca_explained_variance.json')
                    with open(pca_variance_file, 'w') as f:
                        json.dump(self.explained_variance, f, indent=2)
                    
                    logger.info(f"💾 PCA SAVE: Saved all PCA analysis files to session folder")
                    
            except Exception as save_error:
                logger.warning(f"Could not save PCA files: {save_error}")
            
            # Set current method to PCA for visualizations
            data_handler.current_method = 'pca'
            
            logger.info("✅ PCA STORAGE: Stored PCA results in data handler with correct format")
            logger.info(f"📊 PCA RANKINGS: {len(vulnerability_rankings_pca)} wards with vulnerability categories")
            
        except Exception as e:
            logger.error(f"Error storing PCA results: {e}")
    
    def _create_pca_summary(self) -> Dict[str, Any]:
        """Create summary of PCA analysis results."""
        try:
            # Get top vulnerable wards
            top_wards = self.pca_rankings.head(10)
            
            summary = {
                'analysis_type': 'pca',
                'method_description': 'Principal Component Analysis with Z-score standardization',
                'total_wards': len(self.pca_rankings),
                'variables_analyzed': len(self.variable_importance),
                'components_used': self.pca_model.n_components_,
                'variance_explained': {
                    'total_percent': round(self.explained_variance['total_explained'] * 100, 1),
                    'first_component': round(self.explained_variance['per_component'][0] * 100, 1),
                    'components_breakdown': [round(x * 100, 1) for x in self.explained_variance['per_component']]
                },
                'top_vulnerable_wards': [
                    {
                        'ward_name': row['WardName'],
                        'pca_score': round(row['pca_score'], 3),
                        'rank': int(row['pca_rank']),
                        'category': row['vulnerability_category']
                    }
                    for _, row in top_wards.iterrows()
                ],
                'variable_importance': dict(list(self.variable_importance.items())[:5]),  # Top 5 variables
                'vulnerability_distribution': self.pca_rankings['vulnerability_category'].value_counts().to_dict(),
                'methodology_notes': {
                    'standardization': 'Z-score standardization applied to all variables',
                    'missing_values': 'Median imputation used for missing values',
                    'ranking_method': 'Weighted combination of principal components by explained variance',
                    'interpretation': 'Higher PCA scores indicate higher vulnerability based on underlying data patterns'
                }
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error creating PCA summary: {e}")
            return {'analysis_type': 'pca', 'error': str(e)}


def run_independent_pca_analysis(data_handler, selected_variables: Optional[List[str]] = None, 
                                session_id: Optional[str] = None, llm_manager=None) -> Dict[str, Any]:
    """
    Run complete independent PCA analysis pipeline.
    
    This is the main entry point for PCA analysis, completely separate from composite scoring.
    
    Args:
        data_handler: DataHandler with loaded CSV data
        selected_variables: Optional list of variables to use for PCA
        session_id: Session identifier
        llm_manager: Optional LLM manager for variable selection fallback
        
    Returns:
        Dict containing complete PCA analysis results
    """
    try:
        if not session_id:
            session_id = "pca_analysis"
        
        # Create PCA pipeline
        pca_pipeline = PCAAnalysisPipeline(session_id)
        
        # Run complete analysis
        result = pca_pipeline.run_complete_pca_analysis(data_handler, selected_variables, llm_manager)
        
        return result
        
    except Exception as e:
        logger.error(f"Error in independent PCA analysis: {e}")
        return {
            'status': 'error',
            'message': f'Independent PCA analysis failed: {str(e)}',
            'data': None
        } 