"""
Helper methods for enhanced methodology explanation tool

This file contains the implementation of pre-analysis and post-analysis
explanation methods to keep the main file manageable.
"""

import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)


def generate_pre_analysis_composite_explanation(input_data, context) -> str:
    """Generate pre-analysis composite score explanation"""
    
    explanation = []
    explanation.append("## 🎯 **Composite Score Method - What to Expect**")
    explanation.append("")
    
    if input_data.explanation_depth == "overview":
        explanation.append(f"The composite score method will analyze your {context.ward_count} wards using multiple risk indicators.")
        explanation.append("It combines variables using equal weighting to create an overall risk score for each ward.")
    else:
        explanation.append("### 📊 **Process Steps Your Data Will Go Through:**")
        explanation.append("")
        explanation.append("**Step 1: Variable Selection**")
        explanation.append(f"- System will select ~4-5 most relevant variables for {context.region}")
        explanation.append("- Variables scored based on malaria-related keywords and regional patterns")
        explanation.append("")
        explanation.append("**Step 2: Min-Max Normalization**")
        explanation.append("- All variables scaled to 0-1 range for fair comparison")
        explanation.append("- Direct risk factors: higher values = higher risk")
        explanation.append("- Inverse factors (like elevation): automatically inverted")
        explanation.append("")
        explanation.append("**Step 3: Multiple Model Generation**")
        explanation.append("- Creates ALL possible combinations of selected variables")
        explanation.append("- 2-variable models, 3-variable models, up to all-variable model")
        explanation.append(f"- Expected ~15-25 different models for your {context.ward_count} wards")
        explanation.append("")
        explanation.append("**Step 4: Equal Weight Scoring**")
        explanation.append("- Each model: score = (var1 + var2 + ... + varN) / N")
        explanation.append("- Simple arithmetic mean ensures equal contribution")
        explanation.append("")
        explanation.append("**Step 5: Robust Aggregation**")
        explanation.append("- Final score = MEDIAN of all model scores")
        explanation.append("- Provides robustness against outlier combinations")
        explanation.append("")
        explanation.append("**Step 6: Risk Classification**")
        explanation.append(f"- Top 33% of wards (~{context.ward_count//3}) = High Risk")
        explanation.append(f"- Middle 33% (~{context.ward_count//3}) = Medium Risk")
        explanation.append(f"- Bottom 33% (~{context.ward_count//3}) = Low Risk")
    
    explanation.append("")
    explanation.append("---")
    explanation.append("")
    
    return "\n".join(explanation)


def generate_pre_analysis_pca_explanation(input_data, context) -> str:
    """Generate pre-analysis PCA explanation"""
    
    explanation = []
    explanation.append("## 🔬 **PCA Method - What to Expect**")
    explanation.append("")
    
    if input_data.explanation_depth == "overview":
        explanation.append(f"PCA will analyze patterns in your {context.ward_count} wards data.")
        explanation.append("It finds hidden relationships between variables and creates data-driven risk scores.")
    else:
        explanation.append("### 🔍 **Process Steps Your Data Will Go Through:**")
        explanation.append("")
        explanation.append("**Step 1: Variable Preparation**")
        explanation.append(f"- System will use ~8-10 variables for comprehensive pattern analysis")
        explanation.append("- All variables standardized to prevent scale bias")
        explanation.append("")
        explanation.append("**Step 2: Correlation Matrix**")
        explanation.append("- Calculates how each variable relates to every other variable")
        explanation.append("- Identifies which variables move together in your specific data")
        explanation.append("")
        explanation.append("**Step 3: Component Extraction**")
        explanation.append("- Finds 2-4 underlying patterns that explain most variation")
        explanation.append("- Each component represents a different aspect of malaria risk")
        explanation.append("- Expected: ~70-85% of variance explained by selected components")
        explanation.append("")
        explanation.append("**Step 4: Data-Driven Weighting**")
        explanation.append("- Variables weighted based on their contribution to patterns")
        explanation.append("- Important variables get higher weights automatically")
        explanation.append("- Weights determined by your specific data, not assumptions")
        explanation.append("")
        explanation.append("**Step 5: Ward Scoring**")
        explanation.append("- Each ward projected onto the principal components")
        explanation.append("- Final scores reflect how well each ward fits the risk patterns")
        explanation.append("- Higher scores = better match to high-risk patterns")
    
    explanation.append("")
    explanation.append("---")
    explanation.append("")
    
    return "\n".join(explanation)


def generate_pre_analysis_comparison_explanation(input_data, context) -> str:
    """Generate pre-analysis method comparison"""
    
    explanation = []
    explanation.append("## ⚖️ **Method Comparison - What to Expect**")
    explanation.append("")
    explanation.append("### 🔍 **Key Differences:**")
    explanation.append("")
    explanation.append("| Aspect | Composite Score | PCA |")
    explanation.append("|--------|----------------|-----|")
    explanation.append("| **Approach** | Equal weighting | Data-driven weighting |")
    explanation.append("| **Transparency** | Highly interpretable | More complex patterns |")
    explanation.append("| **Robustness** | Multiple model combinations | Statistical variance capture |")
    explanation.append("| **Assumptions** | All variables equally important | Data reveals importance |")
    explanation.append("")
    explanation.append("### 📈 **Expected Agreement:**")
    explanation.append("")
    explanation.append(f"- **High consensus wards**: ~60-80% agreement on top 10 highest risk")
    explanation.append(f"- **Method differences**: Different wards may rank differently")
    explanation.append(f"- **Complementary insights**: Each method reveals different aspects of risk")
    explanation.append("")
    explanation.append("### 🎯 **Use Both Results For:**")
    explanation.append("")
    explanation.append("- **Priority 1**: Wards ranking high in BOTH methods")
    explanation.append("- **Priority 2**: Wards ranking high in ONE method (investigate why)")
    explanation.append("- **Understanding**: Different perspectives on the same risk landscape")
    explanation.append("")
    explanation.append("---")
    explanation.append("")
    
    return "\n".join(explanation)


def generate_pre_analysis_variable_explanation(input_data, context) -> str:
    """Generate pre-analysis variable selection explanation"""
    
    explanation = []
    explanation.append(f"## 📊 **Variable Selection for {context.region} Region**")
    explanation.append("")
    explanation.append("### 🎯 **Selection Process:**")
    explanation.append("")
    explanation.append("**Step 1: Keyword Scoring**")
    explanation.append("- Variables scored based on malaria-related keywords")
    explanation.append("- Higher scores for epidemiological, environmental, and socioeconomic factors")
    explanation.append("")
    explanation.append("**Step 2: Regional Relevance**")
    explanation.append(f"- {context.region} characteristics considered")
    explanation.append("- Climate, geography, and transmission patterns factored in")
    explanation.append("")
    explanation.append("**Step 3: Relationship Detection**")
    explanation.append("- Direct factors: higher values = higher risk (e.g., rainfall, population density)")
    explanation.append("- Inverse factors: higher values = lower risk (e.g., elevation, housing quality)")
    explanation.append("")
    explanation.append("### 🔍 **Expected Variable Types:**")
    explanation.append("")
    explanation.append("**Epidemiological**: Disease burden indicators (pfpr, test positivity)")
    explanation.append("**Environmental**: Climate and geography (rainfall, elevation, temperature)")
    explanation.append("**Socioeconomic**: Living conditions (housing quality, education, poverty)")
    explanation.append("**Intervention**: Coverage indicators (ITN distribution, healthcare access)")
    explanation.append("")
    explanation.append("---")
    explanation.append("")
    
    return "\n".join(explanation)


def generate_data_driven_explanations(input_data, context, analysis_results) -> str:
    """Generate explanations using actual analysis results"""
    
    try:
        from app.data.unified_dataset_builder import load_unified_dataset
        unified_data = load_unified_dataset(input_data.session_id)
        
        if unified_data is None:
            return generate_basic_post_analysis_explanations(input_data, context, analysis_results)
        
        explanations = []
        
        # GENERIC METHODOLOGY EXPLANATIONS FIRST
        explanations.append("## 📚 **Analysis Methods Explained**")
        explanations.append("")
        
        # Detect what variables were actually used
        variable_columns = [col for col in unified_data.columns if not col.startswith(('Ward', 'composite', 'pca', 'overall', 'vulnerability', 'rank', 'median', 'value', 'pc1', 'X.', 'X'))]
        risk_variables = [col for col in variable_columns if any(keyword in col.lower() for keyword in ['pfpr', 'housing', 'elevation', 'tpr', 'malaria', 'population', 'poverty', 'education', 'water', 'sanitation'])]
        
        if 'composite_score' in unified_data.columns:
            explanations.append("### 🎯 **Composite Score Method**")
            explanations.append("")
            explanations.append("**What it is:** A straightforward approach that combines multiple risk indicators using equal weighting to create an overall malaria risk score.")
            explanations.append("")
            explanations.append("**How it works:**")
            explanations.append("1. **Variable Selection** - Choose the most relevant malaria risk indicators")
            explanations.append("2. **Normalization** - Scale all variables to 0-1 range for fair comparison")
            explanations.append("3. **Model Creation** - Generate multiple combinations of variables")
            explanations.append("4. **Equal Weighting** - Each variable contributes equally (arithmetic mean)")
            explanations.append("5. **Robust Aggregation** - Take median across all models for final score")
            explanations.append("")
            explanations.append("**Strengths:** Simple, transparent, gives equal voice to all indicators")
            explanations.append("**Best for:** When you want democratic weighting and easy interpretation")
            explanations.append("")
            
            explanations.append("### 🔬 **Applied to Your Kano State Data:**")
            explanations.append("")
            comp_stats = get_score_statistics(unified_data, 'composite_score')
            explanations.append(f"- **Dataset:** {len(unified_data)} wards with {len(risk_variables)} malaria indicators")
            if risk_variables:
                explanations.append(f"- **Variables used:** {', '.join(risk_variables[:5])}")
            explanations.append(f"- **Process:** Created multiple models, took median score for each ward")
            explanations.append(f"- **Results:** Scores from {comp_stats['min']:.3f} to {comp_stats['max']:.3f}")
            explanations.append(f"- **Highest risk:** {comp_stats['top_ward']} ({comp_stats['top_score']:.3f})")
            explanations.append(f"- **Lowest risk:** {comp_stats['bottom_ward']} ({comp_stats['bottom_score']:.3f})")
            explanations.append("")
        
        if 'pca_score' in unified_data.columns:
            explanations.append("### 📊 **Principal Component Analysis (PCA) Method**")
            explanations.append("")
            explanations.append("**What it is:** A statistical technique that finds the most important patterns in your data and creates new variables (components) that capture maximum information with fewer dimensions.")
            explanations.append("")
            explanations.append("**How it works:**")
            explanations.append("1. **Standardization** - Convert variables to z-scores (mean=0, std=1)")
            explanations.append("2. **Correlation Analysis** - Find relationships between variables")
            explanations.append("3. **Component Extraction** - Create new variables that explain maximum variance")
            explanations.append("4. **Dimensionality Reduction** - Keep only significant components (eigenvalue > 1)")
            explanations.append("5. **Risk Scoring** - Project wards onto first component for risk scores")
            explanations.append("")
            explanations.append("**Strengths:** Data-driven weighting, reduces redundancy, captures complex relationships")
            explanations.append("**Best for:** When variables are correlated and you want optimal statistical weighting")
            explanations.append("")
            
            explanations.append("### 🔬 **Applied to Your Kano State Data:**")
            explanations.append("")
            pca_stats = get_score_statistics(unified_data, 'pca_score')
            explanations.append(f"- **Dataset:** {len(unified_data)} wards with {len(risk_variables)} malaria indicators")
            explanations.append(f"- **Process:** Found principal components, used PC1 for risk ranking")
            explanations.append(f"- **Variable weighting:** Determined by correlation patterns in your data")
            explanations.append(f"- **Results:** Scores from {pca_stats['min']:.3f} to {pca_stats['max']:.3f}")
            explanations.append(f"- **Highest risk:** {pca_stats['top_ward']} ({pca_stats['top_score']:.3f})")
            explanations.append(f"- **Lowest risk:** {pca_stats['bottom_ward']} ({pca_stats['bottom_score']:.3f})")
            explanations.append("")
        
        # Method comparison if both exist
        if 'composite_score' in unified_data.columns and 'pca_score' in unified_data.columns:
            agreement_stats = analyze_method_agreement(unified_data)
            explanations.append("### ⚖️ **Method Comparison on Your Data:**")
            explanations.append("")
            explanations.append("**Different Approaches, Different Results:**")
            explanations.append("- **Composite:** Equal weighting of all variables (democratic approach)")
            explanations.append("- **PCA:** Data-driven weighting based on variable relationships")
            explanations.append("")
            explanations.append("**Agreement Analysis:**")
            explanations.append(f"- **Correlation:** {agreement_stats['correlation']:.3f} ({'strong negative' if agreement_stats['correlation'] < -0.7 else 'moderate negative' if agreement_stats['correlation'] < -0.3 else 'weak' if abs(agreement_stats['correlation']) < 0.3 else 'moderate positive' if agreement_stats['correlation'] < 0.7 else 'strong positive'})")
            explanations.append(f"- **Top 10 consensus:** {agreement_stats['top_consensus']:.0f}% of wards ranked similarly")
            if agreement_stats.get('consensus_wards'):
                explanations.append(f"- **High-risk consensus:** {', '.join(agreement_stats['consensus_wards'][:3])}...")
            explanations.append("")
        
        explanations.append("---")
        explanations.append("")
        
        return "\n".join(explanations)
        
    except Exception as e:
        logger.error(f"Error generating data-driven explanations: {e}")
        return generate_basic_post_analysis_explanations(input_data, context, analysis_results)


def generate_basic_post_analysis_explanations(input_data, context, analysis_results) -> str:
    """Generate basic post-analysis explanations when detailed data unavailable"""
    
    explanations = []
    explanations.append("## 📊 **Analysis Completed**")
    explanations.append("")
    explanations.append(f"**Methods Run:** {', '.join(context.methods_run)}")
    explanations.append(f"**Variables Used:** {len(context.variables_used)} variables")
    explanations.append(f"**Wards Analyzed:** {context.ward_count} wards")
    explanations.append("")
    explanations.append("Your analysis has been completed successfully. You can now:")
    explanations.append("")
    explanations.append("- View detailed ward rankings")
    explanations.append("- Generate visualization maps")
    explanations.append("- Ask specific questions about individual wards")
    explanations.append("- Request correlation analysis of your variables")
    explanations.append("")
    explanations.append("---")
    explanations.append("")
    
    return "\n".join(explanations)


def generate_correlation_analysis(session_id: str) -> str:
    """Generate correlation analysis from actual data"""
    
    try:
        from app.data.unified_dataset_builder import load_unified_dataset
        unified_data = load_unified_dataset(session_id)
        
        if unified_data is None:
            return ""
        
        # Get numeric columns for correlation analysis
        numeric_cols = unified_data.select_dtypes(include=[np.number]).columns
        # Exclude geometry and ID columns
        analysis_cols = [col for col in numeric_cols if col not in ['WardCode', 'LGACode', 'StateCode'] and 'geometry' not in col.lower()]
        
        if len(analysis_cols) < 2:
            return ""
        
        # Calculate correlations
        corr_matrix = unified_data[analysis_cols].corr()
        
        explanations = []
        explanations.append("## 🔗 **Correlation Insights from Your Data**")
        explanations.append("")
        
        # Find strongest correlations
        strong_correlations = find_strong_correlations(corr_matrix)
        
        if strong_correlations:
            explanations.append("### 💪 **Strongest Variable Relationships:**")
            explanations.append("")
            for var1, var2, corr_val in strong_correlations[:5]:
                relationship = "strong positive" if corr_val > 0.6 else "strong negative" if corr_val < -0.6 else "moderate"
                explanations.append(f"- **{var1} ↔ {var2}**: {corr_val:.3f} ({relationship})")
            explanations.append("")
        
        # Score correlations with composite/PCA scores if available
        if 'composite_score' in unified_data.columns:
            comp_corrs = get_score_correlations(unified_data, 'composite_score', analysis_cols)
            explanations.append("### 🎯 **Variables Most Important for Composite Scores:**")
            explanations.append("")
            for var, corr_val in comp_corrs[:3]:
                explanations.append(f"- **{var}**: {corr_val:.3f} correlation")
            explanations.append("")
        
        if 'pca_score' in unified_data.columns:
            pca_corrs = get_score_correlations(unified_data, 'pca_score', analysis_cols)
            explanations.append("### 🔬 **Variables Most Important for PCA Scores:**")
            explanations.append("")
            for var, corr_val in pca_corrs[:3]:
                explanations.append(f"- **{var}**: {corr_val:.3f} correlation")
            explanations.append("")
        
        return "\n".join(explanations)
        
    except Exception as e:
        logger.error(f"Error generating correlation analysis: {e}")
        return ""


def get_score_statistics(data: pd.DataFrame, score_col: str) -> Dict[str, Any]:
    """Get statistical summary for a score column"""
    
    scores = data[score_col]
    top_idx = scores.idxmax()
    bottom_idx = scores.idxmin()
    
    return {
        'min': scores.min(),
        'max': scores.max(),
        'mean': scores.mean(),
        'std': scores.std(),
        'top_ward': data.loc[top_idx, 'WardName'] if 'WardName' in data.columns else 'Unknown',
        'top_score': scores.loc[top_idx],
        'bottom_ward': data.loc[bottom_idx, 'WardName'] if 'WardName' in data.columns else 'Unknown',
        'bottom_score': scores.loc[bottom_idx]
    }


def analyze_method_agreement(data: pd.DataFrame) -> Dict[str, Any]:
    """Analyze agreement between composite and PCA methods"""
    
    # Calculate correlation
    correlation = data['composite_score'].corr(data['pca_score'])
    
    # Get top 10 wards by each method
    comp_top10 = set(data.nlargest(10, 'composite_score')['WardName'])
    pca_top10 = set(data.nlargest(10, 'pca_score')['WardName'])
    
    # Calculate consensus
    consensus = comp_top10.intersection(pca_top10)
    consensus_rate = len(consensus) / 10 * 100
    
    return {
        'correlation': correlation,
        'top_consensus': consensus_rate,
        'consensus_wards': list(consensus)
    }


def find_strong_correlations(corr_matrix: pd.DataFrame, threshold: float = 0.5) -> List[Tuple[str, str, float]]:
    """Find correlations above threshold"""
    
    correlations = []
    
    for i in range(len(corr_matrix.columns)):
        for j in range(i+1, len(corr_matrix.columns)):
            var1 = corr_matrix.columns[i]
            var2 = corr_matrix.columns[j]
            corr_val = corr_matrix.iloc[i, j]
            
            if abs(corr_val) >= threshold:
                correlations.append((var1, var2, corr_val))
    
    # Sort by absolute correlation value
    correlations.sort(key=lambda x: abs(x[2]), reverse=True)
    return correlations


def get_score_correlations(data: pd.DataFrame, score_col: str, var_cols: List[str]) -> List[Tuple[str, float]]:
    """Get correlations between score and variables, sorted by absolute value"""
    
    correlations = []
    
    for var in var_cols:
        if var != score_col:
            try:
                corr_val = data[score_col].corr(data[var])
                if not pd.isna(corr_val):
                    correlations.append((var, corr_val))
            except:
                continue
    
    # Sort by absolute correlation value
    correlations.sort(key=lambda x: abs(x[1]), reverse=True)
    return correlations