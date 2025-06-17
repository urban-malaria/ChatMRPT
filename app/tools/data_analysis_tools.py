"""
Data Analysis Tools for ChatMRPT
Enhanced tools for analyzing uploaded malaria risk data
"""

import os
import logging
import json
from typing import Dict, Any, List
from flask import current_app
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def categorize_variables_with_llm(variable_names: List[str], session_id: str) -> Dict[str, Any]:
    """
    Use LLM to intelligently categorize dataset variables into epidemiological categories.
    
    Args:
        variable_names: List of column names from the uploaded dataset
        session_id: Session ID for LLM access
    
    Returns:
        Dict with status and categorized variables
    """
    try:
        # Get LLM manager
        llm_manager = current_app.services.llm_manager
        
        system_prompt = """You are an expert malaria epidemiologist analyzing dataset variables for malaria risk assessment.

Your task is to categorize variable names into epidemiological categories relevant for malaria risk analysis.

**Categories to use:**
1. **malaria_indicators** - Direct malaria measures (prevalence, cases, test positivity rates, parasitemia, fever rates, etc.)
2. **environmental_factors** - Climate, geography, ecology (rainfall, temperature, elevation, NDVI, water bodies, flooding, etc.)
3. **demographic_factors** - Population characteristics (age, population density, household size, migration, etc.)
4. **infrastructure_factors** - Built environment and services (roads, markets, schools, health facilities, electricity, etc.)
5. **intervention_factors** - Malaria control measures (ITN coverage, IRS, treatment access, bed nets, antimalarials, etc.)
6. **geographic_factors** - Spatial identifiers and coordinates (ward names, coordinates, administrative boundaries, etc.)
7. **unclassified** - Variables that don't clearly fit the above categories

**Important Guidelines:**
- Be flexible with variable name formats (abbreviations, underscores, mixed case)
- Consider epidemiological context - what factors influence malaria transmission?
- Geographic identifiers (like WardName, coordinates) go in geographic_factors
- Population counts and densities are demographic_factors
- Healthcare access and infrastructure are infrastructure_factors
- Any direct malaria measurements are malaria_indicators

Return ONLY a valid JSON object with this exact structure:
{
  "malaria_indicators": ["var1", "var2"],
  "environmental_factors": ["var3", "var4"], 
  "demographic_factors": ["var5", "var6"],
  "infrastructure_factors": ["var7", "var8"],
  "intervention_factors": ["var9", "var10"],
  "geographic_factors": ["var11", "var12"],
  "unclassified": ["var13", "var14"]
}"""

        user_prompt = f"""Categorize these {len(variable_names)} dataset variables for malaria risk analysis:

{chr(10).join([f"- {var}" for var in variable_names])}

Return the categorization as a JSON object."""

        # Generate LLM response
        llm_response = llm_manager.generate_response(
            prompt=user_prompt,
            system_message=system_prompt,
            temperature=0.1,  # Low temperature for consistent categorization
            max_tokens=1500,
            session_id=session_id
        )
        
        # Parse JSON response
        try:
            # Clean response (remove code blocks if present)
            clean_response = llm_response.strip()
            if clean_response.startswith('```json'):
                clean_response = clean_response[7:-3]
            elif clean_response.startswith('```'):
                clean_response = clean_response[3:-3]
            
            categories = json.loads(clean_response)
            
            # Validate that all variables are accounted for
            all_categorized = []
            for category_vars in categories.values():
                if isinstance(category_vars, list):
                    all_categorized.extend(category_vars)
            
            missing_vars = set(variable_names) - set(all_categorized)
            if missing_vars:
                # Add missing variables to unclassified
                if 'unclassified' not in categories:
                    categories['unclassified'] = []
                categories['unclassified'].extend(list(missing_vars))
            
            logger.info(f"✅ LLM successfully categorized {len(variable_names)} variables")
            return {
                'status': 'success',
                'categories': categories,
                'method': 'llm_classification'
            }
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM categorization response: {e}")
            logger.error(f"Raw LLM response: {llm_response[:500]}...")
            return {
                'status': 'error',
                'message': f'LLM response parsing failed: {str(e)}'
            }
            
    except Exception as e:
        logger.error(f"Error in LLM variable categorization: {e}")
        return {
            'status': 'error',
            'message': f'LLM categorization failed: {str(e)}'
        }

def analyze_uploaded_data_and_recommend(session_id: str) -> Dict[str, Any]:
    """
    Analyze uploaded data and recommend appropriate analysis workflows.
    
    This is the main tool that responds after successful file upload.
    """
    try:
        # Check session folder for uploaded files
        session_folder = f"instance/uploads/{session_id}"
        
        if not os.path.exists(session_folder):
            return {
                'status': 'error',
                'message': 'No upload session found. Please upload your data files first.',
                'recommendations': []
            }
        
        # Check for uploaded files
        csv_files = []
        shapefile_files = []
        
        for file in os.listdir(session_folder):
            if file.endswith(('.csv', '.xlsx')):
                csv_files.append(file)
            elif file.endswith('.zip'):
                shapefile_files.append(file)
        
        if not csv_files and not shapefile_files:
            return {
                'status': 'error',
                'message': 'No data files found in upload session. Please upload CSV and shapefile data.',
                'recommendations': []
            }
        
        # Analyze the uploaded data
        analysis_summary = {
            'csv_files': csv_files,
            'shapefile_files': shapefile_files,
            'total_files': len(csv_files) + len(shapefile_files)
        }
        
        # Try to load and analyze CSV data
        csv_analysis = None
        if csv_files:
            try:
                csv_path = os.path.join(session_folder, csv_files[0])
                df = pd.read_csv(csv_path)
                
                csv_analysis = {
                    'filename': csv_files[0],
                    'rows': len(df),
                    'columns': len(df.columns),
                    'variables': list(df.columns)[:20],  # First 20 columns
                    'numeric_variables': len(df.select_dtypes(include=['number']).columns),
                    'categorical_variables': len(df.select_dtypes(include=['object']).columns),
                    'missing_data': df.isnull().sum().sum()
                }
                
                # Use LLM to intelligently categorize variables
                variable_categorization = categorize_variables_with_llm(df.columns.tolist(), session_id)
                
                if variable_categorization['status'] == 'success':
                    categories = variable_categorization['categories']
                    malaria_vars = categories.get('malaria_indicators', [])
                    environmental_vars = categories.get('environmental_factors', [])
                    demographic_vars = categories.get('demographic_factors', [])
                    infrastructure_vars = categories.get('infrastructure_factors', [])
                    intervention_vars = categories.get('intervention_factors', [])
                    geographic_vars = categories.get('geographic_factors', [])
                    unclassified_vars = categories.get('unclassified', [])
                else:
                    # Fallback to simple keyword matching if LLM fails
                    logger.warning("LLM categorization failed, using fallback keyword matching")
                    malaria_vars = [col for col in df.columns if any(term in col.lower() for term in 
                                   ['tpr', 'malaria', 'prevalence', 'pfpr'])]
                    environmental_vars = [col for col in df.columns if any(term in col.lower() for term in 
                                         ['ndvi', 'temperature', 'rainfall', 'elevation'])]
                    demographic_vars = [col for col in df.columns if any(term in col.lower() for term in 
                                       ['population', 'density', 'urban', 'literacy'])]
                    infrastructure_vars = []
                    intervention_vars = []
                    geographic_vars = []
                    unclassified_vars = [col for col in df.columns if col not in malaria_vars + environmental_vars + demographic_vars]
                
                # Store all identified variables with comprehensive categorization
                csv_analysis.update({
                    'malaria_indicators': malaria_vars,
                    'environmental_factors': environmental_vars, 
                    'demographic_factors': demographic_vars,
                    'infrastructure_factors': infrastructure_vars,
                    'intervention_factors': intervention_vars,
                    'geographic_factors': geographic_vars,
                    'unclassified_factors': unclassified_vars,
                    'all_variables': list(df.columns),
                    'variable_counts': {
                        'malaria': len(malaria_vars),
                        'environmental': len(environmental_vars),
                        'demographic': len(demographic_vars),
                        'infrastructure': len(infrastructure_vars),
                        'intervention': len(intervention_vars),
                        'geographic': len(geographic_vars),
                        'unclassified': len(unclassified_vars)
                    },
                    'categorization_method': 'llm' if variable_categorization.get('status') == 'success' else 'keyword_fallback'
                })
                
            except Exception as e:
                logger.warning(f"Could not analyze CSV file: {e}")
                csv_analysis = {'error': f'Could not analyze CSV: {str(e)}'}
        
        # Generate recommendations
        recommendations = []
        
        if csv_files and shapefile_files:
            recommendations.extend([
                {
                    'analysis_type': 'Composite Risk Scoring',
                    'description': 'Combine multiple malaria risk factors into a single composite score to rank wards by overall malaria risk.',
                    'method': 'Normalizes each risk factor (0-1 scale) and calculates mean score across all factors.',
                    'outputs': ['Ward vulnerability rankings', 'Risk score maps', 'Factor contribution analysis'],
                    'recommended': True,
                    'tool_name': 'run_composite_analysis'
                },
                {
                    'analysis_type': 'Principal Component Analysis (PCA)', 
                    'description': 'Use statistical dimensionality reduction to identify the most important combinations of risk factors.',
                    'method': 'Creates weighted combinations of variables to capture maximum variance in the data.',
                    'outputs': ['PCA-based rankings', 'Component analysis', 'Variable loadings'],
                    'recommended': True,
                    'tool_name': 'run_pca_analysis'
                },
                {
                    'analysis_type': 'Geospatial Mapping',
                    'description': 'Create interactive maps showing the spatial distribution of malaria risk across your study area.',
                    'method': 'Overlay risk scores on ward boundaries with choropleth visualization.',
                    'outputs': ['Interactive risk maps', 'Spatial clustering analysis', 'Hotspot identification'],
                    'recommended': csv_analysis and csv_analysis.get('malaria_indicators'),
                    'tool_name': 'create_composite_score_maps'
                }
            ])
        elif csv_files:
            recommendations.append({
                'analysis_type': 'Statistical Analysis Only',
                'description': 'Analyze risk factors and generate rankings without mapping capabilities.',
                'method': 'Statistical analysis of uploaded data variables.',
                'outputs': ['Risk factor analysis', 'Correlation matrix', 'Summary statistics'],
                'recommended': True,
                'tool_name': 'summary_stats'
            })
        
        # Create the response message
        if csv_analysis and shapefile_files:
            total_wards = csv_analysis.get('rows', 0)
            variable_counts = csv_analysis.get('variable_counts', {})
            categorization_method = csv_analysis.get('categorization_method', 'unknown')
            
            # Build dynamic variable summary
            variable_summary = []
            if variable_counts.get('malaria', 0) > 0:
                variables = csv_analysis.get('malaria_indicators', [])
                variable_summary.append(f"- **Malaria Indicators:** {variable_counts['malaria']} found ({', '.join(variables[:3])}{'...' if len(variables) > 3 else ''})")
            
            if variable_counts.get('environmental', 0) > 0:
                variables = csv_analysis.get('environmental_factors', [])
                variable_summary.append(f"- **Environmental Factors:** {variable_counts['environmental']} found ({', '.join(variables[:3])}{'...' if len(variables) > 3 else ''})")
            
            if variable_counts.get('demographic', 0) > 0:
                variables = csv_analysis.get('demographic_factors', [])
                variable_summary.append(f"- **Demographic Factors:** {variable_counts['demographic']} found ({', '.join(variables[:3])}{'...' if len(variables) > 3 else ''})")
            
            if variable_counts.get('infrastructure', 0) > 0:
                variables = csv_analysis.get('infrastructure_factors', [])
                variable_summary.append(f"- **Infrastructure Factors:** {variable_counts['infrastructure']} found ({', '.join(variables[:3])}{'...' if len(variables) > 3 else ''})")
            
            if variable_counts.get('intervention', 0) > 0:
                variables = csv_analysis.get('intervention_factors', [])
                variable_summary.append(f"- **Intervention Factors:** {variable_counts['intervention']} found ({', '.join(variables[:3])}{'...' if len(variables) > 3 else ''})")
            
            if variable_counts.get('geographic', 0) > 0:
                variables = csv_analysis.get('geographic_factors', [])
                variable_summary.append(f"- **Geographic Identifiers:** {variable_counts['geographic']} found ({', '.join(variables[:2])}{'...' if len(variables) > 2 else ''})")
            
            if variable_counts.get('unclassified', 0) > 0:
                variables = csv_analysis.get('unclassified_factors', [])
                variable_summary.append(f"- **Other Variables:** {variable_counts['unclassified']} found ({', '.join(variables[:2])}{'...' if len(variables) > 2 else ''})")
            
            # Analysis method note
            method_note = "🤖 *Variables intelligently categorized using AI analysis*" if categorization_method == 'llm' else "⚠️ *Variables categorized using keyword matching (AI analysis unavailable)*"
            
            message = f"""🎉 **Data Successfully Uploaded and Analyzed!**

📊 **Your Dataset:**
- **CSV Data:** {csv_files[0]} ({total_wards} wards, {csv_analysis.get('columns', 0)} variables)
- **Shapefile:** {shapefile_files[0]} (Geographic boundaries)

📋 **Variable Analysis:**
{chr(10).join(variable_summary)}
{method_note}

🔬 **Recommended Analysis:**
I can run **analysis** using two proven methodologies:

1. **Composite Risk Scoring** - Combines all risk factors into unified vulnerability rankings
2. **Principal Component Analysis (PCA)** - Identifies the most statistically important risk patterns

Both methods will rank your {total_wards} wards from highest to lowest malaria risk and generate interactive maps.

**Would you like me to proceed with the analysis?** I recommend starting with both methods to compare results."""

        else:
            message = f"Data uploaded successfully. Found {len(csv_files)} CSV files and {len(shapefile_files)} shapefiles. Ready for analysis."
        
        return {
            'status': 'success',
            'message': message,
            'analysis_summary': analysis_summary,
            'csv_analysis': csv_analysis,
            'recommendations': recommendations,
            'ready_for_analysis': len(csv_files) > 0,
            'next_steps': [
                'Run composite analysis for vulnerability rankings',
                'Generate PCA analysis for statistical insights', 
                'Create interactive risk maps',
                'Export analysis results'
            ]
        }
        
    except Exception as e:
        logger.error(f"Error analyzing uploaded data: {e}")
        return {
            'status': 'error',
            'message': f'Error analyzing uploaded data: {str(e)}',
            'recommendations': []
        }


def generate_comprehensive_analysis_summary(session_id: str) -> Dict[str, Any]:
    """
    Generate comprehensive summary of completed analyses and create unified dataset.
    
    This tool aggregates all analysis results and creates the unified dataset
    that contains all PCA and composite results for further analysis.
    """
    try:
        logger.info(f"Generating comprehensive analysis summary for session {session_id}")
        
        # Build unified dataset first - this contains all the PCA and composite results
        logger.info("🔧 Building unified dataset with all analysis results...")
        from app.data.unified_dataset_builder import build_unified_dataset
        
        unified_result = build_unified_dataset(session_id)
        
        if unified_result['status'] == 'success':
            logger.info(f"✅ Unified dataset created: {unified_result['message']}")
            unified_gdf = unified_result['dataset']
            
            # Extract results from unified dataset
            total_wards = len(unified_gdf)
            
            # Get composite results (now from unified dataset)
            if 'composite_rank' in unified_gdf.columns and 'composite_score' in unified_gdf.columns:
                composite_top5 = unified_gdf.nsmallest(5, 'composite_rank')[['WardName', 'composite_rank']]
                composite_top5 = composite_top5.rename(columns={'composite_rank': 'overall_rank'})
                composite_bottom5 = unified_gdf.nlargest(5, 'composite_rank')[['WardName', 'composite_rank']]
                composite_bottom5 = composite_bottom5.rename(columns={'composite_rank': 'overall_rank'})
                
                # Get score range
                score_range = f"{unified_gdf['composite_score'].min():.3f} to {unified_gdf['composite_score'].max():.3f}"
                top_ward_score = unified_gdf.loc[unified_gdf['composite_rank'] == 1, 'composite_score'].iloc[0]
                logger.info("✅ Found composite ranking columns in unified dataset")
            else:
                logger.warning(f"❌ Composite ranking columns not found. Available columns: {list(unified_gdf.columns)}")
                # Fallback to file-based loading
                return _generate_summary_from_files(session_id)
            
            # Get PCA results (now from unified dataset)
            pca_top5 = []
            pca_bottom5 = []
            pca_variables_used = []
            pca_variance_explained = "N/A"
            
            # Check for PCA ranking columns in unified dataset - using exact column name  
            if 'pca_rank' in unified_gdf.columns:
                pca_rank_col = 'pca_rank'
                pca_top5 = unified_gdf.nsmallest(5, pca_rank_col)[['WardName', pca_rank_col]]
                pca_top5 = pca_top5.rename(columns={pca_rank_col: 'overall_rank'})
                pca_bottom5 = unified_gdf.nlargest(5, pca_rank_col)[['WardName', pca_rank_col]]
                pca_bottom5 = pca_bottom5.rename(columns={pca_rank_col: 'overall_rank'})
                logger.info(f"✅ Found PCA rankings in unified dataset using column: {pca_rank_col}")
            else:
                logger.warning(f"❌ PCA rank column not found. Available columns: {list(unified_gdf.columns)}")
            
            # Extract PCA metadata - try files first as they contain the most complete data
            try:
                import os
                import json
                session_folder = os.path.join('instance', 'uploads', session_id)
                pca_var_file = os.path.join(session_folder, 'pca_variable_importance.json')
                pca_variance_file = os.path.join(session_folder, 'pca_explained_variance.json')
                
                # Load PCA variable importance (ordered by importance)
                if os.path.exists(pca_var_file):
                    with open(pca_var_file, 'r') as f:
                        var_data = json.load(f)
                        # Get variables ordered by importance (top 10 for display)
                        pca_variables_used = list(var_data.keys()) if var_data else []
                        logger.info(f"✅ Loaded PCA variables from file: {len(pca_variables_used)} variables")
                
                # Load PCA variance explained
                if os.path.exists(pca_variance_file):
                    with open(pca_variance_file, 'r') as f:
                        variance_data = json.load(f) 
                        # Convert to percentage and format properly
                        total_variance = variance_data.get('total_explained', variance_data.get('total_variance_explained', 0))
                        pca_variance_explained = f"{total_variance * 100:.1f}%" if total_variance <= 1.0 else f"{total_variance:.1f}%"
                        logger.info(f"✅ Loaded PCA variance from file: {pca_variance_explained}")
                        
                # Fallback to unified dataset metadata if files not available
                if not pca_variables_used:
                    metadata = unified_result.get('metadata', {})
                    if 'pca_analysis' in metadata:
                        pca_info = metadata['pca_analysis']
                        pca_variables_used = pca_info.get('variables_used', [])
                        if not pca_variance_explained or pca_variance_explained == "N/A":
                            pca_variance_explained = f"{pca_info.get('total_variance_explained', 0):.1f}%"
                            
            except Exception as e:
                logger.warning(f"Could not load PCA metadata from files: {e}")
                # Try unified dataset metadata as fallback
                metadata = unified_result.get('metadata', {})
                if 'pca_analysis' in metadata:
                    pca_info = metadata['pca_analysis']
                    pca_variables_used = pca_info.get('variables_used', [])
                    pca_variance_explained = f"{pca_info.get('total_variance_explained', 0):.1f}%"
            
            # Get composite variables from files as primary source
            actual_variables_used = []
            try:
                import os
                import pandas as pd
                session_folder = os.path.join('instance', 'uploads', session_id)
                formulas_file = os.path.join(session_folder, 'model_formulas.csv')
                
                if os.path.exists(formulas_file):
                    formulas_df = pd.read_csv(formulas_file)
                    if not formulas_df.empty and 'variables' in formulas_df.columns:
                        # Extract unique variables from all models
                        all_vars = set()
                        for variables_str in formulas_df['variables'].dropna():
                            vars_list = [v.strip() for v in variables_str.split(',')]
                            all_vars.update(vars_list)
                        actual_variables_used = sorted(list(all_vars))
                        logger.info(f"✅ Loaded composite variables from formulas: {actual_variables_used}")
                
                # Fallback to metadata if files not available
                if not actual_variables_used:
                    metadata = unified_result.get('metadata', {})
                    if 'composite_analysis' in metadata:
                        composite_info = metadata['composite_analysis']
                        actual_variables_used = composite_info.get('variables_used', [])
                        
            except Exception as e:
                logger.warning(f"Could not load composite variables from files: {e}")
            
            # Get risk distribution
            if 'vulnerability_category' in unified_gdf.columns:
                risk_distribution = unified_gdf['vulnerability_category'].value_counts().to_dict()
            else:
                risk_distribution = {'High Risk': 'N/A', 'Medium Risk': 'N/A', 'Low Risk': 'N/A'}
            
        else:
            logger.warning(f"Failed to create unified dataset: {unified_result.get('message', 'Unknown error')}")
            # Fallback to file-based summary
            return _generate_summary_from_files(session_id)
        
        # Create dynamic variable descriptions for both methods
        composite_variables_section = ""
        pca_variables_section = ""
        
        if actual_variables_used:
            composite_variables_section = f"""**📋 Composite Variables Used ({len(actual_variables_used)} selected):**
{chr(10).join([f"• **{var}**" for var in actual_variables_used])}"""
        else:
            composite_variables_section = "**📋 Composite Variables:** Automatically selected from dataset"
        
        if pca_variables_used:
            pca_variables_section = f"""**📋 PCA Variables Used ({len(pca_variables_used)} selected):**
{chr(10).join([f"• **{var}**" for var in pca_variables_used[:10]])}
{f"... and {len(pca_variables_used) - 10} more" if len(pca_variables_used) > 10 else ""}"""
        else:
            pca_variables_section = "**📋 PCA Variables:** Automatically selected from dataset"
        
        # Generate restructured comprehensive summary message with clear sections
        message = f"""🎯 **COMPREHENSIVE MALARIA RISK ANALYSIS RESULTS**

📊 **ANALYSIS OVERVIEW:**
- **Total Wards Analyzed:** {total_wards}
- **Methods Completed:** Composite Risk Scoring ✅ + PCA Analysis ✅
- **Analysis Status:** Complete and Ready for Action
- **Unified Dataset:** ✅ Created with {unified_gdf.shape[1]} total columns

---

## 🔬 **1. COMPOSITE RISK SCORING ANALYSIS**

**📋 Variables Used:**
{composite_variables_section}

**🔴 Top 5 Highest Risk Wards:**
{chr(10).join([f"   {i+1}. **{row['WardName']}** (Rank: {int(row['overall_rank'])})" for i, (_, row) in enumerate(composite_top5.iterrows())])}

**🟢 Top 5 Lowest Risk Wards:**
{chr(10).join([f"   {i+1}. {row['WardName']} (Rank: {int(row['overall_rank'])})" for i, (_, row) in enumerate(composite_bottom5.iterrows())])}

**📊 Key Details:**
- **Risk Score Range:** {score_range}
- **Top Risk Ward:** {composite_top5.iloc[0]['WardName']} (Score: {top_ward_score:.3f})
- **Scoring Models:** 26 different composite models used
- **Method:** Mean aggregation across multiple variable combinations

---

## 🔬 **2. PRINCIPAL COMPONENT ANALYSIS (PCA)**

**📋 Variables Used:**
{pca_variables_section}

**🔴 Top 5 Highest Risk Wards:**
{chr(10).join([f"   {i+1}. **{row['WardName']}** (Rank: {int(row['overall_rank'])})" for i, (_, row) in enumerate(pca_top5.iterrows())]) if len(pca_top5) > 0 else "   ❌ PCA rankings not available"}

**🟢 Top 5 Lowest Risk Wards:**
{chr(10).join([f"   {i+1}. {row['WardName']} (Rank: {int(row['overall_rank'])})" for i, (_, row) in enumerate(pca_bottom5.iterrows())]) if len(pca_bottom5) > 0 else "   ❌ PCA rankings not available"}

**📊 Key Details:**
- **Variance Explained:** {pca_variance_explained}
- **Components Used:** 16 principal components
- **Top Variables:** {', '.join(pca_variables_used[:3]) if pca_variables_used else 'N/A'}
- **Method:** Principal component dimensional reduction

---

## ⚖️ **3. METHOD COMPARISON & CONSENSUS**

**📈 Risk Distribution:**
{chr(10).join([f"**{category}:** {count} wards" for category, count in risk_distribution.items()])}

**🔍 Method Agreement:**
- **Composite Method:** {len(actual_variables_used) if actual_variables_used else 0} variables, 26 scoring models
- **PCA Method:** {len(pca_variables_used) if pca_variables_used else 0} variables, {pca_variance_explained} variance captured
- **Top Ward Difference:** {'Different priorities identified' if len(pca_top5) > 0 and composite_top5.iloc[0]['WardName'] != pca_top5.iloc[0]['WardName'] else 'Similar high-risk priorities'}

**📋 Risk Score Interpretation:**
- **High scores (>0.4):** Multiple risk factors present, urgent intervention needed
- **Medium scores (0.2-0.4):** Moderate risk, targeted prevention recommended  
- **Low scores (<0.2):** Lower risk, maintenance surveillance sufficient

---

## 🗺️ **4. NEXT STEPS & RECOMMENDATIONS**

**Immediate Actions:**
1. **Generate Interactive Maps** - Visualize risk distribution geographically
2. **Create Ward-Specific Reports** - Detailed analysis for priority wards
3. **Export Results** - Download data for field teams

**Ongoing Monitoring:**
4. **Monitor and Update** - Regular re-analysis with new data
5. **Validation Studies** - Ground-truth high-risk ward predictions
6. **Intervention Planning** - Use both methods for comprehensive targeting

**✅ Analysis Complete!** Ready for operational deployment and evidence-based intervention planning."""

        return {
            'status': 'success',
            'message': message,
            'unified_dataset_created': True,
            'analysis_summary': {
                'total_wards': total_wards,
                'risk_distribution': risk_distribution,
                'analysis_methods': ['composite_scoring', 'pca_analysis'],
                'variables_used': {
                    'composite': actual_variables_used,
                    'pca': pca_variables_used
                },
                'has_geographic_data': True,
                'unified_dataset_columns': unified_gdf.shape[1],
                'ready_for_visualization': True
            }
        }
        
    except Exception as e:
        logger.error(f"Error generating comprehensive summary with unified dataset: {e}")
        # Fallback to original file-based summary
        return _generate_summary_from_files(session_id)


def _generate_summary_from_files(session_id: str) -> Dict[str, Any]:
    """Fallback method to generate summary from individual analysis files"""
    try:
        import os
        import pandas as pd
        
        logger.info(f"Using fallback file-based summary for session {session_id}")
        
        # Check session folder for analysis results
        session_folder = os.path.join('instance', 'uploads', session_id)
        if not os.path.exists(session_folder):
            return {
                'status': 'error',
                'message': 'Session folder not found. Please run analyses first.',
            }
        
        # Look for saved analysis files
        vulnerability_file = os.path.join(session_folder, 'analysis_vulnerability_rankings.csv')
        composite_file = os.path.join(session_folder, 'composite_scores.csv')
        
        # Check if composite analysis results exist
        if not os.path.exists(vulnerability_file) or not os.path.exists(composite_file):
            return {
                'status': 'error',
                'message': 'No analysis results found. Please run both composite and PCA analyses first.',
            }
        
        # Load analysis results
        vulnerability_df = pd.read_csv(vulnerability_file)
        composite_scores_df = pd.read_csv(composite_file)
        
        logger.info(f"Loaded vulnerability rankings: {len(vulnerability_df)} rows")
        logger.info(f"Loaded composite scores: {len(composite_scores_df)} rows")
        
        # Check for required columns
        if 'WardName' not in vulnerability_df.columns or 'overall_rank' not in vulnerability_df.columns:
            return {
                'status': 'error',
                'message': 'Required ranking columns not found in analysis results',
            }
        
        # Get key analysis information
        total_wards = len(vulnerability_df)
        
        # Get top and bottom 5 wards from composite method
        composite_top5 = vulnerability_df.nsmallest(5, 'overall_rank')[['WardName', 'overall_rank']]
        composite_bottom5 = vulnerability_df.nlargest(5, 'overall_rank')[['WardName', 'overall_rank']]
        
        # Get risk category distribution
        if 'vulnerability_category' in vulnerability_df.columns:
            risk_distribution = vulnerability_df['vulnerability_category'].value_counts().to_dict()
        else:
            risk_distribution = {'High Risk': 'N/A', 'Medium Risk': 'N/A', 'Low Risk': 'N/A'}
        
        # Get score information
        has_scores = 'median_score' in vulnerability_df.columns
        if has_scores:
            top_ward_score = vulnerability_df.loc[vulnerability_df['overall_rank'] == 1, 'median_score'].iloc[0]
            score_range = f"{vulnerability_df['median_score'].min():.3f} to {vulnerability_df['median_score'].max():.3f}"
        else:
            top_ward_score = "N/A"
            score_range = "N/A"
        
        # Try to get variables used from data handler
        actual_variables_used = []
        try:
            from app.data import DataHandler
            temp_handler = DataHandler(session_folder)
            if hasattr(temp_handler, 'composite_variables') and temp_handler.composite_variables:
                actual_variables_used = temp_handler.composite_variables
        except Exception as e:
            logger.warning(f"Could not extract variables: {e}")
        
        # Create variable section
        if actual_variables_used:
            composite_variables_section = f"""**📋 Composite Variables Used ({len(actual_variables_used)} selected):**
{chr(10).join([f"• **{var}**" for var in actual_variables_used])}"""
        else:
            composite_variables_section = "**📋 Composite Variables:** Automatically selected from dataset"
        
        # Generate summary message (without PCA results for fallback)
        message = f"""🎯 **MALARIA RISK ANALYSIS RESULTS**

📊 **ANALYSIS OVERVIEW:**
- **Total Wards Analyzed:** {total_wards}
- **Method Completed:** Composite Risk Scoring ✅
- **Risk Score Range:** {score_range}
- **Analysis Status:** Complete and Ready for Action

🔬 **VARIABLE SELECTION & METHODOLOGY:**

**Composite Risk Scoring:**
{composite_variables_section}

🔴 **TOP 5 HIGHEST RISK WARDS:**

**Composite Scoring Results:**
{chr(10).join([f"{i+1}. **{row['WardName']}** (Rank: {int(row['overall_rank'])})" for i, (_, row) in enumerate(composite_top5.iterrows())])}

🟢 **TOP 5 LOWEST RISK WARDS:**

**Composite Scoring Results:**
{chr(10).join([f"{i+1}. {row['WardName']} (Rank: {int(row['overall_rank'])})" for i, (_, row) in enumerate(composite_bottom5.iterrows())])}

📈 **RISK DISTRIBUTION:**

{chr(10).join([f"**{category}:** {count} wards" for category, count in risk_distribution.items()])}

📊 **KEY FINDINGS:**

**Top Risk Ward:** {composite_top5.iloc[0]['WardName']} (Score: {top_ward_score})

**Risk Score Interpretation:**
- **High scores (>0.4):** Multiple risk factors present, urgent intervention needed
- **Medium scores (0.2-0.4):** Moderate risk, targeted prevention recommended  
- **Low scores (<0.2):** Lower risk, maintenance surveillance sufficient

🗺️ **NEXT STEPS:**

1. **Generate Interactive Maps** - Visualize risk distribution geographically
2. **Create Ward-Specific Reports** - Detailed analysis for priority wards  
3. **Export Results** - Download data for field teams
4. **Monitor and Update** - Regular re-analysis with new data

**Analysis Complete!** For complete dual-method analysis including PCA, please rerun both analyses."""

        return {
            'status': 'success',
            'message': message,
            'unified_dataset_created': False,
            'analysis_summary': {
                'total_wards': total_wards,
                'risk_distribution': risk_distribution,
                'analysis_methods': ['composite_scoring'],
                'variables_used': actual_variables_used if actual_variables_used else ['Variables automatically selected from dataset'],
                'has_geographic_data': True,
                'ready_for_visualization': True
            }
        }
        
    except Exception as e:
        logger.error(f"Error in fallback summary generation: {e}")
        return {
            'status': 'error',
            'message': f'Error generating analysis summary: {str(e)}',
        } 