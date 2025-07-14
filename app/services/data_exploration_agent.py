"""
Data Exploration Agent for ChatMRPT
Pure LLM-driven malaria data exploration using existing infrastructure

This service provides:
- Dynamic variable discovery in malaria epidemiological data
- Intelligent exploration without hardcoded assumptions
- Clean presentation of findings for intervention planning
- Assessment of data suitability for ward ranking analysis
"""

import logging
from typing import Dict, Any, Optional

from .conversational_data_access import ConversationalDataAccess
from ..core.llm_manager import LLMManager

logger = logging.getLogger(__name__)

class DataExplorationAgent:
    """Pure LLM-driven malaria data exploration using existing infrastructure"""
    
    def __init__(self, session_id: str):
        self.session_id = session_id  
        self.llm_manager = LLMManager()
        self.data_access = ConversationalDataAccess(session_id)
    
    def explore_data(self) -> Dict[str, Any]:
        """Main exploration using uploaded raw_data.csv"""
        try:
            logger.info(f"Starting data exploration for session {self.session_id}")
            print(f"🔍 EXPLORATION: Starting for session {self.session_id}")
            
            # 1. Get basic data info
            data_info = self._get_data_info()
            if not data_info:
                print("❌ EXPLORATION: Could not get data info")
                return self._create_error_response("Could not access uploaded data")
            
            print(f"✅ EXPLORATION: Got data info ({len(data_info)} chars)")
            
            # 2. LLM generates exploration code  
            exploration_code = self._generate_exploration(data_info)
            if not exploration_code:
                print("❌ EXPLORATION: Could not generate code")
                return self._create_error_response("Could not generate exploration code")
            
            print(f"✅ EXPLORATION: Generated {len(exploration_code)} chars of code")
            print(f"📝 EXPLORATION: First 200 chars of code:\n{exploration_code[:200]}...")
            
            # 3. Execute safely
            results = self.data_access.execute_code(exploration_code)
            if not results.get('success'):
                print(f"❌ EXPLORATION: Execution failed - {results.get('error')}")
                return self._create_error_response(f"Exploration execution failed: {results.get('error')}")
            
            print(f"✅ EXPLORATION: Code executed successfully")
            
            # 4. Format for user
            formatted = self._format_results(results)
            
            print(f"✅ EXPLORATION: Formatted results ({len(formatted)} chars)")
            logger.info(f"Data exploration completed successfully for session {self.session_id}")
            
            # Cache results for polling API
            exploration_result = {
                'status': 'success',
                'exploration': formatted,
                'has_visualizations': 'base64' in results.get('output', '').lower(),
                'session_id': self.session_id
            }
            
            self._cache_results(exploration_result)
            
            return exploration_result
            
        except Exception as e:
            logger.error(f"Error in data exploration for session {self.session_id}: {e}")
            return self._create_error_response(f"Exploration failed: {str(e)}")
    
    def _get_data_info(self) -> Optional[str]:
        """Get basic data schema using existing ConversationalDataAccess"""
        info_code = """
# Basic data overview for LLM context
print("DATASET INFO:")
print(f"Shape: {data.shape}")
print(f"Columns: {list(data.columns)}")
print(f"Data types:")
for col, dtype in data.dtypes.items():
    print(f"  {col}: {dtype}")
print(f"Missing values:")
missing = data.isnull().sum()
for col, count in missing.items():
    if count > 0:
        pct = (count / len(data)) * 100
        print(f"  {col}: {count} ({pct:.1f}%)")
print(f"Sample data:")
print(data.head(2))
"""
        print(f"🔍 EXPLORATION: Executing data info code for session {self.session_id}")
        print(f"🔍 EXPLORATION: Info code: {info_code[:200]}...")
        try:
            result = self.data_access.execute_code(info_code)
            if result.get('success'):
                return result.get('output', '')
            else:
                logger.error(f"Failed to get data info: {result.get('error')}")
                return None
        except Exception as e:
            logger.error(f"Error getting data info: {e}")
            return None
    
    def _generate_exploration(self, data_info: str) -> Optional[str]:
        """LLM generates custom malaria exploration code using best practices"""
        
        # Step 1: Analysis and Planning prompt
        planning_prompt = f"""
You are a malaria epidemiologist. Let's think step by step about exploring this dataset.

Data Overview:
{data_info}

TASK: Plan the exploration approach for malaria intervention targeting.

Step 1: Analyze what variables are present
Step 2: Identify malaria indicators  
Step 3: Find geographic identifiers
Step 4: Detect risk factors
Step 5: Plan visualizations

Respond with a brief analysis plan (under 100 words) for exploring this specific dataset.
"""

        try:
            # Get analysis plan first
            print("🧠 EXPLORATION: Generating analysis plan...")
            analysis_plan = self.llm_manager.generate_response(planning_prompt)
            print(f"📋 EXPLORATION: Plan generated ({len(analysis_plan)} chars)")
            print(f"🔍 EXPLORATION: Plan content: {analysis_plan[:200]}...")
            
            # Step 2: Code generation with specific constraints
            code_prompt = f"""
You are a Python data analyst. Generate EXECUTABLE code that performs comprehensive EDA with actual statistics and visualizations.

ANALYSIS PLAN: {analysis_plan}

DATA CONTEXT: The 'data' DataFrame is already loaded with {data_info.split('Shape: ')[1].split(chr(10))[0] if 'Shape: ' in data_info else 'malaria epidemiological data'}.

REQUIREMENTS:
1. Data is already available as 'data' variable - DO NOT use pd.read_csv()
2. Available: data, plt, sns, np (already imported)
3. CRITICAL: Classify columns first before any statistical operations
4. Generate ACTUAL statistical analysis with numbers
5. Create comprehensive visualizations
6. Print detailed statistical findings

IMPORTANT: Before any statistical analysis, classify columns as:
- Identifier columns (names, codes, IDs): Skip these in numeric analysis
- Numeric variables: Use for statistics and correlations
- Categorical variables: Use for frequency analysis only

Generate code that performs:
1. DATA TYPE ANALYSIS:
   - Separate identifier columns from analysis variables
   - Identify numeric vs categorical columns properly
   - Skip identifier columns (names, codes, IDs) from statistical analysis
   - Print column classification results

2. STATISTICAL ANALYSIS (numeric columns only):
   - Calculate descriptive statistics ONLY for numeric variables
   - Identify missing data patterns with exact percentages
   - Calculate correlations between numeric variables only
   - Print actual numbers and percentages

3. VARIABLE DISCOVERY:
   - Find malaria indicators (columns with 'pfpr', 'tpr', 'malaria', 'positive')
   - Identify geographic identifier columns (ward, state, lga, admin, name, code)
   - Detect demographic/environmental numeric factors
   - Print discovered variable lists with data types

3. VISUALIZATIONS:
   - Create 2x2 subplot figure with plt.subplots(2,2)
   - Plot 1: Distribution of main malaria indicator
   - Plot 2: Correlation heatmap of top variables
   - Plot 3: Geographic distribution (if coordinates available)
   - Plot 4: Missing data pattern visualization
   - Save as PNG with plt.savefig()

4. DETAILED REPORTING:
   - Print actual statistics for each finding
   - Report exact missing data percentages
   - Show correlation values
   - List top/bottom performing areas

CRITICAL: The 'data' DataFrame is already loaded and available. Do NOT import pandas.

MANDATORY REQUIREMENTS:
1. Start with import numpy as np
2. Use the existing 'data' DataFrame variable
3. Generate ONLY valid Python code
4. No explanatory text or comments outside code
5. Follow the exact structure below

You MUST generate code that follows this EXACT structure:

```python
import numpy as np

# Step 1: Define identifier keywords
identifier_keywords = ['name', 'code', 'id', 'ward', 'lga', 'state', 'admin']

# Step 2: Classify columns  
identifier_cols = [col for col in data.columns if any(keyword in col.lower() for keyword in identifier_keywords)]
numeric_cols = data.select_dtypes(include=[np.number]).columns.tolist()
numeric_analysis_cols = [col for col in numeric_cols if col not in identifier_cols]

# Step 3: Print classification results
print("=== COLUMN CLASSIFICATION ===")
print("Total columns:", len(data.columns))
print("Identifier columns:", identifier_cols)
print("Numeric columns:", numeric_cols) 
print("Analysis columns:", numeric_analysis_cols)

# Step 4: Generate basic statistics
if len(numeric_analysis_cols) > 0:
    desc_stats = data[numeric_analysis_cols].describe()
    print("\\n=== DESCRIPTIVE STATISTICS ===")
    print(desc_stats)
else:
    print("No numeric analysis columns found")
```

CRITICAL: Follow this structure exactly. Do NOT rearrange lines or add complex logic.

Write ONLY executable Python code following this pattern - no explanations:
"""
            
            # Generate the exploration code
            print("🧠 EXPLORATION: Generating code with LLM...")
            print(f"🔍 EXPLORATION: Code prompt length: {len(code_prompt)} chars")
            print(f"🔍 EXPLORATION: Code prompt preview: {code_prompt[:300]}...")
            exploration_code = self.llm_manager.generate_response(code_prompt)
            
            # Clean code if wrapped in markdown
            if "```python" in exploration_code:
                exploration_code = exploration_code.split("```python")[1].split("```")[0].strip()
            elif "```" in exploration_code:
                exploration_code = exploration_code.split("```")[1].strip()
            
            # Debug: Print the actual generated code
            print(f"🔍 EXPLORATION: Generated code (first 500 chars):")
            print(exploration_code[:500])
            print("🔍 EXPLORATION: End of code preview")
            
            # Also save the full code for debugging
            print(f"🔍 EXPLORATION: Full generated code length: {len(exploration_code)}")
            if len(exploration_code) > 2000:
                print(f"🔍 EXPLORATION: Code seems complete")
            else:
                print(f"⚠️ EXPLORATION: Code might be truncated")
            
            return exploration_code
            
        except Exception as e:
            logger.error(f"Error generating exploration code: {e}")
            print(f"❌ EXPLORATION: LLM generation error: {e}")
            print(f"❌ EXPLORATION: Error type: {type(e).__name__}")
            import traceback
            print(f"❌ EXPLORATION: Full traceback:")
            traceback.print_exc()
            return None
    
    def _format_results(self, raw_results: Dict) -> str:
        """Format exploration output preserving statistical details"""
        formatting_prompt = f"""
Format this malaria data exploration output into clean, professional EDA report for epidemiologists.

Raw Exploration Output: 
{raw_results.get('output', '')}

PRESERVE ALL STATISTICS AND NUMBERS from the raw output. Format as:

## 📊 Dataset Overview & Statistics
[Keep all actual numbers: shape, missing data percentages, descriptive statistics]

## 🦟 Malaria Indicators Analysis  
[Preserve discovered variable names and their actual statistical values]

## 🌍 Risk Factors & Correlations
[Keep correlation coefficients and statistical relationships found]

## 🎯 Key Findings for Intervention Planning
[Summarize insights while keeping specific numbers and statistics]

CRITICAL: 
- PRESERVE all numerical values, percentages, correlations from raw output
- KEEP variable names exactly as discovered
- MAINTAIN statistical findings and data quality metrics
- Format as clean markdown but DO NOT remove technical details
- Include any visualization information mentioned

Format the raw statistical output professionally but keep all the data science details intact.
"""
        
        try:
            formatted = self.llm_manager.generate_response(formatting_prompt)
            return formatted
        except Exception as e:
            logger.error(f"Error formatting results: {e}")
            return f"## 📊 Data Exploration Results\n\n{raw_results.get('output', '')}"
    
    def _cache_results(self, exploration_result: Dict[str, Any]) -> None:
        """Cache exploration results to file for polling API"""
        try:
            import os
            import json
            from flask import current_app
            
            session_folder = os.path.join(current_app.instance_path, 'uploads', self.session_id)
            if not os.path.exists(session_folder):
                os.makedirs(session_folder, exist_ok=True)
            
            cache_file = os.path.join(session_folder, 'exploration_results.json')
            with open(cache_file, 'w') as f:
                json.dump(exploration_result, f, indent=2)
            
            print(f"💾 EXPLORATION: Results cached to {cache_file}")
            
        except Exception as e:
            logger.error(f"Error caching exploration results: {e}")
    
    def _create_error_response(self, error_message: str) -> Dict[str, Any]:
        """Create standardized error response"""
        return {
            'status': 'error',
            'message': error_message,
            'session_id': self.session_id
        }