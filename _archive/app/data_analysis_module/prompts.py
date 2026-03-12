"""
Production-grade prompts for data analysis code generation.
Based on industry best practices from OpenAI Code Interpreter, PandasAI, 
Open Interpreter, and 2024-2025 research.

Key principles:
1. Role assignment with technical persona
2. Explicit output format requirements  
3. Context-aware variable usage
4. Error prevention through constraints
5. Chain-of-thought reasoning
"""

class AnalysisPrompts:
    """
    Prompt templates optimized for executable Python code generation.
    NO markdown, NO explanations - only runnable code.
    """
    
    @staticmethod
    def build_analysis_prompt(context):
        """
        Build production-grade analysis prompt following 2024-2025 best practices.
        Optimized for Qwen3, GPT-4, and other modern LLMs.
        """
        # Determine data structure and available variables
        if 'sheets' in context:
            # Multi-sheet Excel file
            sheet_names = list(context['sheets'].keys())
            data_vars = [
                f"data = {{dict with {len(sheet_names)} sheets: {sheet_names}}}",
                f"sheets = data  # Alias for clarity"
            ]
            # Add individual sheet variables for convenience
            for sheet_name in sheet_names:
                safe_name = sheet_name.replace(' ', '_').replace('-', '_')
                data_vars.append(f"df_{safe_name} = data['{sheet_name}']")
            
            data_context = f"""
# Available data variables:
# - data: Dictionary containing all sheets
# - sheets: Alias for data dictionary
# - Individual DataFrames: {', '.join([f'df_{s.replace(" ", "_").replace("-", "_")}' for s in sheet_names])}
"""
        else:
            # Single DataFrame (CSV or single-sheet Excel)
            data_vars = [
                f"df = pandas.DataFrame with {context['shape'][0]:,} rows × {context['shape'][1]} columns",
                "data = df  # Alias for compatibility"
            ]
            data_context = """
# Available data variables:
# - df: The main DataFrame
# - data: Alias for df
"""
        
        # CRITICAL: Be extremely explicit for Qwen3-8B
        # The model tends to add pd.read_csv() calls
        
        if 'sheets' in context:
            # Multi-sheet case
            available_vars = "data (dict with sheets), sheets (alias)"
            main_var = "data"
        else:
            # Single dataframe case  
            available_vars = "df (pandas DataFrame)"
            main_var = "df"
        
        task_prompt = f"""Generate Python code for data analysis.

CRITICAL RULES:
1. The data is ALREADY LOADED in variable: {main_var}
2. DO NOT use pd.read_csv() or pd.read_excel() - data is already in memory
3. Available variables: {available_vars}
4. Use print() for all output

Write Python code that analyzes the loaded data:

import pandas as pd
import numpy as np

# The variable {main_var} is already loaded with data
# DO NOT load any files - use {main_var} directly

print("Data Analysis")
print("=" * 50)

# Your analysis code here using {main_var}
"""
        
        return task_prompt

    @staticmethod
    def build_error_fix_prompt(code, error):
        """Self-healing prompt for code error recovery."""
        return f"""You are a Python code executor. Fix the error and output ONLY executable code.

# Previous code that failed:
{code}

# Error encountered:
{error}

# CRITICAL CONTEXT:
# 1. Data is ALREADY LOADED - never use pd.read_excel() or pd.read_csv()
# 2. Available variables depend on data type:
#    - Single file: 'df' and 'data' (both reference the DataFrame)
#    - Multi-sheet: 'data' (dict), 'sheets' (alias), and df_<sheet_name> for each sheet
# 3. If error mentions 'file not found', remove ALL file reading code
# 4. If error mentions undefined variable, use the correct variable name from above

# OUTPUT: Corrected Python code only (no markdown, no explanations)
# BEGIN CORRECTED CODE:
"""

    @staticmethod
    def build_followup_prompt(previous_result, new_query, data_context):
        """Conversational follow-up prompt maintaining context."""
        return f"""You are a Python code executor. Generate ONLY executable code.

# Previous analysis results:
{previous_result[:500]}...

# User's new request:
{new_query}

# Available data (still loaded):
{data_context}

# REQUIREMENTS:
1. Build upon previous analysis
2. Output ONLY Python code (no markdown)
3. Use print() for all output
4. Data is still available in memory

# BEGIN CODE:
"""

    @staticmethod
    def build_smart_preliminary_analysis():
        """Generate smart preliminary analysis code that works with ANY data."""
        return """
# Smart preliminary analysis - adapts to any data structure
try:
    # Determine what kind of data we have
    if 'data' in locals() and isinstance(data, dict):
        print("📊 Multi-Sheet Excel Analysis")
        print("=" * 50)
        print(f"\nFound {len(data)} worksheets:")
        
        for sheet_name, sheet_df in data.items():
            print(f"\n📋 Sheet: '{sheet_name}'")
            print("-" * 40)
            
            # Basic info
            print(f"  • Size: {sheet_df.shape[0]:,} rows × {sheet_df.shape[1]} columns")
            
            # Check if it's a pivot table (has multi-index or many unnamed columns)
            unnamed_cols = [c for c in sheet_df.columns if 'Unnamed' in str(c)]
            if len(unnamed_cols) > 5:
                print(f"  • Type: Appears to be a pivot table or summary report")
                # Show first few named columns only
                named_cols = [c for c in sheet_df.columns if 'Unnamed' not in str(c)][:10]
                if named_cols:
                    print(f"  • Key fields: {', '.join(named_cols[:5])}")
            else:
                # Regular data sheet
                print(f"  • Type: Structured data table")
                
                # Column overview
                cols = [str(c) for c in sheet_df.columns if 'Unnamed' not in str(c)][:8]
                if cols:
                    print(f"  • Key columns: {', '.join(cols)}")
                
                # Data types
                numeric_cols = sheet_df.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    print(f"  • Numeric fields: {len(numeric_cols)} columns")
                
                # Missing data check
                missing = sheet_df.isnull().sum().sum()
                if missing > 0:
                    missing_pct = (missing / (sheet_df.shape[0] * sheet_df.shape[1]) * 100)
                    print(f"  • Data completeness: {100 - missing_pct:.1f}%")
                else:
                    print(f"  • Data completeness: 100% (no missing values)")
    
    elif 'df' in locals():
        print("📊 Data Analysis Overview")
        print("=" * 50)
        
        # Basic info
        print(f"\n📈 Dataset Summary:")
        print(f"  • Records: {df.shape[0]:,}")
        print(f"  • Fields: {df.shape[1]}")
        print(f"  • Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
        
        # Column overview (skip unnamed)
        cols = [str(c) for c in df.columns if 'Unnamed' not in str(c)]
        if cols:
            print(f"\n📋 Available columns ({len(cols)}):")
            # Group columns by type
            numeric_cols = df.select_dtypes(include=['number']).columns
            text_cols = df.select_dtypes(include=['object']).columns
            date_cols = df.select_dtypes(include=['datetime']).columns
            
            if len(numeric_cols) > 0:
                print(f"  • Numeric: {', '.join(list(numeric_cols)[:5])}{'...' if len(numeric_cols) > 5 else ''}")
            if len(text_cols) > 0:
                print(f"  • Text: {', '.join(list(text_cols)[:5])}{'...' if len(text_cols) > 5 else ''}")
            if len(date_cols) > 0:
                print(f"  • Dates: {', '.join(list(date_cols)[:5])}")
        
        # Quick insights
        print(f"\n🔍 Quick Insights:")
        
        # Check for missing data
        missing_pct = (df.isnull().sum().sum() / (df.shape[0] * df.shape[1]) * 100)
        if missing_pct > 0:
            print(f"  • Data completeness: {100 - missing_pct:.1f}%")
            # Which columns have most missing
            missing_cols = df.isnull().sum()
            missing_cols = missing_cols[missing_cols > 0].sort_values(ascending=False)
            if len(missing_cols) > 0:
                worst_col = missing_cols.index[0]
                worst_pct = (missing_cols.iloc[0] / len(df) * 100)
                print(f"  • Most incomplete field: '{worst_col}' ({worst_pct:.1f}% missing)")
        else:
            print(f"  • Data completeness: Perfect! No missing values")
        
        # Check for duplicates
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            print(f"  • Duplicate rows found: {duplicates:,} ({duplicates/len(df)*100:.1f}%)")
        
        # For numeric columns, show range
        if len(numeric_cols) > 0:
            print(f"\n📊 Numeric Summary:")
            for col in list(numeric_cols)[:3]:
                if 'Unnamed' not in str(col):
                    col_min = df[col].min()
                    col_max = df[col].max()
                    col_mean = df[col].mean()
                    print(f"  • {col}: ranges from {col_min:,.0f} to {col_max:,.0f} (avg: {col_mean:,.0f})")
        
        # For text columns, show unique values
        if len(text_cols) > 0:
            print(f"\n📝 Categorical Summary:")
            for col in list(text_cols)[:3]:
                if 'Unnamed' not in str(col):
                    unique_count = df[col].nunique()
                    print(f"  • {col}: {unique_count} unique values")
                    if unique_count <= 10:
                        top_values = df[col].value_counts().head(3)
                        for val, count in top_values.items():
                            print(f"      - '{val}': {count:,} occurrences")
    
    print("\n💡 What would you like to explore next?")
    print("   You can ask me to:")
    print("   • Analyze specific columns or relationships")
    print("   • Create visualizations or charts")
    print("   • Find patterns or anomalies")
    print("   • Build predictive models")
    print("   • Export processed data")
    
except Exception as e:
    print(f"Analysis note: {str(e)}")
    print("\nYour data is loaded. What would you like to know about it?")
"""
    
    # Specialized prompt templates for common analyses
    ANALYSIS_TEMPLATES = {
        'eda': """# Exploratory Data Analysis
# 1. Basic information
print("Dataset Overview")
print("="*50)
print(f"Shape: {df.shape}")
print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
print()

# 2. Data types and missing values
print("Data Types and Missing Values")
print("="*50)
info_df = pd.DataFrame({
    'Type': df.dtypes,
    'Missing': df.isnull().sum(),
    'Missing %': (df.isnull().sum() / len(df) * 100).round(2)
})
print(info_df)
print()

# 3. Numeric columns statistics
if len(df.select_dtypes(include=[np.number]).columns) > 0:
    print("Numeric Columns Statistics")
    print("="*50)
    print(df.describe())
    print()

# 4. Categorical columns
categorical_cols = df.select_dtypes(include=['object']).columns
if len(categorical_cols) > 0:
    print("Categorical Columns Summary")
    print("="*50)
    for col in categorical_cols[:5]:  # First 5 categorical columns
        print(f"\\n{col}:")
        print(df[col].value_counts().head())

# 5. Correlations (if numeric columns exist)
numeric_cols = df.select_dtypes(include=[np.number]).columns
if len(numeric_cols) > 1:
    print("\\nCorrelation Matrix (top correlations)")
    print("="*50)
    corr_matrix = df[numeric_cols].corr()
    # Find top correlations
    corr_pairs = []
    for i in range(len(corr_matrix.columns)):
        for j in range(i+1, len(corr_matrix.columns)):
            corr_pairs.append((
                corr_matrix.columns[i],
                corr_matrix.columns[j],
                corr_matrix.iloc[i, j]
            ))
    corr_pairs.sort(key=lambda x: abs(x[2]), reverse=True)
    for col1, col2, corr in corr_pairs[:5]:
        print(f"{col1} <-> {col2}: {corr:.3f}")

print("\\nSuggested next steps:")
print("1. Handle missing values in columns with >10% missing data")
print("2. Investigate outliers in numeric columns")
print("3. Analyze relationships between correlated variables")
print("4. Consider feature engineering based on domain knowledge")
""",

        'outliers': """# Outlier Detection
import warnings
warnings.filterwarnings('ignore')

numeric_cols = df.select_dtypes(include=[np.number]).columns
print("Outlier Analysis")
print("="*50)

for col in numeric_cols:
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
    
    if len(outliers) > 0:
        print(f"\\n{col}:")
        print(f"  - Outliers: {len(outliers)} ({len(outliers)/len(df)*100:.1f}%)")
        print(f"  - Lower bound: {lower_bound:.2f}")
        print(f"  - Upper bound: {upper_bound:.2f}")
        print(f"  - Min outlier: {outliers[col].min():.2f}")
        print(f"  - Max outlier: {outliers[col].max():.2f}")

# Visualization if matplotlib available
try:
    import matplotlib.pyplot as plt
    if len(numeric_cols) > 0:
        fig, axes = plt.subplots(1, min(3, len(numeric_cols)), figsize=(12, 4))
        if len(numeric_cols) == 1:
            axes = [axes]
        for i, col in enumerate(numeric_cols[:3]):
            axes[i].boxplot(df[col].dropna())
            axes[i].set_title(col)
            axes[i].set_ylabel('Value')
        plt.tight_layout()
        plt.show()
except ImportError:
    pass

print("\\nSuggested next steps:")
print("1. Investigate rows with multiple outliers")
print("2. Consider domain-specific rules for outlier handling")
print("3. Decide on outlier treatment: remove, cap, or transform")
""",

        'patterns': """# Pattern Discovery
print("Pattern and Trend Analysis")
print("="*50)

# Check for time-based columns
date_cols = df.select_dtypes(include=['datetime64']).columns
if len(date_cols) == 0:
    # Try to identify potential date columns
    for col in df.columns:
        try:
            pd.to_datetime(df[col])
            print(f"Found potential date column: {col}")
            df[col] = pd.to_datetime(df[col])
            date_cols = [col]
            break
        except:
            pass

# Analyze patterns
if len(date_cols) > 0:
    print(f"\\nTime-based patterns (using {date_cols[0]}):")
    df_sorted = df.sort_values(date_cols[0])
    
    # Check for seasonality in numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols[:3]:
        print(f"\\n{col} over time:")
        print(f"  - Mean: {df[col].mean():.2f}")
        print(f"  - Trend: {'Increasing' if df_sorted[col].iloc[-1] > df_sorted[col].iloc[0] else 'Decreasing'}")

# Look for group patterns
categorical_cols = df.select_dtypes(include=['object']).columns
if len(categorical_cols) > 0 and len(df.select_dtypes(include=[np.number]).columns) > 0:
    print("\\nGroup patterns:")
    for cat_col in categorical_cols[:2]:
        if df[cat_col].nunique() < 20:
            numeric_col = df.select_dtypes(include=[np.number]).columns[0]
            grouped = df.groupby(cat_col)[numeric_col].agg(['mean', 'std', 'count'])
            print(f"\\n{numeric_col} by {cat_col}:")
            print(grouped.sort_values('mean', ascending=False).head())

print("\\nSuggested next steps:")
print("1. Deep dive into identified patterns")
print("2. Test statistical significance of group differences")
print("3. Build predictive models based on patterns")
"""
    }