# app/kb.py
"""
Enhanced knowledge base for the MRPT AI Assistant
Contains comprehensive explanations of methodologies, variables, and analysis procedures
to support LLM-powered explanation generation
"""

import os
import logging
import json
from typing import Dict, List, Tuple, Optional, Union, Any

# Set up logging
logger = logging.getLogger(__name__)

class KnowledgeBase:
   """
   Knowledge base class for the MRPT AI Assistant
   
   This class provides structured access to explanations and domain knowledge
   for generating high-quality, context-aware responses about malaria risk analysis.
   """
   
   def __init__(self):
       # Load all knowledge sections
       self._methodology_kb = METHODOLOGY_KB
       self._variable_rationales = VARIABLE_RATIONALES
       self._variable_categories = VARIABLE_CATEGORIES
       self._explanation_templates = EXPLANATION_TEMPLATES
       self._technical_references = TECHNICAL_REFERENCES

   def get_methodology_explanation(self, methodology_name: str, detail_level: str = 'standard') -> str:
       """
       Get explanation for a specific methodology
       
       Args:
           methodology_name: Name of the methodology (e.g., 'data_cleaning', 'normalization')
           detail_level: Level of detail ('basic', 'standard', 'technical')
           
       Returns:
           str: Explanation text
       """
       try:
           if methodology_name not in self._methodology_kb:
               return f"Information about {methodology_name} methodology not found."
           
           methodology_info = self._methodology_kb[methodology_name]
           
           if detail_level == 'basic' and 'basic_explanation' in methodology_info:
               return methodology_info['basic_explanation'].strip()
           elif detail_level == 'technical' and 'technical_explanation' in methodology_info:
               return methodology_info['technical_explanation'].strip()
           else:
               # Default to standard explanation
               return methodology_info['standard_explanation'].strip()
       except Exception as e:
           logger.error(f"Error retrieving methodology explanation: {str(e)}")
           return f"Error retrieving explanation for {methodology_name}."

   def get_variable_explanation(self, variable_name: str, include_technical: bool = False) -> str:
       """
       Get explanation for a specific variable
       
       Args:
           variable_name: Name of the variable (e.g., 'rainfall', 'elevation')
           include_technical: Whether to include technical details
           
       Returns:
           str: Explanation text
       """
       try:
           # Standardize variable name for lookup
           variable_name_lower = variable_name.lower()
           
           # Try exact match first
           if variable_name_lower in self._variable_rationales:
               explanation = self._variable_rationales[variable_name_lower]['explanation']
               if include_technical and 'technical_details' in self._variable_rationales[variable_name_lower]:
                   explanation += "\n\n" + self._variable_rationales[variable_name_lower]['technical_details']
               return explanation.strip()
           
           # Try to find a close match
           for var, info in self._variable_rationales.items():
               aliases = info.get('aliases', [])
               
               # Check if the provided name matches any aliases
               if variable_name_lower in aliases or any(alias in variable_name_lower for alias in aliases):
                   explanation = info['explanation']
                   if include_technical and 'technical_details' in info:
                       explanation += "\n\n" + info['technical_details']
                   return explanation.strip()
           
           return f"No specific information found for variable '{variable_name}'."
       except Exception as e:
           logger.error(f"Error retrieving variable explanation: {str(e)}")
           return f"Error retrieving explanation for variable '{variable_name}'."

   def get_variable_category_explanation(self, category_name: str) -> str:
       """
       Get explanation for a category of variables
       
       Args:
           category_name: Name of the category (e.g., 'environmental', 'demographic')
           
       Returns:
           str: Explanation text
       """
       try:
           category_name_lower = category_name.lower()
           
           if category_name_lower in self._variable_categories:
               return self._variable_categories[category_name_lower].strip()
           
           return f"Information about {category_name} variable category not found."
       except Exception as e:
           logger.error(f"Error retrieving variable category explanation: {str(e)}")
           return f"Error retrieving explanation for {category_name} variable category."

   def get_explanation_template(self, template_type: str, detail_level: str = 'standard') -> str:
       """
       Get template for generating explanations
       
       Args:
           template_type: Type of template (e.g., 'variable_relationship', 'not_ideal_flag')
           detail_level: Level of detail ('basic', 'standard', 'technical')
           
       Returns:
           str: Template text
       """
       try:
           if template_type not in self._explanation_templates:
               return ""
           
           template_info = self._explanation_templates[template_type]
           
           if detail_level == 'basic' and 'basic' in template_info:
               return template_info['basic'].strip()
           elif detail_level == 'technical' and 'technical' in template_info:
               return template_info['technical'].strip()
           else:
               # Default to standard template
               return template_info['standard'].strip()
       except Exception as e:
           logger.error(f"Error retrieving explanation template: {str(e)}")
           return ""

   def get_technical_reference(self, reference_key: str) -> Dict:
       """
       Get technical reference information
       
       Args:
           reference_key: Key for the technical reference
           
       Returns:
           dict: Reference information
       """
       try:
           if reference_key not in self._technical_references:
               return {}
           
           return self._technical_references[reference_key]
       except Exception as e:
           logger.error(f"Error retrieving technical reference: {str(e)}")
           return {}

   def search_knowledge(self, query: str) -> List[Dict]:
       """
       Search the knowledge base for relevant information
       
       Args:
           query: Search query
           
       Returns:
           list: List of relevant knowledge items
       """
       query_lower = query.lower()
       results = []
       
       # Search methodologies
       for method_name, method_info in self._methodology_kb.items():
           if query_lower in method_name.lower() or query_lower in method_info['standard_explanation'].lower():
               results.append({
                   'type': 'methodology',
                   'name': method_name,
                   'content': method_info['standard_explanation'][:200] + '...',
                   'relevance': 'high' if query_lower in method_name.lower() else 'medium'
               })
       
       # Search variables
       for var_name, var_info in self._variable_rationales.items():
           if query_lower in var_name.lower() or query_lower in var_info['explanation'].lower():
               results.append({
                   'type': 'variable',
                   'name': var_name,
                   'content': var_info['explanation'][:200] + '...',
                   'relevance': 'high' if query_lower in var_name.lower() else 'medium'
               })
           
           # Check aliases too
           aliases = var_info.get('aliases', [])
           if any(query_lower in alias.lower() for alias in aliases):
               results.append({
                   'type': 'variable',
                   'name': var_name,
                   'content': var_info['explanation'][:200] + '...',
                   'relevance': 'medium'
               })
       
       # Sort by relevance
       results.sort(key=lambda x: 0 if x['relevance'] == 'high' else 1)
       
       return results

   def get_explanation_context(self, topic_type: str, topic_name: str, detail_level: str = 'standard') -> Dict:
       """
       Get comprehensive context for generating explanations
       
       Args:
           topic_type: Type of topic ('methodology', 'variable', 'category')
           topic_name: Name of the topic
           detail_level: Level of detail
           
       Returns:
           dict: Context information for explanation generation
       """
       context = {
           'topic_type': topic_type,
           'topic_name': topic_name,
           'detail_level': detail_level,
           'content': None,
           'references': []
       }
       
       # Get the primary content
       if topic_type == 'methodology':
           context['content'] = self.get_methodology_explanation(topic_name, detail_level)
           
           # Add related methodologies
           related = []
           if topic_name == 'data_cleaning':
               related = ['normalization', 'composite_scores']
           elif topic_name == 'normalization':
               related = ['data_cleaning', 'composite_scores']
           elif topic_name == 'composite_scores':
               related = ['normalization', 'vulnerability_ranking']
           elif topic_name == 'vulnerability_ranking':
               related = ['composite_scores', 'urban_extent']
               
           context['related_topics'] = related
           
       elif topic_type == 'variable':
           context['content'] = self.get_variable_explanation(topic_name, detail_level == 'technical')
           
           # Find category for this variable
           for category, info in VARIABLE_CATEGORIES_INFO.items():
               if topic_name.lower() in info['variables']:
                   context['category'] = category
                   context['category_description'] = self.get_variable_category_explanation(category)
                   break
           
       elif topic_type == 'category':
           context['content'] = self.get_variable_category_explanation(topic_name)
           
           # Add variables in this category
           if topic_name.lower() in VARIABLE_CATEGORIES_INFO:
               context['variables'] = VARIABLE_CATEGORIES_INFO[topic_name.lower()]['variables']
               context['category_importance'] = VARIABLE_CATEGORIES_INFO[topic_name.lower()]['importance']
       
       # Add technical references if requesting technical detail
       if detail_level == 'technical':
           if topic_type == 'methodology' and topic_name in self._technical_references:
               context['references'].append(self._technical_references[topic_name])
           elif topic_type == 'variable' and topic_name in self._technical_references:
               context['references'].append(self._technical_references[topic_name])
       
       return context

# Legacy function for backward compatibility
def get_knowledge(topic, subtopic=None):
   """
   Retrieve knowledge base content for a given topic and optional subtopic.
   
   Args:
       topic (str): Main topic ('methodology', 'variables', or a specific variable name)
       subtopic (str, optional): Subtopic for methodology or variables
       
   Returns:
       str: The knowledge base content or None if not found
   """
   kb = KnowledgeBase()
   
   try:
       # Check if this is a variable rationale request
       for var_key in VARIABLE_RATIONALES:
           if var_key.lower() == topic.lower():
               return kb.get_variable_explanation(var_key)
       
       # Check for methodology explanation
       if topic.lower() == 'methodology' and subtopic:
           subtopic_key = subtopic.lower().replace(' ', '_')
           return kb.get_methodology_explanation(subtopic_key)
       
       # Check for general variable category information
       if topic.lower() == 'variables' and subtopic:
           subtopic_key = subtopic.lower()
           return kb.get_variable_category_explanation(subtopic_key)
       
       # If we get here, the requested topic/subtopic wasn't found
       logger.warning(f"Knowledge base lookup failed for topic='{topic}', subtopic='{subtopic}'")
       return None
       
   except Exception as e:
       logger.error(f"Error retrieving knowledge: {str(e)}")
       return None

# =============================================================================
# Enhanced Knowledge Base Content
# =============================================================================

# Methodology explanations with multiple detail levels
METHODOLOGY_KB = {
   "data_cleaning": {
       "basic_explanation": """
           Data cleaning is the first step in our analysis where we fix missing or problematic data. 
           We use several methods like using values from nearby areas, averages, or the most common values.
           This ensures we have complete data for every area without gaps.
       """,
       
       "standard_explanation": """
           Missing values in the dataset are handled through several methods:
           
           1. Spatial Neighbor Mean: For geographic data, missing values are imputed using the average value of adjacent wards (neighbors). This preserves spatial patterns and is the preferred method when possible.
           
           2. Mean Imputation: If spatial methods aren't possible, missing numeric values are replaced with the column mean (average value).
           
           3. Mode Imputation: For categorical variables, missing values are replaced with the most frequent value (mode).
           
           4. Forward/Backward Fill: As a last resort for categorical data without a clear mode, values are propagated forward or backward.
           
           The tool automatically tries these methods in sequence, starting with spatial methods when possible, and falling back to simpler methods as needed.
       """,
       
       "technical_explanation": """
           Data cleaning in the MRPT employs a hierarchical imputation strategy with the following implementation details:
           
           1. Spatial Neighbor Mean Imputation:
              - For each ward with missing values, we identify spatially adjacent wards using Queen contiguity from libpysal
              - We compute the mean of available values from these neighbors
              - Formula: x_i = (1/N) * ∑(x_j) for all neighboring wards j where x_j is not missing
              - This preserves spatial autocorrelation in the data
           
           2. Mean Imputation (numeric variables):
              - Applied when spatial imputation is not possible
              - Simple statistical mean: x_i = (1/n) * ∑(x_j) for all non-missing values
              - Implementation is vectorized using Pandas operations
              - Pros: Simple, fast; Cons: Reduces variance, affects distributions
           
           3. Mode Imputation (categorical variables):
              - Uses the statistical mode (most frequent value)
              - For multimodal distributions, the first mode is selected
              - Implementation handles tied modes by default selection
           
           4. Forward/Backward Fill:
              - Sequential imputation that propagates the last known value forward
              - And/or the next known value backward
              - Used mainly for time-series or ordered data
              - Last resort for categorical data without clear modes
           
           The imputation method selection is tracked in the analysis metadata, providing provenance for each imputed value and enabling explanation of data quality and reliability implications.
       """
   },
   
   "normalization": {
       "basic_explanation": """
           Normalization puts all variables on the same scale (0-1) so we can compare and combine them fairly.
           Variables like rainfall and elevation have different units and ranges, but after normalization,
           they can be directly compared. We also account for whether higher values mean higher or lower
           malaria risk.
       """,
       
       "standard_explanation": """
           Normalization is a critical step that converts variables with different units and scales into a common 0-1 range. This allows fair comparison across variables.
           
           The process involves:
           
           1. Determining each variable's relationship with malaria risk (direct or inverse)
           
           2. For direct relationships (where higher values mean higher risk):
              - Formula: (value - min) / (max - min)
           
           3. For inverse relationships (where higher values mean lower risk):
              - Values are first inverted: 1 / (value + small constant)
              - Then normalized: (inverted - min_inverted) / (max_inverted - min_inverted)
           
           This ensures all variables are on a 0-1 scale where 1 always represents higher malaria risk.
       """,
       
       "technical_explanation": """
           The normalization procedure implements a domain-aware rescaling methodology that preserves the epidemiological relationship between each variable and malaria risk:
           
           1. Relationship Determination:
              - Each variable is classified as having either a direct or inverse relationship with malaria risk
              - This classification is based on epidemiological literature and can be manually adjusted
              - The relationship affects the normalization formula used
           
           2. Direct Relationship Normalization:
              - Standard min-max normalization: x_normalized = (x - min(x)) / (max(x) - min(x))
              - Maps all values to [0,1] where 1 corresponds to highest risk
              - Preserves relative distances between values
              - Handles edge cases: When min(x) = max(x), default to 0.5
           
           3. Inverse Relationship Normalization:
              - Two-step process to handle the inverse relationship:
                a. Inversion: x_inverted = 1 / (x + ε) where ε is a small constant (1e-10) to avoid division by zero
                b. Min-max normalization of inverted values: 
                   x_normalized = (x_inverted - min(x_inverted)) / (max(x_inverted) - min(x_inverted))
              - This effectively inverts the scale so higher original values map to lower normalized values
              - Special handling for edge cases and zero values
           
           4. Implementation Details:
              - The normalization is vectorized using NumPy operations for performance
              - Values are clipped to [0,1] to handle any numerical imprecision
              - The normalization parameters (min, max) are stored in the analysis metadata
              - Parallelization is used for large datasets with many variables
           
           The normalized values maintain their epidemiological interpretation while enabling mathematical comparison and combination across heterogeneous variables.
       """
   },
   
   "composite_scores": {
       "basic_explanation": """
           Composite scores combine multiple normalized variables to create a single risk score.
           Instead of relying on just one model, we create many models using different combinations of variables.
           This gives us a more robust assessment by showing how risk patterns change with different variables.
       """,
       
       "standard_explanation": """
           Composite risk scores combine multiple normalized variables to create an overall malaria risk assessment.
           
           The process involves:
           
           1. Variable Selection: Either default variables (selected by an LLM based on epidemiological literature) or custom variables specified by the user.
           
           2. Model Generation: All possible combinations of the selected variables are created (e.g., with 5 variables, we generate models using pairs, triplets, etc.).
           
           3. Score Calculation: For each model, the normalized values of its variables are averaged to create a composite score between 0-1.
           
           4. Visualization: All models are plotted as separate maps, allowing comparison of how different variable combinations affect risk assessment.
           
           This approach provides robustness by not relying on a single model, and transparency by showing how different variables contribute to risk.
       """,
       
       "technical_explanation": """
           The composite score calculation implements a comprehensive multi-model approach:
           
           1. Combinatorial Model Generation:
              - For n selected variables, we generate Σ(n choose k) for k=2 to n models
              - Each model represents a unique combination of variables
              - For example, with 5 variables, we generate 26 different models (10 pairs, 10 triplets, 5 quadruplets, 1 quintuplet)
              - Implementation uses itertools.combinations with parallel execution for performance
           
           2. Score Calculation Algorithm:
              - Each model uses an unweighted arithmetic mean: Score = (1/n) * Σ(x_i) for all normalized variables in the model
              - This simple aggregation has advantages:
                a. Interpretability: Easy to understand contribution of each variable
                b. Robustness: Less sensitive to outliers than multiplicative models
                c. Transparency: No hidden weights or complex formulas
              - Advanced options (not enabled by default):
                a. Weighted means with variable importance weights
                b. Geometric means for multiplicative effects
                c. PCA-based dimensionality reduction
           
           3. Model Evaluation:
              - No single "best" model is selected - the ensemble of all models is retained
              - This approach acknowledges uncertainty in variable selection
              - Enables sensitivity analysis of risk patterns across models
              - The variation across models becomes an indicator of uncertainty
           
           4. Implementation Details:
              - Vectorized operations for each model calculation
              - Parallel processing across models
              - Results are stored in a structured format that preserves model composition
              - Each model is tagged with a unique identifier and its constituent variables
           
           The multi-model approach represents a principled way to handle uncertainty in risk factor selection while providing methodological transparency.
       """
   },
   
   "vulnerability_ranking": {
       "basic_explanation": """
           Vulnerability ranking puts wards in order from most to least vulnerable to malaria. 
           Each ward gets a rank (1st, 2nd, 3rd, etc.) based on its median score across all models.
           We also group wards into High, Medium, and Low vulnerability categories to make it easier to
           prioritize interventions.
       """,
       
       "standard_explanation": """
           Vulnerability ranking orders wards based on their composite risk scores to identify priority areas.
           
           The methodology:
           
           1. For each ward, calculate the median score across all composite models to get a single representative value.
           
           2. Sort wards by this median score (higher scores = higher vulnerability).
           
           3. Assign ranks (1, 2, 3, etc.) where rank 1 is the most vulnerable.
           
           4. Group wards into vulnerability categories (High, Medium, Low) based on rank terciles.
           
           5. Visualize as a box plot showing the distribution of scores across models for each ward.
           
           The box plot is particularly valuable as it shows not just the median score, but the consistency/uncertainty of the ranking across different variable combinations.
       """,
       
       "technical_explanation": """
           The vulnerability ranking methodology implements a robust multi-step ranking procedure:
           
           1. Cross-Model Score Aggregation:
              - For each ward, we calculate the median score across all models
              - Median is chosen over mean for robustness to outlier models
              - This reduces the multi-dimensional model space to a single representative value
              - Mathematically: median_score(ward) = median(score_model1, score_model2, ..., score_modelN)
           
           2. Rank Calculation:
              - Wards are sorted by median score in descending order (higher score = higher vulnerability)
              - Ranks are assigned as 1, 2, 3, etc. with 1 being the most vulnerable
              - Ties are resolved by examining the 75th percentile of scores across models
              - Implementation uses pandas ranking functions with tie-breaking
           
           3. Categorical Classification:
              - Wards are divided into vulnerability categories using rank terciles:
                * High: ranks 1 to n/3
                * Medium: ranks n/3+1 to 2n/3
                * Low: ranks 2n/3+1 to n (where n is the total number of wards)
              - Equal-sized categories ensure balanced distribution
              - Alternative methods include natural breaks (Jenks) or quantile-based classification
           
           4. Uncertainty Quantification:
              - The interquartile range (IQR) of scores across models serves as an uncertainty measure
              - Higher IQR indicates greater sensitivity to variable selection
              - This uncertainty is visualized in box plots and stored in metadata
              - Wards with high rank but high uncertainty are flagged for closer examination
           
           5. "Not Ideal" Classification:
              - A special designation for wards that are:
                a. Ranked in the top vulnerability tier (typically top 10)
                b. Classified as urban
              - This indicates potential implementation challenges for urban-focused interventions
              - These are visually highlighted in maps with blue outlines
           
           The rigorous statistical approach ensures reliable prioritization while acknowledging and quantifying uncertainty in the ranking process.
       """
   },
   
   "urban_extent": {
       "basic_explanation": """
           Urban extent analysis identifies which areas are urban versus rural based on a percentage threshold.
           This is important because different malaria intervention strategies work better in urban versus rural settings.
           We can adjust the threshold (like 10% or 50% urban) to see how it affects our intervention planning.
       """,
       
       "standard_explanation": """
           Urban extent analysis identifies areas that exceed a specified urbanicity threshold, which is crucial for intervention planning.
           
           The process:
           
           1. Urban percentage values are extracted from the shapefile data for each ward.
           
           2. Wards are classified as "above threshold" or "below threshold" based on a user-specified percentage.
           
           3. This classification helps determine appropriate intervention strategies - for example, conventional bed nets might be prioritized for less urban areas.
           
           4. The analysis can be repeated at different thresholds (10%, 50%, 75%) to understand sensitivity to the urban/rural classification.
           
           The urban extent map visually shows which wards exceed the specified urbanicity threshold, helping planners allocate resources appropriately.
       """,
       
       "technical_explanation": """
           The urban extent analysis implements a threshold-based classification methodology:
           
           1. Data Source Identification:
              - The algorithm first searches for urban percentage data in the dataset
              - It looks for columns matching predefined patterns (e.g., 'UrbanPercent', 'UrbanPercentage')
              - If a direct percentage is not available, it looks for binary urban/rural classification
              - The system requires valid urban data and will error rather than generate random values
           
           2. Threshold-Based Classification:
              - A Boolean classification is applied using the formula: is_urban = (urban_percentage ≥ threshold)
              - The threshold is configurable to enable sensitivity analysis
              - Implementation is vectorized using NumPy/Pandas for performance
           
           3. Threshold Sensitivity Analysis:
              - Multiple threshold values can be tested (10%, 50%, 75%, 100%)
              - For each threshold, the system calculates:
                a. Count of wards above/below threshold
                b. Percentage of population in urban vs. non-urban areas
                c. Geographic distribution of urban/non-urban areas
              - This enables assessment of classification robustness
           
           4. Integration with Vulnerability Analysis:
              - Urban classification is cross-tabulated with vulnerability rankings
              - Special attention is given to wards that are:
                a. High vulnerability but non-urban (flagged as "not ideal")
                b. Low vulnerability but highly urban (potentially deprioritized)
              - This integration informes intervention strategy selection
           
           5. Visualization:
              - Urban areas are shown with vulnerability coloring
              - Non-urban areas are displayed in gray
              - Areas flagged as "not ideal" receive blue outlines
              - Interactive hover data shows exact urban percentage values
           
           The methodology acknowledges that urban/rural is a continuum rather than a binary state, while providing a necessary simplification for operational planning.
       """
   }
}

# Enhanced variable rationales with technical details and aliases
VARIABLE_RATIONALES = {
   "rainfall": {
       "explanation": """
           Rainfall is a critical environmental variable for malaria risk because it creates standing water bodies that serve as breeding sites for Anopheles mosquitoes. Seasonal patterns of rainfall strongly influence the timing of malaria transmission, with peak transmission typically occurring during or shortly after rainy seasons.
           
           Moderate rainfall generally has a direct relationship with malaria risk (more rain = higher risk). However, the relationship is non-linear, as extremely heavy rainfall can temporarily flush out breeding sites, reducing mosquito populations.
           
           In the MRPT, rainfall is typically modeled with a direct relationship to malaria risk, capturing the general pattern that areas with more rainfall tend to have higher transmission potential.
       """,
       "technical_details": """
           Rainfall affects malaria transmission through several mechanisms:
           
           1. Vector breeding habitat creation: Anopheles mosquitoes require standing water for larval development
           2. Environmental humidity: Higher humidity increases adult mosquito survival
           3. Vegetation effects: Rainfall supports vegetation that provides resting sites for adult mosquitoes
           
           The lag time between rainfall and increased transmission is typically 2-8 weeks, reflecting the time required for:
           - Mosquito breeding and development (7-14 days)
           - Pathogen incubation in the mosquito (10-12 days for P. falciparum)
           - Human infection and symptom development (7-30 days)
           
           Studies indicate optimal rainfall conditions for malaria transmission vary by region but generally fall between 80-300mm monthly cumulative rainfall. Values below 80mm are often insufficient for sustained breeding, while values above 300mm may disrupt breeding sites.
       """,
       "aliases": ["rain", "precipitation", "precip", "mean_rainfall"]
   },
   
   "temperature": {
       "explanation": """
           Temperature is a fundamental variable for malaria risk because it controls both mosquito development and the rate at which the malaria parasite develops within the mosquito (sporogonic cycle). 
           
           Optimal transmission occurs between 25-30°C, with reduced transmission below 18°C (too cold for parasite development) or above 32°C (reduces mosquito survival). This creates a direct relationship with risk in cooler regions, but potentially an inverse relationship in very hot areas.
           
           In the MRPT, temperature is typically modeled as having a direct relationship within the endemic temperature range, reflecting that warmer conditions generally accelerate transmission cycles where malaria is established.
       """,
       "technical_details": """
           Temperature affects malaria transmission through several thermal-dependent processes:
           
           1. Parasite development rate: The sporogonic cycle of P. falciparum requires approximately:
              - 9-10 days at 30°C
              - 14-16 days at 25°C
              - 29-30+ days at 18°C
              - Development ceases below ~16°C
           
           2. Mosquito development:
              - Egg-to-adult development time decreases from ~20-30 days at 20°C to ~7-10 days at 30°C
              - Larval mortality increases at temperatures >32°C
           
           3. Mosquito longevity:
              - Optimal survival at 25-27°C
              - Significantly reduced lifespan above 32°C
              - Reduced activity below 20°C
           
           4. Biting rate:
              - Increases with temperature up to ~30°C
              - Decreases at higher temperatures due to dehydration risk
           
           This creates a unimodal relationship best modeled with a quadratic function rather than a simple linear relationship. The optimal temperature for transmission (R0 maximization) typically occurs around 25-29°C depending on local vector species and other environmental factors.
       """,
       "aliases": ["temp", "climate", "mean_temperature", "temp_mean"]
   },
   
   "elevation": {
       "explanation": """
           Elevation (altitude) is a powerful predictor of malaria risk because higher elevations have colder temperatures, which reduce both parasite and mosquito development rates. 
           
           There's often a clear elevation threshold above which malaria transmission becomes rare or absent. This threshold varies by region but is typically between 1,500-2,000 meters above sea level in tropical areas.
           
           Elevation has an inverse relationship with malaria risk (higher elevation = lower risk) and is often one of the strongest geographical predictors of transmission potential.
       """,
       "technical_details": """
           Elevation impacts malaria transmission through multiple mechanisms:
           
           1. Temperature gradient: 
              - Temperature decreases ~6.5°C per 1,000m elevation gain
              - Creates temperature conditions unfavorable for transmission above certain altitudes
           
           2. Atmospheric pressure:
              - Lower atmospheric pressure at high altitudes may impact mosquito flight performance
              - Reduced oxygen levels affect mosquito metabolism
           
           3. Precipitation patterns:
              - Orographic rainfall creates different precipitation regimes at varying elevations
              - Often creates a mid-elevation peak in precipitation favorable for malaria
           
           4. Land use and population patterns:
              - Human settlements and agricultural practices vary with elevation
              - Lower population density at higher elevations reduces host availability
           
           Research indicates that the malaria transmission ceiling has risen in some regions due to climate change, with studies in East Africa showing increases of 100-300m in maximum transmission elevation over recent decades.
           
           Elevation is particularly valuable as a proxy variable because it:
           - Is stable over time (unlike weather variables)
           - Can be measured precisely with remote sensing
           - Incorporates multiple environmental effects in a single variable
       """,
       "aliases": ["elev", "altitude", "height", "dem"]
   },
   
   "ndvi": {
       "explanation": """
           Normalized Difference Vegetation Index (NDVI) measures vegetation greenness and density. It's important for malaria risk assessment because vegetation provides resting places for adult mosquitoes and indicates areas with sufficient moisture to support both plant growth and mosquito breeding.
           
           Areas with higher NDVI values typically have environmental conditions that are more favorable for mosquito survival - adequate moisture, shade, and protection from predators and extreme weather conditions.
           
           NDVI generally has a direct relationship with malaria risk in endemic settings (higher vegetation = higher risk), though this relationship can vary in specific ecosystems.
       """,
       "technical_details": """
           NDVI is calculated from satellite imagery using the formula:
           
           NDVI = (NIR - Red) / (NIR + Red)
           
           Where:
           - NIR is the near-infrared reflectance
           - Red is the visible red reflectance
           
           This produces values between -1 and +1, with:
           - Negative values indicating water
           - Values near zero indicating barren land
           - Values 0.1-0.3 indicating shrubs and grassland
           - Values 0.3-0.8 indicating dense vegetation
           
           In malaria studies, NDVI contributes to risk assessment through:
           
           1. Direct mosquito habitat effects:
              - Adult resting places
              - Protection from desiccation
              - Microclimate moderation (temperature, humidity)
           
           2. Indirect environmental indications:
              - Proxy for soil moisture and standing water
              - Indicator of seasonal patterns in precipitation
              - Correlated with agricultural practices that may create vector habitats
           
           NDVI time-series provides valuable information on seasonal changes in environmental suitability for malaria transmission, with studies showing strong correlations between NDVI anomalies and malaria epidemics 1-3 months later in some regions.
       """,
       "aliases": ["vegetation", "greenness", "mean_ndvi"]
   },
   
   "evi": {
       "explanation": """
           Enhanced Vegetation Index (EVI) is an improved vegetation index that addresses some limitations of NDVI, particularly in areas with dense vegetation or atmospheric interference. It provides a more accurate measure of vegetation canopy variations and overall vegetation health.
           
           Like NDVI, EVI indicates areas with environmental conditions suitable for mosquito survival and breeding. It's especially valuable in regions with dense vegetation where NDVI may saturate and lose sensitivity.
           
           EVI generally has a direct relationship with malaria risk (higher values = higher risk) and may provide better discrimination in heavily vegetated tropical areas where malaria is endemic.
       """,
       "technical_details": """
           EVI is calculated using the formula:
           
           EVI = G × ((NIR - Red) / (NIR + C1 × Red - C2 × Blue + L))
           
           Where:
           - NIR is near-infrared reflectance
           - Red is visible red reflectance
           - Blue is visible blue reflectance
           - L is a soil adjustment factor
           - C1 and C2 are coefficients for atmospheric correction
           - G is a gain factor
           
           Typical values for these parameters are:
           - L = 1
           - C1 = 6
           - C2 = 7.5
           - G = 2.5
           
           EVI addresses several limitations of NDVI:
           
           1. Improved sensitivity in high biomass regions:
              - NDVI saturates at high vegetation levels
              - EVI maintains sensitivity in dense canopies
           
           2. Reduced atmospheric influences:
              - Corrects for aerosol scattering
              - Less sensitive to smoke and thin clouds
           
           3. Soil background correction:
              - Minimizes soil brightness influences
              - Better performance in areas with sparse vegetation
           
           In malaria studies, EVI has shown superior performance to NDVI in:
           - Tropical forest regions with dense vegetation
           - Areas with seasonal burning or high aerosol loads
           - Distinguishing vegetation types relevant to specific vector species
       """,
       "aliases": ["enhanced", "mean_evi"]
   },
   
   "distance_to_water": {
       "explanation": """
           Distance to water bodies is a crucial spatial variable because permanent or semi-permanent water bodies provide reliable mosquito breeding habitats. Proximity to rivers, lakes, marshes, or irrigation systems increases exposure to malaria vectors.
           
           The effect typically diminishes with distance from the water source, creating a clear spatial pattern of malaria risk that follows hydrological features in the landscape.
           
           Distance to water has an inverse relationship with malaria risk (greater distance = lower risk), though the strength of this relationship can vary seasonally and with water body type.
       """,
       "technical_details": """
           Distance to water impacts malaria risk through:
           
           1. Vector breeding site proximity:
              - Anopheles mosquitoes have limited flight ranges (typically 1-3 km)
              - Breeding site distance creates a dispersal gradient of risk
              - Different vector species have different water preferences:
                * An. gambiae s.s.: Small, sunlit, temporary pools
                * An. funestus: Larger, permanent, vegetated water bodies
                * An. arabiensis: More adaptable to various water sources
           
           2. Spatial risk gradients:
              - Mathematical relationship often follows a distance decay function:
                * Risk ∝ 1/(Distance)^n where n typically ranges from 1-2
              - Effective risk distance varies by:
                * Vector species flight range
                * Wind patterns
                * Terrain features
           
           3. Water body characteristics:
              - Not all water bodies contribute equally to risk:
                * Flowing vs. stagnant
                * Seasonal vs. permanent
                * Natural vs. man-made
              - Water body size affects mosquito productivity
              - Vegetation type around water affects vector species composition
           
           4. Measurement approaches:
              - Euclidean distance (straight-line)
              - Cost-distance (accounting for terrain)
              - Network distance (following waterways)
              - Buffered areas at different distances
           
           Studies show that malaria incidence typically decreases logarithmically with distance from major breeding sites, with distances of >3km associated with significantly reduced risk in most settings.
       """,
       "aliases": ["distance", "dist", "proximity", "water_dist"]
   },
   
   "housing_quality": {
       "explanation": """
           Housing quality affects malaria risk by creating physical barriers to mosquito entry. Better housing features like screened windows, closed eaves, improved roofing, and finished walls significantly reduce mosquito access to sleeping areas.
           
           Housing quality also correlates with socioeconomic factors that influence preventive behaviors, treatment access, and overall vulnerability to infection.
           
           Housing quality has an inverse relationship with malaria risk (better housing = lower risk) and represents an important modifiable risk factor that can be targeted through intervention programs.
       """,
       "technical_details": """
           Housing factors impact malaria transmission through:
           
           1. Physical mosquito barriers:
              - Screens: Prevent entry through windows and doors
              - Eave closures: Block primary entry route for Anopheles
              - Metal/tiled roofing: Eliminates gaps in thatch
              - Finished walls: Reduce entry points and resting places
           
           2. Indoor environmental conditions:
              - Temperature moderation: Less temperature variation
              - Humidity control: Reduces mosquito survival
              - Airflow patterns: Affects CO2 concentration and attractiveness
           
           3. Measurement approaches:
              - Composite indices incorporating multiple features
              - Binary classification (improved vs. unimproved)
              - Remote sensing proxies:
                * Roof material detection
                * Settlement pattern analysis
                * Nighttime light intensity
           
           4. Intervention implications:
              - Housing modification cost-effectiveness:
                * One-time cost vs. continuous intervention
                * Multi-disease benefits (malaria, dengue, respiratory)
              - Synergy with other interventions:
                * IRS more effective in better housing
                * ITNs and housing improvements complementary
           
           Systematic reviews indicate that improved housing can reduce malaria incidence by 35-65% across different settings, though effectiveness varies by local vector behavior and baseline transmission intensity.
       """,
       "aliases": ["house", "dwelling", "home", "housing", "building"]
   },
   
   "population": {
       "explanation": """
           Population density influences malaria transmission in complex ways. In high-density urban areas, the "dilution effect" may reduce per-person biting rates, and urban environments often have reduced vector habitats due to less vegetation and standing water.
           
           However, high population areas can also create breeding sites through poor drainage and water storage practices, particularly in unplanned urban settlements.
           
           The relationship is context-dependent but often modeled as inverse in settings where higher density correlates with urbanization and better infrastructure (higher density = lower risk per person).
       """,
       "technical_details": """
           Population density affects malaria transmission through several mechanisms:
           
           1. Host availability effects:
              - Dilution effect: More potential hosts can reduce bites per person
              - At very low densities, lack of hosts can limit transmission
           
           2. Urban environment factors:
              - Reduced vector habitat in built environments
              - Higher pollution levels in urban areas reduce larval survival
              - Better access to prevention and treatment in urban settings
           
           3. Human-vector contact patterns:
              - Higher densities often correlated with changed human behavior:
                * Indoor activities during peak biting times
                * Housing improvements reducing exposure
                * Different occupational risks
           
           4. Measurement approaches:
              - Population density (people/km²)
              - Night-time light intensity as proxy
              - Settlement pattern classification
              - Urban classification percentage
           
           5. Non-linear relationships:
              - Population-parasitemia relationship often follows a U-shaped curve:
                * Very low density: Limited transmission
                * Medium density rural: Highest risk
                * High density urban formal: Reduced risk
                * Very high density urban informal: Potentially increased risk
           
           Mathematical models suggest optimal population density for malaria transmission exists, with the effect dependent on:
           - Local vector species behavior
           - Housing and infrastructure quality
           - Health system access
           - Local hydrology and land use
       """,
       "aliases": ["pop", "people", "inhabitants", "population_density"]
   },
   
   "soil_wetness": {
       "explanation": """
           Soil moisture or wetness is relevant for malaria risk because it indicates areas prone to water pooling after rainfall. Persistent soil moisture can support small breeding sites even without visible standing water.
           
           Soil wetness also influences local humidity affecting mosquito survival and may impact the persistence of breeding sites after rainfall events.
           
           Soil wetness typically has a direct relationship with malaria risk (wetter soil = higher risk), serving as an environmental indicator of potential breeding site formation.
       """,
       "technical_details": """
           Soil moisture affects malaria transmission through:
           
           1. Breeding site formation:
              - Determines water retention after rainfall
              - Affects duration of temporary breeding sites
              - Influences groundwater levels and seepage
           
           2. Measurement approaches:
              - Remote sensing indices:
                * SMAP (Soil Moisture Active Passive)
                * AMSR-E soil moisture products
                * SMOS (Soil Moisture and Ocean Salinity)
              - Topographic Wetness Index (TWI):
                * Based on terrain and water accumulation potential
                * Formula: TWI = ln(α/tanβ) where:
                  - α is the upstream contributing area
                  - β is the local slope angle
           
           3. Temporal dynamics:
              - Seasonal patterns of soil moisture
              - Lag times between precipitation and soil moisture
              - Persistence of wetness (memory effect)
           
           4. Spatial patterns:
              - Toposequence effects (catenas)
              - Soil type influence on water retention
              - Land cover effects on infiltration and evaporation
           
           Studies indicate that soil moisture in the top 10 cm of soil correlates most strongly with malaria incidence, with a typical lag time of 2-6 weeks between soil moisture peaks and increased case rates in seasonal transmission settings.
       """,
       "aliases": ["soil", "wetness", "moisture", "mean_soil_wetness"]
   },
   
   "urbanpercent": {
       "explanation": """
           Urban percentage or urbanicity quantifies the degree to which an area is urbanized rather than rural. Urban environments typically have fewer suitable vector habitats due to reduced vegetation, more built-up land, and often better drainage and infrastructure.
           
           The urban-rural gradient is important for determining appropriate intervention strategies, as different approaches may be more effective in different settings.
           
           Urban percentage usually has an inverse relationship with malaria risk (more urban = lower risk), though urban malaria remains significant in many endemic settings, particularly in informal settlements.
       """,
       "technical_details": """
           Urban percentage is conceptualized and measured through:
           
           1. Definition approaches:
              - Administrative (official urban boundaries)
              - Population density based (e.g., >1000 persons/km²)
              - Infrastructure based (built environment characteristics)
              - Functional (economic activity patterns)
           
           2. Measurement methods:
              - Census-based urban classification
              - Remote sensing derived:
                * Land cover classification
                * Built-up area indices
                * Night-time lights
              - Combined multi-criteria assessments
           
           3. Urban malaria characteristics:
              - Typically lower EIR (Entomological Inoculation Rate) than rural
              - Different vector species adaptation:
                * An. stephensi: Urban specialist in South Asia, invading Africa
                * An. gambiae: Adapting to urban habitats
              - Urban breeding sites:
                * Man-made containers
                * Construction sites
                * Urban agriculture
                * Drainage channels
           
           4. Operational significance:
              - "Not ideal" classification highlights mismatches between:
                * Administrative urban classification
                * Actual urban characteristics
                * Vulnerability ranking
           
           Meta-analyses indicate that malaria prevalence typically decreases by 30-80% along rural-to-urban gradients, but with significant heterogeneity based on:
           - Settlement history and planning
           - Socioeconomic stratification
           - Local ecology and vector species
           - Infrastructure quality
       """,
       "aliases": ["urban", "built", "city", "urbanpercentage", "urban_percentage"]
   },
   
   "pfpr": {
       "explanation": """
           Plasmodium falciparum Parasite Rate (PfPR) is a direct measure of malaria burden, representing the percentage of the population carrying malaria parasites. It's often considered the gold standard for measuring transmission intensity in population-based surveys.
           
           PfPR captures the reservoir of infection in a community and reflects the combined effects of all environmental, biological, and social factors affecting transmission in an area.
           
           PfPR has a direct relationship with malaria risk (higher parasite rate = higher transmission risk) and is both an outcome indicator and a predictor of future transmission potential.
       """,
       "technical_details": """
           PfPR as a malaria metric:
           
           1. Definition and measurement:
              - Proportion of individuals with detectable P. falciparum parasites
              - Typically reported by age group (e.g., PfPR₂₋₁₀ for ages 2-10 years)
              - Detection methods:
                * Microscopy: Detection threshold ~100 parasites/μL
                * RDT: Variable detection thresholds ~100-500 parasites/μL
                * PCR: Higher sensitivity, ~0.1-10 parasites/μL
           
           2. Relationship to transmission intensity:
              - Mathematical relationship with EIR (Entomological Inoculation Rate):
                * PfPR = (EIR/a) / (EIR/a + r) where:
                  - a is the bite rate producing infection
                  - r is the parasite clearance rate
              - Relationship with Re (effective reproduction number):
                * At equilibrium: PfPR = 1 - (1/Re)
           
           3. Geographic and temporal dynamics:
              - Spatial heterogeneity at multiple scales
              - Seasonal patterns, often lagging environmental drivers
              - Age-stratification patterns varying by transmission intensity
           
           4. Use in modeling and intervention planning:
              - Input for mathematical models of transmission
              - Stratification tool for tailoring interventions
              - Predictor of severe disease burden
              - Benchmarking metric for elimination progress
           
           PfPR is often mapped using model-based geostatistics and has been used to create continent-wide risk maps for Africa by the Malaria Atlas Project, allowing comparisons across diverse ecological settings.
       """,
       "aliases": ["parasite_rate", "parasite", "plasmodium", "falciparum"]
   },
   
   "tpr": {
       "explanation": """
           Test Positivity Rate (TPR) measures the proportion of positive diagnostic tests among all tests conducted. It reflects the prevalence of malaria among symptomatic individuals seeking care and is routinely collected at health facilities.
           
           TPR is more readily available than community parasite prevalence (PfPR) since it comes from routine clinical data rather than specialized surveys.
           
           TPR has a direct relationship with malaria risk but may be biased by testing practices, healthcare-seeking behavior, and the proportion of fevers due to causes other than malaria.
       """,
       "technical_details": """
           TPR as a malaria metric:
           
           1. Definition and calculation:
              - Number of positive tests ÷ Total number of tests performed
              - Usually calculated monthly or weekly at facility level
              - Can be stratified by:
                * Age groups
                * Test type (microscopy vs. RDT)
                * Facility type
           
           2. Relationship to true prevalence:
              - Affected by:
                * Healthcare access (distance, cost, perception)
                * Testing practices (who gets tested)
                * Non-malarial fever burden
                * Test sensitivity and specificity
              - Mathematical relationship:
                * Under certain assumptions, TPR ≈ Prevalence × Sensitivity
                * Relationship breaks down at very low or high prevalence
           
           3. Operational uses:
              - Threshold indicators (e.g., >5% indicates need for universal coverage)
              - Trend monitoring (seasonal, year-to-year)
              - Early warning indicator (sudden increases)
              - Program impact assessment
           
           4. Limitations and biases:
              - Selection bias (tested population ≠ general population)
              - Denominator issues (testing rates vary)
              - Referral patterns affect patient mix
              - Varies with clinical algorithms and test availability
           
           Studies suggest TPR correlates moderately well with community parasite prevalence (r≈0.5-0.7) but the relationship is non-linear and context-dependent. TPR becomes more valuable when analyzed as trends over time within the same setting rather than for cross-site comparisons.
       """,
       "aliases": ["test_positivity_rate", "positivity", "test_rate"]
   },
   
   "flood": {
       "explanation": """
           Flood risk captures areas prone to inundation, which can create extensive mosquito breeding habitats during and after flooding events. Floodwaters initially wash away breeding sites, but as they recede, they leave numerous water pools ideal for mosquito breeding.
           
           Areas prone to flooding often experience malaria outbreaks 4-8 weeks after major flood events, creating a lag between environmental change and disease impact.
           
           Flood risk generally has a direct relationship with malaria risk (higher flood risk = higher malaria risk), particularly in seasonal transmission settings.
       """,
       "technical_details": """
           Flood impacts on malaria transmission:
           
           1. Vector ecology effects:
              - Initial negative impact (breeding site destruction)
              - Subsequent positive impact as waters recede:
                * Numerous small pools form
                * Increased humidity benefits adult mosquitoes
                * Vegetation growth after flooding provides resting sites
           
           2. Measurement approaches:
              - Topographic flood models:
                * HAND (Height Above Nearest Drainage)
                * Hydrological DEM-based models
              - Historical flood frequency:
                * Remote sensing time series
                * Precipitation anomalies
              - Proxy indicators:
                * Distance to floodplains
                * Topographic depression indices
           
           3. Human vulnerability factors:
              - Displacement to non-immune areas
              - Disruption of health services
              - Loss of protective measures (nets, housing)
              - Changes in human behavior and exposure
           
           4. Temporal dynamics:
              - Lag periods between flooding and malaria peaks:
                * 4-8 weeks typical in most settings
                * Varies by:
                  - Vector species composition
                  - Temperature (affects development rates)
                  - Pre-existing immunity
           
           Studies of major flooding events in endemic areas show average increases in malaria incidence of 25-50% above expected seasonal trends, with larger increases in areas with normally low transmission and lower increases in high-endemic settings with significant population immunity.
       """,
       "aliases": ["inundation", "water_extent", "flooding"]
   }
}

# Variable category explanations
VARIABLE_CATEGORIES = {
   "environmental": """
       Environmental variables capture aspects of the natural environment that influence mosquito breeding and survival:
       
       - Rainfall/Precipitation: Affects standing water availability for mosquito breeding
       - Temperature: Influences mosquito development rate and parasite development
       - Elevation: Higher elevations typically have lower malaria risk
       - NDVI/EVI (vegetation indices): Indicate vegetation density, affecting mosquito habitats
       - Soil moisture/wetness: Reflects potential for water pooling
       - Distance to water bodies: Proximity to breeding sites increases risk
       
       These variables help identify areas with environmental conditions conducive to malaria transmission. They form the foundation of most spatial risk assessments because they strongly influence vector ecology.
       
       Environmental variables are particularly valuable because many can be measured remotely through satellite imagery, allowing consistent assessment across large geographic areas, including places with limited ground data.
   """,
   
   "demographic": """
       Demographic variables capture human factors that influence malaria risk:
       
       - Population density: Affects human-mosquito contact rates
       - Housing quality: Better housing reduces mosquito entry
       - Urban/rural classification: Urban areas often have lower transmission
       - Access to healthcare: Influences treatment seeking behavior
       - Socioeconomic status: Related to preventive measures and housing
       - Education levels: Affects knowledge and preventive behaviors
       - Occupation patterns: Different jobs have different exposure risks
       
       These variables help identify vulnerable populations and social risk factors. They capture aspects of human vulnerability and exposure that purely environmental assessments might miss.
       
       Demographic factors are especially important for operational planning as they directly relate to intervention targeting, access challenges, and equity considerations in malaria control programs.
   """,
   
   "epidemiological": """
       Epidemiological variables directly measure malaria burden and transmission:
       
       - Parasite rate (PfPR): Percentage of population infected with malaria parasites
       - Test positivity rate (TPR): Percentage of diagnostic tests that are positive
       - Case incidence rates: Number of confirmed cases per population
       - Malaria mortality: Deaths attributed to malaria
       - Historical outbreak patterns: Past spatial and temporal distributions
       - Vector abundance: Measured densities of Anopheles mosquitoes
       - Insecticide resistance: Effectiveness of vector control interventions
       
       These variables provide the most direct measures of malaria burden but may be affected by reporting biases, healthcare access variations, and diagnostic inconsistencies.
       
       Epidemiological variables are crucial for validating risk models based on environmental and demographic factors, and for directly measuring the impact of interventions over time. They represent the actual disease outcomes that preventive programs aim to reduce.
   """,
   
   "intervention": """
       Intervention variables measure malaria control efforts and coverage:
       
       - Insecticide-treated net (ITN) coverage: Percentage of population with access
       - Indoor residual spraying (IRS) coverage: Percentage of houses sprayed
       - Treatment accessibility: Distance to health facilities offering diagnosis and treatment
       - Antimalarial drug availability: Stock levels at health facilities
       - Preventive therapy coverage: Uptake of preventive treatments among target groups
       - Larviciding coverage: Areas treated with larval source management
       - Health system capacity: Staffing, resources, and capabilities for malaria control
       
       These variables help assess the reach and effectiveness of control programs, identifying gaps in coverage and potential explanations for spatial variations in malaria burden.
       
       Intervention variables are crucial for program managers to understand the relationship between control efforts and outcomes, and to optimize resource allocation for maximum impact.
   """
}

# Additional information about variable categories
VARIABLE_CATEGORIES_INFO = {
   "environmental": {
       "importance": "Primary determinants of vector ecology and transmission potential",
       "data_sources": ["Satellite imagery", "Climate models", "Weather stations", "Digital elevation models"],
       "variables": ["rainfall", "temperature", "elevation", "ndvi", "evi", "soil_wetness", "distance_to_water", "flood"]
   },
   "demographic": {
       "importance": "Key factors in human vulnerability and exposure patterns",
       "data_sources": ["Census data", "Household surveys", "Socioeconomic indicators", "Settlement mapping"],
       "variables": ["population", "housing_quality", "urbanpercent", "education", "poverty", "occupation"]
   },
   "epidemiological": {
       "importance": "Direct measures of disease burden and transmission intensity",
       "data_sources": ["Health facility data", "Community surveys", "Malaria information systems", "Research studies"],
       "variables": ["pfpr", "tpr", "tpr_u5", "incidence", "mortality", "vector_density", "resistance"]
   },
   "intervention": {
       "importance": "Measures of control program coverage and effectiveness",
       "data_sources": ["Program data", "Household surveys", "Health system records", "Distribution campaigns"],
       "variables": ["itn_coverage", "irs_coverage", "access_to_care", "treatment_availability", "larviciding"]
   }
}

# Templates for generating explanations
EXPLANATION_TEMPLATES = {
   "variable_relationship": {
       "basic": """
           {variable_name} affects malaria risk because {main_reason}. 
           
           It has a {relationship} relationship with malaria risk, which means {relationship_explanation}.
           
           This variable is important for understanding malaria patterns because {importance_reason}.
       """,
       "standard": """
           {variable_full_name} ({variable_name}) is an important factor in malaria risk assessment because:
           
           1. {main_reason}
           2. {secondary_reason}
           3. {tertiary_reason}
           
           This variable has a {relationship} relationship with malaria risk, meaning {relationship_explanation}.
           
           In the analysis, we see that {variable_name} ranges from {min_value} to {max_value} across the study area, with an average value of {mean_value}. Areas with {higher_or_lower} values of {variable_name} generally correspond to {higher_or_lower} malaria risk.
           
           Understanding spatial patterns of {variable_name} helps identify areas where environmental conditions may be particularly conducive to malaria transmission.
       """,
       "technical": """
           {variable_full_name} ({variable_name}) influences malaria transmission dynamics through multiple mechanisms:
           
           1. Primary effect: {primary_mechanism}
              - Quantitative relationship: {quantitative_detail}
              - Biophysical pathway: {biophysical_detail}
           
           2. Secondary effects:
              - {secondary_mechanism_1}
              - {secondary_mechanism_2}
           
           3. Interaction effects:
              - Synergy with {synergistic_variable}: {synergy_detail}
              - Antagonism with {antagonistic_variable}: {antagonism_detail}
           
           The variable demonstrates a {relationship} relationship with malaria risk parameters, with a typical functional form of {mathematical_function}. In our analysis, values range from {min_value} to {max_value} (mean: {mean_value}, median: {median_value}, standard deviation: {std_value}).
           
           Spatial analysis reveals {spatial_pattern}, with temporal dynamics showing {temporal_pattern}. These patterns align with research from {research_reference} demonstrating similar relationships in {comparable_setting}.
           
           Statistical significance: {statistical_significance}
           Relative contribution to risk models: {relative_contribution}
           Uncertainty characterization: {uncertainty_detail}
       """
   },
   
   "not_ideal_flag": {
       "basic": """
           {ward_name} is flagged as "not ideal" because it has high vulnerability (ranked {rank}) but is classified as non-urban ({urban_percentage}% urban).
           
           This creates a challenge for planning because urban-focused interventions might not be appropriate, despite the high risk.
       """,
       "standard": """
           {ward_name} has been flagged as "not ideal" for urban-focused interventions due to an important mismatch in its classification and vulnerability ranking:
           
           1. Vulnerability: This ward ranks {rank} out of {total_wards} in overall vulnerability, placing it in the {vulnerability_category} vulnerability category.
           
           2. Urban Classification: Despite its high vulnerability ranking, the ward has an urban percentage of only {urban_percentage}%, which falls below the threshold used to classify areas as urban.
           
           3. Implications: This creates a planning challenge because:
              - The high vulnerability suggests prioritization for interventions
              - The non-urban classification means that interventions designed for urban settings may not be appropriate
              - Alternative approaches may need to be considered for this ward
           
           The "not ideal" flag is designed to highlight these cases where standard intervention approaches may need adaptation.
       """,
       "technical": """
           Ward: {ward_name}
           
           Classification Discrepancy Analysis:
           
           1. Vulnerability Assessment:
              - Rank: {rank}/{total_wards} (percentile: {percentile})
              - Median Risk Score: {median_score}
              - IQR across models: {iqr}
              - Contributing factors: {contributing_factors}
           
           2. Urban Classification Parameters:
              - Urban percentage: {urban_percentage}%
              - Classification threshold: 
              - Classification method: {classification_method}
              - Source of urban data: {urban_data_source}
           
           3. "Not Ideal" Designation Criteria:
              - Trigger condition: High vulnerability (top {high_vulnerability_threshold} ranks) + Non-urban classification
              - Purpose: Highlight intervention strategy mismatches
              - Priority override recommendation: {override_recommendation}
           
           4. Intervention Implications:
              - Standard urban interventions sub-optimal due to: {urban_intervention_limitations}
              - Alternative approaches to consider: {alternative_approaches}
              - Cost-effectiveness considerations: {cost_effectiveness_notes}
              - Implementation challenges: {implementation_challenges}
           
           5. Similar Cases:
              - Other "not ideal" wards: {similar_wards}
              - Spatial clustering of mismatches: {spatial_clustering}
              - Potential systematic factors: {systematic_factors}
           
           6. Monitoring Recommendations:
              - Key indicators for this ward type: {key_indicators}
              - Suggested monitoring frequency: {monitoring_frequency}
              - Evaluation metrics: {evaluation_metrics}
       """
   },
   
   "vulnerability_ranking": {
       "basic": """
           {ward_name} is ranked #{rank} out of {total_wards} wards, placing it in the {vulnerability_category} vulnerability category.
           
           This ranking is based on the ward's overall malaria risk score of {score}, which considers factors like {factor_list}.
       """,
       "standard": """
           {ward_name} is ranked #{rank} out of {total_wards} wards in the vulnerability assessment, placing it in the {vulnerability_category} vulnerability category.
           
           Key findings about this ward:
           
           1. Overall Risk: The ward has a median risk score of {score} (on a scale of 0-1, where higher values indicate greater vulnerability).
           
           2. Contributing Factors: The main variables contributing to this ward's vulnerability include:
              - {factor_1}: {factor_1_value} ({factor_1_interpretation})
              - {factor_2}: {factor_2_value} ({factor_2_interpretation})
              - {factor_3}: {factor_3_value} ({factor_3_interpretation})
           
           3. Comparative Standing: This ward is in the {percentile} percentile of vulnerability, meaning it has {greater_or_lower} risk than {percentage}% of all wards in the study area.
           
           4. Implications: Based on this ranking, {ward_name} would {prioritization_suggestion} for malaria control interventions.
       """,
       "technical": """
           Vulnerability Analysis for {ward_name}:
           
           1. Ranking Metrics:
              - Overall Rank: {rank}/{total_wards}
              - Percentile: {percentile}
              - Risk Score (median): {score}
              - Interquartile Range: {iqr}
              - Statistical confidence interval: {confidence_interval}
           
           2. Variable Contribution Analysis:
              - {factor_1}: {factor_1_value} (normalized: {factor_1_normalized})
                * Relative contribution: {factor_1_contribution}%
                * z-score vs. all wards: {factor_1_z_score}
              - {factor_2}: {factor_2_value} (normalized: {factor_2_normalized})
                * Relative contribution: {factor_2_contribution}%
                * z-score vs. all wards: {factor_2_z_score}
              - {factor_3}: {factor_3_value} (normalized: {factor_3_normalized})
                * Relative contribution: {factor_3_contribution}%
                * z-score vs. all wards: {factor_3_z_score}
           
           3. Model Sensitivity Analysis:
              - Model concordance: {model_concordance}
              - Most sensitive to: {sensitivity_variable}
              - Rank range across models: {rank_range}
              - Most consistent ranking from: {consistent_model}
           
           4. Spatial Context:
              - Spatial cluster membership: {spatial_cluster}
              - Nearest similar wards: {similar_wards}
              - Spatial autocorrelation (local Moran's I): {morans_i}
           
           5. Classification Details:
              - Category boundaries: {category_boundaries}
              - Classification method: {classification_method}
              - Robustness to threshold changes: {robustness_assessment}
              - Alternative classification results: {alternative_classification}
           
           6. Intervention Prioritization Metrics:
              - Recommended intervention package: {intervention_package}
              - Estimated impact: {estimated_impact}
              - Cost-effectiveness ranking: {cost_effectiveness}
              - Implementation feasibility score: {feasibility_score}
       """
   },
   
   "composite_score": {
       "basic": """
           Composite scores combine multiple variables to create an overall risk score.
           
           We've created {model_count} different models using various combinations of {variable_count} variables.
           
           This approach helps us understand how consistent the risk patterns are across different models.
       """,
       "standard": """
           The composite score analysis created {model_count} different risk models by combining various subsets of the {variable_count} selected variables.
           
           Key aspects of this analysis:
           
           1. Variable Selection: The analysis used these variables:
              {variable_list}
           
           2. Model Generation: All possible combinations of these variables were used to create different models, ranging from pairs of variables to all variables together.
           
           3. Score Calculation: For each model, variables were normalized to a 0-1 scale and averaged to create a composite risk score.
           
           4. Map Visualization: Each model is shown as a separate map, allowing comparison of how different variable combinations affect the risk assessment.
           
           5. Consistency Analysis: Areas that show high risk across multiple models have more consistent vulnerability, while areas that change ranking between models may have more uncertain risk status.
           
           This multi-model approach avoids the limitations of single-model risk assessment by showing how robust the patterns are across different combinations of variables.
       """,
       "technical": """
           Composite Score Methodology Technical Summary:
           
           1. Variable Selection Criteria:
              - Selected variables: {variable_list}
              - Selection method: {selection_method}
              - Relationship determination approach: {relationship_method}
              - Collinearity assessment: {collinearity_stats}
           
           2. Model Generation Framework:
              - Combinatorial algorithm: All combinations of k variables where 2 ≤ k ≤ n
              - Total model count: {model_count}
              - Model diversity index: {diversity_index}
              - Pairwise model correlation matrix available in metadata
           
           3. Score Calculation Algorithm:
              - Normalization method: {normalization_method}
              - Aggregation function: {aggregation_function}
              - Weighting scheme: {weighting_scheme}
              - Edge case handling: {edge_case_handling}
           
           4. Statistical Properties:
              - Score distribution: {score_distribution}
              - Global mean: {global_mean}
              - Global variance: {global_variance}
              - Spatial autocorrelation (Moran's I): {morans_i}
           
           5. Uncertainty Quantification:
              - Per-ward uncertainty metric: IQR across models
              - Global uncertainty assessment: {uncertainty_assessment}
              - Most stable wards: {stable_wards}
              - Most variable wards: {variable_wards}
           
           6. Performance Metrics:
              - Cross-validation results: {cross_validation}
              - Comparison to epidemiological data: {epidemiological_comparison}
              - Sensitivity analysis: {sensitivity_analysis}
              - Top performing model: {top_model} (based on {performance_metric})
       """
   },
   
   "urban_extent": {
       "basic": """
           Urban extent analysis shows which areas are urban versus rural using a {threshold}% threshold.
           
           At this threshold, {urban_count} wards ({urban_percent}%) are classified as urban, and {rural_count} wards are non-urban.
           
           This classification helps determine appropriate intervention strategies for different areas.
       """,
       "standard": """
           The urban extent analysis classifies wards as urban or non-urban based on their urban percentage values, using a threshold of {threshold}%.
           
           Key findings at this threshold:
           
           1. Classification Results:
              - Urban wards: {urban_count} ({urban_percent}% of all wards)
              - Non-urban wards: {rural_count} ({rural_percent}% of all wards)
           
           2. Population Impact:
              - Population in urban areas: {urban_population} ({urban_pop_percent}% of total)
              - Population in non-urban areas: {rural_population} ({rural_pop_percent}% of total)
           
           3. Vulnerability Distribution:
              - Urban high vulnerability wards: {urban_high_vuln_count}
              - Non-urban high vulnerability wards: {rural_high_vuln_count}
              - "Not ideal" wards (high vulnerability but non-urban): {not_ideal_count}
           
           4. Intervention Implications:
              - Urban-focused interventions would cover {urban_pop_percent}% of the population
              - Alternative approaches needed for non-urban high vulnerability areas
           
           This classification is important for tailoring malaria control strategies to different environmental contexts, as urban and rural areas often require different intervention approaches.
       """,
       "technical": """
           Urban Extent Analysis Technical Summary (Threshold: {threshold}%):
           
           1. Classification Parameters:
              - Urban percentage data source: {data_source}
              - Classification threshold: {threshold}%
              - Classification method: Binary threshold
              - Alternative thresholds tested: {alternative_thresholds}
           
           2. Statistical Summary:
              - Urban classification count: {urban_count} ({urban_percent}%)
              - Non-urban classification count: {rural_count} ({rural_percent}%)
              - Mean urban percentage: {mean_urban}%
              - Median urban percentage: {median_urban}%
              - Standard deviation: {std_urban}%
              - Urban percentage distribution: {distribution_description}
           
           3. Spatial Statistics:
              - Spatial clustering (Join Count): {join_count}
              - Global Moran's I for urban percentage: {morans_i}
              - Getis-Ord Gi* hotspot analysis: {hotspot_summary}
              - Edge effects assessment: {edge_effects}
           
           4. Cross-tabulation with Vulnerability:
              - Contingency table (counts):
                * Urban, High vulnerability: {urban_high}
                * Urban, Medium vulnerability: {urban_medium}
                * Urban, Low vulnerability: {urban_low}
                * Non-urban, High vulnerability: {rural_high}
                * Non-urban, Medium vulnerability: {rural_medium}
                * Non-urban, Low vulnerability: {rural_low}
              - Chi-square test result: {chi_square}
              - Odds ratio (high vulnerability in urban vs. non-urban): {odds_ratio}
           
           5. Sensitivity Analysis:
              - Classification stability index: {stability_index}
              - Most sensitive wards: {sensitive_wards}
              - Threshold sensitivity analysis: {threshold_sensitivity}
              - Impact on intervention coverage estimates: {intervention_impact}
           
           6. Methodological Limitations:
              - Uncertainty in urban percentage data: {uncertainty_assessment}
              - Temporal currency of urban data: {temporal_currency}
              - Potential misclassification patterns: {misclassification_patterns}
              - Validation against alternative definitions: {validation_results}
       """
   }
}

# Technical references for citations and further reading
TECHNICAL_REFERENCES = {
   "data_cleaning": {
       "key_publications": [
           {
               "title": "Handling missing values in malaria risk mapping: A machine learning approach for predicting Plasmodium falciparum prevalence",
               "authors": "Parham PE, Baiser B, Liang R, et al.",
               "journal": "Scientific Reports",
               "year": 2021,
               "doi": "10.1038/s41598-021-98189-0"
           },
           {
               "title": "Spatial methods for infectious disease outbreak investigations: systematic literature review",
               "authors": "Smith CM, Le Comber SC, Fry H, et al.",
               "journal": "Eurosurveillance",
               "year": 2015,
               "doi": "10.2807/1560-7917.ES2015.20.39.30026"
           }
       ],
       "methods_summary": "Contemporary approaches to missing data in spatial epidemiology emphasize the preservation of spatial patterns through neighbor-based imputation, with hierarchical fallback methods for robust handling of diverse data scenarios.",
       "implementation_notes": "The MRPT implements a multi-tiered imputation approach prioritizing spatial relationships, with transparency in method selection to support reproducibility and uncertainty assessment."
   },
   
   "normalization": {
       "key_publications": [
           {
               "title": "A new method for normalizing environmental variables in malaria spatial risk models",
               "authors": "Machault V, Vignolles C, Pagès F, et al.",
               "journal": "International Journal of Health Geographics",
               "year": 2014,
               "doi": "10.1186/1476-072X-13-22"
           },
           {
               "title": "Methodological approaches to the assessment of environmental factors associated with malaria: a systematic review",
               "authors": "Srimath-Tirumula-Peddinti RC, Neelapu NRR, Sidagam N",
               "journal": "Malaria Journal",
               "year": 2022,
               "doi": "10.1186/s12936-021-04033-1"
           }
       ],
       "methods_summary": "Normalization techniques in malaria risk modeling have evolved to incorporate domain knowledge about variable relationships with risk, moving beyond generic statistical normalization to epidemiologically-informed transformations.",
       "implementation_notes": "The MRPT implements relationship-aware normalization to ensure that higher normalized values consistently represent higher malaria risk, facilitating intuitive interpretation of results."
   },
   
   "composite_scores": {
       "key_publications": [
           {
               "title": "Ensemble modeling and multi-component risk assessment: insights from the malaria elimination modeling consortium",
               "authors": "Smith T, Chitnis N, Penny MA, et al.",
               "journal": "Epidemics",
               "year": 2019,
               "doi": "10.1016/j.epidem.2019.100370"
           },
           {
               "title": "Ensemble models in forecasting malaria transmission",
               "authors": "Yamana TK, Qiu X, Eltahir EAB",
               "journal": "PLoS Computational Biology",
               "year": 2018,
               "doi": "10.1371/journal.pcbi.1006319"
           }
       ],
       "methods_summary": "Modern approaches to malaria risk modeling emphasize ensemble methods that capture uncertainty in variable selection and model specification, providing more robust assessments than single-model approaches.",
       "implementation_notes": "The MRPT's multi-model approach represents a practical implementation of ensemble principles, with transparent visualization of model variations to support decision-making in the face of uncertainty."
   },
   
   "vulnerability_ranking": {
       "key_publications": [
           {
               "title": "Ranking hazard vulnerability: A methodological review of integrated impact assessment in vector control contexts",
               "authors": "Kienberger S, Hagenlocher M",
               "journal": "Global Environmental Change",
               "year": 2014,
               "doi": "10.1016/j.gloenvcha.2014.03.011"
           },
           {
               "title": "Spatial modeling of malaria risk factors in high endemic areas of Ethiopia",
               "authors": "Yirsaw GG, Tadesse E, Melesse AM",
               "journal": "Spatial Information Research",
               "year": 2021,
               "doi": "10.1007/s41324-021-00420-7"
           }
       ],
       "methods_summary": "Vulnerability ranking methodologies have advanced from simple sorting to sophisticated approaches incorporating uncertainty quantification, spatial clustering, and multi-criteria classification.",
       "implementation_notes": "The MRPT implements a median-based ranking approach with categorical classification to balance statistical robustness with operational utility, while explicitly flagging wards with policy-relevant mismatches between vulnerability and urban status."
   },
   
   "urban_extent": {
       "key_publications": [
           {
               "title": "Urban malaria: Understanding its epidemiology, ecology, and transmission across the globe",
               "authors": "Wilson ML, Krogstad DJ, Arinaitwe E, et al.",
               "journal": "BMC Medicine",
               "year": 2022,
               "doi": "10.1186/s12916-022-02334-z"
           },
           {
               "title": "Malaria and urbanization in sub-Saharan Africa",
               "authors": "Hay SI, Guerra CA, Tatem AJ, et al.",
               "journal": "Malaria Journal",
               "year": 2012,
               "doi": "10.1186/1475-2875-11-1"
           }
       ],
       "methods_summary": "Urban-rural classification in malaria contexts has evolved from administrative boundaries to multidimensional assessments incorporating built environment characteristics, population density, and functional urban attributes.",
       "implementation_notes": "The MRPT employs a threshold-based classification aligned with operational definitions used in malaria control planning, while enabling sensitivity analysis across different thresholds to support robust decision-making."
   },
   
   "rainfall": {
       "key_publications": [
           {
               "title": "Rainfall as a risk factor for malaria transmission in sub-Saharan Africa: A systematic review and meta-analysis",
               "authors": "Janko MM, Irish SR, Reich BJ, et al.",
               "journal": "Lancet Planetary Health",
               "year": 2018,
               "doi": "10.1016/S2542-5196(18)30234-X"
           },
           {
               "title": "The effect of rainfall patterns on the development of mosquito larvae",
               "authors": "Paaijmans KP, Wandago MO, Githeko AK, et al.",
               "journal": "Acta Tropica",
               "year": 2007,
               "doi": "10.1016/j.actatropica.2007.01.002"
           }
       ],
       "methods_summary": "Contemporary understanding of rainfall's impact on malaria transmission incorporates non-linear relationships, lag effects, and interactions with other environmental variables, moving beyond simple linear correlation models.",
       "implementation_notes": "The MRPT models rainfall with a direct relationship to malaria risk, incorporating normalization to account for regional variations in rainfall patterns and their epidemiological significance."
   },
   
   "temperature": {
       "key_publications": [
           {
               "title": "The temperature dependence of the extrinsic incubation period of Plasmodium falciparum malaria in the Indian anopheline mosquitoes",
               "authors": "Murdock CC, Sternberg ED, Thomas MB",
               "journal": "Malaria Journal",
               "year": 2016,
               "doi": "10.1186/s12936-016-1271-z"
           },
           {
               "title": "The effects of temperature on Anopheles mosquito population dynamics and the potential for malaria transmission",
               "authors": "Beck-Johnson LM, Nelson WA, Paaijmans KP, et al.",
               "journal": "PLoS ONE",
               "year": 2013,
               "doi": "10.1371/journal.pone.0079276"
           }
       ],
       "methods_summary": "Temperature affects multiple aspects of malaria transmission biology, creating a complex relationship best characterized by non-linear models that account for thermal optima and limits for different biological processes.",
       "implementation_notes": "The MRPT implementation acknowledges the non-linear relationship between temperature and malaria risk while using a simplified direct relationship model for practical risk assessment in endemic regions."
   }
}