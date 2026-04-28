"""
Variable Matcher for ChatMRPT
Intelligent fuzzy matching for user-provided variable names
"""

import logging
import re
from typing import List, Dict, Optional, Tuple, Any
from difflib import SequenceMatcher
import pandas as pd

logger = logging.getLogger(__name__)


class VariableMatcher:
    """
    Intelligent variable matching system that handles:
    - Partial matches (e.g., "temp" matches "temperature")
    - Case-insensitive matching
    - Common abbreviations and variations
    - Typos and misspellings
    - Multiple word variations
    """
    
    def __init__(self):
        # Common variable name patterns and their variations
        self.common_variations = {
            'temperature': ['temp', 'tmp', 'temperature', 'air_temp', 'air_temperature', 'avg_temp'],
            'rainfall': ['rain', 'rainfall', 'precip', 'precipitation', 'rain_fall', 'avg_rainfall'],
            'humidity': ['humid', 'humidity', 'rh', 'relative_humidity', 'rel_humid'],
            'elevation': ['elev', 'elevation', 'alt', 'altitude', 'height', 'dem'],
            'population': ['pop', 'population', 'pop_density', 'population_density', 'pop_dens'],
            'malaria': ['malaria', 'mal', 'malaria_cases', 'malaria_incidence', 'mal_inc'],
            'poverty': ['poverty', 'pov', 'poverty_rate', 'poor', 'poverty_index'],
            'education': ['edu', 'education', 'literacy', 'school', 'education_level'],
            'health': ['health', 'healthcare', 'health_access', 'clinic', 'hospital'],
            'water': ['water', 'water_access', 'water_source', 'clean_water', 'water_body'],
            'sanitation': ['sanitation', 'sanit', 'toilet', 'latrine', 'sewage'],
            'housing': ['housing', 'house', 'housing_quality', 'hq', 'shelter'],
            'vegetation': ['veg', 'vegetation', 'ndvi', 'evi', 'greenness', 'vegetation_index'],
            'urban': ['urban', 'urban_percent', 'urbanization', 'urban_pct', 'urban_percentage'],
            'distance': ['dist', 'distance', 'distance_to', 'dist_to', 'proximity'],
            'nighttime_lights': ['night_lights', 'nighttime_lights', 'ntl', 'lights', 'night_time_lights'],
            'soil': ['soil', 'soil_moisture', 'soil_wetness', 'soil_type', 'soil_quality'],
            'pfpr': ['pfpr', 'parasite_rate', 'parasite_prevalence', 'p_falciparum', 'pf_pr'],
            'tpr': ['tpr', 'test_positivity', 'test_positivity_rate', 'positivity_rate'],
            'u5': ['u5', 'under5', 'under_5', 'under_five', 'children'],
            'ndwi': ['ndwi', 'water_index', 'normalized_difference_water_index'],
            'ndmi': ['ndmi', 'moisture_index', 'normalized_difference_moisture_index']
        }
        
        # Build reverse mapping for quick lookups
        self.variation_to_canonical = {}
        for canonical, variations in self.common_variations.items():
            for var in variations:
                self.variation_to_canonical[var.lower()] = canonical
    
    def match_variables(self, user_variables: List[str], available_variables: List[str], 
                       threshold: float = 0.7) -> Dict[str, Any]:
        """
        Match user-provided variables to available variables in the dataset.
        
        Args:
            user_variables: List of variable names provided by user
            available_variables: List of actual column names in the dataset
            threshold: Minimum similarity score (0-1) to accept a match
            
        Returns:
            Dict with matching results and recommendations
        """
        results = {
            'matched': {},
            'unmatched': [],
            'suggestions': {},
            'confidence_scores': {},
            'warnings': []
        }
        
        for user_var in user_variables:
            match_result = self._find_best_match(user_var, available_variables, threshold)
            
            if match_result['matched_variable']:
                results['matched'][user_var] = match_result['matched_variable']
                results['confidence_scores'][user_var] = match_result['confidence']
                
                if match_result['confidence'] < 0.9:
                    results['warnings'].append(
                        f"'{user_var}' matched to '{match_result['matched_variable']}' "
                        f"with {match_result['confidence']:.0%} confidence"
                    )
            else:
                results['unmatched'].append(user_var)
                if match_result['suggestions']:
                    results['suggestions'][user_var] = match_result['suggestions']
        
        return results
    
    def _find_best_match(self, user_var: str, available_variables: List[str], 
                        threshold: float) -> Dict[str, Any]:
        """
        Find the best match for a single user variable.
        
        Returns dict with:
        - matched_variable: Best matching variable name or None
        - confidence: Confidence score (0-1)
        - suggestions: List of alternative suggestions if no good match
        """
        user_var_lower = user_var.lower().strip()
        
        # First, check for exact match (case-insensitive)
        for avail_var in available_variables:
            if user_var_lower == avail_var.lower():
                return {
                    'matched_variable': avail_var,
                    'confidence': 1.0,
                    'suggestions': []
                }
        
        # Check if user variable is a known variation
        canonical = self.variation_to_canonical.get(user_var_lower)
        if canonical:
            # Look for the canonical form or its variations in available variables
            for avail_var in available_variables:
                avail_lower = avail_var.lower()
                if canonical in avail_lower or avail_lower in self.common_variations.get(canonical, []):
                    return {
                        'matched_variable': avail_var,
                        'confidence': 0.95,
                        'suggestions': []
                    }
        
        # Fuzzy matching with multiple strategies
        best_match = None
        best_score = 0
        all_scores = []
        
        for avail_var in available_variables:
            score = self._calculate_similarity(user_var, avail_var)
            all_scores.append((avail_var, score))
            
            if score > best_score:
                best_score = score
                best_match = avail_var
        
        # Sort by score to get suggestions
        all_scores.sort(key=lambda x: x[1], reverse=True)
        suggestions = [var for var, score in all_scores[:3] if score > 0.5]
        
        if best_score >= threshold:
            return {
                'matched_variable': best_match,
                'confidence': best_score,
                'suggestions': suggestions[1:] if len(suggestions) > 1 else []
            }
        else:
            return {
                'matched_variable': None,
                'confidence': best_score,
                'suggestions': suggestions
            }
    
    def _calculate_similarity(self, var1: str, var2: str) -> float:
        """
        Calculate similarity between two variable names using multiple strategies.
        """
        var1_lower = var1.lower().strip()
        var2_lower = var2.lower().strip()
        
        # Strategy 1: Direct sequence matching
        direct_score = SequenceMatcher(None, var1_lower, var2_lower).ratio()
        
        # Strategy 2: Check if one is substring of the other
        substring_score = 0
        if var1_lower in var2_lower or var2_lower in var1_lower:
            # Give higher score if the match is at the beginning
            if var2_lower.startswith(var1_lower) or var1_lower.startswith(var2_lower):
                substring_score = 0.9
            else:
                substring_score = 0.8
        
        # Strategy 3: Token-based matching (split by underscore, space, dash)
        tokens1 = set(re.split(r'[_\s\-]+', var1_lower))
        tokens2 = set(re.split(r'[_\s\-]+', var2_lower))
        
        if tokens1 and tokens2:
            # Jaccard similarity
            intersection = len(tokens1 & tokens2)
            union = len(tokens1 | tokens2)
            token_score = intersection / union if union > 0 else 0
        else:
            token_score = 0
        
        # Strategy 4: Check for common abbreviations
        abbrev_score = 0
        if self._is_abbreviation(var1_lower, var2_lower) or self._is_abbreviation(var2_lower, var1_lower):
            abbrev_score = 0.85
        
        # Return the highest score from all strategies
        return max(direct_score, substring_score, token_score, abbrev_score)
    
    def _is_abbreviation(self, short: str, long: str) -> bool:
        """
        Check if 'short' could be an abbreviation of 'long'.
        """
        if len(short) >= len(long):
            return False
        
        # Check if short matches the beginning of long
        if long.startswith(short):
            return True
        
        # Check if short matches first letters of tokens in long
        tokens = re.split(r'[_\s\-]+', long)
        if len(tokens) > 1:
            first_letters = ''.join(t[0] for t in tokens if t)
            if short == first_letters:
                return True
        
        return False
    
    def extract_variables_from_text(self, text: str, available_variables: List[str]) -> List[str]:
        """
        Extract variable names from natural language text.
        
        Example: "Run analysis with temperature, rainfall and population density"
        Returns: ["temperature", "rainfall", "population_density"]
        """
        text_lower = text.lower()
        
        # Remove common words that aren't variables
        stop_words = {'and', 'or', 'with', 'using', 'for', 'the', 'a', 'an', 'in', 'on', 'at', 
                     'to', 'from', 'by', 'as', 'of', 'run', 'analysis', 'analyze', 'use', 
                     'include', 'select', 'choose', 'pick', 'variables', 'variable', 'columns',
                     'column', 'please', 'can', 'you', 'i', 'want', 'need'}
        
        # Extract potential variable names
        # Look for: words, phrases in quotes, comma-separated items
        potential_vars = []
        
        # Extract quoted strings first
        quoted = re.findall(r'"([^"]+)"|\'([^\']+)\'', text)
        for match in quoted:
            potential_vars.extend([m for m in match if m])
        
        # Remove quoted parts from text to avoid double processing
        text_clean = re.sub(r'"[^"]+"|\'[^\']+\'', '', text_lower)
        
        # Look for comma-separated lists
        if ',' in text_clean:
            parts = text_clean.split(',')
            for part in parts:
                words = part.strip().split()
                # Join words that might form a variable name
                if words and not all(w in stop_words for w in words):
                    potential_vars.append(' '.join(w for w in words if w not in stop_words))
        
        # Also look for individual words/phrases
        words = text_clean.split()
        current_phrase = []
        for word in words:
            if word not in stop_words and word.replace('_', '').replace('-', '').isalnum():
                current_phrase.append(word)
            elif current_phrase:
                potential_vars.append(' '.join(current_phrase))
                current_phrase = []
        if current_phrase:
            potential_vars.append(' '.join(current_phrase))
        
        # Clean up potential variables
        cleaned_vars = []
        for var in potential_vars:
            # Replace spaces with underscores for matching
            var_clean = var.strip().replace(' ', '_')
            if var_clean and len(var_clean) > 1:  # Skip single characters
                cleaned_vars.append(var_clean)
        
        # Match extracted variables to available ones
        matched_vars = []
        for var in cleaned_vars:
            match_result = self._find_best_match(var, available_variables, threshold=0.6)
            if match_result['matched_variable']:
                matched_vars.append(match_result['matched_variable'])
                logger.info(f"Extracted '{var}' → '{match_result['matched_variable']}' "
                          f"(confidence: {match_result['confidence']:.0%})")
        
        # Remove duplicates while preserving order
        seen = set()
        unique_vars = []
        for var in matched_vars:
            if var not in seen:
                seen.add(var)
                unique_vars.append(var)
        
        return unique_vars


# Global instance for easy access
variable_matcher = VariableMatcher()


def match_user_variables(user_variables: List[str], available_variables: List[str], 
                        threshold: float = 0.7) -> Dict[str, Any]:
    """
    Convenience function to match user variables to available dataset variables.
    """
    return variable_matcher.match_variables(user_variables, available_variables, threshold)


def extract_variables_from_message(message: str, available_variables: List[str]) -> List[str]:
    """
    Convenience function to extract variable names from a natural language message.
    """
    return variable_matcher.extract_variables_from_text(message, available_variables)