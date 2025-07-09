"""
Custom Analysis Parser for ChatMRPT
Extracts analysis parameters from natural language messages
"""

import logging
import re
from typing import Dict, List, Optional, Any, Tuple
from ..core.variable_matcher import extract_variables_from_message

logger = logging.getLogger(__name__)


class CustomAnalysisParser:
    """
    Parser for extracting custom analysis parameters from natural language.
    
    Handles various message formats:
    - "Run analysis with temperature, rainfall and population"
    - "Analyze using PCA with temp and humidity, composite with population and poverty"
    - "Use malaria incidence and housing quality for both methods"
    - "Custom run with these variables: education, water access, health"
    """
    
    def __init__(self):
        # Keywords that indicate custom analysis
        self.custom_keywords = [
            'custom', 'with', 'using', 'variables', 'analyze', 'run', 
            'include', 'select', 'choose', 'pick'
        ]
        
        # Method-specific keywords
        self.composite_keywords = ['composite', 'mean', 'average', 'scoring']
        self.pca_keywords = ['pca', 'principal', 'component', 'dimension']
        self.both_keywords = ['both', 'all', 'each', 'two methods']
    
    def parse_custom_analysis_request(self, message: str, 
                                    available_variables: List[str]) -> Dict[str, Any]:
        """
        Parse a natural language message to extract custom analysis parameters.
        
        Args:
            message: User's natural language message
            available_variables: List of available column names in the dataset
            
        Returns:
            Dict with:
            - is_custom_request: Boolean indicating if this is a custom analysis request
            - composite_variables: List of variables for composite method
            - pca_variables: List of variables for PCA method
            - unified_variables: List of variables to use for both methods
            - confidence: Confidence score (0-1) that this is a custom request
            - extracted_text: The portion of text used for variable extraction
        """
        message_lower = message.lower()
        
        # Check if this is likely a custom analysis request
        is_custom = self._is_custom_request(message_lower)
        confidence = self._calculate_confidence(message_lower)
        
        if not is_custom or confidence < 0.5:
            return {
                'is_custom_request': False,
                'composite_variables': None,
                'pca_variables': None,
                'unified_variables': None,
                'confidence': confidence,
                'extracted_text': None
            }
        
        # Extract method-specific variables
        result = {
            'is_custom_request': True,
            'composite_variables': None,
            'pca_variables': None,
            'unified_variables': None,
            'confidence': confidence,
            'extracted_text': None
        }
        
        # Check if user wants same variables for both methods
        if self._wants_both_methods(message_lower):
            # Extract variables for both methods
            extracted_text, variables = self._extract_unified_variables(message, available_variables)
            result['unified_variables'] = variables
            result['extracted_text'] = extracted_text
            logger.info(f"📊 Extracted {len(variables)} variables for both methods: {variables}")
        else:
            # Try to extract method-specific variables
            composite_text, composite_vars = self._extract_method_variables(
                message, 'composite', available_variables
            )
            pca_text, pca_vars = self._extract_method_variables(
                message, 'pca', available_variables
            )
            
            if composite_vars:
                result['composite_variables'] = composite_vars
                result['extracted_text'] = composite_text
                logger.info(f"📊 Extracted {len(composite_vars)} composite variables: {composite_vars}")
            
            if pca_vars:
                result['pca_variables'] = pca_vars
                if not result['extracted_text']:
                    result['extracted_text'] = pca_text
                else:
                    result['extracted_text'] += f" | {pca_text}"
                logger.info(f"🔬 Extracted {len(pca_vars)} PCA variables: {pca_vars}")
            
            # If no method-specific variables found, extract general variables
            if not composite_vars and not pca_vars:
                extracted_text, variables = self._extract_unified_variables(message, available_variables)
                if variables:
                    result['unified_variables'] = variables
                    result['extracted_text'] = extracted_text
                    logger.info(f"📊 Extracted {len(variables)} general variables: {variables}")
        
        return result
    
    def _is_custom_request(self, message_lower: str) -> bool:
        """Check if the message indicates a custom analysis request."""
        # Look for custom keywords
        for keyword in self.custom_keywords:
            if keyword in message_lower:
                return True
        
        # Look for variable lists (comma-separated items)
        if ',' in message_lower and any(word in message_lower for word in ['and', 'with']):
            return True
        
        return False
    
    def _calculate_confidence(self, message_lower: str) -> float:
        """Calculate confidence that this is a custom analysis request."""
        score = 0.0
        
        # Check for explicit custom keywords
        if 'custom' in message_lower:
            score += 0.3
        
        # Check for variable-related keywords
        variable_keywords = ['variable', 'variables', 'with', 'using', 'include']
        matches = sum(1 for kw in variable_keywords if kw in message_lower)
        score += min(matches * 0.15, 0.3)
        
        # Check for method keywords
        if any(kw in message_lower for kw in self.composite_keywords + self.pca_keywords):
            score += 0.2
        
        # Check for comma-separated lists
        if ',' in message_lower:
            score += 0.1
        
        # Check for quoted strings (likely variable names)
        if '"' in message_lower or "'" in message_lower:
            score += 0.1
        
        return min(score, 1.0)
    
    def _wants_both_methods(self, message_lower: str) -> bool:
        """Check if user wants to use same variables for both methods."""
        # Direct indicators
        for keyword in self.both_keywords:
            if keyword in message_lower:
                return True
        
        # No method-specific keywords mentioned
        has_composite = any(kw in message_lower for kw in self.composite_keywords)
        has_pca = any(kw in message_lower for kw in self.pca_keywords)
        
        return not has_composite and not has_pca
    
    def _extract_unified_variables(self, message: str, 
                                 available_variables: List[str]) -> Tuple[str, List[str]]:
        """Extract variables for both methods."""
        # Find the portion of text containing variables
        extracted_text = self._find_variable_text(message)
        
        # Extract variables from the text
        variables = extract_variables_from_message(extracted_text, available_variables)
        
        return extracted_text, variables
    
    def _extract_method_variables(self, message: str, method: str, 
                                available_variables: List[str]) -> Tuple[str, List[str]]:
        """Extract variables for a specific method."""
        message_lower = message.lower()
        
        # Get method keywords
        if method == 'composite':
            keywords = self.composite_keywords
        else:
            keywords = self.pca_keywords
        
        # Find method-specific section
        for keyword in keywords:
            if keyword in message_lower:
                # Find text after the method keyword
                idx = message_lower.index(keyword)
                # Look for variables after this point
                remaining_text = message[idx:]
                
                # Find the end of this method's section
                # (either another method keyword or end of message)
                other_keywords = self.pca_keywords if method == 'composite' else self.composite_keywords
                end_idx = len(remaining_text)
                for other_kw in other_keywords:
                    if other_kw in remaining_text.lower():
                        end_idx = min(end_idx, remaining_text.lower().index(other_kw))
                
                method_text = remaining_text[:end_idx]
                
                # Extract variables from this section
                variables = extract_variables_from_message(method_text, available_variables)
                
                if variables:
                    return method_text, variables
        
        return "", []
    
    def _find_variable_text(self, message: str) -> str:
        """Find the portion of text most likely to contain variable names."""
        message_lower = message.lower()
        
        # Look for text after key phrases
        key_phrases = [
            'with', 'using', 'variables:', 'include', 'select', 
            'analyze', 'run analysis with', 'custom run with'
        ]
        
        best_start = 0
        for phrase in key_phrases:
            if phrase in message_lower:
                idx = message_lower.index(phrase) + len(phrase)
                if idx > best_start:
                    best_start = idx
        
        if best_start > 0:
            return message[best_start:].strip()
        
        # If no key phrases found, return the whole message
        return message


# Global instance
custom_parser = CustomAnalysisParser()


def parse_custom_analysis(message: str, available_variables: List[str]) -> Dict[str, Any]:
    """
    Convenience function to parse custom analysis requests.
    
    Returns:
        Dict with extracted analysis parameters
    """
    return custom_parser.parse_custom_analysis_request(message, available_variables)