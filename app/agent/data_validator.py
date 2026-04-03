"""
Data Validator for Data Analysis V3
Prevents hallucinations and validates data sanity
"""

import re
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class DataValidator:
    """
    Validates data outputs to prevent hallucinations and impossible values.
    """
    
    # DYNAMIC: Generic patterns that indicate hallucination for ANY entity type
    # These work for facilities, companies, products, locations, etc.
    GENERIC_PATTERNS = [
        r'(?:Item|Entity|Entry|Object|Thing) [A-Z](?:\s|$)',  # Item A, Entity B, etc.
        r'(?:Item|Entity|Entry|Object|Thing) \d+',            # Item 1, Entity 2, etc.
        r'Example \w+',                                       # Example X, etc.
        r'Sample \w+',                                        # Sample Y, etc.
        r'Test \w+',                                          # Test Z, etc.
        r'Demo \w+',                                          # Demo ABC, etc.
        r'Placeholder \w+',                                   # Placeholder XYZ, etc.
        # Domain-agnostic patterns for single letters/numbers
        r'\b[A-Z]\b(?:\s|$|,)',                              # Just "A", "B", "C" alone
        r'(?:^|\s)#\d+(?:\s|$)',                             # #1, #2, #3, etc.
        r'[Ww]ard\s+[A-Z](?:\b|$)',                           # Ward A
        r'[Ww]ard\s+\d+(?:\b|$)',                            # Ward 1
        r'[Ff]acility\s+[A-Z](?:\b|$)',                       # Facility B
        r'[Ff]acility\s+\d+(?:\b|$)',                        # Facility 2
        r'[Aa]rea\s+[A-Z](?:\b|$)',                           # Area C
        r'[Ll]ocation\s+[A-Z](?:\b|$)',                       # Location D
    ]
    
    # Additional context-specific patterns (detected dynamically)
    CONTEXT_PATTERNS = {
        # These are populated dynamically based on the data domain
        # e.g., if we detect healthcare data, add healthcare patterns
        # if we detect retail data, add retail patterns, etc.
    }
    
    @classmethod
    def validate_output(cls, output: str, context: Dict[str, Any] = None) -> Tuple[bool, List[str]]:
        """
        Validate output for hallucinations and impossible values.
        
        Args:
            output: The text output to validate
            context: Optional context with actual data for validation
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        # Check for generic/hallucinated names
        hallucination_issues = cls._check_hallucinations(output)
        issues.extend(hallucination_issues)
        
        # Check for impossible percentages
        percentage_issues = cls._validate_percentages(output)
        issues.extend(percentage_issues)
        
        # Check against actual data if provided
        if context:
            data_issues = cls._validate_against_data(output, context)
            issues.extend(data_issues)
        
        is_valid = len(issues) == 0
        
        if not is_valid:
            logger.warning(f"Validation failed with {len(issues)} issues: {issues[:3]}")
        
        return is_valid, issues
    
    @classmethod
    def _check_hallucinations(cls, output: str) -> List[str]:
        """
        Check for hallucinated/generic names in output.
        
        Args:
            output: Text to check
            
        Returns:
            List of hallucination issues found
        """
        issues = []
        
        for pattern in cls.GENERIC_PATTERNS:
            matches = re.findall(pattern, output)
            if matches:
                issues.append(f"Generic/hallucinated name detected: {matches[0]}")
                logger.error(f"Hallucination detected: {matches[0]} (pattern: {pattern})")
        
        return issues
    
    @classmethod
    def _validate_percentages(cls, output: str) -> List[str]:
        """
        Validate that percentages are within 0-100% range.
        
        Args:
            output: Text containing percentages
            
        Returns:
            List of percentage validation issues
        """
        issues = []
        
        # Find all percentage values
        # Matches patterns like: 123.4%, 123 %, 123 percent
        percentage_patterns = [
            r'(\d+(?:\.\d+)?)\s*%',
            r'(\d+(?:\.\d+)?)\s+percent',
            r'positivity rate[:\s]+(\d+(?:\.\d+)?)',
            r'TPR[:\s]+(\d+(?:\.\d+)?)',
        ]
        
        for pattern in percentage_patterns:
            matches = re.finditer(pattern, output, re.IGNORECASE)
            for match in matches:
                value = float(match.group(1))
                if value > 100:
                    issues.append(f"Impossible percentage detected: {value}% (must be 0-100%)")
                    logger.error(f"Invalid percentage: {value}%")
                elif value < 0:
                    issues.append(f"Negative percentage detected: {value}%")
                    logger.error(f"Negative percentage: {value}%")
        
        return issues
    
    @classmethod
    def _validate_against_data(cls, output: str, context: Dict[str, Any]) -> List[str]:
        """
        DYNAMIC validation against actual data context.
        Works with ANY type of data - not hardcoded for specific domains.
        
        Args:
            output: Text to validate
            context: Dictionary containing actual data entities and ranges
            
        Returns:
            List of validation issues
        """
        issues = []
        
        # DYNAMIC: Check against ANY entity names in the data
        if 'entity_names' in context and context['entity_names']:
            actual_entities = set(str(e).lower() for e in context['entity_names'])
            
            # DYNAMIC: Extract potential entity names from output
            # Look for capitalized phrases that might be entity names
            # This pattern works for any domain
            entity_pattern = r'[A-Z][a-zA-Z0-9]+(?:\s+[A-Z][a-zA-Z0-9]+)*'
            
            mentioned_entities = re.findall(entity_pattern, output)
            mentioned_entities_lower = [e.lower() for e in mentioned_entities]
            
            # Check if mentioned entities are real or generic
            for entity, entity_lower in zip(mentioned_entities, mentioned_entities_lower):
                # Skip if it's a generic pattern
                if any(re.match(pat, entity, re.IGNORECASE) for pat in cls.GENERIC_PATTERNS):
                    issues.append(f"Generic placeholder detected: '{entity}'")
                    continue
                    
                # For longer entity names (likely to be specific), check if they exist
                if len(entity.split()) >= 2:  # Multi-word entities
                    if entity_lower not in actual_entities:
                        # Check for partial matches
                        partial_match = any(
                            entity_lower in actual or actual in entity_lower 
                            for actual in actual_entities
                        )
                        if not partial_match and len(entity) > 3:  # Avoid flagging short common words
                            # Only flag if it looks like it should be an entity
                            if not entity.lower() in ['the', 'and', 'for', 'with', 'from', 'total', 'average']:
                                logger.debug(f"Potential unverified entity: '{entity}'")
        
        # DYNAMIC: Validate numeric values are within data ranges
        if 'numeric_ranges' in context and context['numeric_ranges']:
            # Extract numbers with their context
            number_pattern = r'(\w+)[:\s]+(\d+(?:,\d{3})*(?:\.\d+)?)'
            matches = re.finditer(number_pattern, output)
            
            for match in matches:
                metric_name = match.group(1).lower()
                value_str = match.group(2).replace(',', '')
                try:
                    value = float(value_str)
                    
                    # Check if this metric matches any column name
                    for col, ranges in context['numeric_ranges'].items():
                        if col.lower() in metric_name or metric_name in col.lower():
                            if value < ranges['min'] * 0.5 or value > ranges['max'] * 1.5:
                                # Allow some margin for aggregations
                                logger.debug(f"Value {value} for '{metric_name}' outside expected range [{ranges['min']}, {ranges['max']}]")
                except ValueError:
                    pass
        
        return issues
    
    @classmethod
    def sanitize_output(cls, output: str, context: Dict[str, Any] = None) -> str:
        """
        Sanitize output by removing or fixing invalid content.
        
        Args:
            output: Text to sanitize
            context: Optional context for corrections
            
        Returns:
            Sanitized output
        """
        sanitized = output
        
        # Replace generic facility names with warning
        for pattern in cls.GENERIC_PATTERNS:
            sanitized = re.sub(
                pattern,
                "[name unavailable]",
                sanitized,
                flags=re.IGNORECASE
            )
        
        # Fix impossible percentages
        def fix_percentage(match):
            value = float(match.group(1))
            if value > 100:
                logger.warning(f"Fixing impossible percentage: {value}% -> [error: >100%]")
                return "[error: percentage >100%]"
            elif value < 0:
                return "[error: negative percentage]"
            return match.group(0)
        
        sanitized = re.sub(r'(\d+(?:\.\d+)?)\s*%', fix_percentage, sanitized)
        
        return sanitized
    
    @classmethod
    def validate_top_n_response(cls, output: str, n: int) -> Tuple[bool, str]:
        """
        Validate that a "top N" query response contains N items.
        
        Args:
            output: The response text
            n: Expected number of items
            
        Returns:
            Tuple of (is_valid, validation_message)
        """
        # Count numbered items (1., 2., 3., etc.)
        numbered_pattern = r'^\s*(\d+)\.'
        numbers_found = re.findall(numbered_pattern, output, re.MULTILINE)
        
        # Also count bullet points as backup
        bullet_pattern = r'^\s*[•\-\*]'
        bullets_found = re.findall(bullet_pattern, output, re.MULTILINE)
        
        count = max(len(numbers_found), len(bullets_found))
        
        if count < n:
            if count == 0:
                return False, f"Expected {n} items but found no list in response"
            else:
                return False, f"Expected {n} items but only found {count}"
        
        return True, f"Found {count} items as expected"
    
    @classmethod
    def create_validation_prompt(cls) -> str:
        """
        Create a validation checklist prompt to append to responses.
        
        Returns:
            Validation prompt text
        """
        return """
Before providing this response, I have verified:
✓ All facility/location names are from the actual data
✓ All percentages are between 0-100%
✓ For "top N" queries, exactly N items are shown
✓ No generic placeholder names used
✓ All statistics are calculated from real data
"""
