"""
Tool Execution Validator for ChatMRPT

This module validates that tools actually execute correctly and return
data-driven responses instead of generic fallbacks.

Critical for ensuring production reliability.
"""

import logging
import time
from typing import Dict, Any, List, Callable, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ToolValidationResult:
    """Result of tool validation"""
    tool_name: str
    success: bool
    execution_time: float
    response_type: str  # "data_driven", "generic", "error"
    error_message: Optional[str] = None
    data_indicators: List[str] = None  # Evidence of real data usage

class ToolValidator:
    """
    Validates that tools execute properly and return data-driven responses
    """
    
    def __init__(self):
        self.data_indicators = {
            # Patterns that indicate real data usage
            "ward_names": [
                "Wangara", "Shakogi", "Dutsen Bakoshi", "Durun", "Dunbulum",
                "Tarauni", "Giginyu", "Fagge", "Darmanawa"
            ],
            "specific_numbers": [
                r"\d+\.\d{4}",  # Scores like 0.5618
                r"\d+ wards?",  # "484 wards"
                r"\d+ variables?",  # "94 variables"
                r"Rank: \d+",  # "Rank: 1/484"
            ],
            "data_columns": [
                "pfpr", "distance_to_water", "housing_quality", "flood",
                "mean_rainfall", "temp_mean", "elevation"
            ],
            "analysis_terms": [
                "composite score", "PCA", "correlation coefficient",
                "statistical significance", "p-value", "standard deviation"
            ]
        }
        
        self.generic_indicators = [
            "from an epidemiological perspective",
            "understanding these factors",
            "practical recommendations include",
            "it's important to note",
            "in urban settings like nigeria",
            "malaria transmission occurs when",
            "mosquitoes breed in stagnant water"
        ]
    
    def validate_tool_class(self, tool_class) -> bool:
        """
        Validate that a tool class follows proper structure.
        
        Args:
            tool_class: The tool class to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Check if it has required methods
            required_methods = ['execute', 'get_tool_name', 'get_category', 'get_examples']
            for method in required_methods:
                if not hasattr(tool_class, method):
                    logger.warning(f"Tool class {tool_class.__name__} missing method: {method}")
                    return False
            
            # Try to instantiate with no parameters to check basic structure
            # This will fail if required fields are missing but that's expected
            try:
                tool_class()
            except Exception as e:
                # This is expected for tools with required parameters
                # We just want to make sure the class structure is valid
                if "validation error" not in str(e).lower() and "missing" not in str(e).lower():
                    logger.warning(f"Tool class {tool_class.__name__} has structural issues: {e}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating tool class {tool_class.__name__}: {e}")
            return False
    
    def validate_tool_execution(self, tool_name: str, tool_function: Callable, 
                              parameters: Dict[str, Any]) -> ToolValidationResult:
        """
        Validate that a tool executes correctly and returns data-driven content
        """
        start_time = time.time()
        
        try:
            # Execute tool
            result = tool_function(**parameters)
            execution_time = time.time() - start_time
            
            if not isinstance(result, dict):
                return ToolValidationResult(
                    tool_name=tool_name,
                    success=False,
                    execution_time=execution_time,
                    response_type="error",
                    error_message="Tool did not return dictionary result"
                )
            
            if result.get("status") != "success":
                return ToolValidationResult(
                    tool_name=tool_name,
                    success=False,
                    execution_time=execution_time,
                    response_type="error",
                    error_message=result.get("message", "Tool returned unsuccessful status")
                )
            
            # Analyze response content
            response_content = result.get("response", "")
            response_analysis = self._analyze_response_content(response_content)
            
            return ToolValidationResult(
                tool_name=tool_name,
                success=True,
                execution_time=execution_time,
                response_type=response_analysis["type"],
                data_indicators=response_analysis["indicators"]
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            return ToolValidationResult(
                tool_name=tool_name,
                success=False,
                execution_time=execution_time,
                response_type="error",
                error_message=str(e)
            )
    
    def _analyze_response_content(self, content: str) -> Dict[str, Any]:
        """
        Analyze response content to determine if it's data-driven or generic
        """
        if not content:
            return {"type": "error", "indicators": []}
        
        content_lower = content.lower()
        found_indicators = []
        
        # Check for data indicators
        data_score = 0
        
        # Ward names (strong indicator)
        for ward in self.data_indicators["ward_names"]:
            if ward.lower() in content_lower:
                data_score += 10
                found_indicators.append(f"ward_name:{ward}")
        
        # Data columns (medium indicator)
        for column in self.data_indicators["data_columns"]:
            if column in content_lower:
                data_score += 5
                found_indicators.append(f"data_column:{column}")
        
        # Analysis terms (medium indicator)
        for term in self.data_indicators["analysis_terms"]:
            if term in content_lower:
                data_score += 3
                found_indicators.append(f"analysis_term:{term}")
        
        # Specific numbers (medium indicator)
        import re
        for pattern in self.data_indicators["specific_numbers"]:
            matches = re.findall(pattern, content_lower)
            if matches:
                data_score += len(matches) * 2
                found_indicators.extend([f"number_pattern:{match}" for match in matches])
        
        # Check for generic indicators (negative score)
        generic_score = 0
        for generic_phrase in self.generic_indicators:
            if generic_phrase in content_lower:
                generic_score += 5
                found_indicators.append(f"generic_phrase:{generic_phrase}")
        
        # Determine response type
        net_score = data_score - generic_score
        
        if net_score >= 10:
            response_type = "data_driven"
        elif generic_score > data_score:
            response_type = "generic"
        else:
            response_type = "mixed"
        
        return {
            "type": response_type,
            "indicators": found_indicators,
            "data_score": data_score,
            "generic_score": generic_score,
            "net_score": net_score
        }
    
    def validate_tool_suite(self, tools: Dict[str, Callable], test_session_id: str) -> Dict[str, Any]:
        """
        Validate a complete suite of tools for data-driven responses
        """
        results = {
            "total_tools": len(tools),
            "successful_tools": 0,
            "data_driven_tools": 0,
            "generic_tools": 0,
            "failed_tools": 0,
            "tool_results": {},
            "critical_failures": []
        }
        
        # Define critical tools that MUST work for production
        critical_tools = [
            "get_composite_rankings",
            "get_pca_rankings", 
            "correlation",
            "descriptive_statistics",
            "generate_comprehensive_analysis_summary"
        ]
        
        for tool_name, tool_function in tools.items():
            # Prepare basic parameters
            parameters = {"session_id": test_session_id}
            
            # Add tool-specific parameters
            if "rankings" in tool_name:
                parameters["top_n"] = 5
            elif tool_name == "correlation":
                parameters["target_variable"] = "pfpr"
            
            # Validate tool
            validation_result = self.validate_tool_execution(
                tool_name, tool_function, parameters
            )
            
            results["tool_results"][tool_name] = validation_result
            
            # Update counters
            if validation_result.success:
                results["successful_tools"] += 1
                
                if validation_result.response_type == "data_driven":
                    results["data_driven_tools"] += 1
                elif validation_result.response_type == "generic":
                    results["generic_tools"] += 1
            else:
                results["failed_tools"] += 1
                
                # Track critical failures
                if tool_name in critical_tools:
                    results["critical_failures"].append({
                        "tool": tool_name,
                        "error": validation_result.error_message
                    })
        
        # Calculate rates
        if results["total_tools"] > 0:
            results["success_rate"] = results["successful_tools"] / results["total_tools"]
            results["data_driven_rate"] = results["data_driven_tools"] / results["total_tools"]
        
        # Determine if suite is production ready
        results["production_ready"] = (
            len(results["critical_failures"]) == 0 and
            results["data_driven_rate"] >= 0.7 and
            results["success_rate"] >= 0.9
        )
        
        return results
    
    def generate_validation_report(self, validation_results: Dict[str, Any]) -> str:
        """Generate human-readable validation report"""
        report = []
        
        report.append("🔧 TOOL VALIDATION REPORT")
        report.append("=" * 50)
        
        # Summary
        report.append(f"Total Tools: {validation_results['total_tools']}")
        report.append(f"Successful: {validation_results['successful_tools']}")
        report.append(f"Data-Driven: {validation_results['data_driven_tools']}")
        report.append(f"Generic: {validation_results['generic_tools']}")
        report.append(f"Failed: {validation_results['failed_tools']}")
        report.append(f"Success Rate: {validation_results.get('success_rate', 0):.1%}")
        report.append(f"Data-Driven Rate: {validation_results.get('data_driven_rate', 0):.1%}")
        
        # Production readiness
        if validation_results.get("production_ready"):
            report.append("\n✅ PRODUCTION READY")
        else:
            report.append("\n❌ NOT PRODUCTION READY")
            
            if validation_results["critical_failures"]:
                report.append("\nCritical Failures:")
                for failure in validation_results["critical_failures"]:
                    report.append(f"  - {failure['tool']}: {failure['error']}")
        
        # Detailed results
        report.append("\nDetailed Results:")
        for tool_name, result in validation_results["tool_results"].items():
            status = "✅" if result.success else "❌"
            response_type = result.response_type
            
            report.append(f"{status} {tool_name}: {response_type} ({result.execution_time:.2f}s)")
            
            if result.data_indicators:
                indicators = result.data_indicators[:3]  # Show first 3
                report.append(f"    Indicators: {', '.join(indicators)}")
            
            if result.error_message:
                report.append(f"    Error: {result.error_message}")
        
        return "\n".join(report)