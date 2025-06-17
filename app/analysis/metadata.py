"""
Analysis Metadata Module

This module provides tracking and metadata capabilities for analysis operations.
"""

import time
import logging
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime

# Set up logging
logger = logging.getLogger(__name__)

@dataclass
class AnalysisMetadata:
    """
    Tracks metadata for an analysis operation
    
    This class captures timing, variable usage, and other metadata
    to help with performance optimization and debugging.
    """
    session_id: str
    start_time: float = field(default_factory=lambda: time.time())
    variables_used: List[str] = field(default_factory=list)
    method: str = "unspecified"
    warnings: List[str] = field(default_factory=list)
    events: List[Dict[str, Any]] = field(default_factory=list)
    
    # Legacy tracking attributes
    steps: List[Dict[str, Any]] = field(default_factory=list)
    decisions: List[Dict[str, Any]] = field(default_factory=list)
    calculations: List[Dict[str, Any]] = field(default_factory=list)
    anomalies: List[Dict[str, Any]] = field(default_factory=list)
    logger: Optional[Any] = None
    
    def add_event(self, event_type: str, description: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Add an event to the metadata tracking"""
        self.events.append({
            "timestamp": time.time(),
            "type": event_type,
            "description": description,
            "details": details or {}
        })
    
    def record_warning(self, warning: str) -> None:
        """Record a warning during analysis"""
        self.warnings.append(warning)
    
    def set_variables(self, variables: List[str]) -> None:
        """Set the variables used in this analysis"""
        self.variables_used = variables
    
    def set_method(self, method: str) -> None:
        """Set the analysis method used"""
        self.method = method
    
    def get_elapsed_time(self) -> float:
        """Get the elapsed time since this metadata object was created"""
        return time.time() - self.start_time
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary for storage/reporting"""
        return {
            "session_id": self.session_id,
            "start_time": datetime.fromtimestamp(self.start_time).isoformat(),
            "elapsed_time": self.get_elapsed_time(),
            "variables_used": self.variables_used,
            "method": self.method,
            "warnings": self.warnings,
            "events": self.events,
            "event_count": len(self.events)
        }
        
    # Legacy methods from original implementation
    def record_step(self, step_name, input_data_summary=None, output_data_summary=None, 
                   algorithm=None, parameters=None):
        """Record an analysis step"""
        # Safety check: ensure start_time is set
        if self.start_time is None:
            self.start_time = time.time()
        
        step_info = {
            'step_id': len(self.steps) + 1,
            'step_name': step_name,
            'timestamp': time.time(),
            'execution_time': time.time() - (self.start_time or time.time()),
            'input_summary': input_data_summary,
            'output_summary': output_data_summary,
            'algorithm': algorithm,
            'parameters': parameters
        }
        self.steps.append(step_info)
        
        # Log to database if logger is available
        if self.logger and self.session_id:
            self.logger.log_analysis_event(
                self.session_id,
                "analysis_step_{}".format(step_name),
                step_info,
                True
            )
        
        return step_info['step_id']
    
    def record_decision(self, step_id, decision_type, options=None, criteria=None, 
                       selected_option=None, confidence=None):
        """Record a decision made during analysis"""
        decision_info = {
            'decision_id': len(self.decisions) + 1,
            'step_id': step_id,
            'decision_type': decision_type,
            'timestamp': time.time(),
            'options': options,
            'criteria': criteria,
            'selected_option': selected_option,
            'confidence': confidence
        }
        self.decisions.append(decision_info)
        
        # Log to database if logger is available
        if self.logger and self.session_id:
            self.logger.log_analysis_event(
                self.session_id,
                "analysis_decision_{}".format(decision_type),
                decision_info,
                True
            )
            
        return decision_info['decision_id']
    
    def record_calculation(self, step_id, variable, operation, input_values=None, 
                         output_value=None, context=None):
        """Record a calculation performed during analysis"""
        calc_info = {
            'calculation_id': len(self.calculations) + 1,
            'step_id': step_id,
            'variable': variable,
            'operation': operation,
            'timestamp': time.time(),
            'input_values': input_values,
            'output_value': output_value,
            'context': context
        }
        self.calculations.append(calc_info)
        return calc_info['calculation_id']
    
    def record_anomaly(self, entity_name, anomaly_type, expected_value=None, 
                     actual_value=None, significance=None, context=None):
        """Record an anomaly detected during analysis"""
        anomaly_info = {
            'anomaly_id': len(self.anomalies) + 1,
            'entity_name': entity_name,
            'anomaly_type': anomaly_type,
            'timestamp': time.time(),
            'expected_value': expected_value,
            'actual_value': actual_value,
            'significance': significance,
            'context': context
        }
        self.anomalies.append(anomaly_info)
        
        # Log to database if logger is available
        if self.logger and self.session_id:
            self.logger.log_analysis_event(
                self.session_id,
                "analysis_anomaly_{}".format(anomaly_type),
                anomaly_info,
                True
            )
            
        return anomaly_info['anomaly_id']
    
    def get_step_summary(self, step_id=None):
        """Get summary of steps, or a specific step if ID provided"""
        if step_id is not None:
            for step in self.steps:
                if step['step_id'] == step_id:
                    return step
            return None
        return self.steps
    
    def get_entity_metadata(self, entity_type, entity_name):
        """Get all metadata related to a specific entity"""
        entity_metadata = {
            'calculations': [],
            'decisions': [],
            'anomalies': []
        }
        
        # Gather calculations
        for calc in self.calculations:
            if calc['variable'] == entity_name:
                entity_metadata['calculations'].append(calc)
        
        # Gather decisions
        for decision in self.decisions:
            if entity_name in str(decision['options']) or entity_name in str(decision['selected_option']):
                entity_metadata['decisions'].append(decision)
        
        # Gather anomalies
        for anomaly in self.anomalies:
            if anomaly['entity_name'] == entity_name:
                entity_metadata['anomalies'].append(anomaly)
                
        return entity_metadata
    
    def get_explanation_context(self, context_type, entity_name=None, step_id=None):
        """Assemble context package for LLM explanation generation"""
        context = {
            'type': context_type,
            'steps': self.get_step_summary(step_id),
            'entity_metadata': {}
        }
        
        if entity_name:
            context['entity_metadata'] = self.get_entity_metadata('variable', entity_name)
            
        return context
    
    def get_analysis_summary(self):
        """Get a complete summary of the analysis metadata"""
        return {
            'session_id': self.session_id,
            'total_steps': len(self.steps),
            'total_decisions': len(self.decisions),
            'total_calculations': len(self.calculations),
            'total_anomalies': len(self.anomalies),
            'total_execution_time': time.time() - self.start_time,
            'start_time': self.start_time
        } 