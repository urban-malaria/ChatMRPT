"""
Enhanced Debug Logger for ChatMRPT Workflow Tracking

This module provides comprehensive logging utilities to track:
- User actions and API calls
- Backend route hits
- Tool executions
- Workflow state transitions
- Analysis pipeline stages
"""

import logging
import json
import time
from typing import Dict, Any, Optional
from datetime import datetime
from functools import wraps
from flask import session, request

logger = logging.getLogger(__name__)


class WorkflowDebugger:
    """Central debugging utility for tracking workflow state"""
    
    # Emoji prefixes for different components
    PREFIXES = {
        'frontend': '🎯 FRONTEND',
        'backend': '🔧 BACKEND',
        'analysis': '📊 ANALYSIS',
        'tpr': '🔄 TPR',
        'itn': '🛏️ ITN',
        'tool': '⚡ TOOL',
        'error': '❌ ERROR',
        'success': '✅ SUCCESS',
        'warning': '⚠️ WARNING',
        'session': '🆔 SESSION',
        'data': '📂 DATA',
        'route': '🌐 ROUTE',
        'state': '📊 STATE'
    }
    
    @staticmethod
    def log_request_start(endpoint: str, method: str = 'POST'):
        """Log the start of an API request"""
        logger.info("=" * 60)
        logger.info(f"{WorkflowDebugger.PREFIXES['route']}: {method} {endpoint}")
        logger.info(f"  ⏰ Timestamp: {datetime.now().isoformat()}")
        
        # Log request data
        if request.json:
            logger.info(f"  📝 Request Data:")
            for key, value in request.json.items():
                if key == 'message':
                    logger.info(f"    - {key}: {str(value)[:100]}...")
                else:
                    logger.info(f"    - {key}: {value}")
        
        # Log session state
        WorkflowDebugger.log_session_state()
        logger.info("=" * 60)
    
    @staticmethod
    def log_session_state():
        """Log current session state"""
        logger.info(f"{WorkflowDebugger.PREFIXES['session']}: Current Session State")
        logger.info(f"  🆔 Session ID: {session.get('session_id', 'NO SESSION')}")
        logger.info(f"  📊 Analysis Complete: {session.get('analysis_complete', False)}")
        logger.info(f"  📂 Data Loaded: {session.get('data_loaded', False)}")
        logger.info(f"  🛏️ ITN Planning Complete: {session.get('itn_planning_complete', False)}")
        logger.info(f"  🔄 TPR Workflow Complete: {session.get('tpr_workflow_complete', False)}")
        logger.info(f"  🎯 Current Workflow: {session.get('current_workflow', 'None')}")
        logger.info(f"  📝 Session Keys: {list(session.keys())}")
    
    @staticmethod
    def log_tool_execution(tool_name: str, params: Dict[str, Any], session_id: str):
        """Log tool execution with parameters"""
        logger.info("-" * 40)
        logger.info(f"{WorkflowDebugger.PREFIXES['tool']}: {tool_name}")
        logger.info(f"  🆔 Session ID: {session_id}")
        logger.info(f"  📊 Parameters:")
        for key, value in params.items():
            if isinstance(value, (list, dict)):
                logger.info(f"    - {key}: {json.dumps(value, indent=2)}")
            else:
                logger.info(f"    - {key}: {value}")
        logger.info("-" * 40)
    
    @staticmethod
    def log_workflow_transition(from_stage: str, to_stage: str, trigger: str = None):
        """Log workflow state transitions"""
        logger.info(f"{WorkflowDebugger.PREFIXES['state']}: Workflow Transition")
        logger.info(f"  📍 From: {from_stage}")
        logger.info(f"  ➡️ To: {to_stage}")
        if trigger:
            logger.info(f"  🎯 Trigger: {trigger}")
    
    @staticmethod
    def log_analysis_step(step_name: str, data: Dict[str, Any] = None):
        """Log individual analysis pipeline steps"""
        logger.info(f"{WorkflowDebugger.PREFIXES['analysis']}: {step_name}")
        if data:
            for key, value in data.items():
                if isinstance(value, (list, dict)) and len(str(value)) > 100:
                    logger.info(f"  - {key}: [Complex data - {type(value).__name__}]")
                else:
                    logger.info(f"  - {key}: {value}")
    
    @staticmethod
    def log_error(component: str, error: Exception, context: Dict[str, Any] = None):
        """Log errors with context"""
        logger.error(f"{WorkflowDebugger.PREFIXES['error']}: {component}")
        logger.error(f"  💥 Error: {str(error)}")
        logger.error(f"  📍 Type: {type(error).__name__}")
        if context:
            logger.error(f"  📊 Context:")
            for key, value in context.items():
                logger.error(f"    - {key}: {value}")
    
    @staticmethod
    def log_data_operation(operation: str, details: Dict[str, Any]):
        """Log data operations (load, save, transform)"""
        logger.info(f"{WorkflowDebugger.PREFIXES['data']}: {operation}")
        for key, value in details.items():
            logger.info(f"  - {key}: {value}")


def debug_route(f):
    """Decorator to automatically log route execution"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        start_time = time.time()
        route_name = f.__name__
        
        # Log start
        WorkflowDebugger.log_request_start(request.path, request.method)
        
        try:
            # Execute the route
            result = f(*args, **kwargs)
            
            # Log success
            duration = time.time() - start_time
            logger.info(f"{WorkflowDebugger.PREFIXES['success']}: {route_name} completed in {duration:.2f}s")
            
            return result
            
        except Exception as e:
            # Log error
            duration = time.time() - start_time
            WorkflowDebugger.log_error(route_name, e, {
                'duration': duration,
                'args': args,
                'kwargs': kwargs
            })
            raise
    
    return decorated_function


def debug_tool(f):
    """Decorator to automatically log tool execution"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        tool_name = f.__name__
        start_time = time.time()
        
        # Extract session_id if available
        session_id = kwargs.get('session_id', 'Unknown')
        
        # Log tool start
        WorkflowDebugger.log_tool_execution(tool_name, kwargs, session_id)
        
        try:
            # Execute the tool
            result = f(*args, **kwargs)
            
            # Log success
            duration = time.time() - start_time
            logger.info(f"{WorkflowDebugger.PREFIXES['success']}: {tool_name} completed in {duration:.2f}s")
            
            return result
            
        except Exception as e:
            # Log error
            duration = time.time() - start_time
            WorkflowDebugger.log_error(f"Tool: {tool_name}", e, {
                'duration': duration,
                'session_id': session_id
            })
            raise
    
    return decorated_function


# Singleton instance
workflow_debugger = WorkflowDebugger()


# Export commonly used functions
log_request_start = workflow_debugger.log_request_start
log_session_state = workflow_debugger.log_session_state
log_tool_execution = workflow_debugger.log_tool_execution
log_workflow_transition = workflow_debugger.log_workflow_transition
log_analysis_step = workflow_debugger.log_analysis_step
log_error = workflow_debugger.log_error
log_data_operation = workflow_debugger.log_data_operation