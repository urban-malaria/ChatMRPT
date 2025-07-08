"""
Admin interface blueprint for ChatMRPT application.

This blueprint handles administrative routes for monitoring,
logs viewing, and system management.
"""

import json
import logging
from datetime import datetime
from flask import Blueprint, request, current_app, send_file, redirect, url_for
from flask_login import login_required, current_user
from io import StringIO

from ..core.decorators import handle_errors, log_execution_time

logger = logging.getLogger(__name__)

# Create the admin blueprint
admin_bp = Blueprint('admin', __name__, url_prefix='/admin')


@admin_bp.route('/', methods=['GET'])
@admin_bp.route('/dashboard', methods=['GET'])
@login_required
@handle_errors
def dashboard():
    """Admin dashboard homepage."""
    return {
        'message': 'Welcome to ChatMRPT Admin Dashboard',
        'user': current_user.username,
        'available_endpoints': [
            '/admin/logs - View system logs',
            '/admin/stats - System statistics',
            '/admin/export - Export data',
            '/admin/health - Health check'
        ]
    }


@admin_bp.route('/logs', methods=['GET'])
@login_required
@handle_errors
def logs():
    """
    Display interaction logs with pagination and filtering.
    
    Query Parameters:
        limit (int): Number of logs per page (default: 50)
        offset (int): Starting position (default: 0)
        session_id (str): Filter by session ID
        level (str): Filter by log level
    """
    # Get query parameters
    limit = min(int(request.args.get('limit', 50)), 1000)  # Cap at 1000
    offset = max(int(request.args.get('offset', 0)), 0)
    session_id_filter = request.args.get('session_id')
    level_filter = request.args.get('level')
    
    if not hasattr(current_app, 'services') or not current_app.services.interaction_logger:
        return {
            'error': 'Interaction logger not available',
            'logs': [],
            'total': 0
        }
    
    interaction_logger = current_app.services.interaction_logger
    
    try:
        # Get filtered logs
        logs = interaction_logger.get_logs(
            limit=limit,
            offset=offset,
            session_id=session_id_filter,
            level=level_filter
        )
        
        # Get total count for pagination
        total_count = interaction_logger.get_log_count(
            session_id=session_id_filter,
            level=level_filter
        )
        
        return {
            'logs': logs,
            'total': total_count,
            'limit': limit,
            'offset': offset,
            'has_more': (offset + limit) < total_count
        }
        
    except Exception as e:
        logger.error(f"Error retrieving logs: {str(e)}")
        return {
            'error': f'Error retrieving logs: {str(e)}',
            'logs': [],
            'total': 0
        }, 500


@admin_bp.route('/session/<session_id>', methods=['GET'])
@login_required
@handle_errors
def session_detail(session_id):
    """
    Get detailed information about a specific session.
    
    Args:
        session_id: The session ID to retrieve details for
        
    Returns:
        JSON with session details including interactions, uploads, etc.
    """
    if not hasattr(current_app, 'services') or not current_app.services.interaction_logger:
        return {'error': 'Interaction logger not available'}, 500
    
    interaction_logger = current_app.services.interaction_logger
    
    try:
        # Get session details
        session_info = interaction_logger.get_session_details(session_id)
        
        if not session_info:
            return {'error': f'Session {session_id} not found'}, 404
        
        # Get interactions for this session
        interactions = interaction_logger.get_session_interactions(session_id)
        
        # Get file uploads for this session
        uploads = interaction_logger.get_session_uploads(session_id)
        
        # Get analysis events for this session
        analyses = interaction_logger.get_session_analyses(session_id)
        
        return {
            'session_info': session_info,
            'interactions': interactions,
            'uploads': uploads,
            'analyses': analyses,
            'summary': {
                'total_interactions': len(interactions),
                'total_uploads': len(uploads),
                'total_analyses': len(analyses),
                'session_duration': session_info.get('duration_minutes', 0)
            }
        }
        
    except Exception as e:
        logger.error(f"Error retrieving session details: {str(e)}")
        return {'error': f'Error retrieving session details: {str(e)}'}, 500


@admin_bp.route('/export', methods=['GET'])
@login_required
@handle_errors
def export_logs():
    """
    Export logs in various formats (JSON, CSV).
    
    Query Parameters:
        format (str): Export format ('json' or 'csv', default: 'json')
        days (int): Number of days back to export (default: 7)
        session_id (str): Export logs for specific session only
    """
    export_format = request.args.get('format', 'json').lower()
    days_back = int(request.args.get('days', 7))
    session_id_filter = request.args.get('session_id')
    
    if export_format not in ['json', 'csv']:
        return {'error': 'Invalid format. Use "json" or "csv"'}, 400
    
    if not hasattr(current_app, 'services') or not current_app.services.interaction_logger:
        return {'error': 'Interaction logger not available'}, 500
    
    interaction_logger = current_app.services.interaction_logger
    
    try:
        # Calculate date range
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Get logs for export
        logs = interaction_logger.get_logs_for_export(
            start_date=start_date,
            end_date=end_date,
            session_id=session_id_filter
        )
        
        if export_format == 'json':
            # Return JSON response
            return {
                'export_info': {
                    'format': 'json',
                    'date_range': f'{start_date.isoformat()} to {end_date.isoformat()}',
                    'total_records': len(logs),
                    'session_filter': session_id_filter
                },
                'logs': logs
            }
        
        elif export_format == 'csv':
            # Create CSV content
            import csv
            from io import StringIO
            
            output = StringIO()
            
            if logs:
                fieldnames = logs[0].keys()
                writer = csv.DictWriter(output, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(logs)
            
            # Create response
            from flask import Response
            response = Response(
                output.getvalue(),
                mimetype='text/csv',
                headers={
                    'Content-Disposition': f'attachment; filename=chatmrpt_logs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                }
            )
            return response
            
    except Exception as e:
        logger.error(f"Error exporting logs: {str(e)}")
        return {'error': f'Error exporting logs: {str(e)}'}, 500


@admin_bp.route('/training_data', methods=['GET'])
@login_required
@handle_errors
def export_training_data():
    """
    Export interactions in a format suitable for LLM training.
    
    This creates a dataset of user messages and system responses
    that could be used for fine-tuning or training purposes.
    """
    if not hasattr(current_app, 'services') or not current_app.services.interaction_logger:
        return {'error': 'Interaction logger not available'}, 500
    
    interaction_logger = current_app.services.interaction_logger
    
    try:
        # Get training data format interactions
        training_data = interaction_logger.get_training_data()
        
        return {
            'training_data': training_data,
            'metadata': {
                'total_conversations': len(training_data),
                'export_date': datetime.now().isoformat(),
                'format': 'chat_format'
            }
        }
        
    except Exception as e:
        logger.error(f"Error exporting training data: {str(e)}")
        return {'error': f'Error exporting training data: {str(e)}'}, 500


@admin_bp.route('/stats', methods=['GET'])
@login_required
@handle_errors
def system_stats():
    """
    Get system statistics and metrics.
    
    Returns:
        JSON with system performance and usage statistics
    """
    if not hasattr(current_app, 'services') or not current_app.services.interaction_logger:
        return {'error': 'Interaction logger not available'}, 500
    
    interaction_logger = current_app.services.interaction_logger
    
    try:
        # Get various statistics
        stats = {
            'system': {
                'uptime': 'N/A',  # Would need to track app start time
                'version': '2.0',
                'environment': current_app.config.get('ENV', 'unknown')
            },
            'usage': interaction_logger.get_usage_stats(),
            'performance': {
                'avg_response_time': interaction_logger.get_avg_response_time(),
                'error_rate': interaction_logger.get_error_rate()
            },
            'content': {
                'total_sessions': interaction_logger.get_total_sessions(),
                'total_analyses': interaction_logger.get_total_analyses(),
                'total_uploads': interaction_logger.get_total_uploads()
            }
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Error retrieving system stats: {str(e)}")
        return {'error': f'Error retrieving system stats: {str(e)}'}, 500


@admin_bp.route('/health', methods=['GET'])
@login_required
@handle_errors
def health_check():
    """
    Perform a comprehensive health check of all system components.
    
    Returns:
        JSON with health status of services and dependencies
    """
    if hasattr(current_app, 'services'):
        return current_app.services.health_check()
    else:
        return {
            'status': 'degraded',
            'message': 'Service container not available',
            'services': {}
        } 