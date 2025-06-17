"""
Additional routes for the application.
"""

from flask import send_from_directory, current_app, render_template, jsonify, request, redirect, url_for
import os
from datetime import datetime

def init_routes(app):
    """Initialize additional routes for the application."""
    
    @app.route('/favicon.ico')
    def favicon():
        """Serve the favicon."""
        return send_from_directory(
            os.path.join(app.root_path, 'static'),
            'favicon.ico', 
            mimetype='image/vnd.microsoft.icon'
        )
    
    @app.route('/robots.txt')
    def robots():
        """Serve robots.txt file."""
        return send_from_directory(
            os.path.join(app.root_path, 'static'),
            'robots.txt',
            mimetype='text/plain'
        )
    
    @app.route('/sitemap.xml')
    def sitemap():
        """Serve sitemap.xml file."""
        return send_from_directory(
            os.path.join(app.root_path, 'static'),
            'sitemap.xml',
            mimetype='application/xml'
        )
    
    @app.route('/system-health')
    def system_health():
        """Simple health check endpoint for monitoring."""
        return jsonify({
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
            'version': getattr(app, 'version', 'unknown')
        })
    
    @app.route('/ping')
    def ping():
        """Simple health check endpoint for simple monitoring."""
        return jsonify({'status': 'ok'})
    
    @app.route('/static/<path:filename>')
    def custom_static(filename):
        """
        Custom static file handler with improved caching.
        """
        cache_timeout = app.config.get('SEND_FILE_MAX_AGE_DEFAULT', 43200)
        return send_from_directory(
            os.path.join(app.root_path, 'static'),
            filename,
            cache_timeout=cache_timeout
        )
    
    @app.errorhandler(404)
    def page_not_found(e):
        """Handle 404 errors."""
        return render_template('errors/404.html'), 404
    
    @app.errorhandler(500)
    def server_error(e):
        """Handle 500 errors."""
        return render_template('errors/500.html'), 500
    
    @app.errorhandler(403)
    def forbidden(e):
        """Handle 403 errors."""
        return render_template('errors/403.html'), 403
    
    @app.route('/support')
    def support_redirect():
        """Redirect to support documentation."""
        return redirect('https://github.com/yourusername/ChatMRPT/wiki/support')
    
    @app.after_request
    def apply_security_headers(response):
        """Apply security headers from config to all responses."""
        if current_app.config.get('SECURITY_HEADERS'):
            for header, value in current_app.config.get('SECURITY_HEADERS').items():
                if header not in response.headers:
                    response.headers[header] = value
        return response 