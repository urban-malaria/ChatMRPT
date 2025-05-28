#!/usr/bin/env python3
"""
Main entry point for ChatMRPT application.

This script creates and runs the Flask application using the 
modern configuration and service architecture.
"""

import os
from app import create_app
from flask import redirect, url_for

# Get configuration name from environment
config_name = os.environ.get('FLASK_ENV', 'development')

# Create application with specified configuration
app = create_app(config_name)

# Add a route to redirect root to the Tailwind UI by default
@app.route('/')
def index_redirect():
    return redirect('/?use_tailwind=true')

if __name__ == '__main__':
    # Development server settings
    debug_mode = app.config.get('DEBUG', False)
    port = int(os.environ.get('PORT', 5000))
    host = os.environ.get('HOST', '127.0.0.1')
    
    print(f"🚀 Starting ChatMRPT v3.0")
    print(f"📊 Environment: {config_name}")
    print(f"🌐 URL: http://{host}:{port}")
    print(f"🔧 Debug Mode: {debug_mode}")
    
    app.run(
        host=host,
        port=port,
        debug=debug_mode
    ) 