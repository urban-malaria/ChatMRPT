#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main entry point for ChatMRPT application.

This script creates and runs the Flask application using the 
modern configuration and service architecture.
"""

import os
import sys
from app import create_app
from flask import redirect, url_for

# Production-ready startup with intelligent caching
if '--disable-tool-scoring' in sys.argv:
    os.environ['DISABLE_TOOL_SCORING'] = 'true'
    print("🚫 Tool scoring disabled for faster startup")

# Get configuration name from environment
config_name = os.environ.get('FLASK_ENV', 'development')

# Check for OpenAI API key
openai_api_key = os.environ.get('OPENAI_API_KEY')
if not openai_api_key:
    print("WARNING: OPENAI_API_KEY environment variable not set.")
    print("    The application may not function correctly without an API key.")
    print("    You can set it by running: export OPENAI_API_KEY=your_key_here")
    
    # For Windows users
    if sys.platform.startswith('win'):
        print("    On Windows, use: set OPENAI_API_KEY=your_key_here")

# Create application with specified configuration
app = create_app(config_name)

# Add a route to redirect root to the Tailwind UI by default
@app.route('/')
def index_redirect():
    return redirect('/?use_tailwind=true')

if __name__ == '__main__':
    # Development server settings
    debug_mode = app.config.get('DEBUG', False)
    port = int(os.environ.get('PORT', 5230))
    host = os.environ.get('HOST', '127.0.0.1')
    
    print("Starting ChatMRPT v3.0")
    print("Environment: " + str(config_name))
    print("URL: http://" + str(host) + ":" + str(port))
    print("Debug Mode: " + str(debug_mode))
    
    tool_scoring = os.environ.get('DISABLE_TOOL_SCORING', 'false') != 'true'
    print("Tool Scoring: " + str(tool_scoring))
    
    if openai_api_key:
        print("API Key: Configured [OK]")
    else:
        print("API Key: Not configured [MISSING]")
    
    app.run(
        host=host,
        port=port,
        debug=debug_mode
    ) 