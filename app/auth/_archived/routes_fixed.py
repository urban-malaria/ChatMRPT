"""Authentication routes for React frontend."""
from flask import Blueprint, jsonify, request, session
from flask_login import login_user, logout_user, login_required, current_user
from app.auth.models import User
import secrets

auth = Blueprint('auth', __name__, url_prefix='/auth')


@auth.route('/login', methods=['GET', 'POST'])
def login():
    """API endpoint for login - returns JSON for React."""
    if request.method == 'GET':
        # For GET requests, return status
        if current_user.is_authenticated:
            return jsonify({
                'authenticated': True,
                'user': 'admin',
                'redirect': '/dashboard'
            }), 200
        else:
            return jsonify({
                'authenticated': False,
                'message': 'Please login'
            }), 200

    # Handle POST login attempt
    data = request.get_json() or {}
    admin_key = data.get('admin_key', '')

    if User.check_admin_key(admin_key):
        user = User('admin')
        login_user(user, remember=False)

        # Generate CSRF token for session
        session['csrf_token'] = secrets.token_hex(16)

        return jsonify({
            'success': True,
            'message': 'Logged in successfully',
            'redirect': '/dashboard'
        }), 200
    else:
        return jsonify({
            'success': False,
            'message': 'Invalid admin key'
        }), 401


@auth.route('/register', methods=['GET', 'POST'])
def register():
    """Registration endpoint - returns JSON for React."""
    if request.method == 'GET':
        return jsonify({
            'message': 'Registration endpoint',
            'available': True
        }), 200

    # Handle registration (placeholder for now)
    return jsonify({
        'success': False,
        'message': 'Registration not implemented for admin system'
    }), 501


@auth.route('/logout')
@login_required
def logout():
    """Logout admin user."""
    logout_user()
    session.clear()
    return jsonify({
        'success': True,
        'message': 'You have been logged out',
        'redirect': '/'
    }), 200


@auth.route('/status')
def auth_status():
    """Check authentication status."""
    if current_user.is_authenticated:
        return jsonify({
            'authenticated': True,
            'user': current_user.id
        }), 200
    else:
        return jsonify({
            'authenticated': False
        }), 200