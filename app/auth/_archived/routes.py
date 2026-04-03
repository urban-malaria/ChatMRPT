"""
Complete authentication system with signup/signin for React frontend
"""
from flask import Blueprint, jsonify, request, session
from flask_login import login_user, logout_user, current_user
from app.auth.user_model import User
import re

auth = Blueprint('auth', __name__, url_prefix='/auth')

# Email validation regex
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')


def validate_email(email):
    """Validate email format."""
    return EMAIL_REGEX.match(email) is not None


def validate_password(password):
    """Validate password strength."""
    if len(password) < 8:
        return False, "Password must be at least 8 characters"
    if not re.search(r'[A-Z]', password):
        return False, "Password must contain at least one uppercase letter"
    if not re.search(r'[a-z]', password):
        return False, "Password must contain at least one lowercase letter"
    if not re.search(r'[0-9]', password):
        return False, "Password must contain at least one number"
    return True, None


@auth.route('/signup', methods=['POST', 'OPTIONS'])
def signup():
    """User registration endpoint."""
    if request.method == 'OPTIONS':
        # Handle preflight
        return '', 204

    try:
        data = request.get_json()

        # Validate required fields
        email = data.get('email', '').strip()
        username = data.get('username', '').strip()
        password = data.get('password', '')

        if not email or not username or not password:
            return jsonify({
                'success': False,
                'message': 'Email, username, and password are required'
            }), 400

        # Validate email format
        if not validate_email(email):
            return jsonify({
                'success': False,
                'message': 'Invalid email format'
            }), 400

        # Validate username
        if len(username) < 3:
            return jsonify({
                'success': False,
                'message': 'Username must be at least 3 characters'
            }), 400

        if not re.match(r'^[a-zA-Z0-9_]+$', username):
            return jsonify({
                'success': False,
                'message': 'Username can only contain letters, numbers, and underscores'
            }), 400

        # Validate password strength
        password_valid, password_error = validate_password(password)
        if not password_valid:
            return jsonify({
                'success': False,
                'message': password_error
            }), 400

        # Create user
        user, error = User.create_user(email, username, password)

        if error:
            return jsonify({
                'success': False,
                'message': error
            }), 400

        # Create session token
        token = User.create_session_token(user.id)

        # Log the user in
        login_user(user)

        # Set session data
        session['user_id'] = user.id
        session['user_email'] = user.email
        session['auth_token'] = token

        return jsonify({
            'success': True,
            'message': 'Account created successfully',
            'user': {
                'id': user.id,
                'email': user.email,
                'username': user.username
            },
            'token': token
        }), 201

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Server error: {str(e)}'
        }), 500


@auth.route('/signin', methods=['POST', 'OPTIONS'])
@auth.route('/login', methods=['POST', 'OPTIONS'])
def signin():
    """User login endpoint."""
    if request.method == 'OPTIONS':
        return '', 204

    try:
        data = request.get_json()

        email = data.get('email', '').strip()
        password = data.get('password', '')

        if not email or not password:
            return jsonify({
                'success': False,
                'message': 'Email and password are required'
            }), 400

        # Authenticate user
        user, error = User.authenticate(email, password)

        if error:
            return jsonify({
                'success': False,
                'message': error
            }), 401

        # Create session token
        token = User.create_session_token(user.id)

        # Log the user in
        login_user(user)

        # Set session data
        session['user_id'] = user.id
        session['user_email'] = user.email
        session['auth_token'] = token

        return jsonify({
            'success': True,
            'message': 'Logged in successfully',
            'user': {
                'id': user.id,
                'email': user.email,
                'username': user.username
            },
            'token': token
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Server error: {str(e)}'
        }), 500


@auth.route('/logout', methods=['POST', 'GET'])
def logout():
    """Logout endpoint."""
    try:
        # Get token from session or headers
        token = session.get('auth_token') or request.headers.get('Authorization', '').replace('Bearer ', '')

        if token:
            User.logout(token)

        # Clear session
        logout_user()
        session.clear()

        return jsonify({
            'success': True,
            'message': 'Logged out successfully'
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Logout error: {str(e)}'
        }), 500


@auth.route('/me', methods=['GET'])
@auth.route('/status', methods=['GET'])
def get_current_user():
    """Get current authenticated user."""
    try:
        # Check session
        if current_user.is_authenticated:
            return jsonify({
                'success': True,
                'authenticated': True,
                'user': {
                    'id': current_user.id,
                    'email': current_user.email,
                    'username': current_user.username
                }
            }), 200

        # Check token in headers
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            token = auth_header.replace('Bearer ', '')
            user = User.verify_session_token(token)

            if user:
                return jsonify({
                    'success': True,
                    'authenticated': True,
                    'user': {
                        'id': user.id,
                        'email': user.email,
                        'username': user.username
                    }
                }), 200

        return jsonify({
            'success': True,
            'authenticated': False,
            'user': None
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Server error: {str(e)}'
        }), 500


@auth.route('/verify', methods=['POST'])
def verify_token():
    """Verify authentication token."""
    try:
        data = request.get_json()
        token = data.get('token', '')

        if not token:
            return jsonify({
                'success': False,
                'valid': False,
                'message': 'No token provided'
            }), 400

        user = User.verify_session_token(token)

        if user:
            return jsonify({
                'success': True,
                'valid': True,
                'user': {
                    'id': user.id,
                    'email': user.email,
                    'username': user.username
                }
            }), 200
        else:
            return jsonify({
                'success': True,
                'valid': False,
                'message': 'Invalid or expired token'
            }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Server error: {str(e)}'
        }), 500


# Keep the old register endpoint for compatibility but redirect to signup
@auth.route('/register', methods=['POST', 'OPTIONS'])
def register():
    """Legacy register endpoint - redirects to signup."""
    return signup()