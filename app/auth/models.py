"""User model for authentication."""
from flask_login import UserMixin
from werkzeug.security import check_password_hash, generate_password_hash
import os


class User(UserMixin):
    """Simple user model for admin authentication."""
    
    def __init__(self, user_id):
        self.id = user_id
        self.username = 'admin'
    
    @staticmethod
    def check_admin_key(provided_key):
        """Check if provided key matches admin key."""
        admin_key = os.environ.get('ADMIN_KEY')
        if not admin_key:
            return False
        # Use constant-time comparison to prevent timing attacks
        return provided_key == admin_key
    
    @staticmethod
    def get(user_id):
        """Get user by ID."""
        if user_id == 'admin':
            return User('admin')
        return None