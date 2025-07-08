"""Security utilities for ChatMRPT."""
import re
import secrets
import hashlib
from typing import List, Optional


class SQLSecurity:
    """SQL security utilities."""
    
    # Whitelist of allowed table names
    ALLOWED_TABLES = {
        'sessions', 'messages', 'file_uploads', 'analysis_events',
        'errors', 'analysis_steps', 'algorithm_decisions', 'calculations',
        'anomalies', 'variable_relationships', 'ward_rankings',
        'visualization_metadata', 'llm_interactions', 'explanations'
    }
    
    @classmethod
    def validate_table_name(cls, table_name: str) -> bool:
        """Validate table name against whitelist."""
        return table_name in cls.ALLOWED_TABLES
    
    @classmethod
    def validate_column_name(cls, column_name: str) -> bool:
        """Validate column name format."""
        # Allow only alphanumeric and underscore
        return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name))
    
    @classmethod
    def escape_identifier(cls, identifier: str) -> str:
        """Escape SQL identifier (table/column name)."""
        # For SQLite, use double quotes
        return f'"{identifier.replace('"', '""')}"'


def generate_secure_token(length: int = 32) -> str:
    """Generate a cryptographically secure token."""
    return secrets.token_hex(length)


def constant_time_compare(val1: str, val2: str) -> bool:
    """Compare two strings in constant time to prevent timing attacks."""
    return secrets.compare_digest(val1, val2)


def hash_password(password: str) -> str:
    """Hash a password using a secure algorithm."""
    # In production, use bcrypt or argon2
    # This is a simple example using SHA256
    salt = secrets.token_hex(16)
    pwd_hash = hashlib.sha256(f"{salt}{password}".encode()).hexdigest()
    return f"{salt}${pwd_hash}"


def verify_password(password: str, hashed: str) -> bool:
    """Verify a password against its hash."""
    try:
        salt, pwd_hash = hashed.split('$')
        test_hash = hashlib.sha256(f"{salt}{password}".encode()).hexdigest()
        return constant_time_compare(pwd_hash, test_hash)
    except (ValueError, AttributeError):
        return False


def sanitize_filename(filename: str) -> str:
    """Sanitize a filename to prevent path traversal attacks."""
    # Remove any path components
    filename = filename.replace('/', '').replace('\\', '')
    # Remove leading dots
    filename = filename.lstrip('.')
    # Limit length
    if len(filename) > 255:
        name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
        filename = f"{name[:240]}.{ext}" if ext else name[:255]
    return filename


def validate_file_type(filename: str, allowed_extensions: List[str]) -> bool:
    """Validate file extension against allowed list."""
    if not filename or '.' not in filename:
        return False
    ext = filename.rsplit('.', 1)[1].lower()
    return ext in allowed_extensions