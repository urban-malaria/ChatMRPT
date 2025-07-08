#!/usr/bin/env python3
"""Generate a secure secret key for Flask."""
import secrets

def generate_secret_key():
    """Generate a cryptographically secure secret key."""
    return secrets.token_hex(32)

if __name__ == "__main__":
    key = generate_secret_key()
    print(f"Generated Secret Key: {key}")
    print("\nAdd this to your .env file:")
    print(f"SECRET_KEY={key}")
    print("\nNEVER commit this key to version control!")