#!/usr/bin/env python
"""
Diagnostic script to fix OpenAI initialization issues.
This script will identify OpenAI client initialization problems and fix them.
"""

import os
import sys
import importlib.util
import glob
import re

def check_openai_version():
    """Check the installed OpenAI version."""
    try:
        import openai
        print(f"OpenAI package version: {openai.__version__}")
        return openai.__version__
    except ImportError:
        print("OpenAI package is not installed.")
        return None
    except AttributeError:
        print("OpenAI package found but version information is not available.")
        return "unknown"

def check_api_key():
    """Check if the OpenAI API key is set."""
    api_key = os.environ.get('OPENAI_API_KEY')
    if api_key:
        print(f"OpenAI API key is set: {api_key[:5]}...{api_key[-4:]}")
    else:
        print("WARNING: OpenAI API key is not set in environment variables.")
    return api_key is not None

def find_legacy_file():
    """Find the legacy ai_utils.py file."""
    for path in glob.glob('app/**/*.py', recursive=True):
        if os.path.basename(path) == 'ai_utils.py':
            print(f"Found legacy file: {path}")
            return path
    
    # Try direct path from error message
    direct_path = 'app/ai_utils.py'
    if os.path.exists(direct_path):
        print(f"Found legacy file: {direct_path}")
        return direct_path
    
    print("Could not find legacy ai_utils.py file.")
    return None

def fix_legacy_file(file_path):
    """Fix the OpenAI initialization in the legacy file."""
    if not file_path or not os.path.exists(file_path):
        print("No file to fix.")
        return False
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Create backup
    backup_path = f"{file_path}.bak"
    with open(backup_path, 'w') as f:
        f.write(content)
    print(f"Created backup at {backup_path}")
    
    # Pattern to find the OpenAI initialization with proxies
    pattern = r"openai\.OpenAI\(api_key=([^,)]+)(?:,\s*proxies=[^)]+)?\)"
    
    # Replace with corrected version
    fixed_content = re.sub(pattern, r"openai.OpenAI(api_key=\1)", content)
    
    if fixed_content != content:
        with open(file_path, 'w') as f:
            f.write(fixed_content)
        print(f"Fixed OpenAI initialization in {file_path}")
        return True
    else:
        print("No OpenAI initialization with proxies parameter found.")
        return False

def main():
    """Main function."""
    print("=" * 50)
    print("OpenAI Configuration Diagnostic")
    print("=" * 50)
    
    openai_version = check_openai_version()
    api_key_set = check_api_key()
    
    if openai_version and float(openai_version.split('.')[0]) >= 1:
        print("\nUsing OpenAI API v1.x+ which doesn't support 'proxies' parameter.")
        
        legacy_file = find_legacy_file()
        if legacy_file:
            print("\nAttempting to fix legacy file...")
            fixed = fix_legacy_file(legacy_file)
            if fixed:
                print("\nFix applied successfully. Please restart your application.")
            else:
                print("\nNo fixes were needed or fix couldn't be applied.")
        else:
            print("\nCould not locate the legacy file causing the issue.")
            
            # Try to find any file with OpenAI and proxies
            print("\nSearching for any files using 'proxies' with OpenAI...")
            found = False
            for path in glob.glob('app/**/*.py', recursive=True):
                with open(path, 'r') as f:
                    content = f.read()
                if 'openai.OpenAI(' in content and 'proxies' in content:
                    print(f"Found potential issue in: {path}")
                    found = True
                    fix_legacy_file(path)
            
            if not found:
                print("No files found with OpenAI and proxies parameter.")
    
    print("\nDiagnostic complete.")
    print("=" * 50)

if __name__ == "__main__":
    main() 