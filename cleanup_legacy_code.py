#!/usr/bin/env python
"""
Cleanup script for legacy code in the ChatMRPT project.
This script will identify and clean up legacy code, including ai_utils.py.
"""

import os
import sys
import re
import glob
import shutil
from datetime import datetime

# List of known legacy files and modules
LEGACY_FILES = [
    'ai_utils.py',
    'legacy_llm.py',
    'openai_utils.py',
    'old_llm_manager.py'
]

def find_files(pattern, path='.'):
    """Find files matching the pattern."""
    matches = []
    for root, _, files in os.walk(path):
        for filename in files:
            if any(fnmatch(filename, p) for p in ([pattern] if isinstance(pattern, str) else pattern)):
                matches.append(os.path.join(root, filename))
    return matches

def fnmatch(name, pattern):
    """Simple filename matching."""
    return name == pattern or name.endswith('/' + pattern)

def create_backup(file_path):
    """Create a backup of a file."""
    backup_dir = './backups'
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = os.path.basename(file_path)
    backup_path = os.path.join(backup_dir, f"{filename}.{timestamp}.bak")
    
    shutil.copy2(file_path, backup_path)
    print(f"Created backup: {backup_path}")
    return backup_path

def find_legacy_files():
    """Find all legacy files in the project."""
    found_files = []
    for legacy_file in LEGACY_FILES:
        for file_path in find_files(legacy_file):
            print(f"Found legacy file: {file_path}")
            found_files.append(file_path)
    return found_files

def find_legacy_imports():
    """Find files with imports from legacy modules."""
    py_files = find_files('*.py')
    legacy_imports = []
    
    legacy_patterns = [
        r'from\s+.*\.ai_utils\s+import',
        r'import\s+.*\.ai_utils',
        r'from\s+ai_utils\s+import',
        r'import\s+ai_utils',
        r'openai\.OpenAI\(.*proxies.*\)',
    ]
    
    for file_path in py_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Check for legacy patterns
            has_legacy_code = False
            for pattern in legacy_patterns:
                if re.search(pattern, content):
                    has_legacy_code = True
                    break
                    
            if has_legacy_code:
                print(f"Found file with legacy imports/code: {file_path}")
                legacy_imports.append(file_path)
        except (UnicodeDecodeError, IOError) as e:
            print(f"Error reading {file_path}: {e}")
    
    return legacy_imports

def cleanup_legacy_imports(file_path):
    """Clean up legacy imports and code in a file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Create a backup
        create_backup(file_path)
        
        # Replace legacy imports with modern equivalents
        modified_content = content
        
        # Replace legacy imports
        modified_content = re.sub(
            r'from\s+(.*\.)?ai_utils\s+import\s+(.*)',
            r'from app.core.llm_manager import \2  # Updated import',
            modified_content
        )
        
        modified_content = re.sub(
            r'import\s+(.*\.)?ai_utils(\s+as\s+.*)?',
            r'import app.core.llm_manager\2  # Updated import',
            modified_content
        )
        
        # Fix OpenAI initialization with proxies
        modified_content = re.sub(
            r'openai\.OpenAI\(api_key=([^,)]+)(?:,\s*proxies=[^)]+)?\)',
            r'openai.OpenAI(api_key=\1)',
            modified_content
        )
        
        # Write back changes if modified
        if modified_content != content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(modified_content)
            print(f"Fixed legacy imports in {file_path}")
            return True
        else:
            print(f"No changes needed in {file_path}")
            return False
    except Exception as e:
        print(f"Error fixing {file_path}: {e}")
        return False

def main():
    """Main entry point."""
    print("=" * 60)
    print("Legacy Code Cleanup")
    print("=" * 60)
    
    print("\nSearching for legacy files...")
    legacy_files = find_legacy_files()
    
    print("\nSearching for files with legacy imports...")
    legacy_imports = find_legacy_imports()
    
    if not legacy_files and not legacy_imports:
        print("\nNo legacy files or imports found. Your codebase is clean!")
        return
    
    print("\nCleaning up legacy imports...")
    cleanup_count = 0
    for file_path in legacy_imports:
        if cleanup_legacy_imports(file_path):
            cleanup_count += 1
    
    print(f"\nFixed legacy imports in {cleanup_count} files.")
    
    if legacy_files:
        print("\nWould you like to remove the following legacy files?")
        for i, file_path in enumerate(legacy_files):
            print(f"{i+1}. {file_path}")
        
        print("\nOptions:")
        print("1. Remove all legacy files (backup will be created)")
        print("2. Don't remove any files")
        
        choice = input("\nEnter your choice (1-2): ").strip()
        
        if choice == '1':
            print("\nRemoving legacy files...")
            for file_path in legacy_files:
                create_backup(file_path)
                os.remove(file_path)
                print(f"Removed: {file_path}")
            print(f"\nRemoved {len(legacy_files)} legacy files.")
        else:
            print("\nNo files were removed.")
    
    print("\nCleanup complete. Please restart your application.")
    print("=" * 60)

if __name__ == "__main__":
    main() 