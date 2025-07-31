#!/usr/bin/env python3
"""
Script to fix import statements in test files.
Replaces 'src.amocrm_exporter' with 'amocrm_exporter' in all test files.
"""

import os
import re
from pathlib import Path

def fix_imports_in_file(file_path):
    """Fix import statements in a single file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Replace src.amocrm_exporter with amocrm_exporter
        original_content = content
        content = re.sub(r'from src\.amocrm_exporter', 'from amocrm_exporter', content)
        content = re.sub(r'import src\.amocrm_exporter', 'import amocrm_exporter', content)

        # Also fix any remaining src.amocrm_exporter references
        content = re.sub(r'src\.amocrm_exporter', 'amocrm_exporter', content)

        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Fixed imports in: {file_path}")
            return True
        else:
            print(f"No changes needed in: {file_path}")
            return False

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Fix imports in all test files."""
    tests_dir = Path('tests')

    if not tests_dir.exists():
        print("Tests directory not found!")
        return

    fixed_count = 0
    total_count = 0

    # Process all Python files in tests directory
    for test_file in tests_dir.glob('*.py'):
        total_count += 1
        if fix_imports_in_file(test_file):
            fixed_count += 1

    print(f"\nProcessed {total_count} files, fixed {fixed_count} files.")

if __name__ == '__main__':
    main()