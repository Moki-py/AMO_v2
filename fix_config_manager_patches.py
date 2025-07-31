#!/usr/bin/env python3
"""
Script to fix config_manager patch statements in test files.
"""

import re

def fix_config_manager_patches():
    """Fix config_manager patch statements in test_integration_final.py"""
    file_path = 'tests/test_integration_final.py'

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Replace the problematic patch pattern
        old_pattern = r"with patch\.object\(SheetsExporter, 'config_manager', mock_google_sheets_config\):"
        new_pattern = r"with patch('amocrm_exporter.exporters.sheets_exporter.GoogleSheetsConfigManager') as mock_config_class:\n                    mock_config_class.return_value = mock_google_sheets_config"

        content = re.sub(old_pattern, new_pattern, content)

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

        print(f"Fixed config_manager patches in {file_path}")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

if __name__ == '__main__':
    fix_config_manager_patches()