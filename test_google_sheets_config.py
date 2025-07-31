#!/usr/bin/env python3
"""
Test script for Google Sheets configuration management
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from amocrm_exporter.core.google_sheets_config import GoogleSheetsConfigManager

    def test_configuration():
        """Test Google Sheets configuration validation"""
        print("Testing Google Sheets Configuration Manager...")

        config_manager = GoogleSheetsConfigManager()

        # Test configuration validation
        print("\n1. Testing configuration validation...")
        validation_result = config_manager.validate_configuration()

        print(f"Configuration valid: {validation_result.is_valid}")

        if validation_result.errors:
            print("Errors:")
            for error in validation_result.errors:
                print(f"  - {error}")

        if validation_result.warnings:
            print("Warnings:")
            for warning in validation_result.warnings:
                print(f"  - {warning}")

        if validation_result.missing_configs:
            print("Missing configurations:")
            for config in validation_result.missing_configs:
                print(f"  - {config}")

        # Test configuration summary
        print("\n2. Testing configuration summary...")
        summary = config_manager.get_configuration_summary()

        print(f"Credentials file exists: {summary['credentials_file_exists']}")
        print(f"Token file exists: {summary['token_file_exists']}")

        print("Configured spreadsheets:")
        for entity_type, config in summary['configured_spreadsheets'].items():
            print(f"  {entity_type}: configured={config['configured']}, valid_format={config['valid_format']}")

        # Test setup guide
        print("\n3. Testing setup guide...")
        setup_guide = config_manager.setup_guided_configuration()

        print(f"Setup progress: {setup_guide['current_status']['completed_steps']}/{setup_guide['current_status']['total_steps']} steps completed")
        print(f"Progress: {setup_guide['current_status']['progress_percentage']:.1f}%")

        if setup_guide['next_actions']:
            print("Next actions:")
            for action in setup_guide['next_actions']:
                print(f"  - {action}")

        print("\nTest completed successfully!")
        return validation_result.is_valid

    if __name__ == "__main__":
        success = test_configuration()
        sys.exit(0 if success else 1)

except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure all dependencies are installed:")
    print("pip install -r requirements.txt")
    sys.exit(1)
except Exception as e:
    print(f"Error during testing: {e}")
    sys.exit(1)