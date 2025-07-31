"""
Example script demonstrating preset integration with the AmoCRM exporter

This script shows how to:
1. Create and manage export presets
2. Use presets for customized Google Sheets exports
3. Handle advanced preset features like custom field mappings
4. Integrate presets with the export workflow

Usage:
    python examples/preset_integration_demo.py
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Import the necessary components
from src.amocrm_exporter.storage.storage import Storage
from src.amocrm_exporter.web.export_presets import ExportPresetManager, ExportPreset
from src.amocrm_exporter.exporters.sheets_exporter import SheetsExporter
from src.amocrm_exporter.core.logger import log_event


class PresetIntegrationDemo:
    """
    Demonstrates how to integrate export presets with the AmoCRM exporter

    This class provides a comprehensive example of:
    - Creating and managing export presets
    - Using presets for customized exports
    - Advanced preset features and validation
    - Error handling and best practices
    """

    def __init__(self):
        """Initialize the demo with required components"""
        self.storage = Storage()
        self.preset_manager = ExportPresetManager(self.storage)
        self.sheets_exporter = SheetsExporter(self.storage)
        print("✓ Initialized preset integration demo components")

    async def create_sample_presets(self) -> Dict[str, str]:
        """
        Create sample presets for different entity types

        Returns:
            Dict[str, str]: Dictionary mapping entity types to preset IDs
        """
        print("\n=== Creating Sample Export Presets ===")

        # Sample preset configurations for different use cases
        preset_configs = [
            {
                "name": "Sales Pipeline Report",
                "entity_type": "deals",
                "description": "Essential deal information for sales pipeline analysis",
                "selected_fields": [
                    "id", "name", "price", "status_id", "pipeline_id",
                    "responsible_user_id", "created_at", "updated_at", "closed_at",
                    "loss_reason_id", "source_id"
                ],
                "use_case": "Sales reporting and pipeline analysis"
            },
            {
                "name": "Marketing Contact List",
                "entity_type": "contacts",
                "description": "Contact information optimized for marketing campaigns",
                "selected_fields": [
                    "id", "name", "first_name", "last_name", "responsible_user_id",
                    "created_at", "updated_at", "phone", "email", "tags"
                ],
                "use_case": "Marketing campaigns and outreach"
            },
            {
                "name": "Company Directory",
                "entity_type": "companies",
                "description": "Company data for business development and partnerships",
                "selected_fields": [
                    "id", "name", "responsible_user_id", "created_at", "updated_at",
                    "phone", "email", "web", "address"
                ],
                "use_case": "Business development and partner management"
            },
            {
                "name": "Team Performance",
                "entity_type": "users",
                "description": "User information for team performance tracking",
                "selected_fields": [
                    "id", "name", "email", "role", "group", "is_active", "created_at"
                ],
                "use_case": "Team management and performance analysis"
            }
        ]

        created_presets = {}

        for config in preset_configs:
            try:
                # Create preset object with validation
                preset = ExportPreset(
                    name=config["name"],
                    entity_type=config["entity_type"],
                    description=config["description"],
                    selected_fields=config["selected_fields"],
                    field_order=config["selected_fields"],  # Use same order as selection
                    custom_field_mappings={}
                )

                # Validate preset before saving
                validation_errors = preset.validate()
                if validation_errors:
                    print(f"⚠ Validation warnings for '{config['name']}': {', '.join(validation_errors)}")

                # Save preset
                preset_id = self.preset_manager.save_preset(preset)
                created_presets[config["entity_type"]] = preset_id

                print(f"✓ Created '{config['name']}' preset")
                print(f"  Entity: {config['entity_type']}")
                print(f"  Fields: {len(config['selected_fields'])}")
                print(f"  Use case: {config['use_case']}")
                print(f"  ID: {preset_id}")
                print()

            except Exception as e:
                print(f"✗ Failed to create preset '{config['name']}': {e}")

        print(f"Created {len(created_presets)} presets successfully")
        return created_presets

    async def demonstrate_preset_management(self):
        """Demonstrate comprehensive preset management operations"""
        print("\n=== Preset Management Operations Demo ===")

        # Create sample presets
        preset_ids = await self.create_sample_presets()

        # List all presets
        print("\n--- Listing All Presets ---")
        all_presets = self.preset_manager.list_presets()
        print(f"Total presets in system: {len(all_presets)}")

        for preset in all_presets:
            print(f"• {preset.name}")
            print(f"  Entity: {preset.entity_type}")
            print(f"  Fields: {len(preset.selected_fields)}")
            print(f"  Created: {preset.created_at.strftime('%Y-%m-%d %H:%M')}")
            if preset.description:
                print(f"  Description: {preset.description}")
            print()

        # Demonstrate filtering by entity type
        print("--- Filtering Presets by Entity Type ---")
        for entity_type in ["deals", "contacts", "companies"]:
            entity_presets = self.preset_manager.list_presets(entity_type)
            print(f"{entity_type.capitalize()}: {len(entity_presets)} presets")

        # Demonstrate preset loading and inspection
        if preset_ids:
            first_preset_id = list(preset_ids.values())[0]
            print(f"\n--- Detailed Preset Inspection ---")
            print(f"Loading preset with ID: {first_preset_id}")

            loaded_preset = self.preset_manager.load_preset(first_preset_id)
            if loaded_preset:
                print(f"✓ Successfully loaded: {loaded_preset.name}")
                print(f"  Entity type: {loaded_preset.entity_type}")
                print(f"  Selected fields ({len(loaded_preset.selected_fields)}):")
                for i, field in enumerate(loaded_preset.selected_fields, 1):
                    print(f"    {i}. {field}")
                print(f"  Field order: {loaded_preset.field_order[:3]}...")
                print(f"  Custom mappings: {len(loaded_preset.custom_field_mappings)}")
            else:
                print("✗ Failed to load preset")

        # Demonstrate preset duplication
        if preset_ids and "deals" in preset_ids:
            original_id = preset_ids["deals"]
            print(f"\n--- Preset Duplication ---")
            print(f"Duplicating deals preset: {original_id}")

            duplicate_id = self.preset_manager.duplicate_preset(original_id, "Sales Pipeline Report (Copy)")
            if duplicate_id:
                print(f"✓ Created duplicate with ID: {duplicate_id}")

                # Verify the duplicate
                duplicate_preset = self.preset_manager.load_preset(duplicate_id)
                if duplicate_preset:
                    print(f"  Duplicate name: {duplicate_preset.name}")
                    print(f"  Same fields: {len(duplicate_preset.selected_fields)} fields")
            else:
                print("✗ Failed to duplicate preset")

        return preset_ids

    async def demonstrate_preset_export(self, preset_ids: Dict[str, str]):
        """
        Demonstrate exporting with presets

        Args:
            preset_ids: Dictionary mapping entity types to preset IDs
        """
        print("\n=== Preset-based Export Demo ===")

        if not preset_ids:
            print("No presets available for export demo")
            return

        try:
            # Load presets for export
            presets = {}
            print("Loading presets for export...")

            for entity_type, preset_id in preset_ids.items():
                preset = self.preset_manager.load_preset(preset_id)
                if preset:
                    presets[entity_type] = preset
                    print(f"✓ Loaded {entity_type} preset: {preset.name}")
                    print(f"  Will export {len(preset.selected_fields)} fields")
                else:
                    print(f"✗ Failed to load preset for {entity_type}")

            if not presets:
                print("✗ No presets could be loaded for export")
                return

            # Set date range for last 30 days (configurable)
            date_to = datetime.now()
            date_from = date_to - timedelta(days=30)

            print(f"\n--- Export Configuration ---")
            print(f"Date range: {date_from.date()} to {date_to.date()}")
            print(f"Entities to export: {', '.join(presets.keys())}")
            print(f"Total presets: {len(presets)}")

            # Display what will be exported
            print(f"\n--- Export Preview ---")
            for entity_type, preset in presets.items():
                print(f"{entity_type.capitalize()}:")
                print(f"  Preset: {preset.name}")
                print(f"  Fields: {', '.join(preset.selected_fields[:5])}...")
                if len(preset.selected_fields) > 5:
                    print(f"  ... and {len(preset.selected_fields) - 5} more fields")

            # Start export with presets
            print(f"\n--- Starting Export ---")
            print("This may take a few minutes depending on data size...")

            export_results = await self.sheets_exporter.export_with_presets(
                presets=presets,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat()
            )

            # Display results
            if export_results:
                print("\n✓ Export completed successfully!")
                print("\n--- Export Results ---")
                for entity_type, url in export_results.items():
                    preset_name = presets[entity_type].name
                    print(f"{entity_type.capitalize()}:")
                    print(f"  Preset used: {preset_name}")
                    print(f"  Spreadsheet: {url}")
                    print()

                print("You can now open these spreadsheets to view your customized exports!")
            else:
                print("✗ Export completed but no results returned")

        except Exception as e:
            print(f"✗ Export failed: {e}")
            log_event("demo", "error", f"Preset export demo failed: {e}")

            # Provide troubleshooting guidance
            print("\n--- Troubleshooting Tips ---")
            print("• Check Google Sheets configuration")
            print("• Verify spreadsheet IDs are correct")
            print("• Ensure proper authentication")
            print("• Check network connectivity")

    async def demonstrate_advanced_preset_features(self):
        """Demonstrate advanced preset features and customization"""
        print("\n=== Advanced Preset Features Demo ===")

        try:
            # Create a preset with custom field mappings
            print("--- Custom Field Mapping ---")
            advanced_preset = ExportPreset(
                name="Advanced Deals Analysis",
                entity_type="deals",
                description="Deals export with custom field mappings and optimized field ordering",
                selected_fields=[
                    "id", "name", "price", "status_id", "pipeline_id",
                    "custom_field_123", "custom_field_456", "custom_field_789"
                ],
                field_order=[
                    "name", "price", "status_id", "pipeline_id", "id",
                    "custom_field_123", "custom_field_456", "custom_field_789"
                ],
                custom_field_mappings={
                    "custom_field_123": "Lead Source",
                    "custom_field_456": "Deal Priority",
                    "custom_field_789": "Expected Close Date"
                }
            )

            preset_id = self.preset_manager.save_preset(advanced_preset)
            print(f"✓ Created advanced preset with ID: {preset_id}")

            # Demonstrate custom field mapping usage
            print("\n--- Custom Field Mappings ---")
            for field_id, display_name in advanced_preset.custom_field_mappings.items():
                print(f"  {field_id} → '{display_name}'")

            # Demonstrate field ordering
            print(f"\n--- Field Ordering ---")
            print("Fields will appear in this order in the export:")
            for i, field in enumerate(advanced_preset.field_order, 1):
                display_name = advanced_preset.get_display_name(field)
                print(f"  {i}. {display_name}")

            # Demonstrate preset validation
            print(f"\n--- Preset Validation ---")
            validation_errors = advanced_preset.validate()
            if validation_errors:
                print(f"⚠ Validation issues found:")
                for error in validation_errors:
                    print(f"  • {error}")
            else:
                print("✓ Preset validation passed successfully")

            # Demonstrate preset modification
            print(f"\n--- Dynamic Preset Modification ---")
            print("Adding new fields to existing preset...")

            original_field_count = len(advanced_preset.selected_fields)
            advanced_preset.update_fields(
                selected_fields=advanced_preset.selected_fields + ["responsible_user_id", "created_at"],
                field_order=advanced_preset.field_order + ["responsible_user_id", "created_at"]
            )

            print(f"✓ Updated preset: {original_field_count} → {len(advanced_preset.selected_fields)} fields")

            # Add new custom field mapping
            advanced_preset.add_custom_field_mapping("custom_field_999", "Deal Category")
            print("✓ Added new custom field mapping: custom_field_999 → 'Deal Category'")

            # Save updated preset
            updated_id = self.preset_manager.save_preset(advanced_preset)
            print(f"✓ Saved updated preset with ID: {updated_id}")

            # Demonstrate preset summary
            print(f"\n--- Preset Summary ---")
            summary = self.preset_manager.get_preset_summary(preset_id)
            if summary:
                print(f"Name: {summary['name']}")
                print(f"Entity: {summary['entity_type']}")
                print(f"Fields: {summary['field_count']}")
                print(f"Created: {summary['created_at']}")
                print(f"Updated: {summary['updated_at']}")

        except Exception as e:
            print(f"✗ Advanced preset demo failed: {e}")
            log_event("demo", "error", f"Advanced preset demo failed: {e}")

    async def demonstrate_error_handling(self):
        """Demonstrate error handling and validation scenarios"""
        print("\n=== Error Handling and Validation Demo ===")

        # Test invalid preset creation
        print("--- Testing Invalid Preset Creation ---")
        try:
            invalid_preset = ExportPreset(
                name="",  # Invalid: empty name
                entity_type="invalid_entity",  # Invalid: unsupported entity type
                description="This preset should fail validation",
                selected_fields=[],  # Invalid: no fields selected
                field_order=["field1", "field2"],  # Invalid: mismatch with selected_fields
            )

            validation_errors = invalid_preset.validate()
            print(f"✓ Validation caught {len(validation_errors)} errors:")
            for error in validation_errors:
                print(f"  • {error}")

        except Exception as e:
            print(f"✓ Exception caught during invalid preset creation: {e}")

        # Test duplicate name handling
        print("\n--- Testing Duplicate Name Handling ---")
        try:
            # Create first preset
            preset1 = ExportPreset(
                name="Duplicate Test Preset",
                entity_type="deals",
                description="First preset with this name",
                selected_fields=["id", "name"],
                field_order=["id", "name"]
            )
            preset1_id = self.preset_manager.save_preset(preset1)
            print(f"✓ Created first preset: {preset1_id}")

            # Try to create second preset with same name and entity type
            preset2 = ExportPreset(
                name="Duplicate Test Preset",  # Same name
                entity_type="deals",  # Same entity type
                description="Second preset with duplicate name",
                selected_fields=["id", "price"],
                field_order=["id", "price"]
            )

            try:
                preset2_id = self.preset_manager.save_preset(preset2)
                print(f"✗ Unexpected: duplicate preset was created: {preset2_id}")
            except ValueError as e:
                print(f"✓ Duplicate name properly rejected: {e}")

        except Exception as e:
            print(f"Error in duplicate name test: {e}")

        # Test loading non-existent preset
        print("\n--- Testing Non-existent Preset Loading ---")
        fake_id = "507f1f77bcf86cd799439011"  # Valid ObjectId format but non-existent
        result = self.preset_manager.load_preset(fake_id)
        if result is None:
            print(f"✓ Non-existent preset properly returned None")
        else:
            print(f"✗ Unexpected: non-existent preset returned data")

    async def cleanup_demo_presets(self):
        """Clean up presets created during the demo"""
        print("\n=== Cleanup Demo Presets ===")

        # List all presets and delete demo ones
        all_presets = self.preset_manager.list_presets()
        demo_preset_names = [
            "Sales Pipeline Report", "Marketing Contact List", "Company Directory",
            "Team Performance", "Sales Pipeline Report (Copy)", "Advanced Deals Analysis",
            "Duplicate Test Preset"
        ]

        deleted_count = 0
        for preset in all_presets:
            if preset.name in demo_preset_names:
                success = self.preset_manager.delete_preset(preset.preset_id)
                if success:
                    print(f"✓ Deleted preset: {preset.name}")
                    deleted_count += 1
                else:
                    print(f"✗ Failed to delete preset: {preset.name}")

        print(f"\nCleanup completed. Deleted {deleted_count} demo presets.")

    async def run_full_demo(self, include_export: bool = True, cleanup_after: bool = False):
        """
        Run the complete preset integration demonstration

        Args:
            include_export: Whether to include the actual export demo (requires Google Sheets setup)
            cleanup_after: Whether to clean up demo presets after completion
        """
        print("=" * 60)
        print("AmoCRM Export Preset Integration Demo")
        print("=" * 60)
        print("This demo demonstrates the complete preset system including:")
        print("• Preset creation and management")
        print("• Advanced preset features")
        print("• Error handling and validation")
        if include_export:
            print("• Preset-based Google Sheets export")
        print()

        try:
            # Step 1: Demonstrate preset management
            preset_ids = await self.demonstrate_preset_management()

            # Step 2: Demonstrate preset-based export (optional)
            if include_export and preset_ids:
                await self.demonstrate_preset_export(preset_ids)
            elif not include_export:
                print("\n(Skipping export demo - set include_export=True to enable)")

            # Step 3: Demonstrate advanced features
            await self.demonstrate_advanced_preset_features()

            # Step 4: Demonstrate error handling
            await self.demonstrate_error_handling()

            # Summary
            print("\n" + "=" * 60)
            print("Demo Summary")
            print("=" * 60)
            print("✓ Preset creation and management")
            print("✓ Advanced preset features (custom mappings, field ordering)")
            print("✓ Error handling and validation")
            if include_export:
                print("✓ Preset-based Google Sheets export")
            print("✓ Comprehensive error scenarios")
            print("\nThe preset system is ready for production use!")

        except Exception as e:
            print(f"\n✗ Demo failed with error: {e}")
            log_event("demo", "error", f"Full demo failed: {e}")
            raise

        finally:
            # Optional cleanup
            if cleanup_after:
                await self.cleanup_demo_presets()


async def main():
    """
    Main function to run the preset integration demo

    Configure the demo behavior by modifying these parameters:
    - include_export: Set to False if Google Sheets is not configured
    - cleanup_after: Set to True to remove demo presets after completion
    """
    demo = PresetIntegrationDemo()

    # Configure demo options
    include_export = False  # Set to True if Google Sheets is configured
    cleanup_after = False   # Set to True to clean up demo presets

    await demo.run_full_demo(
        include_export=include_export,
        cleanup_after=cleanup_after
    )


if __name__ == "__main__":
    print("Starting AmoCRM Export Preset Integration Demo...")
    print("Make sure MongoDB is running and accessible.")
    if input("Continue? (y/N): ").lower().strip() == 'y':
        asyncio.run(main())
    else:
        print("Demo cancelled.")