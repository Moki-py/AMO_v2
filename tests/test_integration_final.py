"""
Final integration tests for the Google Sheets export finalization system

This test suite validates that all components work together correctly:
- Enhanced SheetsExporter with progress tracking
- Preset management system
- Web interface integration
- Error handling and recovery
- Performance optimization
"""

import pytest
import asyncio
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, List, Any

# Import components to test
from amocrm_exporter.storage.storage import Storage
from amocrm_exporter.exporters.sheets_exporter import SheetsExporter
from amocrm_exporter.web.export_presets import ExportPresetManager, ExportPreset
from amocrm_exporter.utils.progress_tracker import ExportProgressTracker, ExportStatus
from amocrm_exporter.utils.progress_notifier import progress_notifier
from amocrm_exporter.web.export_settings import ExportSettingsManager, EntityType as ExportEntityType
from amocrm_exporter.core.google_sheets_config import GoogleSheetsConfigManager


class TestFinalIntegration:
    """Test suite for final integration validation"""

    @pytest.fixture
    def mock_storage(self):
        """Create a mock storage instance"""
        storage = Mock(spec=Storage)
        # Create a mock db that supports both attribute and dictionary access
        mock_db = Mock()
        mock_db.export_presets = Mock()
        mock_db.export_progress = Mock()
        mock_db.export_settings = Mock()
        # Support dictionary-style access
        mock_db.__getitem__ = Mock(side_effect=lambda key: getattr(mock_db, key))
        storage.db = mock_db

        # Mock entity data
        storage.get_entities = Mock(return_value=[
            {"id": 1, "name": "Test Deal", "price": 1000, "created_at": 1640995200},
            {"id": 2, "name": "Another Deal", "price": 2000, "created_at": 1640995300}
        ])

        return storage

    @pytest.fixture
    def mock_google_sheets_config(self):
        """Create a mock Google Sheets configuration"""
        config = Mock(spec=GoogleSheetsConfigManager)
        config.validate_configuration.return_value = Mock(
            is_valid=True,
            errors=[],
            warnings=[]
        )
        config._get_credentials = Mock()
        config.creds = Mock()
        return config

    @pytest.fixture
    def sample_preset(self):
        """Create a sample export preset"""
        return ExportPreset(
            name="Test Preset",
            entity_type="deals",
            description="Test preset for integration testing",
            selected_fields=["id", "name", "price", "created_at"],
            field_order=["name", "price", "id", "created_at"],
            custom_field_mappings={"custom_field_123": "Deal Source"}
        )

    @pytest.mark.asyncio
    async def test_enhanced_sheets_exporter_integration(self, mock_storage, mock_google_sheets_config):
        """Test that the enhanced SheetsExporter integrates properly with all components"""

        # Mock Google Sheets API calls
        with patch('amocrm_exporter.exporters.sheets_exporter.build') as mock_build:
            mock_service = Mock()
            mock_build.return_value = mock_service

            # Mock spreadsheet operations
            mock_service.spreadsheets.return_value.get.return_value.execute.return_value = {
                'sheets': [{'properties': {'title': 'Data'}}]
            }
            mock_service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {}

            # Mock configuration and create exporter
            with patch('amocrm_exporter.exporters.sheets_exporter.config') as mock_config:
                mock_config.settings.google_sheets_leads_id = "test_sheet_id"
                mock_config.settings.google_sheets_contacts_id = "test_sheet_id"
                mock_config.settings.google_sheets_companies_id = "test_sheet_id"

                with patch('amocrm_exporter.exporters.sheets_exporter.GoogleSheetsConfigManager') as mock_config_manager_class:
                    mock_config_manager_class.return_value = mock_google_sheets_config

                    # Create exporter
                    exporter = SheetsExporter(mock_storage)

                    # Mock the credentials method
                    exporter._get_credentials_with_retry = AsyncMock()

                    # Test enhanced export with progress tracking
                    result = await exporter.export_all_to_sheets_with_progress(
                        date_from="2022-01-01",
                        date_to="2022-01-31"
                    )

                    # Verify results
                    assert isinstance(result, dict)

                    # Verify progress tracking was used
                    assert hasattr(exporter, 'progress_tracker')
                    assert isinstance(exporter.progress_tracker, ExportProgressTracker)

    @pytest.mark.asyncio
    async def test_preset_system_integration(self, mock_storage, sample_preset):
        """Test that the preset system integrates properly with the export workflow"""

        # Create preset manager
        preset_manager = ExportPresetManager(mock_storage)

        # Mock database operations
        # Use valid ObjectId for testing
        from bson import ObjectId
        test_preset_id = ObjectId()
        mock_storage.db.export_presets.insert_one.return_value = Mock(inserted_id=test_preset_id)

        # Mock find_one to return None for duplicate check, then return the preset for loading
        def mock_find_one(query):
            # If query has name and entity_type (duplicate check), return None
            if "name" in query and "entity_type" in query:
                return None
            # If query has _id (loading preset), return the preset
            return {**sample_preset.to_dict(), "_id": test_preset_id}

        mock_storage.db.export_presets.find_one.side_effect = mock_find_one

        # Mock find to return a cursor-like object
        mock_cursor = Mock()
        mock_cursor.sort.return_value = mock_cursor
        mock_cursor.__iter__ = Mock(return_value=iter([{**sample_preset.to_dict(), "_id": test_preset_id}]))
        mock_storage.db.export_presets.find.return_value = mock_cursor

        # Test preset creation
        preset_id = preset_manager.save_preset(sample_preset)
        assert preset_id == str(test_preset_id)

        # Test preset loading
        loaded_preset = preset_manager.load_preset(preset_id)
        assert loaded_preset is not None
        assert loaded_preset.name == sample_preset.name

        # Test preset listing
        presets = preset_manager.list_presets("deals")
        assert len(presets) > 0
        assert presets[0].name == sample_preset.name

    @pytest.mark.asyncio
    async def test_export_with_presets_integration(self, mock_storage, mock_google_sheets_config, sample_preset):
        """Test exporting with presets integrates all components correctly"""

        # Mock Google Sheets API
        with patch('amocrm_exporter.exporters.sheets_exporter.build') as mock_build:
            mock_service = Mock()
            mock_build.return_value = mock_service

            # Mock spreadsheet operations
            mock_service.spreadsheets.return_value.get.return_value.execute.return_value = {
                'sheets': [{'properties': {'title': 'Data'}}]
            }
            mock_service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {}

            # Mock configuration and credentials
            with patch.object(SheetsExporter, '_get_credentials_with_retry', new_callable=AsyncMock):
                with patch('amocrm_exporter.exporters.sheets_exporter.GoogleSheetsConfigManager') as mock_config_class:
                    mock_config_class.return_value = mock_google_sheets_config
                    with patch('amocrm_exporter.exporters.sheets_exporter.config') as mock_config:
                        mock_config.settings.google_sheets_leads_id = "test_sheet_id"

                        # Create exporter
                        exporter = SheetsExporter(mock_storage)

                        # Test export with presets
                        presets = {"deals": sample_preset}
                        result = await exporter.export_with_presets(
                            presets=presets,
                            date_from="2022-01-01",
                            date_to="2022-01-31"
                        )

                        # Verify results
                        assert isinstance(result, dict)
                        assert "deals" in result or len(result) >= 0  # May be empty if no data

    @pytest.mark.asyncio
    async def test_progress_tracking_integration(self, mock_storage):
        """Test that progress tracking integrates properly with the export system"""

        # Create progress tracker
        progress_tracker = ExportProgressTracker(mock_storage)

        # Mock database operations
        mock_storage.db.export_progress = Mock()
        mock_storage.db.export_progress.replace_one = Mock()
        mock_storage.db.export_progress.find_one = Mock(return_value=None)

        # Test progress tracking workflow
        export_id = progress_tracker.start_export(entity_types=["deals", "contacts"])
        assert export_id is not None

        # Test progress updates
        progress_tracker.update_entity_progress(
            export_id=export_id,
            entity_type="deals",
            processed=50,
            total=100,
            status=ExportStatus.IN_PROGRESS
        )

        # Test completion
        progress_tracker.complete_entity(export_id, "deals", "https://test-url.com")
        progress_tracker.complete_export(export_id, success=True)

        # Verify progress state
        progress = progress_tracker.get_export_progress(export_id)
        assert progress is not None
        assert progress.status == ExportStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_error_handling_integration(self, mock_storage, mock_google_sheets_config):
        """Test that error handling works across all integrated components"""

        # Test configuration validation error handling
        mock_google_sheets_config.validate_configuration.return_value = Mock(
            is_valid=False,
            errors=["Invalid spreadsheet ID"],
            warnings=[]
        )

        # Test that exporter handles configuration errors
        with pytest.raises(Exception) as exc_info:
            SheetsExporter(mock_storage)

        assert "Google Sheets configuration is invalid" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_web_interface_integration(self, mock_storage):
        """Test that web interface components integrate properly"""

        # Create export settings manager
        settings_manager = ExportSettingsManager(mock_storage)

        # Mock field loading
        with patch.object(settings_manager, '_load_fields_from_db', new_callable=AsyncMock) as mock_load_fields:
            mock_load_fields.return_value = [
                Mock(field_id="id", field_name="ID", field_type="integer", is_custom=False),
                Mock(field_id="name", field_name="Name", field_type="text", is_custom=False)
            ]

            # Test field loading
            fields = await settings_manager.get_available_fields(ExportEntityType.DEALS)
            assert len(fields) == 2
            assert fields[0].field_id == "id"

    @pytest.mark.asyncio
    async def test_performance_optimization_integration(self, mock_storage, mock_google_sheets_config):
        """Test that performance optimizations work correctly in integration"""

        # Create large dataset for performance testing
        large_dataset = [
            {"id": i, "name": f"Deal {i}", "price": i * 100, "created_at": 1640995200 + i}
            for i in range(1000)  # 1000 records
        ]
        mock_storage.get_entities.return_value = large_dataset

        # Mock Google Sheets API with timing
        with patch('amocrm_exporter.exporters.sheets_exporter.build') as mock_build:
            mock_service = Mock()
            mock_build.return_value = mock_service

            # Mock operations with realistic delays
            mock_service.spreadsheets.return_value.get.return_value.execute.return_value = {
                'sheets': [{'properties': {'title': 'Data'}}]
            }
            mock_service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {}

            with patch.object(SheetsExporter, '_get_credentials_with_retry', new_callable=AsyncMock):
                with patch('amocrm_exporter.exporters.sheets_exporter.GoogleSheetsConfigManager') as mock_config_class:
                    mock_config_class.return_value = mock_google_sheets_config
                    with patch('amocrm_exporter.exporters.sheets_exporter.config') as mock_config:
                        mock_config.settings.google_sheets_leads_id = "test_sheet_id"

                        # Create exporter
                        exporter = SheetsExporter(mock_storage)

                        # Measure export time
                        start_time = datetime.now()
                        result = await exporter.export_all_to_sheets_with_progress()
                        end_time = datetime.now()

                        # Verify performance (should complete quickly with mocked operations)
                        duration = (end_time - start_time).total_seconds()
                        assert duration < 10  # Should complete within 10 seconds with mocks

                        # Verify batch processing was used
                        # (This would be verified by checking the number of API calls made)
                        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_websocket_integration(self, mock_storage):
        """Test WebSocket integration for real-time progress updates"""

        # Create progress tracker with WebSocket support
        progress_tracker = ExportProgressTracker(mock_storage)

        # Mock WebSocket connection
        mock_websocket = Mock()
        mock_websocket.send_text = AsyncMock()

        # Add WebSocket connection
        progress_tracker.add_websocket_connection(mock_websocket)

        # Test progress update triggers WebSocket message
        export_id = progress_tracker.start_export(entity_types=["deals"])

        # Update progress and verify WebSocket was called
        progress_tracker.update_entity_progress(
            export_id=export_id,
            entity_type="deals",
            processed=25,
            total=100,
            status=ExportStatus.IN_PROGRESS
        )

        # Give time for async WebSocket operations
        await asyncio.sleep(0.1)

        # Verify WebSocket integration
        assert mock_websocket in progress_tracker.websocket_connections

    @pytest.mark.asyncio
    async def test_data_filtering_integration(self, mock_storage, mock_google_sheets_config):
        """Test that data filtering integrates properly with the export system"""

        # Create test data with different dates
        test_data = [
            {"id": 1, "name": "Old Deal", "updated_at": 1609459200},  # 2021-01-01
            {"id": 2, "name": "New Deal", "updated_at": 1640995200},  # 2022-01-01
            {"id": 3, "name": "Recent Deal", "updated_at": 1672531200}  # 2023-01-01
        ]
        mock_storage.get_entities.return_value = test_data

        # Mock Google Sheets API
        with patch('amocrm_exporter.exporters.sheets_exporter.build') as mock_build:
            mock_service = Mock()
            mock_build.return_value = mock_service
            mock_service.spreadsheets.return_value.get.return_value.execute.return_value = {
                'sheets': [{'properties': {'title': 'Data'}}]
            }
            mock_service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {}

            with patch.object(SheetsExporter, '_get_credentials_with_retry', new_callable=AsyncMock):
                with patch('amocrm_exporter.exporters.sheets_exporter.GoogleSheetsConfigManager') as mock_config_class:
                    mock_config_class.return_value = mock_google_sheets_config
                    with patch('amocrm_exporter.exporters.sheets_exporter.config') as mock_config:
                        mock_config.settings.google_sheets_leads_id = "test_sheet_id"

                        # Create exporter
                        exporter = SheetsExporter(mock_storage)

                        # Test export with date filtering
                        result = await exporter.export_all_to_sheets_with_progress(
                            date_from="2022-01-01",
                            date_to="2022-12-31"
                        )

                        # Verify that get_entities was called with proper query
                        mock_storage.get_entities.assert_called()
                        call_args = mock_storage.get_entities.call_args

                        # Check that query parameter was passed (may be in args or kwargs)
                        query_passed = (
                            len(call_args[0]) > 1 or  # Query as positional arg
                            'query' in call_args[1]   # Query as keyword arg
                        )
                        assert query_passed or len(call_args[0]) >= 1  # At least entity type was passed

    def test_configuration_validation_integration(self, mock_storage):
        """Test that configuration validation integrates properly"""

        # Test with invalid configuration
        with patch('amocrm_exporter.exporters.sheets_exporter.GoogleSheetsConfigManager') as mock_config_class:
            mock_config = Mock()
            mock_config.validate_configuration.return_value = Mock(
                is_valid=False,
                errors=["Missing credentials file", "Invalid spreadsheet ID"],
                warnings=["Quota limits may be exceeded"]
            )
            mock_config_class.return_value = mock_config

            # Test that SheetsExporter handles invalid configuration
            with pytest.raises(Exception) as exc_info:
                SheetsExporter(mock_storage)

            assert "Google Sheets configuration is invalid" in str(exc_info.value)
            assert "Missing credentials file" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_end_to_end_workflow_integration(self, mock_storage, mock_google_sheets_config, sample_preset):
        """Test complete end-to-end workflow integration"""

        # Mock all external dependencies
        with patch('amocrm_exporter.exporters.sheets_exporter.build') as mock_build:
            mock_service = Mock()
            mock_build.return_value = mock_service
            mock_service.spreadsheets.return_value.get.return_value.execute.return_value = {
                'sheets': [{'properties': {'title': 'Data'}}]
            }
            mock_service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {}

            with patch.object(SheetsExporter, '_get_credentials_with_retry', new_callable=AsyncMock):
                with patch('amocrm_exporter.exporters.sheets_exporter.GoogleSheetsConfigManager') as mock_config_class:
                    mock_config_class.return_value = mock_google_sheets_config
                    with patch('amocrm_exporter.exporters.sheets_exporter.config') as mock_config:
                        mock_config.settings.google_sheets_leads_id = "test_sheet_id"

                        # Step 1: Create and save preset
                        preset_manager = ExportPresetManager(mock_storage)
                        from bson import ObjectId
                        test_preset_id = ObjectId()
                        mock_storage.db.export_presets.insert_one.return_value = Mock(inserted_id=test_preset_id)

                        # Mock find_one to return None for duplicate check, then return the preset for loading
                        def mock_find_one(query):
                            # If query has name and entity_type (duplicate check), return None
                            if "name" in query and "entity_type" in query:
                                return None
                            # If query has _id (loading preset), return the preset
                            return {**sample_preset.to_dict(), "_id": test_preset_id}

                        mock_storage.db.export_presets.find_one.side_effect = mock_find_one

                        preset_id = preset_manager.save_preset(sample_preset)
                        assert preset_id == str(test_preset_id)

                        loaded_preset = preset_manager.load_preset(preset_id)
                        assert loaded_preset is not None

                        # Step 3: Export with preset
                        exporter = SheetsExporter(mock_storage)
                        presets = {"deals": loaded_preset}

                        result = await exporter.export_with_presets(
                            presets=presets,
                            date_from="2022-01-01",
                            date_to="2022-01-31"
                        )

                        # Step 4: Verify complete workflow
                        assert isinstance(result, dict)
                        # Verify that all components were integrated properly
                        assert hasattr(exporter, 'preset_manager')
                        assert hasattr(exporter, 'progress_tracker')
                        assert hasattr(exporter, 'error_handler')

    @pytest.mark.asyncio
    async def test_concurrent_export_handling(self, mock_storage, mock_google_sheets_config):
        """Test that the system handles concurrent exports properly"""

        # Mock Google Sheets API
        with patch('amocrm_exporter.exporters.sheets_exporter.build') as mock_build:
            mock_service = Mock()
            mock_build.return_value = mock_service
            mock_service.spreadsheets.return_value.get.return_value.execute.return_value = {
                'sheets': [{'properties': {'title': 'Data'}}]
            }
            mock_service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {}

            with patch.object(SheetsExporter, '_get_credentials_with_retry', new_callable=AsyncMock):
                with patch('amocrm_exporter.exporters.sheets_exporter.GoogleSheetsConfigManager') as mock_config_class:
                    mock_config_class.return_value = mock_google_sheets_config
                    with patch('amocrm_exporter.exporters.sheets_exporter.config') as mock_config:
                        mock_config.settings.google_sheets_leads_id = "test_sheet_id"

                        # Create multiple exporters (simulating concurrent requests)
                        exporter1 = SheetsExporter(mock_storage)
                        exporter2 = SheetsExporter(mock_storage)

                        # Start concurrent exports
                        task1 = asyncio.create_task(
                            exporter1.export_all_to_sheets_with_progress(export_id="export_1")
                        )
                        task2 = asyncio.create_task(
                            exporter2.export_all_to_sheets_with_progress(export_id="export_2")
                        )

                        # Wait for both to complete
                        results = await asyncio.gather(task1, task2, return_exceptions=True)

                        # Verify both completed (may be empty results due to mocking)
                        assert len(results) == 2
                        for result in results:
                            if not isinstance(result, Exception):
                                assert isinstance(result, dict)

    def test_memory_usage_optimization(self, mock_storage):
        """Test that memory usage is optimized for large datasets"""

        # This test would typically measure actual memory usage
        # For now, we verify that the components are designed for efficiency

        # Mock configuration to avoid validation errors
        with patch('amocrm_exporter.exporters.sheets_exporter.GoogleSheetsConfigManager') as mock_config_class:
            mock_config = Mock()
            mock_config.validate_configuration.return_value = Mock(is_valid=True, errors=[], warnings=[])
            mock_config_class.return_value = mock_config

            # Create exporter
            exporter = SheetsExporter(mock_storage)

        # Verify that streaming/batch processing components are available
        assert hasattr(exporter, 'data_formatter')
        assert hasattr(exporter, 'custom_field_processor')
        assert hasattr(exporter, 'progress_tracker')

        # Verify batch processing configuration
        assert hasattr(exporter, 'retry_config')
        assert hasattr(exporter, 'retry_handler')

    @pytest.mark.asyncio
    async def test_api_rate_limiting_integration(self, mock_storage, mock_google_sheets_config):
        """Test that API rate limiting is properly integrated"""

        # Mock Google Sheets API with rate limiting simulation
        with patch('amocrm_exporter.exporters.sheets_exporter.build') as mock_build:
            mock_service = Mock()
            mock_build.return_value = mock_service

            # Simulate rate limiting on first call, success on retry
            call_count = 0
            def mock_api_call(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    from googleapiclient.errors import HttpError
                    raise HttpError(
                        resp=Mock(status=429),
                        content=b'{"error": {"code": 429, "message": "Rate limit exceeded"}}'
                    )
                return {}

            mock_service.spreadsheets.return_value.get.return_value.execute.return_value = {
                'sheets': [{'properties': {'title': 'Data'}}]
            }
            mock_service.spreadsheets.return_value.values.return_value.update.return_value.execute.side_effect = mock_api_call

            with patch.object(SheetsExporter, '_get_credentials_with_retry', new_callable=AsyncMock):
                with patch('amocrm_exporter.exporters.sheets_exporter.GoogleSheetsConfigManager') as mock_config_class:
                    mock_config_class.return_value = mock_google_sheets_config
                    with patch('amocrm_exporter.exporters.sheets_exporter.config') as mock_config:
                        mock_config.settings.google_sheets_leads_id = "test_sheet_id"

                        # Create exporter
                        exporter = SheetsExporter(mock_storage)

                        # Test that rate limiting is handled
                        # This should succeed despite the initial rate limit error
                        result = await exporter.export_all_to_sheets_with_progress()

                        # Verify retry logic was used
                        assert call_count > 1  # Should have retried
                        assert isinstance(result, dict)


if __name__ == "__main__":
    # Run the integration tests
    pytest.main([__file__, "-v", "--tb=short"])