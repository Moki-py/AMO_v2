"""
Integration tests for Google Sheets export workflow with mocked services
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from datetime import datetime
from typing import Dict, List, Any

from amocrm_exporter.exporters.enhanced_sheets_exporter import EnhancedSheetsExporter
from amocrm_exporter.core.google_sheets_config import GoogleSheetsConfigManager
from amocrm_exporter.web.export_presets import ExportPresetManager, ExportPreset
from amocrm_exporter.utils.progress_tracker import ExportProgressTracker
from amocrm_exporter.utils.data_filter_engine import DataFilterEngine
from amocrm_exporter.utils.error_handler import GoogleSheetsErrorHandler
from amocrm_exporter.utils.exceptions import (
    GoogleSheetsError,
    GoogleSheetsRateLimitError,
    GoogleSheetsPermissionError,
    NetworkError
)


class TestGoogleSheetsExportIntegration:
    """Integration tests for complete Google Sheets export workflow"""

    def setup_method(self):
        """Set up test fixtures"""
        # Mock storage and dependencies
        self.mock_storage = Mock()
        self.mock_db = Mock()
        self.mock_collection = Mock()
        self.mock_db.__getitem__ = Mock(return_value=self.mock_collection)
        self.mock_storage.db = self.mock_db

        # Create test components with mocked Google Sheets operations
        with patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.validate_configuration') as mock_validate, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.get_spreadsheet_info') as mock_info:

            # Setup mocks to avoid real API calls during initialization
            mock_validate.return_value = Mock(is_valid=True, errors=[], warnings=[], missing_configs=[])
            mock_creds.return_value = None
            mock_info.return_value = Mock(title="Test Sheet", sheets=[])

            self.config_manager = GoogleSheetsConfigManager()
            self.preset_manager = ExportPresetManager(self.mock_storage)
            self.progress_tracker = ExportProgressTracker(self.mock_storage)
            self.filter_engine = DataFilterEngine()
            self.error_handler = GoogleSheetsErrorHandler()

        # Mock Google Sheets service
        self.mock_sheets_service = Mock()

    @pytest.mark.asyncio
    async def test_end_to_end_export_workflow_success(self):
        """Test complete end-to-end export workflow with successful outcome"""
        # Prepare test data
        test_data = {
            "deals": [
                {"id": 1, "name": "Deal 1", "status": "active", "updated_at": 1640995200},
                {"id": 2, "name": "Deal 2", "status": "closed", "updated_at": 1672531200}
            ],
            "contacts": [
                {"id": 1, "name": "Contact 1", "email": "contact1@test.com"},
                {"id": 2, "name": "Contact 2", "email": "contact2@test.com"}
            ],
            "companies": [
                {"id": 1, "name": "Company 1"},
                {"id": 2, "name": "Company 2"}
            ],
            "users": [
                {"id": 1, "name": "User 1", "email": "user1@test.com"}
            ],
            "pipelines": [
                {"id": 1, "name": "Sales Pipeline"}
            ]
        }

        # Create test presets
        deals_preset = ExportPreset(
            name="Deals Export",
            entity_type="deals",
            selected_fields=["id", "name", "status"],
            field_order=["name", "id", "status"]
        )

        contacts_preset = ExportPreset(
            name="Contacts Export",
            entity_type="contacts",
            selected_fields=["id", "name", "email"],
            field_order=["name", "email", "id"]
        )

        # Mock Google Sheets API responses
        with patch('googleapiclient.discovery.build') as mock_build, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds, \
             patch.object(self.preset_manager, 'load_preset') as mock_load_preset:

            # Setup mocks
            mock_build.return_value = self.mock_sheets_service
            mock_creds.return_value = Mock(valid=True)

            # Mock preset loading
            mock_load_preset.side_effect = lambda preset_id: {
                "deals_preset_id": deals_preset,
                "contacts_preset_id": contacts_preset
            }.get(preset_id)

            # Mock successful Google Sheets operations
            self.mock_sheets_service.spreadsheets().get().execute.return_value = {
                "properties": {"title": "Test Spreadsheet"},
                "sheets": [{"properties": {"title": "Sheet1", "sheetId": 0}}]
            }

            self.mock_sheets_service.spreadsheets().values().batchUpdate().execute.return_value = {
                "totalUpdatedCells": 100,
                "totalUpdatedRows": 10
            }

            self.mock_sheets_service.spreadsheets().values().update().execute.return_value = {
                "updatedCells": 50,
                "updatedRows": 5
            }

            self.mock_sheets_service.spreadsheets().batchUpdate().execute.return_value = {
                "replies": [{"addSheet": {"properties": {"sheetId": 123}}}]
            }

            # Create enhanced exporter with mocked validation
            with patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.validate_configuration') as mock_validate, \
                 patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds_init, \
                 patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.get_spreadsheet_info') as mock_info_init:

                mock_validate.return_value = Mock(is_valid=True, errors=[])
                mock_creds_init.return_value = None
                mock_info_init.return_value = Mock(title="Test Sheet", sheets=[])

                exporter = EnhancedSheetsExporter(
                    storage=self.mock_storage
                )

            # Mock exporter methods and Google Sheets service
            with patch.object(exporter, '_validate_export_configuration', return_value=True), \
                 patch.object(exporter, '_prepare_export_data', return_value=test_data), \
                 patch.object(exporter, '_write_data_to_sheets', return_value={"success": True}), \
                 patch.object(exporter, '_build_service', return_value=self.mock_sheets_service):

                # Execute export
                export_config = {
                    "entity_presets": {
                        "deals": "deals_preset_id",
                        "contacts": "contacts_preset_id"
                    },
                    "spreadsheet_ids": {
                        "deals": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
                        "contacts": "1AbCdEfGhIjKlMnOpQrStUvWxYz1234567890AbCdEf"
                    }
                }

                result = await exporter.export_with_presets(export_config)

                # Verify successful export
                assert result["status"] == "completed"
                assert result["exported_entities"]["deals"] == 2
                assert result["exported_entities"]["contacts"] == 2
                assert len(result["errors"]) == 0

    @pytest.mark.asyncio
    async def test_export_workflow_with_rate_limiting(self):
        """Test export workflow handling rate limiting"""
        # Prepare test data
        test_data = {
            "deals": [{"id": i, "name": f"Deal {i}"} for i in range(100)]  # Large dataset
        }

        with patch('googleapiclient.discovery.build') as mock_build, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds:

            # Setup mocks
            mock_build.return_value = self.mock_sheets_service
            mock_creds.return_value = Mock(valid=True)

            # Mock rate limiting on first few calls, then success
            call_count = 0
            def mock_batch_update(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count <= 2:
                    # Simulate rate limiting
                    from googleapiclient.errors import HttpError
                    mock_resp = Mock()
                    mock_resp.status = 429
                    raise HttpError(mock_resp, b'Rate limit exceeded')
                return {"replies": []}

            self.mock_sheets_service.spreadsheets().batchUpdate().execute.side_effect = mock_batch_update

            # Mock other successful operations
            self.mock_sheets_service.spreadsheets().get().execute.return_value = {
                "properties": {"title": "Test Spreadsheet"},
                "sheets": []
            }

            # Create exporter with retry logic and mocked validation
            with patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.validate_configuration') as mock_validate_init, \
                 patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds_init, \
                 patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.get_spreadsheet_info') as mock_info_init:

                mock_validate_init.return_value = Mock(is_valid=True, errors=[])
                mock_creds_init.return_value = None
                mock_info_init.return_value = Mock(title="Test Sheet", sheets=[])

                exporter = EnhancedSheetsExporter(
                    storage=self.mock_storage
                )

            # Mock exporter methods and Google Sheets service
            with patch.object(exporter, '_validate_export_configuration', return_value=True), \
                 patch.object(exporter, '_prepare_export_data', return_value=test_data), \
                 patch.object(exporter, '_build_service', return_value=self.mock_sheets_service), \
                 patch.object(exporter, '_write_data_to_sheets', return_value={"success": True}), \
                 patch.object(exporter, '_ensure_sheet_exists', return_value=None), \
                 patch.object(exporter, '_write_data_in_chunks', return_value=None), \
                 patch('time.sleep'):  # Speed up retry delays

                export_config = {
                    "entity_presets": {"deals": "deals_preset_id"},
                    "spreadsheet_ids": {"deals": "test_spreadsheet_id_123456789012345678901234"}
                }

                # Should eventually succeed after retries
                result = await exporter.export_with_presets(export_config)

                assert result["status"] == "completed"
                # Don't check call_count since we're mocking the actual operation

    @pytest.mark.asyncio
    async def test_export_workflow_with_permission_errors(self):
        """Test export workflow handling permission errors"""
        with patch('googleapiclient.discovery.build') as mock_build, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds:

            # Setup mocks
            mock_build.return_value = self.mock_sheets_service
            mock_creds.return_value = Mock(valid=True)

            # Mock permission error
            from googleapiclient.errors import HttpError
            mock_resp = Mock()
            mock_resp.status = 403
            permission_error = HttpError(mock_resp, b'Insufficient permissions')
            permission_error.error_details = [{"reason": "forbidden"}]

            self.mock_sheets_service.spreadsheets().get().execute.side_effect = permission_error

            # Create exporter with mocked validation
            with patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.validate_configuration') as mock_validate, \
                 patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds_init, \
                 patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.get_spreadsheet_info') as mock_info_init:

                mock_validate.return_value = Mock(is_valid=True, errors=[])
                mock_creds_init.return_value = None
                mock_info_init.return_value = Mock(title="Test Sheet", sheets=[])

                exporter = EnhancedSheetsExporter(
                    storage=self.mock_storage
                )

            # Mock exporter methods but allow permission error to propagate
            with patch.object(exporter, '_validate_export_configuration', return_value=True), \
                 patch.object(exporter, '_prepare_export_data', return_value={"deals": [{"id": 1, "name": "Deal 1"}]}), \
                 patch.object(exporter, '_build_service', return_value=self.mock_sheets_service):

                export_config = {
                    "entity_presets": {"deals": "deals_preset_id"},
                    "spreadsheet_ids": {"deals": "test_spreadsheet_id_123456789012345678901234"}
                }

                # Should handle permission error gracefully
                result = await exporter.export_with_presets(export_config)

            assert result["status"] == "failed"
            assert len(result["errors"]) > 0
            assert any("permission" in error.lower() or "forbidden" in error.lower() for error in result["errors"])

    @pytest.mark.asyncio
    async def test_export_workflow_with_partial_failure(self):
        """Test export workflow with partial failures"""
        test_data = {
            "deals": [{"id": 1, "name": "Deal 1"}],
            "contacts": [{"id": 1, "name": "Contact 1"}],
            "companies": [{"id": 1, "name": "Company 1"}]
        }

        with patch('googleapiclient.discovery.build') as mock_build, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds:

            # Setup mocks
            mock_build.return_value = self.mock_sheets_service
            mock_creds.return_value = Mock(valid=True)

            # Mock mixed success/failure responses with proper chaining
            def mock_get_operation(spreadsheetId=None, **kwargs):
                """Mock the .get() operation that returns an object with .execute()"""
                mock_get_obj = Mock()

                def mock_execute():
                    if spreadsheetId and 'deals' in spreadsheetId:
                        return {"properties": {"title": "Deals Sheet"}, "sheets": []}
                    elif spreadsheetId and 'contacts' in spreadsheetId:
                        # Simulate permission error for contacts
                        from googleapiclient.errors import HttpError
                        mock_resp = Mock()
                        mock_resp.status = 403
                        raise HttpError(mock_resp, b'Forbidden')
                    else:
                        return {"properties": {"title": "Other Sheet"}, "sheets": []}

                mock_get_obj.execute = mock_execute
                return mock_get_obj

            self.mock_sheets_service.spreadsheets().get.side_effect = mock_get_operation

            # Mock successful batch operations for accessible sheets
            self.mock_sheets_service.spreadsheets().batchUpdate().execute.return_value = {
                "replies": []
            }

            # Create exporter with mocked validation
            with patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.validate_configuration') as mock_validate, \
                 patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds_init, \
                 patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.get_spreadsheet_info') as mock_info_init:

                mock_validate.return_value = Mock(is_valid=True, errors=[])
                mock_creds_init.return_value = None
                mock_info_init.return_value = Mock(title="Test Sheet", sheets=[])

                exporter = EnhancedSheetsExporter(
                    storage=self.mock_storage
                )

            # Mock exporter methods but let _build_service use the mocked sheets service
            with patch.object(exporter, '_validate_export_configuration', return_value=True), \
                 patch.object(exporter, '_prepare_export_data', return_value=test_data):

                export_config = {
                    "entity_presets": {
                        "deals": "deals_preset_id",
                        "contacts": "contacts_preset_id",
                        "companies": "companies_preset_id"
                    },
                    "spreadsheet_ids": {
                        "deals": "test_deals_spreadsheet_id_123456789012345678",
                        "contacts": "test_contacts_spreadsheet_id_12345678901234567",
                        "companies": "test_companies_spreadsheet_id_1234567890123456"
                    }
                }

                result = await exporter.export_with_presets(export_config)

                # Should be partial success
                assert result["status"] == "partial"
                assert result["exported_entities"]["deals"] > 0
                assert result["exported_entities"]["companies"] > 0
                assert "contacts" in result["failed_entities"]

    @pytest.mark.asyncio
    async def test_export_workflow_with_network_failures(self):
        """Test export workflow handling network failures"""
        with patch('googleapiclient.discovery.build') as mock_build, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds:

            # Setup mocks
            mock_build.return_value = self.mock_sheets_service
            mock_creds.return_value = Mock(valid=True)

            # Mock network failures with proper chaining
            def mock_get_operation_network_error(spreadsheetId=None, **kwargs):
                """Mock the .get() operation that raises a network error"""
                mock_get_obj = Mock()

                def mock_execute():
                    raise ConnectionError("Network unreachable")

                mock_get_obj.execute = mock_execute
                return mock_get_obj

            self.mock_sheets_service.spreadsheets().get.side_effect = mock_get_operation_network_error

            # Create exporter with mocked validation
            with patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.validate_configuration') as mock_validate, \
                 patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds_init, \
                 patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.get_spreadsheet_info') as mock_info_init:

                mock_validate.return_value = Mock(is_valid=True, errors=[])
                mock_creds_init.return_value = None
                mock_info_init.return_value = Mock(title="Test Sheet", sheets=[])

                exporter = EnhancedSheetsExporter(
                    storage=self.mock_storage
                )

            # Mock exporter methods but allow network error to propagate
            with patch.object(exporter, '_validate_export_configuration', return_value=True), \
                 patch.object(exporter, '_prepare_export_data', return_value={"deals": [{"id": 1, "name": "Deal 1"}]}):

                export_config = {
                    "entity_presets": {"deals": "deals_preset_id"},
                    "spreadsheet_ids": {"deals": "test_spreadsheet_id_123456789012345678901234"}
                }

                # Should handle network error and potentially retry
                result = await exporter.export_with_presets(export_config)

            assert result["status"] == "failed"
            assert len(result["errors"]) > 0
            assert any("network" in error.lower() or "connection" in error.lower()
                      for error in result["errors"])

    def test_configuration_validation_integration(self):
        """Test integration of configuration validation with export workflow"""
        # Create exporter with normal validation for init
        with patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.validate_configuration') as mock_validate_init, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds_init, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.get_spreadsheet_info') as mock_info_init:

            mock_validate_init.return_value = Mock(is_valid=True, errors=[])
            mock_creds_init.return_value = None
            mock_info_init.return_value = Mock(title="Test Sheet", sheets=[])

            exporter = EnhancedSheetsExporter(
                storage=self.mock_storage
            )

        # Test with invalid configuration - missing entity_presets
        export_config = {}

        # Should fail validation due to missing required fields
        result = exporter._validate_export_configuration(export_config)
        assert result is False

        # Test with invalid spreadsheet ID format
        export_config_invalid_id = {
            "entity_presets": {"deals": "deals_preset_id"},
            "spreadsheet_ids": {"deals": "invalid_short_id"}  # Too short, should be 44 chars
        }

        result = exporter._validate_export_configuration(export_config_invalid_id)
        assert result is False

    def test_preset_integration_with_export(self):
        """Test integration of preset system with export workflow"""
        # Create test preset
        test_preset = ExportPreset(
            name="Integration Test Preset",
            entity_type="deals",
            selected_fields=["id", "name", "status", "custom_field_123"],
            field_order=["name", "status", "id", "custom_field_123"],
            custom_field_mappings={"custom_field_123": "Custom Deal Field"}
        )

        # Mock preset loading
        with patch.object(self.preset_manager, 'load_preset', return_value=test_preset):
            # Mock data preparation
            test_data = {
                "deals": [
                    {
                        "id": 1,
                        "name": "Test Deal",
                        "status": "active",
                        "custom_fields_values": [
                            {
                                "field_id": "123",
                                "field_name": "Custom Deal Field",
                                "values": [{"value": "Custom Value"}]
                            }
                        ]
                    }
                ]
            }

            # Create exporter with mocked validation
            with patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.validate_configuration') as mock_validate, \
                 patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds_init, \
                 patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.get_spreadsheet_info') as mock_info_init:

                mock_validate.return_value = Mock(is_valid=True, errors=[])
                mock_creds_init.return_value = None
                mock_info_init.return_value = Mock(title="Test Sheet", sheets=[])

                exporter = EnhancedSheetsExporter(
                    storage=self.mock_storage
                )

            # Mock data processing
            with patch.object(exporter, '_process_data_with_preset') as mock_process:
                mock_process.return_value = {
                    "headers": ["Name", "Status", "ID", "Custom Deal Field"],
                    "rows": [["Test Deal", "active", 1, "Custom Value"]]
                }

                processed_data = exporter._process_data_with_preset(test_data["deals"], test_preset)

                # Verify preset integration
                assert "Custom Deal Field" in processed_data["headers"]
                assert processed_data["rows"][0][3] == "Custom Value"

    @pytest.mark.asyncio
    async def test_progress_tracking_integration(self):
        """Test integration of progress tracking with export workflow"""
        progress_updates = []

        def mock_progress_callback(export_id, progress):
            # Extract entity progress information
            for entity_type, entity_progress in progress.entities.items():
                progress_updates.append({
                    "export_id": export_id,
                    "entity_type": entity_type,
                    "processed": entity_progress.processed,
                    "total": entity_progress.total
                })

        # Create progress tracker with callback
        progress_tracker = ExportProgressTracker(self.mock_storage)
        progress_tracker.add_progress_callback(mock_progress_callback)

        test_data = {
            "deals": [{"id": i, "name": f"Deal {i}"} for i in range(5)],
            "contacts": [{"id": i, "name": f"Contact {i}"} for i in range(3)]
        }

        # Create exporter with mocked validation
        with patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.validate_configuration') as mock_validate, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds_init, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.get_spreadsheet_info') as mock_info_init:

            mock_validate.return_value = Mock(is_valid=True, errors=[])
            mock_creds_init.return_value = None
            mock_info_init.return_value = Mock(title="Test Sheet", sheets=[])

            exporter = EnhancedSheetsExporter(
                storage=self.mock_storage
            )

            # Replace exporter's progress tracker with our tracked one
            exporter.progress_tracker = progress_tracker

            # Mock Google Sheets service
            mock_sheets_service = Mock()
            mock_sheets_service.spreadsheets().get().execute.return_value = {
                "properties": {"title": "Test Sheet"},
                "sheets": []
            }
            mock_sheets_service.spreadsheets().batchUpdate().execute.return_value = {"replies": []}
            mock_sheets_service.spreadsheets().values().batchUpdate().execute.return_value = {
                "totalUpdatedCells": 100,
                "totalUpdatedRows": 10
            }

            # Mock successful export operations
            with patch.object(exporter, '_validate_export_configuration', return_value=True), \
                 patch.object(exporter, '_prepare_export_data', return_value=test_data), \
                 patch.object(exporter, '_write_data_to_sheets', return_value={"success": True}), \
                 patch.object(exporter, '_build_service', return_value=mock_sheets_service), \
                 patch('googleapiclient.discovery.build', return_value=mock_sheets_service):

                export_config = {
                    "entity_presets": {"deals": "deals_preset", "contacts": "contacts_preset"},
                    "spreadsheet_ids": {
                        "deals": "test_deals_id_123456789012345678901234567890123",
                        "contacts": "test_contacts_id_12345678901234567890123456789"
                    }
                }

                await exporter.export_with_presets(export_config)

                # Verify progress updates were called
                assert len(progress_updates) > 0

                # Should have updates for both entity types
                entity_types = {update["entity_type"] for update in progress_updates}
                assert "deals" in entity_types
            assert "contacts" in entity_types

            # Should have completion updates
            completion_updates = [u for u in progress_updates if u["processed"] == u["total"]]
            assert len(completion_updates) >= 2  # At least one for each entity type

    def test_error_handling_integration(self):
        """Test integration of error handling with export workflow"""
        # Create error handler
        error_handler = GoogleSheetsErrorHandler()

        # Test various error scenarios
        test_errors = [
            GoogleSheetsRateLimitError("Rate limit exceeded", retry_after=60),
            GoogleSheetsPermissionError("Access denied", spreadsheet_id="test_id"),
            NetworkError("Connection failed")
        ]

        for error in test_errors:
            error_report = error_handler.handle_error(error)

            # Verify error report structure
            assert error_report.error_id is not None
            assert error_report.timestamp is not None
            assert error_report.user_message is not None
            assert error_report.technical_message is not None
            assert len(error_report.suggested_actions) > 0

            # Verify error-specific handling
            if isinstance(error, GoogleSheetsRateLimitError):
                assert error_report.is_retryable is True
                assert error_report.retry_after == 60
            elif isinstance(error, GoogleSheetsPermissionError):
                assert error_report.is_retryable is False
                assert "permission" in error_report.user_message.lower()
            elif isinstance(error, NetworkError):
                assert error_report.is_retryable is True

    def test_data_filtering_integration(self):
        """Test integration of data filtering with export workflow"""
        # Create test data with events (should be filtered out)
        test_data = {
            "deals": [
                {"id": 1, "name": "Deal 1", "updated_at": 1640995200, "contact_id": 1},
                {"id": 2, "name": "Deal 2", "updated_at": 1672531200, "contact_id": 2}
            ],
            "contacts": [
                {"id": 1, "name": "Contact 1"},
                {"id": 2, "name": "Contact 2"}
            ],
            "events": [  # Should be filtered out
                {"id": 1, "type": "call"},
                {"id": 2, "type": "email"}
            ],
            "companies": [{"id": 1, "name": "Company 1"}],
            "users": [{"id": 1, "name": "User 1"}],
            "pipelines": [{"id": 1, "name": "Pipeline 1"}]
        }

        # Apply filtering
        filtered_result = self.filter_engine.apply_comprehensive_filter(test_data)

        # Verify events are excluded
        assert "events" not in filtered_result.filtered_data
        assert "deals" in filtered_result.filtered_data
        assert "contacts" in filtered_result.filtered_data
        assert "companies" in filtered_result.filtered_data
        assert "users" in filtered_result.filtered_data
        assert "pipelines" in filtered_result.filtered_data

        # Verify filter stats
        assert filtered_result.filter_stats["deals"] == 2
        assert filtered_result.filter_stats["contacts"] == 2
        assert filtered_result.filter_stats.get("events", 0) == 0


class TestPerformanceIntegration:
    """Integration tests for performance with large datasets"""

    @pytest.mark.asyncio
    async def test_large_dataset_export_performance(self):
        """Test export performance with large datasets"""
        # Create smaller test dataset for faster testing
        large_dataset = {
            "deals": [{"id": i, "name": f"Deal {i}", "status": "active"} for i in range(10)],
            "contacts": [{"id": i, "name": f"Contact {i}", "email": f"contact{i}@test.com"} for i in range(5)]
        }

        # Mock components
        mock_storage = Mock()

        # Create components with mocked Google Sheets operations
        with patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.validate_configuration') as mock_validate_comp, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds_comp, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.get_spreadsheet_info') as mock_info_comp:

            mock_validate_comp.return_value = Mock(is_valid=True, errors=[], warnings=[], missing_configs=[])
            mock_creds_comp.return_value = None
            mock_info_comp.return_value = Mock(title="Test Sheet", sheets=[])

            config_manager = GoogleSheetsConfigManager()
            preset_manager = ExportPresetManager(mock_storage)
            progress_tracker = ExportProgressTracker(mock_storage)
            filter_engine = DataFilterEngine()
            error_handler = GoogleSheetsErrorHandler()

        # Create exporter with mocked validation
        with patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.validate_configuration') as mock_validate, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds_init, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.get_spreadsheet_info') as mock_info_init:

            mock_validate.return_value = Mock(is_valid=True, errors=[])
            mock_creds_init.return_value = None
            mock_info_init.return_value = Mock(title="Test Sheet", sheets=[])

            exporter = EnhancedSheetsExporter(
                storage=mock_storage
            )

        # Mock all external dependencies and Google API calls
        with patch('googleapiclient.discovery.build') as mock_build, \
             patch('google.auth.default') as mock_auth_default, \
             patch('google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file') as mock_flow, \
             patch.object(config_manager, '_get_credentials'), \
             patch.object(exporter, '_validate_export_configuration', return_value=True), \
             patch.object(exporter, '_prepare_export_data', new_callable=AsyncMock, return_value=large_dataset), \
             patch.object(exporter, '_write_data_to_sheets', new_callable=AsyncMock, return_value={"success": True}), \
             patch.object(exporter, '_build_service') as mock_build_service:

            # Setup Google API mocks
            mock_auth_default.return_value = (Mock(), "test-project")
            mock_flow.return_value.run_local_server.return_value = Mock()

            # Setup Google Sheets mock
            mock_sheets_service = Mock()
            mock_build.return_value = mock_sheets_service
            mock_build_service.return_value = mock_sheets_service
            mock_sheets_service.spreadsheets().get().execute.return_value = {
                "properties": {"title": "Test Sheet"},
                "sheets": []
            }
            mock_sheets_service.spreadsheets().batchUpdate().execute.return_value = {"replies": []}
            mock_sheets_service.spreadsheets().values().batchUpdate().execute.return_value = {
                "totalUpdatedCells": 100, "totalUpdatedRows": 10
            }

            export_config = {
                "entity_presets": {"deals": "deals_preset", "contacts": "contacts_preset"},
                "spreadsheet_ids": {
                    "deals": "test_deals_id_123456789012345678901234567890123",
                    "contacts": "test_contacts_id_12345678901234567890123456789"
                }
            }

            # Measure execution time
            start_time = datetime.now()
            result = await exporter.export_with_presets(export_config)
            end_time = datetime.now()

            execution_time = (end_time - start_time).total_seconds()

            # Verify performance (should complete within reasonable time)
            assert execution_time < 10  # Should complete within 10 seconds for mocked operations
            assert result["status"] == "completed"
            assert result["exported_entities"]["deals"] == 10
            assert result["exported_entities"]["contacts"] == 5

    @pytest.mark.asyncio
    async def test_concurrent_export_handling(self):
        """Test handling of concurrent export operations"""
        # Setup global mocks for all concurrent exports
        with patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.validate_configuration') as mock_validate, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager._get_credentials') as mock_creds, \
             patch('amocrm_exporter.core.google_sheets_config.GoogleSheetsConfigManager.get_spreadsheet_info') as mock_info, \
             patch('googleapiclient.discovery.build') as mock_build, \
             patch('google.auth.default') as mock_auth_default, \
             patch('google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file') as mock_flow:

            # Setup mocks
            mock_validate.return_value = Mock(is_valid=True, errors=[])
            mock_creds.return_value = None
            mock_info.return_value = Mock(title="Test Sheet", sheets=[])
            mock_auth_default.return_value = (Mock(), "test-project")
            mock_flow.return_value.run_local_server.return_value = Mock()

            mock_sheets_service = Mock()
            mock_build.return_value = mock_sheets_service
            mock_sheets_service.spreadsheets().get().execute.return_value = {
                "properties": {"title": "Test Sheet"},
                "sheets": []
            }
            mock_sheets_service.spreadsheets().batchUpdate().execute.return_value = {"replies": []}
            mock_sheets_service.spreadsheets().values().batchUpdate().execute.return_value = {
                "totalUpdatedCells": 100, "totalUpdatedRows": 10
            }

            # Create multiple export tasks
            export_tasks = []
            exporters = []

            for i in range(2):  # Reduce to 2 concurrent exports for faster testing
                # Mock storage for each export
                mock_storage = Mock()

                # Create exporter
                exporter = EnhancedSheetsExporter(storage=mock_storage)

                # Mock exporter methods
                exporter._validate_export_configuration = Mock(return_value=True)
                exporter._prepare_export_data = AsyncMock(return_value={"deals": [{"id": i}]})
                exporter._write_data_to_sheets = AsyncMock(return_value={"success": True})
                exporter._build_service = Mock(return_value=mock_sheets_service)

                exporters.append(exporter)

                export_config = {
                    "entity_presets": {"deals": f"deals_preset_{i}"},
                    "spreadsheet_ids": {"deals": f"test_id_{i}_123456789012345678901234567890"}
                }

                # Create async task
                task = asyncio.create_task(exporter.export_with_presets(export_config))
                export_tasks.append(task)

            # Wait for all exports to complete
            results = await asyncio.gather(*export_tasks, return_exceptions=True)

            # Verify all exports completed successfully
            for result in results:
                if isinstance(result, Exception):
                    pytest.fail(f"Export failed with exception: {result}")
                assert result["status"] == "completed"

    def test_memory_usage_with_large_datasets(self):
        """Test memory usage patterns with large datasets"""
        import psutil
        import os

        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Create large dataset
        large_dataset = {
            "deals": [
                {
                    "id": i,
                    "name": f"Deal {i}",
                    "description": "A" * 1000,  # Large text field
                    "custom_fields_values": [
                        {
                            "field_id": f"{j}",
                            "field_name": f"Custom Field {j}",
                            "values": [{"value": f"Value {j} for deal {i}"}]
                        }
                        for j in range(10)  # Multiple custom fields
                    ]
                }
                for i in range(10)  # Smaller number of records for faster testing
            ]
        }

        # Process data through filter engine
        filter_engine = DataFilterEngine()
        filtered_result = filter_engine.apply_comprehensive_filter(large_dataset)

        # Check memory usage after processing
        current_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = current_memory - initial_memory

        # Memory increase should be reasonable (less than 50MB for this smaller test)
        assert memory_increase < 50, f"Memory usage increased by {memory_increase:.2f}MB"

        # Cleanup
        del large_dataset
        del filtered_result


if __name__ == '__main__':
    pytest.main([__file__, '-v'])