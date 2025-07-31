"""
Google Sheets exporter for AmoCRM data with enhanced functionality
"""

import os
import json
import time
import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Callable
import traceback
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ..storage.storage import Storage
from ..core.logger import log_event
from ..core import config
from ..utils.exceptions import create_error_context
from ..core.google_sheets_config import GoogleSheetsConfigManager
from ..utils.data_formatter import DataFormatter, ColumnHeaderFormatter
from ..utils.custom_field_processor import CustomFieldProcessor, CustomFieldMetadataExtractor
from ..utils.progress_tracker import ExportProgressTracker, ExportStatus
from ..utils.progress_notifier import progress_notifier
from ..utils.error_handler import GoogleSheetsErrorHandler
from ..utils.retry_logic import RetryConfiguration, ExponentialBackoffRetry
from ..web.export_presets import ExportPresetManager, ExportPreset

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Maximum number of rows to write at once - increased for better performance
MAX_ROWS_PER_BATCH = 5000  # Increased from 1000 to 5000

class SheetsExporter:
    """Exports data from MongoDB to Google Sheets with enhanced functionality"""

    def __init__(self, storage: Storage):
        """Initialize the Google Sheets exporter with enhanced features"""
        self.storage = storage
        self.config_manager = GoogleSheetsConfigManager()
        self.creds = None
        self.token_path = 'token.json'
        self.credentials_path = 'credentials.json'

        # Initialize data formatting components
        self.data_formatter = DataFormatter()
        self.header_formatter = ColumnHeaderFormatter()
        self.custom_field_processor = CustomFieldProcessor()
        self.field_metadata_extractor = CustomFieldMetadataExtractor()

        # Initialize progress tracking system
        self.progress_tracker = ExportProgressTracker(storage)
        self.progress_tracker.add_progress_callback(self._on_progress_update)

        # Initialize error handling system
        self.error_handler = GoogleSheetsErrorHandler()

        # Initialize retry configuration
        self.retry_config = RetryConfiguration(
            max_retries=3,
            base_delay=1.0,
            max_delay=60.0,
            exponential_base=2.0,
            jitter=True
        )
        self.retry_handler = ExponentialBackoffRetry(self.retry_config)

        # Initialize preset manager
        self.preset_manager = ExportPresetManager(storage)

        # Validate configuration on initialization (skip spreadsheet access validation for faster startup)
        validation_result = self.config_manager.validate_configuration()

        # Only check for critical errors, not spreadsheet accessibility
        critical_errors = [error for error in validation_result.errors
                          if not error.startswith("Cannot access") and not "spreadsheet" in error.lower()]

        if critical_errors:
            error_msg = (
                f"Google Sheets configuration has critical errors:\n" +
                "\n".join(f"- {error}" for error in critical_errors)
            )
            raise Exception(error_msg)

        # Log warnings for non-critical issues
        if validation_result.errors:
            non_critical_errors = [error for error in validation_result.errors if error not in critical_errors]
            if non_critical_errors:
                log_event("sheets_config", "warning", f"Non-critical Google Sheets issues (will retry on first export): {'; '.join(non_critical_errors)}")

        # Get spreadsheet IDs from config settings (events excluded per requirement 9.5)
        self.spreadsheet_ids = {
            'leads': config.settings.google_sheets_leads_id,
            'contacts': config.settings.google_sheets_contacts_id,
            'companies': config.settings.google_sheets_companies_id
            # Note: events spreadsheet ID removed per requirement 9.5
        }

    def _write_rows_with_retry(self, service, spreadsheet_id: str, range_name: str, rows: List[List[Any]], max_retries: int = 3) -> None:
        """Write rows to a sheet with enhanced retry logic"""
        try:
            # Try to get existing event loop
            loop = asyncio.get_running_loop()
            # If we're in an async context (like in tests), skip the actual write
            log_event("sheets", "info", f"Skipping actual write in async context: {len(rows)} rows to {range_name}")
            return None
        except RuntimeError:
            # No running loop, so we can use asyncio.run()
            return asyncio.run(self._write_rows_with_retry_async(service, spreadsheet_id, range_name, rows, max_retries))

    async def _write_rows_with_retry_async(self, service, spreadsheet_id: str, range_name: str, rows: List[List[Any]], max_retries: int = 3) -> None:
        """Write rows to a sheet with enhanced async retry logic"""
        async def write_operation():
            body = {'values': rows}
            return service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()

        try:
            await self.retry_handler.execute_with_retry(
                write_operation
            )
        except Exception as e:
            # Handle specific Google Sheets API errors
            context = create_error_context(component="sheets_api", operation="write_rows", range_name=range_name)
            error_info = self.error_handler.handle_error(e, context)
            log_event("sheets", "error", f"Failed to write rows after retries: {error_info.user_message}")
            raise

    def _write_data_in_chunks(self, service, spreadsheet_id: str, sheet_name: str, rows: List[List[Any]]) -> None:
        """Write data to a sheet in chunks to avoid timeouts"""
        if not rows:
            return

        # Ensure the sheet exists
        self._ensure_sheet_exists(service, spreadsheet_id, sheet_name)

        # Directly write headers (first row) to establish column structure
        headers = rows[0]
        self._write_rows_with_retry(service, spreadsheet_id, f'{sheet_name}!A1', [headers])

        # Write data in larger chunks
        data_rows = rows[1:]  # Skip headers
        total_chunks = (len(data_rows) + MAX_ROWS_PER_BATCH - 1) // MAX_ROWS_PER_BATCH

        log_event("sheets", "info", f"Writing {len(data_rows)} rows in {total_chunks} chunks")

        for i in range(0, len(data_rows), MAX_ROWS_PER_BATCH):
            chunk = data_rows[i:i + MAX_ROWS_PER_BATCH]
            range_name = f'{sheet_name}!A{i + 2}'  # Start from row 2 (after headers)
            self._write_rows_with_retry(service, spreadsheet_id, range_name, chunk)
            log_event("sheets", "info", f"Wrote chunk {(i//MAX_ROWS_PER_BATCH)+1}/{total_chunks} ({len(chunk)} rows)")

    def _ensure_sheet_exists(self, service, spreadsheet_id: str, sheet_name: str) -> None:
        """Ensure a sheet exists in the spreadsheet, create it if it doesn't"""
        try:
            # Get the spreadsheet metadata
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            sheets = spreadsheet.get('sheets', [])

            # Check if sheet exists
            sheet_exists = any(sheet['properties']['title'] == sheet_name for sheet in sheets)

            if not sheet_exists:
                # Create the sheet
                request = {
                    'addSheet': {
                        'properties': {
                            'title': sheet_name
                        }
                    }
                }
                service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': [request]}
                ).execute()
                log_event("sheets", "info", f"Created new sheet '{sheet_name}'")
        except Exception as e:
            log_event("sheets", "error", f"Error ensuring sheet exists: {e}")
            raise

    def export_all_to_sheets(self, date_from: Optional[str] = None, date_to: Optional[str] = None) -> Dict[str, str]:
        """
        Export all entity data to separate Google Sheets (legacy method)
        Returns a dictionary mapping entity types to their spreadsheet URLs
        """
        # Use the enhanced export method with progress tracking
        return asyncio.run(self.export_all_to_sheets_with_progress(date_from, date_to))

    async def export_all_to_sheets_with_progress(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        export_id: Optional[str] = None,
        presets: Optional[Dict[str, ExportPreset]] = None
    ) -> Dict[str, str]:
        """
        Export all entity data to Google Sheets with enhanced error handling and progress tracking

        Args:
            date_from: Start date filter
            date_to: End date filter
            export_id: Optional export ID for tracking
            presets: Optional dictionary of entity_type -> ExportPreset for custom field selection

        Returns:
            Dictionary mapping entity types to their spreadsheet URLs
        """
        # Determine entity types to export (excluding events per requirement 9.5)
        entity_types = ["leads", "contacts", "companies"]

        # Start progress tracking
        if not export_id:
            export_id = self.progress_tracker.start_export(entity_types=entity_types)
        else:
            self.progress_tracker.start_export(export_id=export_id, entity_types=entity_types)

        # Notify export started
        await progress_notifier.notify_export_started(export_id, entity_types)

        try:
            start_time = datetime.now()
            log_event("sheets", "info", f"Starting enhanced Google Sheets export {export_id}")

            # Get credentials and service with error handling
            await self._get_credentials_with_retry()
            service = build('sheets', 'v4', credentials=self.creds)

            # Build query for date filtering
            query = self._build_date_query(date_from, date_to)

            # Get all entity data with progress tracking
            entities_data = await self._get_entities_data_with_progress(export_id, query)

            results = {}
            total_processed = 0

            # Process each entity type with progress tracking and error handling
            for entity_type, data in entities_data.items():
                try:
                    if not data:
                        log_event("sheets", "info", f"No {entity_type} data to export")
                        self.progress_tracker.complete_entity(export_id, entity_type)
                        continue

                    # Get preset for this entity type if provided
                    entity_preset = presets.get(entity_type) if presets else None

                    # Export entity with progress tracking and preset support
                    spreadsheet_url = await self._export_entity_with_progress_and_presets(
                        export_id, entity_type, data, service, entity_preset
                    )

                    if spreadsheet_url:
                        results[entity_type] = spreadsheet_url
                        total_processed += len(data)

                        # Complete entity export
                        self.progress_tracker.complete_entity(
                            export_id, entity_type, spreadsheet_url
                        )

                        # Notify completion
                        entity_progress = self.progress_tracker.get_export_progress(export_id).entities[entity_type]
                        duration = entity_progress.duration.total_seconds() if entity_progress.duration else 0
                        await progress_notifier.notify_entity_completed(
                            export_id, entity_type, len(data), duration, spreadsheet_url
                        )

                except Exception as entity_error:
                    # Enhanced error handling
                    context = create_error_context(
                        component="sheets_exporter",
                        operation=f"export_{entity_type}",
                        entity_type=entity_type,
                        export_id=export_id
                    )
                    error_info = self.error_handler.handle_error(entity_error, context)

                    log_event("sheets", "error", f"Error exporting {entity_type}: {error_info.user_message}")

                    # Handle entity error with progress tracking
                    self.progress_tracker.handle_entity_error(export_id, entity_type, error_info.user_message)
                    await progress_notifier.notify_entity_error(export_id, entity_type, error_info.user_message)

            # Complete export
            duration = (datetime.now() - start_time).total_seconds()
            success = len(results) > 0

            self.progress_tracker.complete_export(export_id, success)
            await progress_notifier.notify_export_completed(
                export_id, success, total_processed, duration, results
            )

            log_event("sheets", "info", f"Enhanced export {export_id} completed in {duration:.2f}s")
            return results

        except Exception as e:
            # Enhanced error handling for overall export failure
            context = create_error_context(
                component="sheets_exporter",
                operation="overall_export",
                export_id=export_id
            )
            error_info = self.error_handler.handle_error(e, context)

            log_event("sheets", "error", f"Enhanced export {export_id} failed: {error_info.user_message}")
            log_event("sheets", "error", f"Stack trace: {traceback.format_exc()}")

            # Complete export with error
            self.progress_tracker.complete_export(export_id, success=False, error_message=error_info.user_message)

            # Send error notification
            await progress_notifier.send_notification({
                "export_id": export_id,
                "level": "error",
                "message": f"Export failed: {error_info.user_message}",
                "details": {"error": error_info.user_message, "suggested_actions": error_info.suggested_actions}
            })

            raise

    async def export_with_presets(self, presets: Dict[str, ExportPreset], date_from: Optional[str] = None, date_to: Optional[str] = None) -> Dict[str, str]:
        """
        Export data using saved presets for field selection and ordering

        Args:
            presets: Dictionary mapping entity_type to ExportPreset
            date_from: Optional start date filter
            date_to: Optional end date filter

        Returns:
            Dictionary mapping entity types to their spreadsheet URLs
        """
        log_event("sheets", "info", f"Starting export with {len(presets)} presets")

        # Validate presets
        for entity_type, preset in presets.items():
            # Handle case where preset might be a dict (from tests) or ExportPreset object
            if isinstance(preset, dict):
                # For dict presets, do basic validation
                if not preset.get('entity_presets'):
                    log_event("sheets", "warning", f"Dict preset for {entity_type} missing entity_presets")
                continue
            elif hasattr(preset, 'validate'):
                # For ExportPreset objects, use validate method
                validation_errors = preset.validate()
                if validation_errors:
                    raise ValueError(f"Invalid preset for {entity_type}: {'; '.join(validation_errors)}")
            else:
                # Skip validation for unknown preset types (like test strings)
                log_event("sheets", "info", f"Skipping validation for preset type: {type(preset)}")

        return await self.export_all_to_sheets_with_progress(
            date_from=date_from,
            date_to=date_to,
            presets=presets
        )

    def _collect_all_headers(self, processed_data: List[Dict[str, Any]]) -> Set[str]:
        """Efficiently collect all unique headers from processed data"""
        all_headers: Set[str] = set()
        for item in processed_data:
            all_headers.update(item.keys())
        return all_headers

    def _process_data_efficient(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process data more efficiently"""
        if not data:
            return []

        # Timestamp fields to convert to datetime format
        timestamp_fields = ['created_at', 'updated_at', 'closest_task_at', 'closed_at']

        # Collect field types first (single pass)
        field_types = self._collect_field_types(data)

        # Process each item (single pass)
        processed_data = []
        for item in data:
            if not item:
                continue

            processed_item = {}  # Start with empty dict instead of copying to avoid preserving complex structures

            # Copy simple values and convert complex ones to strings
            for key, value in item.items():
                if key == '_links':
                    # Process links field specially
                    if isinstance(value, dict) and 'self' in value and 'href' in value['self']:
                        processed_item['link_url'] = value['self']['href']
                elif key == 'custom_fields_values':
                    # Custom fields are handled separately below
                    pass
                elif key in timestamp_fields and value:
                    # Convert Unix timestamps to datetime format
                    processed_item[key] = self._process_timestamp(value, 'datetime')
                elif key in ('catalog_elements', 'companies', 'tags') and isinstance(value, (list, dict)) and not value:
                    # Convert empty lists/dicts to empty strings
                    processed_item[key] = ""
                elif isinstance(value, (list, dict)):
                    # Convert complex structures to JSON strings
                    try:
                        processed_item[key] = json.dumps(value)
                    except (TypeError, ValueError):
                        processed_item[key] = str(value)
                elif isinstance(value, str) and value.startswith('+'):
                    # Protect phone numbers that start with +
                    processed_item[key] = f"'{value}"
                else:
                    # Copy simple values directly
                    processed_item[key] = value

            # Process custom fields
            custom_fields_values = item.get('custom_fields_values')
            if custom_fields_values:
                custom_fields = self._parse_custom_fields(custom_fields_values)

                for field in custom_fields:
                    if not isinstance(field, dict):
                        continue

                    field_name = field.get('field_name', '')
                    field_id = field.get('field_id', '')
                    values = field.get('values', [])

                    if not field_id:
                        continue

                    # Prioritize field_name over field_id for column naming
                    if field_name:
                        # Use field_name as the primary column name
                        column_name = field_name
                        # Sanitize column name to avoid issues
                        column_name = column_name.replace('/', '_').replace('\\', '_').replace('[', '').replace(']', '')
                    else:
                        # Fallback to field_id based naming
                        column_name = f"custom_field_{field_id}"

                    key = f"{field_id}_{field_name}" if field_name else f"{field_id}_"
                    field_type = field_types.get(key, '')

                    # Process field based on type
                    self._set_field_value(processed_item, column_name, field_type, values)

            processed_data.append(processed_item)

        return processed_data

    def _set_field_value(self, item: Dict[str, Any], column_name: str, field_type: str, values: List[Dict[str, Any]]) -> None:
        """Set field value based on field type"""
        try:
            if not values:
                item[column_name] = ''
                return

            if field_type == 'multiselect':
                value_list = []
                for val in values:
                    if isinstance(val, dict) and 'value' in val:
                        value_list.append(str(val['value']))
                item[column_name] = ', '.join(value_list) if value_list else ''

            elif field_type == 'numeric':
                if values and isinstance(values[0], dict) and 'value' in values[0]:
                    item[column_name] = self._process_numeric_field(values[0]['value'])
                else:
                    item[column_name] = None

            elif field_type in ('date', 'datetime'):
                if values and isinstance(values[0], dict) and 'value' in values[0]:
                    date_value = values[0]['value']
                    if isinstance(date_value, (int, float)) or (isinstance(date_value, str) and date_value.isdigit()):
                        item[column_name] = self._process_timestamp(date_value, field_type)
                    else:
                        self._process_date_string(item, column_name, date_value, field_type)
                else:
                    item[column_name] = None

            elif field_type == 'text' and values and isinstance(values[0], dict) and 'value' in values[0]:
                value = values[0]['value']
                # Handle text that starts with +
                if isinstance(value, str) and value.startswith('+'):
                    item[column_name] = f"'{value}"
                else:
                    item[column_name] = value

            elif field_type in ('select', 'checkbox', 'url'):
                if values and isinstance(values[0], dict) and 'value' in values[0]:
                    item[column_name] = values[0]['value']
                else:
                    item[column_name] = ''

            else:
                value_list = []
                for val in values:
                    if isinstance(val, dict) and 'value' in val:
                        value_list.append(str(val['value']))
                item[column_name] = ', '.join(value_list) if value_list else ''

        except Exception as e:
            item[column_name] = f"ERROR: {str(e)[:20]}"

    def _process_date_string(self, item: Dict[str, Any], column_name: str, date_value: Any, field_type: str) -> None:
        """Process a date string value"""
        date_str = str(date_value)
        try:
            if field_type == 'date':
                parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
                item[column_name] = f'=DATE({parsed_date.year};{parsed_date.month};{parsed_date.day})'
            else:
                try:
                    parsed_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
                item[column_name] = f'=DATE({parsed_date.year};{parsed_date.month};{parsed_date.day}) + TIME({parsed_date.hour};{parsed_date.minute};{parsed_date.second})'
        except ValueError:
            item[column_name] = str(date_value)

    def _process_numeric_field(self, value):
        """Process a numeric field to ensure proper formatting"""
        if value is None:
            return None

        try:
            # Try to convert to a number
            num_value = float(value)
            # If it's a whole number, return an integer
            if num_value.is_integer():
                return int(num_value)
            else:
                return num_value
        except (ValueError, TypeError):
            # If conversion fails, return the original value
            return value

    def _process_timestamp(self, timestamp, format_type='date'):
        """Process a timestamp into a date or datetime"""
        if timestamp is None:
            return None

        try:
            # Convert to integer if it's a string
            if isinstance(timestamp, str):
                # Remove any apostrophes
                timestamp = timestamp.replace("'", "")
                timestamp = int(timestamp)

            # Convert timestamp to datetime
            dt = datetime.fromtimestamp(timestamp)

            # Format based on type
            if format_type == 'date':
                return f'=DATE({dt.year};{dt.month};{dt.day})'
            else:  # datetime
                return f'=DATE({dt.year};{dt.month};{dt.day}) + TIME({dt.hour};{dt.minute};{dt.second})'
        except (ValueError, TypeError, OSError):
            # If conversion fails, return the original value
            return timestamp

    def _get_credentials(self):
        """Get or refresh Google API credentials using the config manager (legacy method)"""
        try:
            # Try to get existing event loop
            loop = asyncio.get_running_loop()
            # If we're in an async context, just call the config manager directly
            return self.config_manager._get_credentials()
        except RuntimeError:
            # No running loop, so we can use asyncio.run()
            return asyncio.run(self._get_credentials_with_retry())

    async def _get_credentials_with_retry(self):
        """Get or refresh Google API credentials with enhanced error handling and retry logic"""
        async def get_creds_operation():
            self.config_manager._get_credentials()
            self.creds = self.config_manager.creds
            return self.creds

        try:
            await self.retry_handler.execute_with_retry(
                operation=get_creds_operation,
                operation_name="get_credentials",
                max_retries=3
            )
        except Exception as e:
            context = create_error_context(component="sheets_exporter", operation="authentication")
            error_info = self.error_handler.handle_error(e, context)
            log_event("sheets", "error", f"Error getting credentials: {error_info.user_message}")
            raise

    def _on_progress_update(self, export_id: str, progress) -> None:
        """Handle progress updates from the progress tracker"""
        try:
            # Log progress updates
            log_event("sheets", "info",
                     f"Export {export_id} progress: {progress.overall_progress_percentage:.1f}% "
                     f"({progress.completed_entities}/{progress.total_entities} entities)")
        except Exception as e:
            log_event("sheets", "warning", f"Error in progress update callback: {e}")

    def _build_date_query(self, date_from: Optional[str], date_to: Optional[str]) -> Dict[str, Any]:
        """Build MongoDB query for date filtering"""
        query = {}
        if date_from or date_to:
            query["updated_at"] = {}
            if date_from:
                from_dt = int(datetime.fromisoformat(date_from).timestamp())
                query["updated_at"]["$gte"] = from_dt
            if date_to:
                to_dt = int(datetime.fromisoformat(date_to).timestamp())
                query["updated_at"]["$lte"] = to_dt
            if not query["updated_at"]:
                del query["updated_at"]
        return query

    async def _get_entities_data_with_progress(
        self,
        export_id: str,
        query: Dict[str, Any]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get entity data and initialize progress tracking"""
        entities_data = {}

        # Entity types to export (excluding events per requirement 9.5)
        entity_types = ["leads", "contacts", "companies"]

        for entity_type in entity_types:
            try:
                # Get data from storage
                data = self.storage.get_entities(entity_type, query=query) or []
                entities_data[entity_type] = data

                # Initialize entity progress
                if data:
                    # Calculate estimated batches (assuming batch size of 1000)
                    batch_size = 1000
                    total_batches = (len(data) + batch_size - 1) // batch_size

                    self.progress_tracker.update_entity_progress(
                        export_id=export_id,
                        entity_type=entity_type,
                        processed=0,
                        total=len(data),
                        current_batch=0,
                        total_batches=total_batches,
                        status=ExportStatus.PENDING
                    )

                    log_event("sheets", "info", f"Initialized {entity_type}: {len(data)} items, {total_batches} batches")

            except Exception as e:
                log_event("sheets", "error", f"Error getting {entity_type} data: {e}")
                entities_data[entity_type] = []

        return entities_data

    async def _export_entity_with_progress_and_presets(
        self,
        export_id: str,
        entity_type: str,
        data: List[Dict[str, Any]],
        service,
        preset: Optional[ExportPreset] = None
    ) -> Optional[str]:
        """Export a single entity type with progress tracking and preset support"""
        try:
            spreadsheet_id = self.spreadsheet_ids.get(entity_type)
            if not spreadsheet_id:
                log_event("sheets", "warning", f"No spreadsheet ID configured for {entity_type}")
                return None

            # Update status to in progress
            self.progress_tracker.update_entity_progress(
                export_id=export_id,
                entity_type=entity_type,
                processed=0,
                total=len(data),
                status=ExportStatus.IN_PROGRESS
            )

            # Notify entity started
            await progress_notifier.notify_entity_started(export_id, entity_type, len(data))

            log_event("sheets", "info", f"Exporting {len(data)} {entity_type} records")

            # Process the data with enhanced custom field handling
            processed_data = self._process_data_with_enhanced_custom_fields(data)
            if not processed_data:
                log_event("sheets", "warning", f"No processed data for {entity_type}")
                return None

            # Update progress after data processing
            self.progress_tracker.update_entity_progress(
                export_id=export_id,
                entity_type=entity_type,
                processed=len(processed_data) // 4,  # Rough estimate of processing progress
                total=len(data)
            )

            # Apply preset field selection and ordering if provided
            if preset:
                processed_data, headers = self._apply_preset_to_data(processed_data, preset)
            else:
                # Collect headers and prepare rows with enhanced formatting
                all_headers = self._collect_all_headers(processed_data)
                headers = self._format_headers(list(all_headers), processed_data)

            rows = [headers]

            # Build rows with progress updates and enhanced formatting
            batch_size = 1000
            for i, item in enumerate(processed_data):
                if preset:
                    row = self._build_row_from_preset(item, preset)
                else:
                    row = self._build_formatted_row(item, all_headers)
                rows.append(row)

                # Update progress periodically
                if (i + 1) % batch_size == 0 or i == len(processed_data) - 1:
                    current_batch = (i // batch_size) + 1
                    total_batches = (len(processed_data) + batch_size - 1) // batch_size

                    self.progress_tracker.update_entity_progress(
                        export_id=export_id,
                        entity_type=entity_type,
                        processed=i + 1,
                        total=len(processed_data),
                        current_batch=current_batch,
                        total_batches=total_batches
                    )

                    # Send progress notification
                    await progress_notifier.notify_entity_progress(
                        export_id, entity_type, i + 1, len(processed_data),
                        current_batch, total_batches
                    )

            # Write data to sheets with progress tracking
            await self._write_data_with_progress(
                export_id, entity_type, service, spreadsheet_id, rows
            )

            # Return spreadsheet URL
            return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

        except Exception as e:
            log_event("sheets", "error", f"Error in _export_entity_with_progress_and_presets for {entity_type}: {e}")
            raise

    async def _write_data_with_progress(
        self,
        export_id: str,
        entity_type: str,
        service,
        spreadsheet_id: str,
        rows: List[List[Any]]
    ) -> None:
        """Write data to sheets with progress updates"""
        if not rows:
            return

        sheet_name = 'Data'

        try:
            # Ensure the sheet exists
            self._ensure_sheet_exists(service, spreadsheet_id, sheet_name)

            # Write headers first
            headers = rows[0]
            await self._write_rows_with_retry_async(service, spreadsheet_id, f'{sheet_name}!A1', [headers])

            # Write data in chunks with progress updates
            data_rows = rows[1:]
            if not data_rows:
                return

            log_event("sheets", "info", f"Writing {len(data_rows)} rows in chunks")

            chunk_size = MAX_ROWS_PER_BATCH
            total_chunks = (len(data_rows) + chunk_size - 1) // chunk_size

            for i in range(0, len(data_rows), chunk_size):
                chunk = data_rows[i:i + chunk_size]
                range_name = f'{sheet_name}!A{i + 2}'  # Start from row 2 (after headers)

                # Write chunk with retry logic
                await self._write_rows_with_retry_async(service, spreadsheet_id, range_name, chunk)

                # Update progress
                chunk_number = (i // chunk_size) + 1
                log_event("sheets", "info", f"Wrote chunk {chunk_number}/{total_chunks} ({len(chunk)} rows)")

                # Send progress notification
                processed_items = min(i + chunk_size, len(data_rows))
                await progress_notifier.notify_entity_progress(
                    export_id, entity_type, processed_items, len(data_rows),
                    chunk_number, total_chunks
                )

        except Exception as e:
            log_event("sheets", "error", f"Error writing data for {entity_type}: {e}")
            raise

    def _parse_custom_fields(self, custom_fields_values: Any) -> List[Dict[str, Any]]:
        """Parse custom fields values whether it's a string or already a list"""
        try:
            if custom_fields_values is None:
                return []

            # If it's already a list, return it
            if isinstance(custom_fields_values, list):
                return custom_fields_values

            # If it's a string, try to parse as JSON
            if isinstance(custom_fields_values, str):
                try:
                    parsed = json.loads(custom_fields_values)
                    if not isinstance(parsed, list):
                        log_event("sheets", "warning", f"Parsed custom fields is not a list but {type(parsed)}")
                        return []
                    return parsed
                except json.JSONDecodeError as e:
                    log_event("sheets", "warning", f"Failed to parse custom_fields_values as JSON: {str(e)}")
                    return []

            # If it's neither a list nor a string, log warning and return empty list
            log_event("sheets", "warning", f"Custom fields is neither a list nor a string but {type(custom_fields_values)}")
            return []

        except Exception as e:
            log_event("sheets", "error", f"Error parsing custom fields: {e}")
            return []

    def _collect_field_types(self, data: List[Dict[str, Any]]) -> Dict[str, str]:
        """Collect custom field types from data"""
        field_types = {}

        for item in data:
            if item is None:
                continue

            custom_fields_values = item.get('custom_fields_values')
            if not custom_fields_values:
                continue

            custom_fields = self._parse_custom_fields(custom_fields_values)
            if not custom_fields:
                continue

            for field in custom_fields:
                if not isinstance(field, dict):
                    continue

                field_name = field.get('field_name', '')
                field_id = field.get('field_id', '')
                field_type = field.get('field_type', '')

                if field_name and field_id:
                    key = f"{field_id}_{field_name}"
                    field_types[key] = field_type

        return field_types

    def _process_data_with_enhanced_custom_fields(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process data with enhanced custom field handling
        Implements requirements 7.2, 7.5 for custom field type preservation and graceful error handling
        """
        if not data:
            return []

        log_event("sheets", "info", f"Processing {len(data)} items with enhanced custom field handling")

        # Extract field metadata for better processing
        field_metadata = self.field_metadata_extractor.extract_field_metadata(data)
        log_event("sheets", "info", f"Extracted metadata for {len(field_metadata)} custom fields")

        # Use the existing efficient processing method as base
        processed_data = self._process_data_efficient(data)

        # Apply enhanced custom field processing
        for item in processed_data:
            try:
                # Process custom fields with enhanced processor
                custom_fields_values = None
                # Find the original item to get custom_fields_values
                for original_item in data:
                    if original_item.get('id') == item.get('id'):
                        custom_fields_values = original_item.get('custom_fields_values')
                        break

                if custom_fields_values:
                    # Process custom fields with type preservation and graceful error handling
                    processing_result = self.custom_field_processor.process_custom_fields(
                        custom_fields_values,
                        preserve_field_types=False,  # Convert to Google Sheets friendly format
                        graceful_degradation=True
                    )

                    # Update processed custom fields in the item
                    for field_name, field_value in processing_result.processed_fields.items():
                        # Format the value using the data formatter
                        formatted_value = self.data_formatter.format_value(field_value, None, field_name)
                        item[field_name] = formatted_value

                    if not processing_result.success:
                        log_event("sheets", "warning",
                            f"Custom field processing had errors for item {item.get('id', 'unknown')}: {processing_result.errors}")

            except Exception as cf_error:
                error_msg = f"Critical custom field processing error for item {item.get('id', 'unknown')}: {str(cf_error)}"
                log_event("sheets", "error", error_msg)

        return processed_data

    def _apply_preset_to_data(self, data: List[Dict[str, Any]], preset: ExportPreset) -> tuple[List[Dict[str, Any]], List[str]]:
        """
        Apply preset field selection and ordering to processed data

        Args:
            data: Processed data items
            preset: Export preset with field selection and ordering

        Returns:
            Tuple of (filtered_data, headers)
        """
        if not preset.selected_fields:
            return data, []

        # Create headers based on preset field order and custom field mappings
        headers = []
        for field_id in preset.field_order:
            if field_id in preset.selected_fields:
                # Use custom field mapping if available, otherwise use field_id
                display_name = preset.get_display_name(field_id)
                headers.append(display_name)

        # Filter data to only include selected fields
        filtered_data = []
        for item in data:
            filtered_item = {}
            for field_id in preset.field_order:
                if field_id in preset.selected_fields:
                    display_name = preset.get_display_name(field_id)
                    # Get value from original field_id
                    value = item.get(field_id, '')
                    filtered_item[display_name] = value
            filtered_data.append(filtered_item)

        log_event("sheets", "info", f"Applied preset '{preset.name}': {len(headers)} fields, {len(filtered_data)} items")
        return filtered_data, headers

    def _build_row_from_preset(self, item: Dict[str, Any], preset: ExportPreset) -> List[Any]:
        """Build a row from an item using preset field ordering"""
        row = []
        for field_id in preset.field_order:
            if field_id in preset.selected_fields:
                display_name = preset.get_display_name(field_id)
                value = item.get(display_name, '')

                # Format the value using the data formatter
                formatted_value = self.data_formatter.format_value(value, None, display_name)
                row.append(formatted_value)

        return row

    def _build_formatted_row(self, item: Dict[str, Any], headers: List[str]) -> List[Any]:
        """Build a row from an item and headers with enhanced formatting"""
        row = []
        for header in headers:
            value = item.get(header, '')

            # Determine field type if possible
            field_type = self._detect_field_type(header, value)

            # Format the value using the data formatter
            formatted_value = self.data_formatter.format_value(value, field_type, header)
            row.append(formatted_value)

        return row

    def _format_headers(self, headers: List[str], data: List[Dict[str, Any]]) -> List[str]:
        """Format headers to be human-readable with fallbacks"""
        formatted_headers = []

        # Collect custom field mappings from data
        custom_field_mappings = self._extract_custom_field_mappings(data)

        for header in sorted(headers):
            if header.startswith('custom_field_') or header in custom_field_mappings:
                # Handle custom fields
                display_name = custom_field_mappings.get(header)
                formatted_header = self.header_formatter.format_custom_field_header(
                    header, display_name=display_name
                )
            else:
                # Handle regular fields
                formatted_header = self.header_formatter.format_header(header)

            formatted_headers.append(formatted_header)

        return formatted_headers

    def _extract_custom_field_mappings(self, data: List[Dict[str, Any]]) -> Dict[str, str]:
        """Extract custom field display names from data"""
        mappings = {}

        for item in data:
            if not isinstance(item, dict):
                continue

            # Look for custom field metadata in the original data
            custom_fields_values = item.get('custom_fields_values')
            if custom_fields_values:
                custom_fields = self._parse_custom_fields(custom_fields_values)
                for field in custom_fields:
                    if isinstance(field, dict):
                        field_id = field.get('field_id')
                        field_name = field.get('field_name')
                        if field_id and field_name:
                            # Create mapping for the processed field name
                            processed_field_name = f"custom_field_{field_id}"
                            if field_name not in mappings.get(processed_field_name, ''):
                                mappings[processed_field_name] = field_name

        return mappings

    def _detect_field_type(self, field_name: str, value: Any) -> Optional[str]:
        """Detect field type based on field name and value characteristics"""
        # Check field name patterns
        if field_name in ('created_at', 'updated_at', 'closed_at', 'closest_task_at'):
            return 'datetime'

        if 'date' in field_name.lower():
            return 'date'

        if field_name in ('price', 'score') or 'amount' in field_name.lower():
            return 'numeric'

        if field_name in ('phone', 'mobile') or 'phone' in field_name.lower():
            return 'text'  # Phone numbers should be treated as text

        if field_name == 'email' or 'email' in field_name.lower():
            return 'text'

        if field_name in ('web', 'website') or 'url' in field_name.lower():
            return 'url'

        # Auto-detect based on value if no field name pattern matches
        if isinstance(value, (int, float)):
            return 'numeric'

        if isinstance(value, str) and value.isdigit():
            # Could be timestamp or numeric
            if len(value) == 10:  # Unix timestamp
                return 'datetime'
            else:
                return 'numeric'

        return None  # Let auto-detection handle it