"""
Enhanced Google Sheets exporter with integrated progress tracking and error handling
"""

import asyncio
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
import traceback

from .sheets_exporter import SheetsExporter
from ..utils.progress_tracker import ExportProgressTracker, ExportStatus
from ..utils.progress_notifier import progress_notifier, ProgressNotification, NotificationLevel
from ..utils.data_formatter import DataFormatter, ColumnHeaderFormatter
from ..utils.custom_field_processor import CustomFieldProcessor, CustomFieldMetadataExtractor
from ..utils.batch_processor import (
    ConfigurableBatchProcessor, BatchProcessingConfig,
    create_large_dataset_config, create_memory_constrained_config, create_api_optimized_config
)
from ..utils.intelligent_retry_scheduler import (
    IntelligentRetryScheduler, RetrySchedulerConfig, RetryPriority
)
from ..utils.concurrent_export_manager import (
    ConcurrentExportManager, ResourceLimits, ExportPriority
)
from ..storage.storage import Storage
from ..core.logger import log_event
from ..web.export_presets import ExportPreset


class EnhancedSheetsExporter(SheetsExporter):
    """
    Enhanced Google Sheets exporter with integrated progress tracking,
    real-time updates, and comprehensive error handling
    """

    def __init__(self, storage: Storage, batch_config: Optional[BatchProcessingConfig] = None,
                 resource_limits: Optional[ResourceLimits] = None, enable_concurrent_management: bool = True,
                 progress_tracker: Optional[ExportProgressTracker] = None):
        """Initialize the enhanced sheets exporter"""
        super().__init__(storage, progress_tracker=progress_tracker)
        # Use provided progress tracker or create a new one
        self.progress_tracker = progress_tracker or ExportProgressTracker(storage)

        # Initialize data formatting components
        self.data_formatter = DataFormatter()
        self.header_formatter = ColumnHeaderFormatter()
        self.custom_field_processor = CustomFieldProcessor()
        self.field_metadata_extractor = CustomFieldMetadataExtractor()

        # Initialize batch processing system
        self.batch_config = batch_config or create_api_optimized_config()
        self.batch_processor = ConfigurableBatchProcessor(self.batch_config)

        # Initialize intelligent retry scheduler
        retry_config = RetrySchedulerConfig(
            max_concurrent_retries=3,
            default_max_retries=5,
            enable_adaptive_retry=True
        )
        self.retry_scheduler = IntelligentRetryScheduler(retry_config)

        # Initialize concurrent export manager
        self.enable_concurrent_management = enable_concurrent_management
        if enable_concurrent_management:
            self.resource_limits = resource_limits or ResourceLimits(
                max_concurrent_exports=2,  # Conservative default for Google Sheets API
                max_memory_usage_mb=1024.0,
                max_api_calls_per_minute=600,  # Google Sheets API limit
                max_api_calls_per_hour=6000
            )
            self.concurrent_manager = ConcurrentExportManager(self.resource_limits)
        else:
            self.concurrent_manager = None

        # Register progress callback to send updates
        self.progress_tracker.add_progress_callback(self._on_progress_update)

    def _validate_export_configuration(self, export_config: Dict[str, Any]) -> bool:
        """Validate export configuration before starting export"""
        try:
            # Check if required fields are present
            if not export_config:
                return False

            # Check if entity_presets is provided
            if 'entity_presets' not in export_config:
                log_event("sheets", "warning", "No entity_presets in export configuration")
                return False

            # Check if spreadsheet_ids is provided
            if 'spreadsheet_ids' not in export_config:
                log_event("sheets", "warning", "No spreadsheet_ids in export configuration")
                return False

            # Validate spreadsheet IDs format
            for entity_type, spreadsheet_id in export_config.get('spreadsheet_ids', {}).items():
                if spreadsheet_id and len(spreadsheet_id) != 44:
                    log_event("sheets", "error", f"Invalid spreadsheet ID format for {entity_type}: {spreadsheet_id}")
                    return False

            return True
        except Exception as e:
            log_event("sheets", "error", f"Error validating export configuration: {str(e)}")
            return False

    async def _prepare_export_data(self, entity_presets: Dict[str, str], date_from: Optional[str] = None, date_to: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """Prepare data for export based on entity presets"""
        try:
            # Build query for date filtering
            query = self._build_date_query(date_from, date_to)

            # Get entity data from storage
            entities_data = {}
            for entity_type in entity_presets.keys():
                try:
                    if entity_type == "leads":
                        # Get leads data
                        entities_data[entity_type] = self.storage.get_data("leads", query)
                    elif entity_type == "contacts":
                        # Get contacts data
                        entities_data[entity_type] = self.storage.get_data("contacts", query)
                    elif entity_type == "companies":
                        # Get companies data
                        entities_data[entity_type] = self.storage.get_data("companies", query)
                    elif entity_type == "events":
                        # Get events data
                        entities_data[entity_type] = self.storage.get_data("events", query)
                    else:
                        log_event("sheets", "warning", f"Unknown entity type: {entity_type}")
                        entities_data[entity_type] = []
                except Exception as e:
                    log_event("sheets", "error", f"Error getting {entity_type} data: {str(e)}")
                    entities_data[entity_type] = []

            return entities_data
        except Exception as e:
            log_event("sheets", "error", f"Error preparing export data: {str(e)}")
            return {}

    async def _write_data_to_sheets(self, entities_data: Dict[str, List[Dict[str, Any]]], spreadsheet_ids: Dict[str, str], entity_presets: Dict[str, str]) -> Dict[str, Any]:
        """Write prepared data to Google Sheets"""
        try:
            # Get credentials and service with automatic refresh check
            self._get_credentials_with_refresh_check()
            service = self._build_service()

            results = {"success": True, "entity_results": {}}

            for entity_type, data in entities_data.items():
                try:
                    spreadsheet_id = spreadsheet_ids.get(entity_type)
                    if not spreadsheet_id:
                        log_event("sheets", "warning", f"No spreadsheet ID for {entity_type}")
                        continue

                    if not data:
                        log_event("sheets", "info", f"No data to export for {entity_type}")
                        continue

                    # Process data for export
                    processed_data = self._process_data_efficient(data)

                    # Export to sheets using existing method
                    spreadsheet_url = await self._export_entity_with_progress_and_presets(
                        export_id="default",
                        entity_type=entity_type,
                        data=processed_data,
                        service=service
                    )

                    results["entity_results"][entity_type] = {
                        "success": True,
                        "url": spreadsheet_url,
                        "records_count": len(processed_data)
                    }

                except Exception as e:
                    log_event("sheets", "error", f"Error writing {entity_type} to sheets: {str(e)}")
                    results["entity_results"][entity_type] = {
                        "success": False,
                        "error": str(e)
                    }

            return results
        except Exception as e:
            log_event("sheets", "error", f"Error writing data to sheets: {str(e)}")
            return {"success": False, "error": str(e)}

    def _process_data_with_preset(self, data: List[Dict[str, Any]], preset: ExportPreset) -> List[Dict[str, Any]]:
        """Process data according to preset configuration"""
        try:
            if not data or not preset:
                return data

            processed_data = []

            for item in data:
                processed_item = {}

                # Apply field selection from preset
                if preset.selected_fields:
                    for field in preset.selected_fields:
                        if field in item:
                            processed_item[field] = item[field]
                        elif field.startswith('custom_field_'):
                            # Handle custom fields
                            custom_field_id = field.replace('custom_field_', '')
                            custom_fields = item.get('custom_fields_values', [])
                            for custom_field in custom_fields:
                                if str(custom_field.get('field_id', '')) == custom_field_id:
                                    # Get custom field name from preset mapping
                                    field_name = preset.custom_field_mappings.get(field, field)
                                    values = custom_field.get('values', [])
                                    if values:
                                        processed_item[field_name] = values[0].get('value', '')
                                    break
                else:
                    # If no field selection, include all fields
                    processed_item = item.copy()

                # Process custom fields
                if 'custom_fields_values' in item:
                    custom_fields = self._parse_custom_fields(item['custom_fields_values'])
                    for custom_field in custom_fields:
                        if isinstance(custom_field, dict):
                            field_name = custom_field.get('field_name', '')
                            if field_name:
                                processed_item[field_name] = custom_field.get('value', '')

                processed_data.append(processed_item)

            # Apply field ordering from preset
            if preset.field_order:
                ordered_data = []
                for item in processed_data:
                    ordered_item = {}
                    # First add fields in specified order
                    for field in preset.field_order:
                        if field in item:
                            ordered_item[field] = item[field]
                    # Then add any remaining fields
                    for field, value in item.items():
                        if field not in ordered_item:
                            ordered_item[field] = value
                    ordered_data.append(ordered_item)
                processed_data = ordered_data

            return processed_data
        except Exception as e:
            log_event("sheets", "error", f"Error processing data with preset: {str(e)}")
            return data

    async def export_all_to_sheets_with_progress(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        export_id: Optional[str] = None,
        presets: Optional[Dict[str, Any]] = None
    ) -> Dict[str, str]:
        """
        Export all entity data to Google Sheets with real-time progress tracking

        Args:
            date_from: Start date filter
            date_to: End date filter
            export_id: Optional export ID for tracking
            presets: Optional presets configuration for export

        Returns:
            Dictionary mapping entity types to their spreadsheet URLs
        """
        # Determine entity types to export
        entity_types = ["leads", "contacts", "companies", "events"]

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

            # Get credentials and service
            self._get_credentials()
            service = self._build_service()

            # Build query for date filtering
            query = self._build_date_query(date_from, date_to)

            # Update spreadsheet IDs if provided in presets
            if presets and hasattr(presets, 'get') and 'spreadsheet_ids' in presets:
                log_event("sheets", "info", f"Updating spreadsheet IDs from presets: {presets['spreadsheet_ids']}")
                self.spreadsheet_ids.update(presets['spreadsheet_ids'])

            # Get all entity data with counts for progress tracking
            if presets and hasattr(presets, 'get') and 'entity_presets' in presets:
                # Use _prepare_export_data when presets config is provided (for testing)
                entities_data = await self._prepare_export_data(presets.get('entity_presets', {}), date_from, date_to)
            else:
                # Use direct data retrieval for normal operation
                entities_data = await self._get_entities_data_with_progress(export_id, query)

            results = {}
            total_processed = 0

            # Process each entity type with progress tracking
            for entity_type, data in entities_data.items():
                try:
                    if not data:
                        log_event("sheets", "info", f"No {entity_type} data to export")
                        self.progress_tracker.complete_entity(export_id, entity_type)
                        continue

                    # Export entity with progress tracking
                    spreadsheet_url = await self._export_entity_with_progress(
                        export_id, entity_type, data, service
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
                    error_msg = str(entity_error)
                    log_event("sheets", "error", f"Error exporting {entity_type}: {error_msg}")

                    # Handle entity error
                    self.progress_tracker.handle_entity_error(export_id, entity_type, error_msg)
                    await progress_notifier.notify_entity_error(export_id, entity_type, error_msg)

            # Complete export
            duration = (datetime.now() - start_time).total_seconds()
            success = len(results) > 0

            self.progress_tracker.complete_export(export_id, success)
            await progress_notifier.notify_export_completed(
                export_id, success, total_processed, duration, results
            )

            log_event("sheets", "info", f"Enhanced export {export_id} completed in {duration:.2f}s")

            # Return detailed export results
            exported_entities = {}
            failed_entities = []
            errors = []

            # Get progress information to extract errors
            progress = self.progress_tracker.get_export_progress(export_id)

            for entity_type, data in entities_data.items():
                exported_entities[entity_type] = len(data) if data else 0

                # Check if entity failed and collect errors
                if progress and entity_type in progress.entities:
                    entity_progress = progress.entities[entity_type]
                    if entity_progress.status == ExportStatus.FAILED and entity_progress.error_message:
                        failed_entities.append(entity_type)
                        errors.append(f"{entity_type}: {entity_progress.error_message}")

            # Determine final status based on results
            if not success or errors:
                final_status = "partial" if results else "failed"
            else:
                final_status = "completed"

            result = {
                "status": final_status,
                "exported_entities": exported_entities,
                "spreadsheet_urls": results,
                "errors": errors,
                "total_processed": total_processed,
                "duration": duration
            }

            if failed_entities:
                result["failed_entities"] = failed_entities

            return result

        except Exception as e:
            error_msg = str(e)
            log_event("sheets", "error", f"Enhanced export {export_id} failed: {error_msg}")
            log_event("sheets", "error", f"Stack trace: {traceback.format_exc()}")

            # Complete export with error
            self.progress_tracker.complete_export(export_id, success=False, error_message=error_msg)

            # Send error notification
            notification = ProgressNotification(
                export_id=export_id,
                level=NotificationLevel.ERROR,
                message=f"Export failed: {error_msg}",
                details={"error": error_msg}
            )
            await progress_notifier.send_notification(notification)

            # Return error result instead of raising
            return {
                "status": "failed",
                "exported_entities": {},
                "spreadsheet_urls": {},
                "errors": [error_msg],
                "total_processed": 0,
                "duration": 0
            }

    async def _get_entities_data_with_progress(
        self,
        export_id: str,
        query: Dict[str, Any]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get entity data and initialize progress tracking"""
        entities_data = {}

        # Entity types to export
        entity_types = ["leads", "contacts", "companies", "events"]

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

    async def _export_entity_with_progress(
        self,
        export_id: str,
        entity_type: str,
        data: List[Dict[str, Any]],
        service
    ) -> Optional[str]:
        """Export a single entity type with progress tracking"""
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

            # Collect headers and prepare rows with enhanced formatting
            all_headers = self._collect_all_headers(processed_data)
            formatted_headers = self._format_headers(all_headers, processed_data)
            rows = [formatted_headers]

            # Build rows with progress updates and enhanced formatting
            batch_size = 1000
            for i, item in enumerate(processed_data):
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
            log_event("sheets", "error", f"Error in _export_entity_with_progress for {entity_type}: {e}")
            raise

    async def _write_data_with_progress(
        self,
        export_id: str,
        entity_type: str,
        service,
        spreadsheet_id: str,
        rows: List[List[Any]]
    ) -> None:
        """Write data to sheets with progress updates using configurable batch processing"""
        if not rows:
            return

        sheet_name = 'Data'

        try:
            # Ensure the sheet exists
            self._ensure_sheet_exists(service, spreadsheet_id, sheet_name)

            # Write headers first
            headers = rows[0]
            self._write_rows_with_retry(service, spreadsheet_id, f'{sheet_name}!A1', [headers])

            # Use configurable batch processor for data rows
            data_rows = rows[1:]
            if not data_rows:
                return

            log_event("sheets", "info", f"Writing {len(data_rows)} rows using configurable batch processing")

            # Start retry scheduler if not already running
            await self.retry_scheduler.start_scheduler()

            # Define batch processor function
            def write_batch_to_sheets(batch_data: List[List[Any]]) -> Dict[str, Any]:
                """Process a batch of rows for writing to sheets"""
                if not batch_data:
                    return {"rows_written": 0, "success": True}

                # Calculate range for this batch
                start_row = 2  # Start after headers
                batch_start_index = data_rows.index(batch_data[0]) if batch_data[0] in data_rows else 0
                actual_start_row = start_row + batch_start_index
                range_name = f'{sheet_name}!A{actual_start_row}'

                # Write batch with retry logic
                body = {'values': batch_data}
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption='USER_ENTERED',
                    body=body
                ).execute()

                return {
                    "rows_written": len(batch_data),
                    "range": range_name,
                    "success": True
                }

            # Progress callback for batch processing
            def batch_progress_callback(batch_id: str, processed: int, total: int):
                """Handle progress updates from batch processor"""
                # Calculate batch number and total batches
                current_batch_size = self.batch_processor.batch_adjuster.get_optimal_batch_size()
                current_batch = (processed // current_batch_size) + 1
                total_batches = (total + current_batch_size - 1) // current_batch_size

                # Update entity progress
                self.progress_tracker.update_entity_progress(
                    export_id=export_id,
                    entity_type=entity_type,
                    processed=processed + 1,  # +1 for headers
                    total=len(rows),
                    current_batch=current_batch,
                    total_batches=total_batches
                )

                # Send progress notification
                asyncio.create_task(progress_notifier.notify_entity_progress(
                    export_id, entity_type, processed + 1, len(rows),
                    current_batch, total_batches
                ))

            # Process data in configurable batches
            batch_results = await self.batch_processor.process_data_in_batches(
                data=data_rows,
                processor_func=write_batch_to_sheets,
                progress_callback=batch_progress_callback,
                batch_id_prefix=f"{export_id}_{entity_type}_write"
            )

            # Handle failed batches with intelligent retry
            failed_batches = [result for result in batch_results if result.status != "completed"]
            if failed_batches:
                log_event("sheets", "warning", f"Scheduling {len(failed_batches)} failed batches for retry")

                for failed_batch in failed_batches:
                    # Get the original data for this batch
                    batch_start = int(failed_batch.batch_id.split('_')[-1]) - 1
                    batch_size = self.batch_processor.batch_adjuster.get_optimal_batch_size()
                    batch_data = data_rows[batch_start * batch_size:(batch_start + 1) * batch_size]

                    # Schedule for intelligent retry
                    self.retry_scheduler.schedule_retry(
                        batch_result=failed_batch,
                        retry_data=batch_data,
                        processor_func=write_batch_to_sheets,
                        priority=RetryPriority.HIGH,
                        max_retries=3
                    )

            # Log batch processing statistics
            processing_stats = self.batch_processor.get_processing_stats()
            log_event("sheets", "info",
                     f"Batch processing completed for {entity_type}: "
                     f"{processing_stats['successful_batches']}/{processing_stats['total_batches']} batches successful, "
                     f"avg batch size: {processing_stats.get('batch_adjustment', {}).get('current_batch_size', 'N/A')}")

        except Exception as e:
            log_event("sheets", "error", f"Error writing data for {entity_type}: {e}")
            raise

    async def _write_chunk_with_retry_and_progress(
        self,
        export_id: str,
        entity_type: str,
        service,
        spreadsheet_id: str,
        range_name: str,
        chunk: List[List[Any]],
        chunk_index: int,
        total_chunks: int,
        max_retries: int = 3
    ) -> None:
        """Write a chunk with retry logic and progress notifications"""
        for attempt in range(max_retries):
            try:
                body = {'values': chunk}
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption='USER_ENTERED',
                    body=body
                ).execute()
                return

            except Exception as e:
                if "rate limit" in str(e).lower() or "quota" in str(e).lower():
                    # Handle rate limiting
                    retry_after = min(60, 2 ** attempt)  # Exponential backoff, max 60 seconds
                    await progress_notifier.notify_rate_limit_warning(export_id, entity_type, retry_after)
                    await asyncio.sleep(retry_after)

                elif attempt == max_retries - 1:  # Last attempt
                    raise
                else:
                    # General retry
                    await progress_notifier.notify_batch_retry(
                        export_id, entity_type, (chunk_index // 5000) + 1, attempt + 1
                    )
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

    def _build_service(self) -> Any:
        """Build Google Sheets service"""
        from googleapiclient.discovery import build

        # Ensure credentials are available
        if not self.creds:
            raise Exception(
                "Google Sheets credentials not available. This usually means:\n"
                "1. The credentials.json file is missing from the project root\n"
                "2. The OAuth token has expired and cannot be refreshed\n"
                "3. The authentication process failed\n\n"
                "Please check:\n"
                "- Ensure credentials.json exists in the project root\n"
                "- Run the authentication setup if needed\n"
                "- Check the logs for authentication errors"
            )

        return build('sheets', 'v4', credentials=self.creds)

    def _get_credentials_with_refresh_check(self) -> None:
        """Get credentials with proactive refresh check before use"""
        # First get credentials normally
        self._get_credentials()

        # Then check if they need proactive refresh
        if self.creds and self.creds.valid and self.creds.expiry:
            from datetime import datetime, timedelta
            time_until_expiry = self.creds.expiry - datetime.utcnow()

            # If token expires within 10 minutes, refresh it now
            if time_until_expiry < timedelta(minutes=10):
                log_event("sheets", "info", f"Token expires in {time_until_expiry}, refreshing before export")
                if self.creds.refresh_token:
                    try:
                        # Use the config manager's enhanced refresh method
                        refresh_success = self.config_manager._refresh_token_with_retry()
                        if refresh_success:
                            self.creds = self.config_manager.creds
                            log_event("sheets", "info", "Successfully refreshed token before export")
                        else:
                            log_event("sheets", "warning", "Token refresh failed, continuing with current token")
                    except Exception as e:
                        log_event("sheets", "warning", f"Error during proactive refresh: {e}, continuing with current token")
                else:
                    log_event("sheets", "warning", "No refresh token available, cannot refresh proactively")

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

    def _build_row(self, item: Dict[str, Any], headers: List[str]) -> List[Any]:
        """Build a row from an item and headers (legacy method for compatibility)"""
        return self._build_formatted_row(item, headers)

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

        # Timestamp fields to convert to datetime format
        timestamp_fields = ['created_at', 'updated_at', 'closest_task_at', 'closed_at']

        processed_data = []
        processing_errors = []
        processing_warnings = []

        for item_index, item in enumerate(data):
            if not item:
                continue

            try:
                processed_item = {}

                # Process standard fields
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
                        processed_item[key] = self.data_formatter.format_value(value, 'datetime', key)
                    elif key in ('catalog_elements', 'companies', 'tags') and isinstance(value, (list, dict)) and not value:
                        # Convert empty lists/dicts to empty strings
                        processed_item[key] = ""
                    elif isinstance(value, (list, dict)):
                        # Convert complex structures using data formatter
                        processed_item[key] = self.data_formatter.format_value(value, None, key)
                    else:
                        # Process simple values with data formatter
                        processed_item[key] = self.data_formatter.format_value(value, None, key)

                # Process custom fields with enhanced processor
                custom_fields_values = item.get('custom_fields_values')
                if custom_fields_values:
                    try:
                        # Process custom fields with type preservation and graceful error handling
                        processing_result = self.custom_field_processor.process_custom_fields(
                            custom_fields_values,
                            preserve_field_types=False,  # Convert to Google Sheets friendly format
                            graceful_degradation=True
                        )

                        # Add processed custom fields to the item
                        processed_item.update(processing_result.processed_fields)

                        # Collect errors and warnings
                        processing_errors.extend(processing_result.errors)
                        processing_warnings.extend(processing_result.warnings)

                        if not processing_result.success:
                            log_event("sheets", "warning",
                                f"Custom field processing had errors for item {item_index}: {processing_result.errors}")

                    except Exception as cf_error:
                        error_msg = f"Critical custom field processing error for item {item_index}: {str(cf_error)}"
                        processing_errors.append(error_msg)
                        log_event("sheets", "error", error_msg)

                        # Add fallback custom field processing
                        try:
                            fallback_fields = self._fallback_custom_field_processing(custom_fields_values)
                            processed_item.update(fallback_fields)
                        except Exception as fallback_error:
                            log_event("sheets", "error", f"Fallback custom field processing also failed: {fallback_error}")

                processed_data.append(processed_item)

            except Exception as item_error:
                error_msg = f"Error processing item {item_index}: {str(item_error)}"
                processing_errors.append(error_msg)
                log_event("sheets", "error", error_msg)

                # Try to add a minimal processed item
                try:
                    minimal_item = {
                        'id': item.get('id', f'error_item_{item_index}'),
                        'processing_error': f"Error: {str(item_error)[:100]}"
                    }
                    processed_data.append(minimal_item)
                except Exception:
                    log_event("sheets", "error", f"Could not create minimal item for index {item_index}")

        # Log processing summary
        if processing_errors:
            log_event("sheets", "warning", f"Custom field processing completed with {len(processing_errors)} errors")
        if processing_warnings:
            log_event("sheets", "info", f"Custom field processing completed with {len(processing_warnings)} warnings")

        log_event("sheets", "info", f"Enhanced processing completed: {len(processed_data)} items processed")

        return processed_data

    def _fallback_custom_field_processing(self, custom_fields_values: Any) -> Dict[str, Any]:
        """
        Fallback custom field processing when enhanced processing fails
        Uses the original processing logic as a safety net
        """
        try:
            # Use the parent class method as fallback
            custom_fields = self._parse_custom_fields(custom_fields_values)
            if not custom_fields:
                return {}

            fallback_fields = {}
            for field in custom_fields:
                if not isinstance(field, dict):
                    continue

                field_name = field.get('field_name', '')
                field_id = field.get('field_id', '')
                values = field.get('values', [])

                if not field_id:
                    continue

                # Create simple field name
                if field_name:
                    column_name = field_name.replace('/', '_').replace('\\', '_').replace('[', '').replace(']', '')
                else:
                    column_name = f"custom_field_{field_id}"

                # Simple value extraction
                if values and isinstance(values[0], dict) and 'value' in values[0]:
                    value = values[0]['value']
                    fallback_fields[f"cf_{field_id}_{column_name}"] = self.data_formatter.format_value(value, None, column_name)
                else:
                    fallback_fields[f"cf_{field_id}_{column_name}"] = ''

            return fallback_fields

        except Exception as e:
            log_event("sheets", "error", f"Fallback custom field processing failed: {e}")
            return {'custom_fields_error': f'Processing failed: {str(e)[:50]}'}

    def get_custom_field_processing_stats(self) -> Dict[str, Any]:
        """Get statistics about custom field processing"""
        return {
            'errors': self.custom_field_processor.get_processing_errors(),
            'warnings': self.custom_field_processor.get_processing_warnings(),
            'formatter_errors': self.data_formatter.get_formatting_errors()
        }

    def _on_progress_update(self, export_id: str, progress) -> None:
        """Handle progress updates from the tracker"""
        # This method is called by the progress tracker
        # We can add additional logic here if needed
        log_event("sheets", "debug", f"Progress update for export {export_id}: {progress.overall_progress_percentage:.1f}%")

    def get_export_progress(self, export_id: str):
        """Get current export progress"""
        return self.progress_tracker.get_export_progress(export_id)

    def get_all_active_exports(self):
        """Get all active exports"""
        return self.progress_tracker.get_all_active_exports()

    def cancel_export(self, export_id: str) -> bool:
        """Cancel an export"""
        return self.progress_tracker.cancel_export(export_id)

    def get_batch_processing_stats(self) -> Dict[str, Any]:
        """Get comprehensive batch processing statistics"""
        batch_stats = self.batch_processor.get_processing_stats()
        retry_stats = self.retry_scheduler.get_retry_stats()

        return {
            "batch_processing": batch_stats,
            "retry_scheduling": retry_stats,
            "active_batches": self.batch_processor.get_active_batches(),
            "active_retries": self.retry_scheduler.get_active_retries()
        }

    def configure_batch_processing(self, config: BatchProcessingConfig) -> None:
        """Update batch processing configuration"""
        self.batch_config = config
        self.batch_processor = ConfigurableBatchProcessor(config)
        log_event("sheets", "info", "Batch processing configuration updated")

    def reset_batch_processing_stats(self) -> None:
        """Reset batch processing statistics"""
        self.batch_processor.reset_stats()
        log_event("sheets", "info", "Batch processing statistics reset")

    async def cleanup_batch_processing(self) -> None:
        """Cleanup batch processing resources"""
        try:
            await self.retry_scheduler.stop_scheduler()
            log_event("sheets", "info", "Batch processing cleanup completed")
        except Exception as e:
            log_event("sheets", "error", f"Error during batch processing cleanup: {e}")

    def get_optimal_batch_size(self) -> int:
        """Get current optimal batch size"""
        return self.batch_processor.batch_adjuster.get_optimal_batch_size()

    def get_memory_usage_stats(self) -> Dict[str, Any]:
        """Get memory usage statistics"""
        return self.batch_processor.memory_monitor.get_memory_stats()

    def cancel_batch(self, batch_id: str) -> bool:
        """Cancel a specific batch"""
        return self.batch_processor.cancel_batch(batch_id)

    def cancel_retry(self, batch_id: str) -> bool:
        """Cancel a scheduled retry"""
        return self.retry_scheduler.cancel_retry(batch_id)

    async def start_concurrent_management(self) -> None:
        """Start the concurrent export management system"""
        if self.concurrent_manager:
            await self.concurrent_manager.start_manager()
            log_event("sheets", "info", "Concurrent export management started")

    async def stop_concurrent_management(self) -> None:
        """Stop the concurrent export management system"""
        if self.concurrent_manager:
            await self.concurrent_manager.stop_manager()
            log_event("sheets", "info", "Concurrent export management stopped")

    async def queue_concurrent_export(
        self,
        export_id: str,
        entity_types: List[str],
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        priority: ExportPriority = ExportPriority.NORMAL,
        progress_callback: Optional[Callable] = None,
        completion_callback: Optional[Callable] = None
    ) -> bool:
        """Queue an export for concurrent processing"""
        if not self.concurrent_manager:
            log_event("sheets", "warning", "Concurrent management not enabled")
            return False

        export_config = {
            "date_from": date_from,
            "date_to": date_to,
            "spreadsheet_ids": self.spreadsheet_ids
        }

        return self.concurrent_manager.queue_export(
            export_id=export_id,
            entity_types=entity_types,
            export_config=export_config,
            priority=priority,
            batch_config=self.batch_config,
            progress_callback=progress_callback,
            completion_callback=completion_callback
        )

    def get_concurrent_export_status(self, export_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a concurrent export"""
        if not self.concurrent_manager:
            return None
        return self.concurrent_manager.get_export_status(export_id)

    def get_all_concurrent_exports(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get all concurrent exports by status"""
        if not self.concurrent_manager:
            return {"queued": [], "running": [], "completed": []}
        return self.concurrent_manager.get_all_exports()

    async def cancel_concurrent_export(self, export_id: str) -> bool:
        """Cancel a concurrent export"""
        if not self.concurrent_manager:
            return False
        return await self.concurrent_manager.cancel_export(export_id)

    def get_concurrent_manager_stats(self) -> Dict[str, Any]:
        """Get comprehensive concurrent manager statistics"""
        if not self.concurrent_manager:
            return {"concurrent_management_disabled": True}
        return self.concurrent_manager.get_manager_stats()

    def update_resource_limits(self, new_limits: ResourceLimits) -> None:
        """Update resource limits for concurrent management"""
        if self.concurrent_manager:
            self.concurrent_manager.update_resource_limits(new_limits)
            self.resource_limits = new_limits
            log_event("sheets", "info", "Resource limits updated")

    def get_resource_usage_stats(self) -> Dict[str, Any]:
        """Get current resource usage statistics"""
        if not self.concurrent_manager:
            return {"concurrent_management_disabled": True}

        manager_stats = self.concurrent_manager.get_manager_stats()
        return {
            "resource_usage": manager_stats.get("resources", {}),
            "quota_usage": manager_stats.get("quota", {}),
            "active_exports": manager_stats.get("queue", {}).get("running_exports", 0),
            "queued_exports": manager_stats.get("queue", {}).get("queued_exports", 0)
        }

    def clear_completed_concurrent_exports(self) -> int:
        """Clear completed concurrent exports and return count"""
        if not self.concurrent_manager:
            return 0
        return self.concurrent_manager.clear_completed_exports()

    async def export_with_concurrent_management(
        self,
        entity_types: List[str],
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        priority: ExportPriority = ExportPriority.NORMAL,
        export_id: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Export with concurrent management - queues the export and waits for completion
        """
        if not self.concurrent_manager:
            # Fall back to regular export
            return await self.export_all_to_sheets_with_progress(date_from, date_to, export_id)

        if not export_id:
            export_id = f"export_{int(time.time())}"

        # Start concurrent manager if not already running
        await self.start_concurrent_management()

        # Queue the export
        success = await self.queue_concurrent_export(
            export_id=export_id,
            entity_types=entity_types,
            date_from=date_from,
            date_to=date_to,
            priority=priority
        )

        if not success:
            raise Exception(f"Failed to queue export {export_id}")

        # Wait for completion (with timeout)
        timeout = 3600  # 1 hour timeout
        start_time = time.time()

        while time.time() - start_time < timeout:
            status = self.get_concurrent_export_status(export_id)
            if not status:
                raise Exception(f"Export {export_id} not found")

            if status["status"] == "completed":
                return status.get("results", {})
            elif status["status"] == "failed":
                errors = status.get("errors", ["Unknown error"])
                raise Exception(f"Export failed: {'; '.join(errors)}")
            elif status["status"] == "cancelled":
                raise Exception("Export was cancelled")

            # Wait before checking again
            await asyncio.sleep(5)

        # Timeout reached
        await self.cancel_concurrent_export(export_id)
        raise Exception(f"Export {export_id} timed out after {timeout} seconds")