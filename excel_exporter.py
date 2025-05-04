"""
Excel exporter for AmoCRM data
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
import traceback

from storage import Storage
from logger import log_event


class ExcelExporter:
    """Exports data from MongoDB to Excel files"""

    def __init__(self, storage: Storage):
        """Initialize the Excel exporter"""
        self.storage = storage
        self.export_dir = "exports"

        # Create exports directory if it doesn't exist
        if not os.path.exists(self.export_dir):
            os.makedirs(self.export_dir)

    def export_all_to_excel(self) -> str:
        """
        Export all entity data to a single Excel file with different sheets
        Returns the path to the created Excel file
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.export_dir}/amocrm_export_{timestamp}.xlsx"

            # Get all entity data
            entities_data = {
                "leads": self.storage.get_entities("leads") or [],
                "contacts": self.storage.get_entities("contacts") or [],
                "companies": self.storage.get_entities("companies") or [],
                "events": self.storage.get_entities("events") or []
            }

            # Debug log the structure of the first item
            for entity_type, data in entities_data.items():
                if data and len(data) > 0:
                    try:
                        first_item = data[0]
                        if first_item is None:
                            log_event("excel", "warning", f"First {entity_type} item is None")
                            continue

                        # Log _links structure if present
                        if '_links' in first_item:
                            links = first_item.get('_links')
                            log_event("excel", "info", f"First {entity_type} item _links type: {type(links)}")
                            if links:
                                log_event("excel", "info", f"_links sample: {json.dumps(links)[:200]}")

                        if 'custom_fields_values' in first_item:
                            custom_fields = first_item.get('custom_fields_values')
                            if custom_fields is None:
                                log_event("excel", "warning", f"First {entity_type} item has None custom_fields_values")
                                continue

                            log_event("excel", "info", f"First {entity_type} item custom_fields_values type: {type(custom_fields)}")
                            if isinstance(custom_fields, str):
                                try:
                                    parsed = json.loads(custom_fields)
                                    if parsed:
                                        first_sample = parsed[0] if isinstance(parsed, list) and len(parsed) > 0 else 'Not a list or empty'
                                        log_event("excel", "info", f"Sample parsed custom fields: {json.dumps(first_sample) if first_sample != 'Not a list or empty' else first_sample}")
                                except Exception as e:
                                    log_event("excel", "warning", f"Sample custom fields not valid JSON: {str(custom_fields)[:100]} - Error: {e}")
                            elif isinstance(custom_fields, list):
                                first_sample = custom_fields[0] if custom_fields and len(custom_fields) > 0 else None
                                log_event("excel", "info", f"Sample custom fields (list): {json.dumps(first_sample) if first_sample else 'Empty list or None'}")
                        else:
                            log_event("excel", "info", f"First {entity_type} item has no custom_fields_values key")
                    except Exception as sample_error:
                        log_event("excel", "error", f"Error examining sample {entity_type}: {sample_error}")
                    break

            # Track if we have any data at all
            has_any_data = any(len(data) > 0 for data in entities_data.values())

            # Create a writer with default engine
            writer = pd.ExcelWriter(filename, engine='openpyxl')

            # Always ensure we have at least one valid sheet
            created_sheets = False

            # Process each entity type
            for entity_type, data in entities_data.items():
                try:
                    if data and len(data) > 0:
                        # Process custom fields and other complex columns
                        log_event("excel", "info", f"Processing data for {entity_type} ({len(data)} items)")
                        processed_data = self._process_data([item.copy() if item is not None else {} for item in data])

                        # Debug log the column names after processing
                        if processed_data and len(processed_data) > 0:
                            try:
                                column_names = list(processed_data[0].keys()) if processed_data[0] else []
                                custom_column_names = [col for col in column_names if col.startswith('custom_')]
                                log_event("excel", "info", f"Processed {entity_type} columns: {len(column_names)} total, {len(custom_column_names)} custom columns")
                                if custom_column_names:
                                    log_event("excel", "info", f"Sample custom columns: {custom_column_names[:5]}")
                                if 'custom_fields_values' in column_names:
                                    log_event("excel", "warning", f"'custom_fields_values' still present in processed data!")
                                if '_links' in column_names:
                                    log_event("excel", "warning", f"'_links' column still present in processed data!")
                            except Exception as column_error:
                                log_event("excel", "error", f"Error getting column names: {column_error}")

                        # Create DataFrame - ensure we handle None values safely
                        safe_data = [item if item is not None else {} for item in processed_data]
                        df = pd.DataFrame(safe_data)

                        # Handle empty DataFrame
                        if df.empty or len(df.columns) == 0:
                            log_event("excel", "warning", f"Empty DataFrame for {entity_type}, creating placeholder")
                            df = pd.DataFrame([{"Info": f"No processable {entity_type} data available"}])
                        else:
                            # Reorder columns to put custom fields at the end
                            try:
                                columns = df.columns.tolist()
                                custom_columns = [col for col in columns if col.startswith('custom_')]
                                regular_columns = [col for col in columns if not (col.startswith('custom_') or
                                                                                col == 'custom_fields_values' or
                                                                                col == '_links')]

                                # Ensure unwanted columns are removed
                                for col_to_remove in ['custom_fields_values', '_links']:
                                    if col_to_remove in columns:
                                        log_event("excel", "info", f"Removing '{col_to_remove}' column from {entity_type} sheet")
                                        df = df.drop(columns=[col_to_remove], errors='ignore')

                                # Add link column if it exists in the processed data
                                link_column = 'link_url'
                                if link_column in columns:
                                    regular_columns.append(link_column)

                                # Reorder columns only if we have valid columns
                                if regular_columns or custom_columns:
                                    df = df[regular_columns + custom_columns]
                            except Exception as column_error:
                                log_event("excel", "error", f"Error reordering columns for {entity_type}: {column_error}")

                        # Write to Excel
                        df.to_excel(writer, sheet_name=entity_type, index=False)
                        log_event("excel", "info", f"Added sheet '{entity_type}' with {len(df)} rows and {len(df.columns)} columns")
                        created_sheets = True
                    else:
                        # Create a sheet with a message for this entity type
                        df = pd.DataFrame([{"Info": f"No {entity_type} data available"}])
                        df.to_excel(writer, sheet_name=entity_type, index=False)
                        log_event("excel", "info", f"Added empty '{entity_type}' sheet with placeholder message")
                        created_sheets = True
                except Exception as sheet_error:
                    log_event("excel", "error", f"Error creating sheet for {entity_type}: {sheet_error}")
                    log_event("excel", "error", f"Sheet error stack trace: {traceback.format_exc()}")
                    # Create error sheet
                    try:
                        error_df = pd.DataFrame([{"Error": f"Error processing {entity_type}: {sheet_error}"}])
                        error_df.to_excel(writer, sheet_name=f"{entity_type}_error", index=False)
                        created_sheets = True
                    except:
                        pass  # If even this fails, continue to next sheet

            # If somehow no sheets were created, add a default sheet
            if not created_sheets:
                try:
                    summary_df = pd.DataFrame([
                        {"Message": "No data available to export"},
                        {"Message": "Please export data from AmoCRM first"}
                    ])
                    summary_df.to_excel(writer, sheet_name="Summary", index=False)
                    log_event("excel", "warning", "Created fallback summary sheet")
                except Exception as fallback_error:
                    log_event("excel", "error", f"Error creating fallback sheet: {fallback_error}")

            # Save the Excel file
            try:
                writer.close()
                log_event("excel", "info", "Excel writer closed successfully")
            except Exception as close_error:
                log_event("excel", "error", f"Error closing Excel writer: {close_error}")
                raise

            if not os.path.exists(filename) or os.path.getsize(filename) == 0:
                raise Exception("Excel file was not created properly")

            log_event("excel", "info", f"Successfully exported all data to {filename}")
            return filename

        except Exception as e:
            log_event("excel", "error", f"Error exporting to Excel: {e}")
            # Log the full stack trace for debugging
            trace = traceback.format_exc()
            log_event("excel", "error", f"Stack trace: {trace}")
            raise

    def _process_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process data by handling complex fields like custom_fields_values and _links
        """
        try:
            if data is None:
                log_event("excel", "warning", "Input data is None, returning empty list")
                return []

            processed_data = []

            # First, process custom fields to collect field types
            field_types = self._collect_field_types(data)

            # Process each item
            for item in data:
                if item is None:
                    processed_data.append({})
                    continue

                # Make a deep copy to avoid modifying the original
                processed_item = item.copy()

                # Process _links field
                self._process_links(processed_item)

                # Process custom fields
                self._process_custom_fields(processed_item, field_types)

                processed_data.append(processed_item)

            return processed_data
        except Exception as e:
            log_event("excel", "error", f"Error processing data: {e}")
            log_event("excel", "error", f"Stack trace: {traceback.format_exc()}")
            # If there's an error, try to return a safe version of the data
            try:
                return [{k: v for k, v in item.items() if k != 'custom_fields_values'} if item else {} for item in data]
            except:
                return []

    def _process_links(self, item: Dict[str, Any]) -> None:
        """
        Process _links field to extract URL
        """
        try:
            links = item.get('_links')
            if not links:
                return

            # Remove _links field
            item.pop('_links', None)

            # Extract URL from the structure
            if isinstance(links, dict) and 'self' in links and isinstance(links['self'], dict):
                href = links['self'].get('href')
                if href:
                    item['link_url'] = href
            elif isinstance(links, str):
                try:
                    parsed_links = json.loads(links)
                    if isinstance(parsed_links, dict) and 'self' in parsed_links and isinstance(parsed_links['self'], dict):
                        href = parsed_links['self'].get('href')
                        if href:
                            item['link_url'] = href
                except:
                    item['link_url'] = str(links)
            else:
                # Try to extract from any structure
                try:
                    item['link_url'] = str(links)
                except:
                    pass
        except Exception as e:
            log_event("excel", "error", f"Error processing _links: {e}")

    def _collect_field_types(self, data: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Collect custom field types from data
        """
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
                    # Use field_id in the key to handle fields with the same name
                    key = f"{field_id}_{field_name}"
                    field_types[key] = field_type

        # Debug log the detected field types
        if field_types:
            log_event("excel", "info", f"Detected {len(field_types)} custom field types: {list(field_types.keys())[:5]}...")
        else:
            log_event("excel", "warning", "No custom field types detected")

        return field_types

    def _process_custom_fields(self, item: Dict[str, Any], field_types: Dict[str, str]) -> None:
        """
        Process custom fields in a single item
        """
        try:
            custom_fields_values = item.get('custom_fields_values')
            if not custom_fields_values:
                # Remove empty custom_fields_values
                item.pop('custom_fields_values', None)
                return

            # Remove the original field FIRST - must happen BEFORE adding custom columns
            item.pop('custom_fields_values', None)

            custom_fields = self._parse_custom_fields(custom_fields_values)
            if not custom_fields:
                return

            for field in custom_fields:
                if not isinstance(field, dict):
                    continue

                field_name = field.get('field_name', '')
                field_id = field.get('field_id', '')
                values = field.get('values', [])

                if not field_name or not field_id or not values:
                    continue

                # Format column name to be more readable - include field ID for uniqueness
                column_name = f"custom_{field_name}_{field_id}"

                # Get field type
                key = f"{field_id}_{field_name}"
                field_type = field_types.get(key, '')

                try:
                    # Extract values based on field type
                    if field_type == 'multiselect':
                        value_list = []
                        for val in values:
                            if isinstance(val, dict) and 'value' in val:
                                value_list.append(str(val['value']))
                        item[column_name] = ', '.join(value_list) if value_list else ''

                    elif field_type in ('select', 'text', 'numeric', 'date', 'datetime', 'checkbox'):
                        # For single-value fields, just take the first value
                        if values and isinstance(values[0], dict) and 'value' in values[0]:
                            item[column_name] = values[0]['value']
                        else:
                            item[column_name] = ''

                    elif field_type == 'url':
                        # Format URL fields as clickable links in Excel
                        if values and isinstance(values[0], dict) and 'value' in values[0]:
                            item[column_name] = values[0]['value']
                        else:
                            item[column_name] = ''

                    else:
                        # Default handling for unknown types
                        value_list = []
                        for val in values:
                            if isinstance(val, dict) and 'value' in val:
                                value_list.append(str(val['value']))
                        item[column_name] = ', '.join(value_list) if value_list else ''
                except Exception as value_error:
                    log_event("excel", "error", f"Error processing field {field_name}: {value_error}")
                    item[column_name] = f"ERROR: {str(value_error)[:20]}"
        except Exception as e:
            log_event("excel", "error", f"Error processing custom fields: {e}")

    def _parse_custom_fields(self, custom_fields) -> Optional[List[Dict[str, Any]]]:
        """Parse custom_fields_values whether it's a string or already a list"""
        if custom_fields is None:
            return None

        if isinstance(custom_fields, str):
            try:
                parsed = json.loads(custom_fields)
                if not isinstance(parsed, list):
                    log_event("excel", "warning", f"Parsed custom fields is not a list but {type(parsed)}")
                    return []
                return parsed
            except json.JSONDecodeError as e:
                log_event("excel", "warning", f"Failed to parse custom_fields_values as JSON: {str(e)}")
                return []

        if not isinstance(custom_fields, list):
            log_event("excel", "warning", f"Custom fields is not a list but {type(custom_fields)}")
            return []

        return custom_fields