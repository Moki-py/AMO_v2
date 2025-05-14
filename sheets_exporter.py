"""
Google Sheets exporter for AmoCRM data
"""

import os
import json
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Set
import traceback
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from storage import Storage
from logger import log_event
import config

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Maximum number of rows to write at once - increased for better performance
MAX_ROWS_PER_BATCH = 5000  # Increased from 1000 to 5000

class SheetsExporter:
    """Exports data from MongoDB to Google Sheets"""

    def __init__(self, storage: Storage):
        """Initialize the Google Sheets exporter"""
        self.storage = storage
        self.creds = None
        self.token_path = 'token.json'
        self.credentials_path = 'credentials.json'
        # Get spreadsheet IDs from config settings
        self.spreadsheet_ids = {
            'leads': config.settings.google_sheets_leads_id,
            'contacts': config.settings.google_sheets_contacts_id,
            'companies': config.settings.google_sheets_companies_id,
            'events': config.settings.google_sheets_events_id
        }
        # Validate spreadsheet IDs
        missing_ids = [entity for entity, sheet_id in self.spreadsheet_ids.items() if not sheet_id]
        if missing_ids:
            raise Exception(
                f"Missing Google Sheets IDs for: {', '.join(missing_ids)}\n"
                "Please set the following in your .env file:\n" +
                "\n".join(f"- GOOGLE_SHEETS_{entity.upper()}_ID" for entity in missing_ids)
            )

    def _write_rows_with_retry(self, service, spreadsheet_id: str, range_name: str, rows: List[List[Any]], max_retries: int = 3) -> None:
        """Write rows to a sheet with retry logic"""
        for attempt in range(max_retries):
            try:
                body = {'values': rows}
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption='USER_ENTERED',  # Changed from 'RAW' to 'USER_ENTERED'
                    body=body
                ).execute()
                return
            except Exception as e:
                if attempt == max_retries - 1:  # Last attempt
                    raise
                log_event("sheets", "warning", f"Attempt {attempt + 1} failed, retrying in 2 seconds... Error: {e}")
                time.sleep(2)  # Wait before retrying

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

    def export_all_to_sheets(self, date_from: str = None, date_to: str = None) -> Dict[str, str]:
        """
        Export all entity data to separate Google Sheets
        Returns a dictionary mapping entity types to their spreadsheet URLs
        """
        try:
            start_time = time.time()
            log_event("sheets", "info", "Starting Google Sheets export")

            self._get_credentials()
            service = build('sheets', 'v4', credentials=self.creds)

            # Build MongoDB query for updated_at filter
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

            # Get all entity data with filter
            entities_data = {
                "leads": self.storage.get_entities("leads", query=query) or [],
                "contacts": self.storage.get_entities("contacts", query=query) or [],
                "companies": self.storage.get_entities("companies", query=query) or [],
                "events": self.storage.get_entities("events", query=query) or []
            }

            results = {}
            # Process each entity type
            for entity_type, data in entities_data.items():
                entity_start_time = time.time()
                try:
                    spreadsheet_id = self.spreadsheet_ids.get(entity_type)
                    if not spreadsheet_id:
                        log_event("sheets", "warning", f"No spreadsheet ID configured for {entity_type}")
                        continue

                    # Skip empty data sets
                    if not data:
                        log_event("sheets", "info", f"No {entity_type} data to export")
                        continue

                    log_event("sheets", "info", f"Exporting {len(data)} {entity_type} records")

                    # Process the data more efficiently
                    processed_data = self._process_data_efficient(data)
                    if not processed_data:
                        log_event("sheets", "warning", f"No processed data for {entity_type}")
                        continue

                    # Collect all headers from all items
                    all_headers = self._collect_all_headers(processed_data)
                    headers = sorted(list(all_headers))

                    # Start with headers row
                    rows = [headers]

                    # Build rows more efficiently, ensuring all values are safe for Google Sheets
                    for item in processed_data:
                        row = []
                        for header in headers:
                            value = item.get(header, '')
                            # Make sure the value is a safe type for Google Sheets
                            if isinstance(value, (list, dict)):
                                try:
                                    # Try to convert complex types to JSON strings
                                    row.append(json.dumps(value))
                                except (TypeError, ValueError):
                                    # If JSON conversion fails, use string representation
                                    row.append(str(value))
                            elif value is None:
                                row.append("")
                            elif isinstance(value, str) and value.startswith('+'):
                                # Protect values that start with + by prefixing with a single quote
                                row.append(f"'{value}")
                            else:
                                row.append(value)
                        rows.append(row)

                    # Write data in chunks
                    self._write_data_in_chunks(service, spreadsheet_id, 'Data', rows)

                    # Store the URL for this entity type
                    results[entity_type] = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

                    entity_time = time.time() - entity_start_time
                    log_event("sheets", "info", f"Exported {entity_type} in {entity_time:.2f} seconds")

                except Exception as sheet_error:
                    log_event("sheets", "error", f"Error exporting {entity_type}: {sheet_error}")
                    log_event("sheets", "error", f"Sheet error stack trace: {traceback.format_exc()}")

            total_time = time.time() - start_time
            log_event("sheets", "info", f"Completed export in {total_time:.2f} seconds")
            return results

        except Exception as e:
            log_event("sheets", "error", f"Error exporting to Google Sheets: {e}")
            log_event("sheets", "error", f"Stack trace: {traceback.format_exc()}")
            raise

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

                    if not field_name or not field_id:
                        continue

                    column_name = field_name
                    key = f"{field_id}_{field_name}"
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
        """Get or refresh Google API credentials"""
        if not os.path.exists(self.credentials_path):
            raise Exception(
                "credentials.json not found. Please follow these steps:\n"
                "1. Go to https://console.cloud.google.com/\n"
                "2. Create a project or select an existing one\n"
                "3. Enable the Google Sheets API\n"
                "4. Go to 'APIs & Services' > 'Credentials'\n"
                "5. Click 'Create Credentials' > 'OAuth client ID'\n"
                "6. Choose 'Desktop app' as the application type\n"
                "7. Download the JSON file and save it as 'credentials.json' in this directory"
            )

        try:
            # Load the credentials file to validate its format
            with open(self.credentials_path, 'r') as f:
                creds_data = json.load(f)
                if 'installed' not in creds_data and 'web' not in creds_data:
                    raise ValueError(
                        "Invalid credentials format. The credentials must be for a desktop or web application.\n"
                        "Please make sure you selected 'Desktop app' when creating the OAuth client ID."
                    )
        except json.JSONDecodeError:
            raise ValueError(
                "Invalid credentials.json file. The file must be a valid JSON file.\n"
                "Please download a new credentials file from the Google Cloud Console."
            )

        if os.path.exists(self.token_path):
            try:
                self.creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)
            except Exception as e:
                log_event("sheets", "warning", f"Error loading existing token: {e}")
                # If token is invalid, delete it
                os.remove(self.token_path)
                self.creds = None

        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                try:
                    self.creds.refresh(Request())
                except Exception as e:
                    log_event("sheets", "warning", f"Error refreshing token: {e}")
                    # If refresh fails, delete the token and start fresh
                    os.remove(self.token_path)
                    self.creds = None

            if not self.creds:
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(self.credentials_path, SCOPES)
                    self.creds = flow.run_local_server(port=0)
                except Exception as e:
                    raise Exception(
                        f"Error during OAuth flow: {str(e)}\n"
                        "Please make sure you have:\n"
                        "1. Enabled the Google Sheets API in your project\n"
                        "2. Created OAuth 2.0 credentials for a desktop application\n"
                        "3. Added your email as a test user in the OAuth consent screen"
                    )

            # Save the credentials for the next run
            try:
                with open(self.token_path, 'w') as token:
                    token.write(self.creds.to_json())
            except Exception as e:
                log_event("sheets", "warning", f"Error saving token: {e}")

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