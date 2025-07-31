"""
Data formatting utilities for Google Sheets export
Provides consistent formatting for dates, numbers, text, and special characters
"""

import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from decimal import Decimal, InvalidOperation

from ..core.logger import log_event


class DataFormatter:
    """
    Handles consistent data formatting for Google Sheets export
    Implements requirements 7.1, 7.3, 7.4 for consistent formatting
    """

    # Special characters that need escaping in Google Sheets
    SPECIAL_CHARS_PATTERN = re.compile(r'[=+\-@]')

    # Phone number patterns
    PHONE_PATTERN = re.compile(r'^\+?[\d\s\-\(\)]+$')

    # Email pattern
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

    # URL pattern
    URL_PATTERN = re.compile(r'^https?://[^\s]+$')

    def __init__(self):
        """Initialize the data formatter"""
        self.formatting_errors = []

    def format_value(self, value: Any, field_type: Optional[str] = None, field_name: Optional[str] = None) -> Any:
        """
        Format a value for Google Sheets export with appropriate type handling

        Args:
            value: The value to format
            field_type: Optional field type hint (date, datetime, numeric, text, etc.)
            field_name: Optional field name for context in error messages

        Returns:
            Formatted value safe for Google Sheets
        """
        try:
            # Handle None/empty values
            if value is None or value == '':
                return ''

            # Handle complex data structures
            if isinstance(value, (list, dict)):
                return self._format_complex_value(value)

            # Format based on field type if provided
            if field_type:
                return self._format_by_type(value, field_type, field_name)

            # Auto-detect and format based on value characteristics
            return self._auto_format_value(value, field_name)

        except Exception as e:
            error_msg = f"Formatting error for field '{field_name or 'unknown'}': {str(e)}"
            self.formatting_errors.append(error_msg)
            log_event("formatter", "warning", error_msg)

            # Return safe fallback value
            return self._safe_string_conversion(value)

    def _format_by_type(self, value: Any, field_type: str, field_name: Optional[str] = None) -> Any:
        """Format value based on explicit field type"""
        try:
            if field_type in ('date', 'datetime'):
                return self._format_date_value(value, field_type)
            elif field_type == 'numeric':
                return self._format_numeric_value(value)
            elif field_type == 'text':
                return self._format_text_value(value)
            elif field_type == 'multiselect':
                return self._format_multiselect_value(value)
            elif field_type in ('select', 'checkbox', 'url'):
                return self._format_simple_value(value)
            else:
                return self._auto_format_value(value, field_name)
        except Exception as e:
            log_event("formatter", "warning", f"Type-specific formatting failed for {field_type}: {e}")
            return self._auto_format_value(value, field_name)

    def _auto_format_value(self, value: Any, field_name: Optional[str] = None) -> Any:
        """Auto-detect value type and apply appropriate formatting"""
        # Convert to string for analysis
        str_value = str(value).strip()

        # Handle empty strings
        if not str_value:
            return ''

        # Check for timestamps (Unix timestamps)
        if self._is_timestamp(str_value):
            return self._format_timestamp(str_value)

        # Check for numeric values
        if self._is_numeric(str_value):
            return self._format_numeric_value(str_value)

        # Check for dates in string format
        if self._is_date_string(str_value):
            return self._format_date_string(str_value)

        # Check for phone numbers
        if self._is_phone_number(str_value):
            return self._format_phone_number(str_value)

        # Check for emails
        if self._is_email(str_value):
            return self._format_email(str_value)

        # Check for URLs
        if self._is_url(str_value):
            return self._format_url(str_value)

        # Default to text formatting
        return self._format_text_value(str_value)

    def _format_date_value(self, value: Any, field_type: str = 'date') -> str:
        """Format date/datetime values for Google Sheets"""
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
            return self._format_timestamp(value, field_type)
        elif isinstance(value, str):
            return self._format_date_string(value, field_type)
        else:
            return str(value)

    def _format_timestamp(self, timestamp: Union[str, int, float], format_type: str = 'date') -> str:
        """Convert Unix timestamp to Google Sheets date formula"""
        try:
            # Convert to integer if it's a string
            if isinstance(timestamp, str):
                timestamp = timestamp.replace("'", "").strip()
                timestamp = int(float(timestamp))

            # Convert timestamp to datetime
            dt = datetime.fromtimestamp(timestamp)

            # Return Google Sheets date formula
            if format_type == 'date':
                return f'=DATE({dt.year},{dt.month},{dt.day})'
            else:  # datetime
                return f'=DATE({dt.year},{dt.month},{dt.day})+TIME({dt.hour},{dt.minute},{dt.second})'

        except (ValueError, TypeError, OSError) as e:
            log_event("formatter", "warning", f"Timestamp formatting failed: {e}")
            return str(timestamp)

    def _format_date_string(self, date_str: str, format_type: str = 'date') -> str:
        """Format date string to Google Sheets date formula"""
        try:
            # Try common date formats
            date_formats = [
                '%Y-%m-%d',
                '%Y-%m-%d %H:%M:%S',
                '%d.%m.%Y',
                '%d/%m/%Y',
                '%m/%d/%Y'
            ]

            parsed_date = None
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue

            if parsed_date:
                if format_type == 'date':
                    return f'=DATE({parsed_date.year},{parsed_date.month},{parsed_date.day})'
                else:
                    return f'=DATE({parsed_date.year},{parsed_date.month},{parsed_date.day})+TIME({parsed_date.hour},{parsed_date.minute},{parsed_date.second})'
            else:
                return date_str

        except Exception as e:
            log_event("formatter", "warning", f"Date string formatting failed: {e}")
            return date_str

    def _format_numeric_value(self, value: Any) -> Union[int, float, str]:
        """Format numeric values with proper type preservation"""
        try:
            # Handle None or empty
            if value is None or value == '':
                return ''

            # Convert to string and clean
            str_value = str(value).strip().replace(',', '.')

            # Try to parse as Decimal for precision
            try:
                decimal_value = Decimal(str_value)

                # Check if it's a whole number
                if decimal_value % 1 == 0:
                    return int(decimal_value)
                else:
                    # Return as float, but limit decimal places for readability
                    float_value = float(decimal_value)
                    # Round to 6 decimal places to avoid floating point precision issues
                    return round(float_value, 6)

            except InvalidOperation:
                # Try regular float conversion
                return float(str_value)

        except (ValueError, TypeError) as e:
            log_event("formatter", "warning", f"Numeric formatting failed: {e}")
            return str(value)

    def _format_text_value(self, value: Any) -> str:
        """Format text values with proper escaping for Google Sheets"""
        if value is None:
            return ''

        text = str(value).strip()

        # Handle empty text
        if not text:
            return ''

        # Escape special characters that could be interpreted as formulas
        if self.SPECIAL_CHARS_PATTERN.match(text):
            # Prefix with single quote to prevent formula interpretation
            return f"'{text}"

        # Handle text that starts with + (like phone numbers)
        if text.startswith('+'):
            return f"'{text}"

        # Handle very long text (truncate if necessary)
        if len(text) > 50000:  # Google Sheets cell limit
            log_event("formatter", "warning", f"Text truncated: length {len(text)} > 50000")
            return text[:49997] + "..."

        return text

    def _format_phone_number(self, phone: str) -> str:
        """Format phone numbers with proper escaping"""
        # Always prefix phone numbers with single quote to preserve formatting
        return f"'{phone.strip()}"

    def _format_email(self, email: str) -> str:
        """Format email addresses"""
        return email.strip().lower()

    def _format_url(self, url: str) -> str:
        """Format URLs"""
        return url.strip()

    def _format_multiselect_value(self, value: Any) -> str:
        """Format multiselect field values"""
        if isinstance(value, list):
            # Join multiple values with comma and space
            return ', '.join(str(v).strip() for v in value if v)
        elif isinstance(value, str):
            return value.strip()
        else:
            return str(value)

    def _format_simple_value(self, value: Any) -> str:
        """Format simple field values (select, checkbox, url)"""
        return str(value).strip() if value else ''

    def _format_complex_value(self, value: Union[list, dict]) -> str:
        """Format complex data structures as JSON strings"""
        try:
            if not value:  # Empty list or dict
                return ''

            # Convert to JSON with proper formatting
            json_str = json.dumps(value, ensure_ascii=False, separators=(',', ':'))

            # Limit length to prevent cell overflow
            if len(json_str) > 32767:  # Google Sheets formula limit
                log_event("formatter", "warning", f"JSON truncated: length {len(json_str)} > 32767")
                return json_str[:32764] + "..."

            return json_str

        except (TypeError, ValueError) as e:
            log_event("formatter", "warning", f"JSON formatting failed: {e}")
            return str(value)

    def _safe_string_conversion(self, value: Any) -> str:
        """Safely convert any value to string as fallback"""
        try:
            if value is None:
                return ''

            str_value = str(value)

            # Apply basic escaping for safety
            if str_value and str_value[0] in '=+-@':
                return f"'{str_value}"

            return str_value

        except Exception:
            return 'ERROR: Cannot format value'

    # Helper methods for type detection
    def _is_timestamp(self, value: str) -> bool:
        """Check if value is a Unix timestamp"""
        try:
            num_value = float(value)
            # Check if it's in reasonable timestamp range (1970-2050)
            return 0 < num_value < 2524608000
        except (ValueError, TypeError):
            return False

    def _is_numeric(self, value: str) -> bool:
        """Check if value is numeric"""
        try:
            float(value.replace(',', '.'))
            return True
        except (ValueError, TypeError):
            return False

    def _is_date_string(self, value: str) -> bool:
        """Check if value looks like a date string"""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',  # YYYY-MM-DD HH:MM:SS
            r'^\d{2}\.\d{2}\.\d{4}$',  # DD.MM.YYYY
            r'^\d{2}/\d{2}/\d{4}$',  # DD/MM/YYYY or MM/DD/YYYY
        ]

        return any(re.match(pattern, value) for pattern in date_patterns)

    def _is_phone_number(self, value: str) -> bool:
        """Check if value looks like a phone number"""
        return bool(self.PHONE_PATTERN.match(value)) and len(value.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')) >= 7

    def _is_email(self, value: str) -> bool:
        """Check if value looks like an email"""
        return bool(self.EMAIL_PATTERN.match(value))

    def _is_url(self, value: str) -> bool:
        """Check if value looks like a URL"""
        return bool(self.URL_PATTERN.match(value))

    def get_formatting_errors(self) -> List[str]:
        """Get list of formatting errors encountered"""
        return self.formatting_errors.copy()

    def clear_formatting_errors(self) -> None:
        """Clear the list of formatting errors"""
        self.formatting_errors.clear()


class ColumnHeaderFormatter:
    """
    Handles creation of human-readable column headers with fallbacks
    Implements requirement 7.3 for human-readable column headers
    """

    # Common field name mappings for better readability
    FIELD_NAME_MAPPINGS = {
        'id': 'ID',
        'name': 'Name',
        'created_at': 'Created Date',
        'updated_at': 'Updated Date',
        'closed_at': 'Closed Date',
        'closest_task_at': 'Next Task Date',
        'responsible_user_id': 'Responsible User',
        'group_id': 'Group',
        'status_id': 'Status',
        'pipeline_id': 'Pipeline',
        'loss_reason_id': 'Loss Reason',
        'source_id': 'Source',
        'created_by': 'Created By',
        'updated_by': 'Updated By',
        'account_id': 'Account',
        'is_deleted': 'Deleted',
        'custom_fields_values': 'Custom Fields',
        'score': 'Score',
        'price': 'Price',
        'company_id': 'Company',
        'contact_id': 'Contact',
        'lead_id': 'Lead',
        'deal_id': 'Deal',
        'first_name': 'First Name',
        'last_name': 'Last Name',
        'phone': 'Phone',
        'email': 'Email',
        'position': 'Position',
        'company_name': 'Company Name',
        'web': 'Website',
        'address': 'Address',
        'tags': 'Tags',
        'link_url': 'Link URL'
    }

    def __init__(self):
        """Initialize the column header formatter"""
        pass

    def format_header(self, field_name: str, field_id: Optional[str] = None, custom_field_name: Optional[str] = None) -> str:
        """
        Format a column header to be human-readable

        Args:
            field_name: The original field name
            field_id: Optional field ID for fallback
            custom_field_name: Optional custom field display name

        Returns:
            Human-readable column header
        """
        # Use custom field name if available (highest priority)
        if custom_field_name and custom_field_name.strip():
            return self._clean_header_name(custom_field_name)

        # Check for predefined mappings
        if field_name in self.FIELD_NAME_MAPPINGS:
            return self.FIELD_NAME_MAPPINGS[field_name]

        # Format the field name to be more readable
        readable_name = self._make_readable(field_name)

        # If we still don't have a good name and have field_id, use it as fallback
        if not readable_name or readable_name == field_name:
            if field_id:
                return f"Field {field_id}"
            else:
                return field_name or "Unknown Field"

        return readable_name

    def _make_readable(self, field_name: str) -> str:
        """Convert field name to human-readable format"""
        if not field_name:
            return "Unknown Field"

        # Clean the field name
        clean_name = self._clean_header_name(field_name)

        # Convert snake_case to Title Case
        if '_' in clean_name:
            words = clean_name.split('_')
            return ' '.join(word.capitalize() for word in words if word)

        # Convert camelCase to Title Case
        if any(c.isupper() for c in clean_name[1:]):
            # Insert spaces before uppercase letters
            spaced = re.sub(r'([a-z])([A-Z])', r'\1 \2', clean_name)
            return spaced.title()

        # Just capitalize first letter
        return clean_name.capitalize()

    def _clean_header_name(self, name: str) -> str:
        """Clean header name of problematic characters"""
        if not name:
            return "Unknown Field"

        # Remove or replace problematic characters
        clean_name = name.strip()

        # Replace problematic characters with safe alternatives
        clean_name = clean_name.replace('/', ' or ')
        clean_name = clean_name.replace('\\', ' ')
        clean_name = clean_name.replace('[', '(')
        clean_name = clean_name.replace(']', ')')
        clean_name = clean_name.replace('|', ' or ')
        clean_name = clean_name.replace('\n', ' ')
        clean_name = clean_name.replace('\r', ' ')
        clean_name = clean_name.replace('\t', ' ')

        # Remove multiple spaces
        clean_name = re.sub(r'\s+', ' ', clean_name).strip()

        # Limit length
        if len(clean_name) > 100:
            clean_name = clean_name[:97] + "..."

        return clean_name or "Unknown Field"

    def format_custom_field_header(self, field_id: str, field_name: Optional[str] = None, display_name: Optional[str] = None) -> str:
        """
        Format header for custom fields with proper fallback logic

        Args:
            field_id: The custom field ID
            field_name: The field name from AmoCRM
            display_name: Custom display name if available

        Returns:
            Formatted header name
        """
        # Priority order: display_name > field_name > field_id
        if display_name and display_name.strip():
            return self._clean_header_name(display_name)

        if field_name and field_name.strip():
            return self._clean_header_name(field_name)

        # Fallback to field ID
        return f"Custom Field {field_id}"

    def get_headers_for_data(self, data: List[Dict[str, Any]], custom_field_mappings: Optional[Dict[str, str]] = None) -> List[str]:
        """
        Generate formatted headers for a dataset

        Args:
            data: List of data items
            custom_field_mappings: Optional mapping of field_id to display names

        Returns:
            List of formatted header names
        """
        if not data:
            return []

        # Collect all unique field names
        all_fields = set()
        for item in data:
            if isinstance(item, dict):
                all_fields.update(item.keys())

        # Format headers
        formatted_headers = []
        for field in sorted(all_fields):
            # Check if it's a custom field
            if field.startswith('custom_field_') or (custom_field_mappings and field in custom_field_mappings):
                display_name = custom_field_mappings.get(field) if custom_field_mappings else None
                header = self.format_custom_field_header(field, display_name=display_name)
            else:
                header = self.format_header(field)

            formatted_headers.append(header)

        return formatted_headers