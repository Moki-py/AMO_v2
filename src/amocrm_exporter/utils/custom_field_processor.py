"""
Enhanced custom field processing for AmoCRM data export
Preserves custom field types and handles complex structures with graceful error handling
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass

from ..core.logger import log_event
from .data_formatter import DataFormatter


@dataclass
class CustomFieldInfo:
    """Information about a custom field"""
    field_id: str
    field_name: Optional[str]
    field_type: str
    values: List[Dict[str, Any]]
    raw_data: Dict[str, Any]


@dataclass
class ProcessingResult:
    """Result of custom field processing"""
    success: bool
    processed_fields: Dict[str, Any]
    errors: List[str]
    warnings: List[str]


class CustomFieldProcessor:
    """
    Enhanced processor for AmoCRM custom fields
    Implements requirements 7.2, 7.5 for custom field type preservation and graceful error handling
    """

    # Supported custom field types and their processing methods
    FIELD_TYPE_PROCESSORS = {
        'text': '_process_text_field',
        'textarea': '_process_textarea_field',
        'numeric': '_process_numeric_field',
        'checkbox': '_process_checkbox_field',
        'select': '_process_select_field',
        'multiselect': '_process_multiselect_field',
        'date': '_process_date_field',
        'datetime': '_process_datetime_field',
        'url': '_process_url_field',
        'phone': '_process_phone_field',
        'email': '_process_email_field',
        'radiobutton': '_process_radiobutton_field',
        'streetaddress': '_process_address_field',
        'smart_address': '_process_smart_address_field',
        'birthday': '_process_birthday_field',
        'legal_entity': '_process_legal_entity_field',
        'items': '_process_items_field',
        'category': '_process_category_field',
        'price': '_process_price_field',
        'tracking_data': '_process_tracking_data_field'
    }

    def __init__(self):
        """Initialize the custom field processor"""
        self.data_formatter = DataFormatter()
        self.processing_errors = []
        self.processing_warnings = []

    def process_custom_fields(
        self,
        custom_fields_values: Any,
        preserve_field_types: bool = True,
        graceful_degradation: bool = True
    ) -> ProcessingResult:
        """
        Process custom fields with type preservation and error handling

        Args:
            custom_fields_values: Raw custom fields data from AmoCRM
            preserve_field_types: Whether to preserve original field types
            graceful_degradation: Whether to continue processing on errors

        Returns:
            ProcessingResult with processed fields and error information
        """
        self.processing_errors.clear()
        self.processing_warnings.clear()

        try:
            # Parse custom fields data
            custom_fields = self._parse_custom_fields_data(custom_fields_values)
            if not custom_fields:
                return ProcessingResult(
                    success=True,
                    processed_fields={},
                    errors=[],
                    warnings=[]
                )

            processed_fields = {}

            # Process each custom field
            for field_info in custom_fields:
                try:
                    field_result = self._process_single_field(
                        field_info, preserve_field_types, graceful_degradation
                    )

                    if field_result:
                        field_key, field_value = field_result
                        processed_fields[field_key] = field_value

                except Exception as e:
                    error_msg = f"Error processing field {field_info.field_id}: {str(e)}"
                    self.processing_errors.append(error_msg)
                    log_event("custom_fields", "error", error_msg)

                    if graceful_degradation:
                        # Add fallback value
                        fallback_key = self._generate_field_key(field_info)
                        processed_fields[fallback_key] = f"ERROR: {str(e)[:50]}"
                    else:
                        raise

            return ProcessingResult(
                success=len(self.processing_errors) == 0,
                processed_fields=processed_fields,
                errors=self.processing_errors.copy(),
                warnings=self.processing_warnings.copy()
            )

        except Exception as e:
            error_msg = f"Critical error in custom field processing: {str(e)}"
            log_event("custom_fields", "error", error_msg)

            return ProcessingResult(
                success=False,
                processed_fields={},
                errors=[error_msg],
                warnings=self.processing_warnings.copy()
            )

    def _parse_custom_fields_data(self, custom_fields_values: Any) -> List[CustomFieldInfo]:
        """Parse custom fields data into structured format"""
        try:
            if custom_fields_values is None:
                return []

            # Handle different input formats
            if isinstance(custom_fields_values, list):
                raw_fields = custom_fields_values
            elif isinstance(custom_fields_values, str):
                try:
                    raw_fields = json.loads(custom_fields_values)
                    if not isinstance(raw_fields, list):
                        self.processing_warnings.append(f"Parsed custom fields is not a list: {type(raw_fields)}")
                        return []
                except json.JSONDecodeError as e:
                    self.processing_warnings.append(f"Failed to parse custom fields JSON: {str(e)}")
                    return []
            else:
                self.processing_warnings.append(f"Unsupported custom fields format: {type(custom_fields_values)}")
                return []

            # Convert to CustomFieldInfo objects
            field_infos = []
            for field_data in raw_fields:
                if not isinstance(field_data, dict):
                    continue

                field_info = CustomFieldInfo(
                    field_id=str(field_data.get('field_id', '')),
                    field_name=field_data.get('field_name'),
                    field_type=field_data.get('field_type', 'text'),
                    values=field_data.get('values', []),
                    raw_data=field_data
                )

                if field_info.field_id:
                    field_infos.append(field_info)

            return field_infos

        except Exception as e:
            self.processing_errors.append(f"Error parsing custom fields data: {str(e)}")
            return []

    def _process_single_field(
        self,
        field_info: CustomFieldInfo,
        preserve_field_types: bool,
        graceful_degradation: bool
    ) -> Optional[Tuple[str, Any]]:
        """Process a single custom field"""
        try:
            # Generate field key
            field_key = self._generate_field_key(field_info)

            # Get processor method for field type
            processor_method_name = self.FIELD_TYPE_PROCESSORS.get(
                field_info.field_type, '_process_generic_field'
            )

            processor_method = getattr(self, processor_method_name, self._process_generic_field)

            # Process the field
            processed_value = processor_method(field_info, preserve_field_types)

            return field_key, processed_value

        except Exception as e:
            if graceful_degradation:
                self.processing_warnings.append(f"Field processing failed, using fallback: {str(e)}")
                field_key = self._generate_field_key(field_info)
                fallback_value = self._create_fallback_value(field_info)
                return field_key, fallback_value
            else:
                raise

    def _generate_field_key(self, field_info: CustomFieldInfo) -> str:
        """Generate a unique key for the field"""
        # Use field name if available, otherwise use field ID
        if field_info.field_name and field_info.field_name.strip():
            # Clean field name for use as key
            clean_name = field_info.field_name.strip()
            clean_name = clean_name.replace('/', '_').replace('\\', '_')
            clean_name = clean_name.replace('[', '').replace(']', '')
            clean_name = clean_name.replace(' ', '_').lower()
            return f"cf_{field_info.field_id}_{clean_name}"
        else:
            return f"cf_{field_info.field_id}"

    # Field type processors
    def _process_text_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process text field"""
        if not field_info.values:
            return ''

        value = field_info.values[0].get('value', '')
        return self.data_formatter.format_value(value, 'text', field_info.field_name)

    def _process_textarea_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process textarea field"""
        if not field_info.values:
            return ''

        value = field_info.values[0].get('value', '')
        # Handle multiline text
        if isinstance(value, str):
            # Replace line breaks with spaces for Google Sheets compatibility
            value = value.replace('\n', ' ').replace('\r', ' ')

        return self.data_formatter.format_value(value, 'text', field_info.field_name)

    def _process_numeric_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process numeric field"""
        if not field_info.values:
            return None if preserve_types else ''

        value = field_info.values[0].get('value')
        return self.data_formatter.format_value(value, 'numeric', field_info.field_name)

    def _process_checkbox_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process checkbox field"""
        if not field_info.values:
            return False if preserve_types else ''

        value = field_info.values[0].get('value')

        # Convert to boolean if preserving types
        if preserve_types:
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            elif isinstance(value, (int, float)):
                return bool(value)
            else:
                return False
        else:
            # Convert to readable text
            if isinstance(value, bool):
                return 'Yes' if value else 'No'
            elif isinstance(value, str):
                bool_val = value.lower() in ('true', '1', 'yes', 'on')
                return 'Yes' if bool_val else 'No'
            else:
                return str(value) if value else 'No'

    def _process_select_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process select field"""
        if not field_info.values:
            return ''

        value = field_info.values[0].get('value', '')
        return self.data_formatter.format_value(value, 'text', field_info.field_name)

    def _process_multiselect_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process multiselect field"""
        if not field_info.values:
            return [] if preserve_types else ''

        values = []
        for val_item in field_info.values:
            if isinstance(val_item, dict) and 'value' in val_item:
                values.append(str(val_item['value']))

        if preserve_types:
            return values
        else:
            return ', '.join(values) if values else ''

    def _process_date_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process date field"""
        if not field_info.values:
            return None if preserve_types else ''

        value = field_info.values[0].get('value')
        return self.data_formatter.format_value(value, 'date', field_info.field_name)

    def _process_datetime_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process datetime field"""
        if not field_info.values:
            return None if preserve_types else ''

        value = field_info.values[0].get('value')
        return self.data_formatter.format_value(value, 'datetime', field_info.field_name)

    def _process_url_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process URL field"""
        if not field_info.values:
            return ''

        value = field_info.values[0].get('value', '')
        return self.data_formatter.format_value(value, 'url', field_info.field_name)

    def _process_phone_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process phone field"""
        if not field_info.values:
            return ''

        value = field_info.values[0].get('value', '')
        # Always format phone numbers as text to preserve formatting
        return self.data_formatter.format_value(value, 'text', field_info.field_name)

    def _process_email_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process email field"""
        if not field_info.values:
            return ''

        value = field_info.values[0].get('value', '')
        return self.data_formatter.format_value(value, 'text', field_info.field_name)

    def _process_radiobutton_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process radio button field"""
        return self._process_select_field(field_info, preserve_types)

    def _process_address_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process address field"""
        if not field_info.values:
            return ''

        value = field_info.values[0].get('value')

        # Handle complex address structures
        if isinstance(value, dict):
            # Extract address components
            address_parts = []
            for key in ['street', 'city', 'state', 'zip', 'country']:
                if key in value and value[key]:
                    address_parts.append(str(value[key]))

            return ', '.join(address_parts) if address_parts else ''
        else:
            return self.data_formatter.format_value(value, 'text', field_info.field_name)

    def _process_smart_address_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process smart address field"""
        return self._process_address_field(field_info, preserve_types)

    def _process_birthday_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process birthday field"""
        return self._process_date_field(field_info, preserve_types)

    def _process_legal_entity_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process legal entity field"""
        if not field_info.values:
            return ''

        value = field_info.values[0].get('value')

        # Handle complex legal entity structures
        if isinstance(value, dict):
            # Extract relevant information
            entity_parts = []
            for key in ['name', 'type', 'tax_number', 'address']:
                if key in value and value[key]:
                    entity_parts.append(f"{key}: {value[key]}")

            return '; '.join(entity_parts) if entity_parts else ''
        else:
            return self.data_formatter.format_value(value, 'text', field_info.field_name)

    def _process_items_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process items field (catalog items)"""
        if not field_info.values:
            return [] if preserve_types else ''

        items = []
        for val_item in field_info.values:
            if isinstance(val_item, dict):
                # Extract item information
                item_info = []
                for key in ['name', 'quantity', 'price']:
                    if key in val_item and val_item[key]:
                        item_info.append(f"{key}: {val_item[key]}")

                if item_info:
                    items.append('; '.join(item_info))
                elif 'value' in val_item:
                    items.append(str(val_item['value']))

        if preserve_types:
            return items
        else:
            return ' | '.join(items) if items else ''

    def _process_category_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process category field"""
        return self._process_select_field(field_info, preserve_types)

    def _process_price_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process price field"""
        if not field_info.values:
            return None if preserve_types else ''

        value = field_info.values[0].get('value')

        # Handle price with currency
        if isinstance(value, dict) and 'amount' in value:
            amount = value.get('amount', 0)
            currency = value.get('currency', '')

            formatted_amount = self.data_formatter.format_value(amount, 'numeric', field_info.field_name)

            if currency and not preserve_types:
                return f"{formatted_amount} {currency}"
            else:
                return formatted_amount
        else:
            return self.data_formatter.format_value(value, 'numeric', field_info.field_name)

    def _process_tracking_data_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Process tracking data field"""
        if not field_info.values:
            return {} if preserve_types else ''

        value = field_info.values[0].get('value')

        if isinstance(value, dict):
            if preserve_types:
                return value
            else:
                # Convert to readable format
                tracking_parts = []
                for key, val in value.items():
                    if val:
                        tracking_parts.append(f"{key}: {val}")
                return '; '.join(tracking_parts) if tracking_parts else ''
        else:
            return self.data_formatter.format_value(value, 'text', field_info.field_name)

    def _process_generic_field(self, field_info: CustomFieldInfo, preserve_types: bool) -> Any:
        """Generic processor for unknown field types"""
        if not field_info.values:
            return ''

        # Log unknown field type
        self.processing_warnings.append(f"Unknown field type '{field_info.field_type}' for field {field_info.field_id}")

        # Try to extract value(s)
        if len(field_info.values) == 1:
            value = field_info.values[0].get('value')
            return self.data_formatter.format_value(value, None, field_info.field_name)
        else:
            # Multiple values - treat as multiselect
            values = []
            for val_item in field_info.values:
                if isinstance(val_item, dict) and 'value' in val_item:
                    values.append(str(val_item['value']))

            if preserve_types:
                return values
            else:
                return ', '.join(values) if values else ''

    def _create_fallback_value(self, field_info: CustomFieldInfo) -> str:
        """Create a fallback value when processing fails"""
        try:
            if field_info.values:
                # Try to extract any value
                first_value = field_info.values[0]
                if isinstance(first_value, dict) and 'value' in first_value:
                    return str(first_value['value'])
                else:
                    return str(first_value)
            else:
                return f"Empty field ({field_info.field_type})"
        except Exception:
            return f"Error processing field {field_info.field_id}"

    def get_processing_errors(self) -> List[str]:
        """Get list of processing errors"""
        return self.processing_errors.copy()

    def get_processing_warnings(self) -> List[str]:
        """Get list of processing warnings"""
        return self.processing_warnings.copy()

    def clear_processing_logs(self) -> None:
        """Clear processing error and warning logs"""
        self.processing_errors.clear()
        self.processing_warnings.clear()


class CustomFieldMetadataExtractor:
    """
    Extracts metadata about custom fields for better processing
    """

    def __init__(self):
        """Initialize the metadata extractor"""
        pass

    def extract_field_metadata(self, data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Extract metadata about custom fields from a dataset

        Args:
            data: List of data items containing custom fields

        Returns:
            Dictionary mapping field IDs to their metadata
        """
        field_metadata = {}

        for item in data:
            if not isinstance(item, dict):
                continue

            custom_fields_values = item.get('custom_fields_values')
            if not custom_fields_values:
                continue

            # Parse custom fields
            processor = CustomFieldProcessor()
            custom_fields = processor._parse_custom_fields_data(custom_fields_values)

            for field_info in custom_fields:
                field_id = field_info.field_id

                if field_id not in field_metadata:
                    field_metadata[field_id] = {
                        'field_id': field_id,
                        'field_name': field_info.field_name,
                        'field_type': field_info.field_type,
                        'value_examples': [],
                        'value_types': set(),
                        'is_multivalue': False,
                        'has_complex_values': False
                    }

                # Update metadata
                metadata = field_metadata[field_id]

                # Collect value examples and types
                for value_item in field_info.values:
                    if isinstance(value_item, dict) and 'value' in value_item:
                        value = value_item['value']

                        # Track value types
                        metadata['value_types'].add(type(value).__name__)

                        # Check for complex values
                        if isinstance(value, (dict, list)):
                            metadata['has_complex_values'] = True

                        # Store example values (limit to 5)
                        if len(metadata['value_examples']) < 5:
                            metadata['value_examples'].append(value)

                # Check if field has multiple values
                if len(field_info.values) > 1:
                    metadata['is_multivalue'] = True

        # Convert sets to lists for JSON serialization
        for metadata in field_metadata.values():
            metadata['value_types'] = list(metadata['value_types'])

        return field_metadata

    def get_field_complexity_score(self, field_metadata: Dict[str, Any]) -> int:
        """
        Calculate complexity score for a field (0-10)
        Higher scores indicate more complex fields that need special handling
        """
        score = 0

        # Base complexity by type
        complex_types = ['items', 'legal_entity', 'smart_address', 'tracking_data']
        if field_metadata.get('field_type') in complex_types:
            score += 5

        # Multiple value types increase complexity
        value_types = field_metadata.get('value_types', [])
        if len(value_types) > 1:
            score += 2

        # Complex values increase complexity
        if field_metadata.get('has_complex_values'):
            score += 3

        # Multivalue fields are more complex
        if field_metadata.get('is_multivalue'):
            score += 2

        return min(score, 10)  # Cap at 10